//! The stages a node runs, the endpoints beneath a receive or a send stage,
//! and the handoff links between the stages of different nodes.

use std::collections::BTreeSet;

use node::{Capability, Stage};
use observe::{NodeKind, Origin, Pattern, Snapshot, TopologyLink, TopologyNode};

use super::{drawn, mood, node_id, origin, worst};
use crate::handoff::Hop;
use crate::support::cluster_root;

/// The stage nodes of the node called `name`, each followed by its
/// endpoints: every stage the node **declared** it can serve, drawn before it
/// has reported, and any stage it has reported on. Its name says nothing
/// here (ADR-0056).
pub(super) fn nodes(snapshot: &Snapshot, name: &str, scope: &str) -> Vec<TopologyNode> {
    let declared = declared(snapshot, scope);
    let mut drawn = Vec::new();
    for stage in Stage::ALL {
        let at = format!("{scope}/{}", stage.name());
        if !declared.can(stage) && snapshot.health(&at).is_empty() {
            continue;
        }
        let id = format!("{}/{}", node_id(name), stage.name());
        drawn.push(part(
            snapshot,
            &id,
            &node_id(name),
            stage.name(),
            NodeKind::Stage,
            &at,
        ));
        // A process stage touches no transport: it has no endpoints.
        if stage != Stage::Process {
            for transport in transports(snapshot, &at) {
                let endpoint = format!("{at}/{transport}");
                let endpoint_id = format!("{id}/{transport}");
                drawn.push(part(
                    snapshot,
                    &endpoint_id,
                    &id,
                    &transport,
                    NodeKind::Endpoint,
                    &endpoint,
                ));
            }
        }
    }
    drawn
}

/// What the node published at `<scope>/capability`: what it declared it can
/// do. Nothing published is nothing declared — the node has yet to say, and
/// only what it reports is drawn. A record naming a word that is no
/// capability is refused and draws no declared stage either; the record
/// itself stays on the node's `capability` scope in the publisher's words.
fn declared(snapshot: &Snapshot, scope: &str) -> Capability {
    let at = observe::capability::scope(scope);
    snapshot
        .health(&at)
        .into_iter()
        .find(|record| record.scope == at)
        .and_then(|record| {
            observe::capability::declared(&record.scope, &record.evidence)
                .and_then(|(_, said)| said.ok())
        })
        .unwrap_or_else(Capability::none)
}

/// The transports a stage has reported on: the segment after the stage in
/// every scope beneath it, once each, in order.
fn transports(snapshot: &Snapshot, stage_scope: &str) -> BTreeSet<String> {
    let beneath = format!("{stage_scope}/");
    snapshot
        .health(stage_scope)
        .iter()
        .filter_map(|record| record.scope.strip_prefix(&beneath))
        .filter_map(|rest| rest.split('/').next())
        .filter(|transport| !transport.is_empty())
        .map(str::to_string)
        .collect()
}

/// A stage or an endpoint: its own mood, or the rollup over the worst
/// beneath its scope (ADR-0041).
fn part(
    snapshot: &Snapshot,
    id: &str,
    parent: &str,
    label: &str,
    kind: NodeKind,
    scope: &str,
) -> TopologyNode {
    let (state, evidence) = drawn(worst(snapshot, scope).as_ref(), scope);
    TopologyNode {
        id: id.to_string(),
        parent: parent.to_string(),
        label: label.to_string(),
        kind,
        scope: scope.to_string(),
        state,
        origin: origin(snapshot, scope),
        load: 0.0,
        activity: 0.0,
        evidence,
    }
}

/// One link per pair of stages that exchanged handoffs, from the sender's
/// stage to the receiver's: its volume the hops, its mood the worst leaf at
/// either end. The stages are the hop's own, recorded when the handoff was
/// delivered; a hop that names neither is not drawn.
pub(super) fn handoff_links(snapshot: &Snapshot, hops: &[Hop]) -> Vec<TopologyLink> {
    let root = cluster_root();
    hops.iter()
        .filter_map(|hop| {
            let from = Stage::named(&hop.from_stage)?;
            let to = Stage::named(&hop.to_stage)?;
            let ends = [
                format!("{root}/node/{}/{}", hop.from, from.name()),
                format!("{root}/node/{}/{}", hop.to, to.name()),
            ];
            let worse = ends
                .iter()
                .filter_map(|scope| worst(snapshot, scope))
                .max_by_key(|record| (record.health, record.severity));
            let (state, evidence) = mood(worse.as_ref());
            Some(TopologyLink {
                id: format!("handoff/{}/{}", hop.from, hop.to),
                from: format!("{}/{}", node_id(&hop.from), from.name()),
                to: format!("{}/{}", node_id(&hop.to), to.name()),
                pattern: Pattern::SendReceive,
                origin: Origin::Both,
                protocol: "handoff".to_string(),
                state,
                volume: hop.count,
                rate: 0.0,
                latency_ms: 0.0,
                progress: 0.0,
                attempts: 0,
                evidence: format!(
                    "{} handoffs {} to {}; worst leaf at either end: {evidence}",
                    hop.count, hop.from, hop.to
                ),
            })
        })
        .collect()
}
