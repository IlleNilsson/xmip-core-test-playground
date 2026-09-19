//! The stages a node runs, the endpoints beneath a receive or a send stage,
//! and the handoff links between the stages of different nodes.

use std::collections::BTreeSet;

use observe::Snapshot;

use super::{TopologyLink, TopologyNode, mood, node_id, origin, worst};
use crate::handoff::Hop;
use crate::role::Role;
use crate::support::cluster_root;
use crate::verdict::Stage;

/// The stage nodes of the node called `name`, each followed by its
/// endpoints: the stage its name's role gives it, drawn before it has
/// reported, and any stage it has reported on.
pub(super) fn nodes(snapshot: &Snapshot, name: &str, scope: &str) -> Vec<TopologyNode> {
    let own = Role::of(name).stage();
    let mut drawn = Vec::new();
    for stage in Stage::ALL {
        let at = format!("{scope}/{}", stage.name());
        if own != Some(stage) && snapshot.health(&at).is_empty() {
            continue;
        }
        let id = format!("{}/{}", node_id(name), stage.name());
        drawn.push(part(
            snapshot,
            &id,
            &node_id(name),
            stage.name(),
            "stage",
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
                    "endpoint",
                    &endpoint,
                ));
            }
        }
    }
    drawn
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

/// A stage or an endpoint: its mood the worst beneath its scope.
fn part(
    snapshot: &Snapshot,
    id: &str,
    parent: &str,
    label: &str,
    kind: &str,
    scope: &str,
) -> TopologyNode {
    let (state, evidence) = mood(worst(snapshot, scope).as_ref());
    TopologyNode {
        id: id.to_string(),
        parent: parent.to_string(),
        label: label.to_string(),
        kind: kind.to_string(),
        scope: scope.to_string(),
        state,
        origin: origin(snapshot, scope),
        load: 0.0,
        activity: 0.0,
        evidence,
    }
}

/// One link per pair of nodes that exchanged handoffs, from the sender's
/// stage to the receiver's: its volume the hops, its mood the worst leaf at
/// either end. A hop between nodes without a role on the path is not drawn.
pub(super) fn handoff_links(snapshot: &Snapshot, hops: &[Hop]) -> Vec<TopologyLink> {
    let root = cluster_root();
    hops.iter()
        .filter_map(|hop| {
            let from = Role::of(&hop.from).stage()?;
            let to = Role::of(&hop.to).stage()?;
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
                pattern: "send-receive".to_string(),
                origin: "both".to_string(),
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
