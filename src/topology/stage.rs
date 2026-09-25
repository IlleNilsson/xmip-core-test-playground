//! The stages a node runs, the endpoints beneath a receive or a send stage,
//! and the handoff links between the stages of different nodes.

use std::collections::BTreeSet;

use node::{Capability, Stage};
use observe::{NodeKind, Origin, Pattern, Snapshot, TopologyLink, TopologyNode};

use super::{drawn, mood, node_id, origin, worst};
use crate::handoff::Hop;
use crate::roster::Roster;
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

/// One pair of stages a handoff runs between: from a node's stage to a
/// node's stage.
type Pair = (String, Stage, String, Stage);

/// The handoff links: every pair of stages the configuration says a handoff
/// may run between, and every pair a handoff was delivered over, once each.
///
/// What is configured is the roster — who declared receive, process and send
/// (ADR-0056) — when the run drives `RoundTrip` over the stages: a receiving
/// node may hand to any node that declared process, and a processing node to
/// any that declared send, since the roster picks one by a hash of the pair
/// (`Roster::target`). What is observed is the hops. A pair configured and
/// handed over is both; configured and not yet handed over is drawn as
/// configured, its volume zero and its evidence saying so, so a path with no
/// traffic on it is seen rather than missing (the owner, 2026-09-25: *even
/// when testing, the topology does not show configured traffic or its
/// usage*); a pair handed over that the roster does not configure is
/// observed. The volume is the hops, the mood the worst leaf at either end.
pub(super) fn handoff_links(
    snapshot: &Snapshot,
    configured: Option<&Roster>,
    hops: &[Hop],
) -> Vec<TopologyLink> {
    let mut pairs: Vec<(Pair, bool, u64)> = configured
        .map(configured_pairs)
        .unwrap_or_default()
        .into_iter()
        .map(|pair| (pair, true, 0))
        .collect();
    for hop in hops {
        let (Some(from), Some(to)) = (Stage::named(&hop.from_stage), Stage::named(&hop.to_stage))
        else {
            continue;
        };
        let pair = (hop.from.clone(), from, hop.to.clone(), to);
        match pairs.iter_mut().find(|(known, _, _)| *known == pair) {
            Some((_, _, count)) => *count += hop.count,
            None => pairs.push((pair, false, hop.count)),
        }
    }
    pairs
        .into_iter()
        .map(|(pair, configured, count)| link(snapshot, &pair, configured, count))
        .collect()
}

/// Every pair of stages the roster configures a handoff between: receive to
/// process, process to send, over the nodes that declared each, in the order
/// they were named.
fn configured_pairs(roster: &Roster) -> Vec<Pair> {
    [
        (Stage::Receive, Stage::Process),
        (Stage::Process, Stage::Send),
    ]
    .into_iter()
    .flat_map(|(from, to)| {
        let receivers = roster.with(to);
        roster.with(from).into_iter().flat_map(move |sender| {
            receivers
                .clone()
                .into_iter()
                .map(move |receiver| (sender.to_string(), from, receiver.to_string(), to))
        })
    })
    .collect()
}

/// One handoff link, drawn from what is known of its pair.
fn link(snapshot: &Snapshot, pair: &Pair, configured: bool, count: u64) -> TopologyLink {
    let (sender, from, receiver, to) = pair;
    let root = cluster_root();
    let ends = [
        format!("{root}/node/{sender}/{}", from.name()),
        format!("{root}/node/{receiver}/{}", to.name()),
    ];
    let worse = ends
        .iter()
        .filter_map(|scope| worst(snapshot, scope))
        .max_by_key(|record| (record.health, record.severity));
    let (state, worst_said) = mood(worse.as_ref());
    let (origin, said) = match (configured, count) {
        (true, 0) => (
            Origin::Configured,
            format!("configured {sender} to {receiver}; no handoff observed yet"),
        ),
        (true, _) => (
            Origin::Both,
            format!("{count} handoffs {sender} to {receiver}"),
        ),
        (false, _) => (
            Origin::Observed,
            format!("{count} handoffs {sender} to {receiver}, which no configuration declares"),
        ),
    };
    TopologyLink {
        id: format!("handoff/{sender}/{}/{receiver}/{}", from.name(), to.name()),
        from: format!("{}/{}", node_id(sender), from.name()),
        to: format!("{}/{}", node_id(receiver), to.name()),
        pattern: Pattern::SendReceive,
        origin,
        protocol: "handoff".to_string(),
        state,
        volume: count,
        rate: 0.0,
        latency_ms: 0.0,
        progress: 0.0,
        attempts: 0,
        evidence: format!("{said}; worst leaf at either end: {worst_said}"),
    }
}
