//! The shared store — the one directory the nodes claim from and drain — and
//! the links to it, drawn only for the nodes that ran a test over it.

use observe::{
    Counted, Health, NodeKind, Origin, Pattern, Snapshot, Topology, TopologyLink, TopologyNode,
};

use super::{CLUSTER, drawn, fraction, mood, node_id, worst};
use crate::scenario::{DAILY_BACKLOG, EXCLUSIVE_CLAIM};
use crate::support::cluster_root;

const SHARED: &str = "shared";

/// Add the store and every node's links to it, when any node reported on
/// `ExclusiveClaim` or `DailyBacklog`; nothing when none did.
pub(super) fn draw(snapshot: &Snapshot, names: &[&str], topology: &mut Topology) {
    let root = cluster_root();
    let mut links = Vec::new();
    for name in names {
        let scope = format!("{root}/node/{name}");
        if reported(snapshot, &scope, EXCLUSIVE_CLAIM) {
            links.push(exclusive_claim_link(snapshot, name, &scope));
        }
        if reported(snapshot, &scope, DAILY_BACKLOG) {
            links.push(daily_backlog_link(snapshot, name, &scope));
        }
    }
    if links.is_empty() {
        return;
    }
    topology.nodes.push(store(snapshot, names));
    topology.links.extend(links);
}

fn reported(snapshot: &Snapshot, scope: &str, test: &str) -> bool {
    !snapshot.health(&format!("{scope}/{test}")).is_empty()
}

/// The one directory every node claims from and drains: Fine or Holding over
/// the worst any node reported over it (ADR-0041).
fn store(snapshot: &Snapshot, names: &[&str]) -> TopologyNode {
    let root = cluster_root();
    let over_store = names
        .iter()
        .flat_map(|name| {
            let scope = format!("{root}/node/{name}");
            [
                worst(snapshot, &format!("{scope}/{EXCLUSIVE_CLAIM}")),
                worst(snapshot, &format!("{scope}/{DAILY_BACKLOG}")),
            ]
        })
        .flatten()
        .max_by_key(|record| (record.health, record.severity));
    let (state, evidence) = drawn(over_store.as_ref(), &format!("{root}/{SHARED}"));
    TopologyNode {
        id: SHARED.to_string(),
        parent: CLUSTER.to_string(),
        label: "shared store".to_string(),
        kind: NodeKind::Location,
        scope: format!("{root}/{SHARED}"),
        state,
        origin: Origin::Both,
        load: 0.0,
        activity: 0.0,
        evidence,
    }
}

/// Exclusive pickup over the shared `exclusive-claim` directory (ADR-0024's
/// property).
fn exclusive_claim_link(snapshot: &Snapshot, name: &str, scope: &str) -> TopologyLink {
    let claim = format!("{scope}/{EXCLUSIVE_CLAIM}");
    let (state, evidence) = mood(worst(snapshot, &claim).as_ref());
    link(name, EXCLUSIVE_CLAIM, state, evidence)
}

/// The backlog drained over the shared `daily-backlog` directory: the Streams
/// the node drained are the volume, and the backlog left is what progress is
/// against. Both are read at the test's own scope, so what the node counted
/// on the message path is not mistaken for them.
fn daily_backlog_link(snapshot: &Snapshot, name: &str, scope: &str) -> TopologyLink {
    let daily_backlog = format!("{scope}/{DAILY_BACKLOG}");
    let (state, evidence) = mood(worst(snapshot, &daily_backlog).as_ref());
    let count = |counted| {
        snapshot
            .measure(&daily_backlog, counted)
            .map_or(0, |count| count.value)
    };
    let (drained, backlog) = (count(Counted::Streams), count(Counted::Messages));
    let mut link = link(name, DAILY_BACKLOG, state, evidence);
    link.volume = drained;
    link.progress = fraction(
        usize::try_from(drained).unwrap_or(usize::MAX),
        usize::try_from(drained + backlog).unwrap_or(usize::MAX),
    );
    link
}

fn link(name: &str, what: &str, state: Health, evidence: String) -> TopologyLink {
    TopologyLink {
        id: format!("{name}/{what}"),
        from: node_id(name),
        to: SHARED.to_string(),
        pattern: Pattern::PublishConsume,
        origin: Origin::Both,
        protocol: "file".to_string(),
        state,
        volume: 0,
        rate: 0.0,
        latency_ms: 0.0,
        progress: 0.0,
        attempts: 0,
        evidence,
    }
}
