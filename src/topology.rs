//! The communication topology a roll publishes beside its snapshot: Xmip's
//! own communication, drawn from what the fleet configured and what its nodes
//! reported (ADR-0052, amendment 2026-09-14, ruling 3). Nothing here is
//! inferred from a socket — a node is in the picture because `-Nodes` named
//! it, and a link because a scenario the node runs reported on it.
//!
//! The picture is the fleet's: the fleet as the service, each node as the
//! System Process it is (ADR-0028 clause 2), the shared directory every node
//! claims from and drains as the one location they meet at, and three links
//! per node — the exclusive pickup over `exclusive-claim`, the backlog drained
//! over `daily-backlog`, and the snapshot the node publishes each round for the fleet to
//! merge. The surface reads the words this file writes (`Xmip.Surface`,
//! `SnapshotOperator`), so they are the surface's, not chosen here.

use observe::{Counted, Health, HealthRecord, Snapshot};
use serde::{Deserialize, Serialize};

use crate::report::state;
use crate::support::cluster_root;

/// The nodes and links a snapshot carries under `[topology]`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Topology {
    pub source: String,
    pub observed_unix_nanos: i64,
    pub nodes: Vec<TopologyNode>,
    pub links: Vec<TopologyLink>,
}

/// One thing that communicates. `parent` is empty at the top.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TopologyNode {
    pub id: String,
    pub parent: String,
    pub label: String,
    pub kind: String,
    pub scope: String,
    pub state: String,
    pub origin: String,
    pub load: f64,
    pub activity: f64,
    pub evidence: String,
}

/// One communication relationship, `from` one node id `to` another.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TopologyLink {
    pub id: String,
    pub from: String,
    pub to: String,
    pub pattern: String,
    pub origin: String,
    pub protocol: String,
    pub state: String,
    pub volume: u64,
    pub rate: f64,
    pub latency_ms: f64,
    pub progress: f64,
    pub attempts: u32,
    pub evidence: String,
}

const FLEET: &str = "fleet";
const SHARED: &str = "shared";

/// The fleet's topology from what its nodes published this round: the fleet,
/// the shared store, one process per named node, and the node's three links.
#[must_use]
pub fn fleet_topology<'a>(
    snapshot: &Snapshot,
    names: impl Iterator<Item = &'a str>,
    now: i64,
) -> Topology {
    let names: Vec<&str> = names.collect();
    let root = cluster_root();
    let mut topology = Topology {
        source: "playground — the fleet".to_string(),
        observed_unix_nanos: now,
        nodes: vec![fleet_node(snapshot, &names), shared_node(snapshot, &names)],
        links: Vec::new(),
    };
    for name in names {
        let scope = format!("{root}/node/{name}");
        topology.nodes.push(process_node(snapshot, name, &scope));
        topology
            .links
            .push(exclusive_claim_link(snapshot, name, &scope));
        topology
            .links
            .push(daily_backlog_link(snapshot, name, &scope));
        topology.links.push(snapshot_link(snapshot, name, &scope));
    }
    topology
}

/// The worst record at or beneath `scope`, if anything was published there.
fn worst(snapshot: &Snapshot, scope: &str) -> Option<HealthRecord> {
    snapshot.health(scope).into_iter().next()
}

/// The mood word and the evidence of a record, or what an empty scope says.
fn mood(record: Option<&HealthRecord>) -> (String, String) {
    record.map_or_else(
        || {
            (
                state(Health::Working).to_string(),
                "nothing published yet".to_string(),
            )
        },
        |record| (state(record.health).to_string(), record.evidence.clone()),
    )
}

/// Whether the fleet's own record says the node's process is running.
fn alive(snapshot: &Snapshot, scope: &str) -> bool {
    worst(snapshot, &format!("{scope}/process"))
        .is_some_and(|record| record.evidence.starts_with("alive"))
}

fn fleet_node(snapshot: &Snapshot, names: &[&str]) -> TopologyNode {
    let root = cluster_root();
    let scope = format!("{root}/{FLEET}");
    let (state, evidence) = mood(worst(snapshot, &scope).as_ref());
    let running = names
        .iter()
        .filter(|name| alive(snapshot, &format!("{root}/node/{name}")))
        .count();
    TopologyNode {
        id: FLEET.to_string(),
        parent: String::new(),
        label: "fleet".to_string(),
        kind: "service".to_string(),
        scope,
        state,
        origin: "configured".to_string(),
        load: 0.0,
        activity: fraction(running, names.len()),
        evidence,
    }
}

/// The one directory every node claims from and drains: its mood is the worst
/// any node reported over it.
fn shared_node(snapshot: &Snapshot, names: &[&str]) -> TopologyNode {
    let root = cluster_root();
    let over_store = names
        .iter()
        .flat_map(|name| {
            let scope = format!("{root}/node/{name}");
            [
                worst(snapshot, &format!("{scope}/exclusive-claim")),
                worst(snapshot, &format!("{scope}/daily-backlog")),
            ]
        })
        .flatten()
        .max_by_key(|record| (record.health, record.severity));
    let (state, evidence) = mood(over_store.as_ref());
    TopologyNode {
        id: SHARED.to_string(),
        parent: String::new(),
        label: "shared store".to_string(),
        kind: "location".to_string(),
        scope: format!("{root}/{FLEET}/{SHARED}"),
        state,
        origin: "configured".to_string(),
        load: 0.0,
        activity: 0.0,
        evidence,
    }
}

fn process_node(snapshot: &Snapshot, name: &str, scope: &str) -> TopologyNode {
    let (state, evidence) = mood(worst(snapshot, scope).as_ref());
    TopologyNode {
        id: format!("node/{name}"),
        parent: FLEET.to_string(),
        label: name.to_string(),
        kind: "process".to_string(),
        scope: scope.to_string(),
        state,
        origin: origin(snapshot, &format!("{scope}/exclusive-claim")),
        load: 0.0,
        activity: if alive(snapshot, scope) { 1.0 } else { 0.0 },
        evidence,
    }
}

/// Exclusive pickup over the shared `exclusive-claim` directory (ADR-0024's property).
fn exclusive_claim_link(snapshot: &Snapshot, name: &str, scope: &str) -> TopologyLink {
    let claim = format!("{scope}/exclusive-claim");
    let (state, evidence) = mood(worst(snapshot, &claim).as_ref());
    link(
        name,
        "exclusive-claim",
        SHARED,
        "publish-consume",
        origin(snapshot, &claim),
        state,
        evidence,
    )
}

/// The backlog drained over the shared `daily-backlog` directory: the Streams the node
/// drained are the volume, and the backlog left is what progress is against.
fn daily_backlog_link(snapshot: &Snapshot, name: &str, scope: &str) -> TopologyLink {
    let daily_backlog = format!("{scope}/daily-backlog");
    let (state, evidence) = mood(worst(snapshot, &daily_backlog).as_ref());
    let drained = snapshot
        .measure(scope, Counted::Streams)
        .map_or(0, |count| count.value);
    let backlog = snapshot
        .measure(scope, Counted::Messages)
        .map_or(0, |count| count.value);
    let mut link = link(
        name,
        "daily-backlog",
        SHARED,
        "publish-consume",
        origin(snapshot, &daily_backlog),
        state,
        evidence,
    );
    link.volume = drained;
    link.progress = fraction(
        usize::try_from(drained).unwrap_or(usize::MAX),
        usize::try_from(drained + backlog).unwrap_or(usize::MAX),
    );
    link
}

/// The snapshot the node writes each round and the fleet merges.
fn snapshot_link(snapshot: &Snapshot, name: &str, scope: &str) -> TopologyLink {
    let (state, evidence) = mood(worst(snapshot, &format!("{scope}/process")).as_ref());
    link(
        name,
        "snapshot",
        FLEET,
        "fire-and-forget",
        "observed".to_string(),
        state,
        evidence,
    )
}

fn link(
    name: &str,
    what: &str,
    to: &str,
    pattern: &str,
    origin: String,
    state: String,
    evidence: String,
) -> TopologyLink {
    TopologyLink {
        id: format!("{name}/{what}"),
        from: format!("node/{name}"),
        to: to.to_string(),
        pattern: pattern.to_string(),
        origin,
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

/// Configured by the fleet, and observed too once the node reported on it.
fn origin(snapshot: &Snapshot, scope: &str) -> String {
    if snapshot.health(scope).is_empty() {
        "configured".to_string()
    } else {
        "both".to_string()
    }
}

#[allow(clippy::cast_precision_loss)]
fn fraction(part: usize, whole: usize) -> f64 {
    if whole == 0 {
        0.0
    } else {
        part as f64 / whole as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fleet::ROOT;
    use crate::report::{from_toml, to_toml_with};
    use observe::Count;

    fn record(scope: &str, health: Health, evidence: &str) -> HealthRecord {
        HealthRecord {
            scope: scope.to_string(),
            health,
            severity: if health == Health::Fine { 0 } else { 60 },
            evidence: evidence.to_string(),
            observed_unix_nanos: 7,
        }
    }

    fn published(name: &str) -> Snapshot {
        let scope = format!("{ROOT}/node/{name}");
        let mut snapshot = Snapshot::new();
        snapshot.record_health(record(&format!("{scope}/process"), Health::Fine, "alive"));
        snapshot.record_health(record(
            &format!("{scope}/exclusive-claim/file"),
            Health::Stressed,
            "raced",
        ));
        snapshot.record_health(record(
            &format!("{scope}/daily-backlog/drain"),
            Health::Fine,
            "drained",
        ));
        snapshot.record_health(record(
            &format!("{ROOT}/fleet"),
            Health::Holding,
            "one node",
        ));
        for (counted, value) in [(Counted::Streams, 6), (Counted::Messages, 2)] {
            snapshot.record_count(Count {
                scope: scope.clone(),
                counted,
                value,
                window_start_unix_nanos: 7,
                window_end_unix_nanos: 7,
                observed_unix_nanos: 7,
            });
        }
        snapshot
    }

    #[test]
    fn the_fleet_its_store_and_every_node_are_drawn_with_three_links_each() {
        let topology = fleet_topology(&published("R1"), ["R1", "P1"].into_iter(), 9);

        let ids: Vec<&str> = topology.nodes.iter().map(|node| node.id.as_str()).collect();
        assert_eq!(ids, ["fleet", "shared", "node/R1", "node/P1"]);
        assert_eq!(topology.links.len(), 6);

        let fleet = &topology.nodes[0];
        assert_eq!((fleet.state.as_str(), fleet.activity), ("holding", 0.5));
        let store = &topology.nodes[1];
        assert_eq!(
            (store.kind.as_str(), store.state.as_str()),
            ("location", "stressed")
        );
        let r1 = &topology.nodes[2];
        assert_eq!(
            (r1.parent.as_str(), r1.origin.as_str(), r1.activity),
            ("fleet", "both", 1.0)
        );
        let p1 = &topology.nodes[3];
        assert_eq!(
            (p1.state.as_str(), p1.origin.as_str(), p1.activity),
            ("working", "configured", 0.0)
        );

        let daily_backlog = &topology.links[1];
        assert_eq!(
            (
                daily_backlog.id.as_str(),
                daily_backlog.to.as_str(),
                daily_backlog.volume
            ),
            ("R1/daily-backlog", "shared", 6)
        );
        assert!((daily_backlog.progress - 0.75).abs() < f64::EPSILON);
        assert_eq!(topology.links[2].to, "fleet");
    }

    #[test]
    fn the_topology_rides_the_snapshot_and_the_records_still_read_back() {
        let snapshot = published("S1");
        let topology = fleet_topology(&snapshot, ["S1"].into_iter(), 9);

        let text = to_toml_with(ROOT, &snapshot, Some(topology.clone()));
        assert!(text.contains("[[topology.nodes]]"));
        assert!(text.contains("[[topology.links]]"));
        let parsed: toml::Value = text.parse().expect("valid TOML");
        assert_eq!(
            parsed["topology"]["nodes"].as_array().map(Vec::len),
            Some(3)
        );

        let back = from_toml(&text).expect("reads back");
        assert_eq!(back.health(ROOT).len(), snapshot.health(ROOT).len());
    }
}
