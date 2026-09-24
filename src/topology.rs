//! The communication topology a roll publishes beside its snapshot: Xmip's
//! own communication, drawn from what the cluster configured and what its
//! nodes reported (ADR-0052, amendment 2026-09-14, ruling 3). Nothing here is
//! inferred from a socket — a node is in the picture because `-Nodes` named
//! it, a stage because the node declared it (ADR-0056) or reported on
//! it, and a link because a handoff was delivered over it.
//!
//! The picture is the owner's, 2026-09-19: *cluster, nodes, receive, process,
//! send*. The cluster holds its nodes; a node holds the stages of the message
//! path it declared; a receive or a send stage holds one endpoint per transport
//! it reported on. Between them run the handoffs, receive to process to send,
//! one link per pair of stages that exchanged any, its volume the hops. A
//! drawn thing above its leaves is Fine or Holding (`Health::rolled`,
//! ADR-0041); a link shows the worst leaf at either end. The
//! shared directory is drawn only when a node ran a test over it. The model
//! and its words are `observe::topology`'s, which writes and reads them; this
//! file only draws (open problem 25).

mod shared;
mod stage;

use observe::{Health, HealthRecord, NodeKind, Origin, Scope, Snapshot, Topology, TopologyNode};

use crate::handoff::Hop;
use crate::support::cluster_root;

/// The id of the cluster, the one node with no parent.
const CLUSTER: &str = "cluster";

/// The cluster's topology from what its nodes published this round and the
/// handoffs they delivered: the cluster, one node per name, each node's
/// stages and their endpoints, the handoff links, and the shared store with
/// its links when a node ran a test over it.
#[must_use]
pub fn cluster_topology<'a>(
    snapshot: &Snapshot,
    names: impl Iterator<Item = &'a str>,
    hops: &[Hop],
    now: i64,
) -> Topology {
    let names: Vec<&str> = names.collect();
    let root = cluster_root();
    let mut topology = Topology {
        source: format!("playground — cluster {}", label(&root)),
        observed_unix_nanos: now,
        nodes: vec![cluster_node(snapshot, &root, &names)],
        links: Vec::new(),
    };
    for name in &names {
        let scope = format!("{root}/node/{name}");
        topology.nodes.push(node(snapshot, name, &scope));
        topology.nodes.extend(stage::nodes(snapshot, name, &scope));
    }
    topology.links.extend(stage::handoff_links(snapshot, hops));
    shared::draw(snapshot, &names, &mut topology);
    topology
}

/// The id of the node called `name`.
fn node_id(name: &str) -> String {
    format!("node/{name}")
}

/// The last segment of a scope: what a thing is called.
fn label(scope: &str) -> &str {
    scope.rsplit('/').next().unwrap_or(scope)
}

/// The worst record at or beneath `scope`, if anything was published there.
fn worst(snapshot: &Snapshot, scope: &str) -> Option<HealthRecord> {
    snapshot.health(scope).into_iter().next()
}

/// What a drawn thing at `scope` shows (ADR-0041): the mood of the record
/// at the scope itself, or — for a parent — `Health::rolled` over the worst
/// record beneath it, Fine or Holding; the evidence is the worst record's
/// either way, so a Holding cluster says what it is holding on.
fn drawn(record: Option<&HealthRecord>, scope: &str) -> (Health, String) {
    let (health, evidence) = mood(record);
    match record {
        Some(record) if Scope::new(&record.scope) != Scope::new(scope) => {
            (health.rolled(), evidence)
        }
        _ => (health, evidence),
    }
}

/// The mood and the evidence of a record, or what an empty scope says: what
/// a link between two things shows, which is no parent of anything.
fn mood(record: Option<&HealthRecord>) -> (Health, String) {
    record.map_or_else(
        || (Health::Working, "nothing published yet".to_string()),
        |record| (record.health, record.evidence.clone()),
    )
}

/// Whether the cluster's own record says the node's process is running.
fn alive(snapshot: &Snapshot, scope: &str) -> bool {
    worst(snapshot, &format!("{scope}/system-process"))
        .is_some_and(|record| record.evidence.starts_with("alive"))
}

/// The cluster: its mood the rollup over its nodes, its activity the share
/// of them running.
fn cluster_node(snapshot: &Snapshot, root: &str, names: &[&str]) -> TopologyNode {
    let (state, evidence) = drawn(worst(snapshot, &format!("{root}/node")).as_ref(), root);
    let running = names
        .iter()
        .filter(|name| alive(snapshot, &format!("{root}/node/{name}")))
        .count();
    TopologyNode {
        id: CLUSTER.to_string(),
        parent: String::new(),
        label: label(root).to_string(),
        kind: NodeKind::Cluster,
        scope: root.to_string(),
        state,
        origin: Origin::Configured,
        load: 0.0,
        activity: fraction(running, names.len()),
        evidence,
    }
}

/// One node of the cluster, the System Process it is (ADR-0028 clause 2).
fn node(snapshot: &Snapshot, name: &str, scope: &str) -> TopologyNode {
    let (state, evidence) = drawn(worst(snapshot, scope).as_ref(), scope);
    TopologyNode {
        id: node_id(name),
        parent: CLUSTER.to_string(),
        label: name.to_string(),
        kind: NodeKind::Node,
        scope: scope.to_string(),
        state,
        origin: origin(snapshot, scope),
        load: 0.0,
        activity: if alive(snapshot, scope) { 1.0 } else { 0.0 },
        evidence,
    }
}

/// Configured by the cluster, and observed too once something reported on it.
fn origin(snapshot: &Snapshot, scope: &str) -> Origin {
    if snapshot.health(scope).is_empty() {
        Origin::Configured
    } else {
        Origin::Both
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
    use crate::cluster::ROOT;
    use crate::report::roll_toml;
    use node::Capability;
    use observe::{Count, Counted, Publication};

    fn record(scope: &str, health: Health, evidence: &str) -> HealthRecord {
        HealthRecord {
            scope: scope.to_string(),
            health,
            severity: if health == Health::Fine { 0 } else { 60 },
            evidence: evidence.to_string(),
            observed_unix_nanos: 7,
        }
    }

    fn hop(from: &str, from_stage: &str, to: &str, to_stage: &str, count: u64) -> Hop {
        Hop {
            from: from.to_string(),
            from_stage: from_stage.to_string(),
            to: to.to_string(),
            to_stage: to_stage.to_string(),
            count,
            last_unix_nanos: 7,
        }
    }

    /// The capability record a node publishes each round (ADR-0056), by the
    /// words `--can` takes.
    fn declares(stages: &str) -> String {
        Capability::parse(stages).expect("a capability").evidence()
    }

    /// `R1` received over tcp and file, `P1` processed, `S1` sent over tcp
    /// with one pair stressed; `S1` also ran the two shared-directory tests.
    /// `S2` has only said what it can do and reported nothing yet.
    fn published() -> Snapshot {
        let mut snapshot = Snapshot::new();
        for (leaf, capability) in [
            ("R1", "receive"),
            ("P1", "process"),
            ("S1", "send"),
            ("S2", "send"),
        ] {
            let scope = format!("{ROOT}/node/{leaf}/capability");
            snapshot.record_health(record(&scope, Health::Fine, &declares(capability)));
        }
        for (leaf, health, evidence) in [
            ("R1/system-process", Health::Fine, "alive"),
            ("R1/receive/tcp/json", Health::Fine, "3/3 rounds passed"),
            ("R1/receive/tcp/json/identification", Health::Fine, "held"),
            ("R1/receive/file/text", Health::Fine, "3/3 rounds passed"),
            ("P1/system-process", Health::Fine, "alive"),
            ("P1/process/tcp/json", Health::Fine, "3/3 rounds passed"),
            ("S1/system-process", Health::Fine, "alive"),
            (
                "S1/send/tcp/json",
                Health::Stressed,
                "2/3 rounds passed, 1 failed",
            ),
            ("S1/exclusive-claim/file", Health::Fine, "one holder"),
            ("S1/daily-backlog/drain", Health::Fine, "drained"),
        ] {
            snapshot.record_health(record(&format!("{ROOT}/node/{leaf}"), health, evidence));
        }
        snapshot.record_health(record(&format!("{ROOT}/node"), Health::Stressed, "3 nodes"));
        for (counted, value) in [(Counted::Streams, 6), (Counted::Messages, 2)] {
            snapshot.record_count(Count {
                scope: format!("{ROOT}/node/S1/daily-backlog"),
                counted,
                value,
                window_start_unix_nanos: 7,
                window_end_unix_nanos: 7,
                observed_unix_nanos: 7,
            });
        }
        snapshot
    }

    fn drawn() -> Topology {
        let hops = [
            hop("R1", "receive", "P1", "process", 3),
            hop("P1", "process", "S1", "send", 2),
        ];
        cluster_topology(&published(), ["R1", "P1", "S1", "S2"].into_iter(), &hops, 9)
    }

    fn find<'a>(topology: &'a Topology, id: &str) -> &'a TopologyNode {
        topology
            .nodes
            .iter()
            .find(|node| node.id == id)
            .unwrap_or_else(|| panic!("{id} is drawn"))
    }

    #[test]
    fn the_cluster_holds_nodes_that_hold_stages_that_hold_endpoints() {
        let topology = drawn();
        let shape: Vec<(&str, &str, &str)> = topology
            .nodes
            .iter()
            .map(|node| (node.id.as_str(), node.kind.word(), node.parent.as_str()))
            .collect();
        assert_eq!(
            shape,
            [
                ("cluster", "cluster", ""),
                ("node/R1", "node", "cluster"),
                ("node/R1/receive", "stage", "node/R1"),
                ("node/R1/receive/file", "endpoint", "node/R1/receive"),
                ("node/R1/receive/tcp", "endpoint", "node/R1/receive"),
                ("node/P1", "node", "cluster"),
                ("node/P1/process", "stage", "node/P1"),
                ("node/S1", "node", "cluster"),
                ("node/S1/send", "stage", "node/S1"),
                ("node/S1/send/tcp", "endpoint", "node/S1/send"),
                ("node/S2", "node", "cluster"),
                ("node/S2/send", "stage", "node/S2"),
                ("shared", "location", "cluster"),
            ]
        );

        let cluster = find(&topology, "cluster");
        assert_eq!(
            (
                cluster.label.as_str(),
                cluster.scope.as_str(),
                cluster.state.word()
            ),
            ("playground", ROOT, "holding")
        );
        assert!((cluster.activity - 0.75).abs() < f64::EPSILON);
        // ADR-0041: a parent is Fine or Holding, never its leaf's mood.
        assert_eq!(find(&topology, "node/R1").state, Health::Fine);
        assert_eq!(find(&topology, "node/S1").state, Health::Holding);
        let endpoint = find(&topology, "node/S1/send/tcp");
        assert_eq!(
            (
                endpoint.label.as_str(),
                endpoint.scope.as_str(),
                endpoint.state.word()
            ),
            ("tcp", "xmip:///playground/node/S1/send/tcp", "holding")
        );
        let idle = find(&topology, "node/S2/send");
        assert_eq!(
            (idle.origin.word(), idle.state.word()),
            ("configured", "working")
        );
    }

    #[test]
    fn handoffs_link_the_stages_and_the_store_is_linked_only_by_who_ran_over_it() {
        let topology = drawn();
        let links: Vec<(&str, &str, &str, &str, u64)> = topology
            .links
            .iter()
            .map(|link| {
                (
                    link.from.as_str(),
                    link.to.as_str(),
                    link.pattern.word(),
                    link.protocol.as_str(),
                    link.volume,
                )
            })
            .collect();
        assert_eq!(
            links,
            [
                (
                    "node/R1/receive",
                    "node/P1/process",
                    "send-receive",
                    "handoff",
                    3
                ),
                (
                    "node/P1/process",
                    "node/S1/send",
                    "send-receive",
                    "handoff",
                    2
                ),
                ("node/S1", "shared", "publish-consume", "file", 0),
                ("node/S1", "shared", "publish-consume", "file", 6),
            ]
        );
        assert_eq!(topology.links[0].state, Health::Fine);
        assert_eq!(
            topology.links[1].state,
            Health::Stressed,
            "the worst leaf involved"
        );
        assert_eq!(topology.links[1].origin, Origin::Both);
        assert!((topology.links[3].progress - 0.75).abs() < f64::EPSILON);

        let bare = cluster_topology(&Snapshot::new(), ["node-01"].into_iter(), &[], 9);
        let ids: Vec<&str> = bare.nodes.iter().map(|node| node.id.as_str()).collect();
        assert_eq!(
            ids,
            ["cluster", "node/node-01"],
            "no store until a test runs over it"
        );
        assert!(bare.links.is_empty());
    }

    #[test]
    fn the_topology_rides_the_snapshot_and_the_records_still_read_back() {
        let snapshot = published();
        let topology = drawn();

        let text = roll_toml(ROOT, &snapshot, Some(topology.clone()), None);
        assert!(text.contains("[[topology.nodes]]"));
        assert!(text.contains("[[topology.links]]"));
        let parsed: toml::Value = text.parse().expect("valid TOML");
        assert_eq!(
            parsed["topology"]["nodes"].as_array().map(Vec::len),
            Some(topology.nodes.len())
        );

        let back = Publication::read(&text).expect("reads back");
        assert_eq!(back.records.len(), snapshot.health(ROOT).len());
        assert_eq!(back.topology, Some(topology));
    }
}
