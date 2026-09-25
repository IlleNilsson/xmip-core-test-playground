//! The communication topology a roll publishes beside its snapshot: Xmip's
//! own communication, drawn from what the cluster configured and what its
//! nodes reported (ADR-0052, amendment 2026-09-14, ruling 3). Nothing here is
//! inferred from a socket — a node is in the picture because `-Nodes` named
//! it, a stage because the node declared it (ADR-0056) or reported on
//! it, and a link because the roster configures a handoff there or one was
//! delivered over it — configured, observed or both, said on the link.
//!
//! The picture is the owner's, 2026-09-19: *cluster, nodes, receive, process,
//! send*. The cluster holds its nodes; a node holds the stages of the message
//! path it declared; a receive or a send stage holds one endpoint per transport
//! it reported on. Between them run the handoffs, receive to process to send,
//! one link per pair of stages configured to exchange them or that exchanged
//! any, its volume the hops and its rate their rise per second since the
//! roll's last publication (`Topology::rate_since`). A
//! drawn thing above its leaves is Fine or Holding (`Health::rolled`,
//! ADR-0041); a link shows the worst leaf at either end. The
//! shared directory is drawn only when a node ran a test over it. The model
//! and its words are `observe::topology`'s, which writes and reads them; this
//! file only draws (open problem 25).

mod shared;
mod stage;

use observe::{Health, HealthRecord, NodeKind, Origin, Scope, Snapshot, Topology, TopologyNode};

use crate::handoff::Hop;
use crate::roster::Roster;
use crate::support::cluster_root;

/// The id of the cluster, the one node with no parent.
const CLUSTER: &str = "cluster";

/// The cluster's topology from what it was configured with, what its nodes
/// published this round and the handoffs they delivered: the cluster, one
/// node per name on the roster, each node's stages and their endpoints, the
/// handoff links — configured where `relayed` says the run hands `RoundTrip`
/// along the stages the roster declares, observed where a hop was
/// delivered — and the shared store with its links when a node ran a test
/// over it.
#[must_use]
pub fn cluster_topology(
    snapshot: &Snapshot,
    roster: &Roster,
    relayed: bool,
    hops: &[Hop],
    now: i64,
) -> Topology {
    let names: Vec<&str> = roster.names();
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
    let configured = relayed.then_some(roster);
    topology
        .links
        .extend(stage::handoff_links(snapshot, configured, hops));
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

    /// `alpha` received over tcp and file, `beta` processed, `gamma` sent over tcp
    /// with one pair stressed; `gamma` also ran the two shared-directory tests.
    /// `zeta` has only said what it can do and reported nothing yet.
    fn published() -> Snapshot {
        let mut snapshot = Snapshot::new();
        for (leaf, capability) in [
            ("alpha", "receive"),
            ("beta", "process"),
            ("gamma", "send"),
            ("zeta", "send"),
        ] {
            let scope = format!("{ROOT}/node/{leaf}/capability");
            snapshot.record_health(record(&scope, Health::Fine, &declares(capability)));
        }
        for (leaf, health, evidence) in [
            ("alpha/system-process", Health::Fine, "alive"),
            ("alpha/receive/tcp/json", Health::Fine, "3/3 rounds passed"),
            (
                "alpha/receive/tcp/json/identification",
                Health::Fine,
                "held",
            ),
            ("alpha/receive/file/text", Health::Fine, "3/3 rounds passed"),
            ("beta/system-process", Health::Fine, "alive"),
            ("beta/process/tcp/json", Health::Fine, "3/3 rounds passed"),
            ("gamma/system-process", Health::Fine, "alive"),
            (
                "gamma/send/tcp/json",
                Health::Stressed,
                "2/3 rounds passed, 1 failed",
            ),
            ("gamma/exclusive-claim/file", Health::Fine, "one holder"),
            ("gamma/daily-backlog/drain", Health::Fine, "drained"),
        ] {
            snapshot.record_health(record(&format!("{ROOT}/node/{leaf}"), health, evidence));
        }
        snapshot.record_health(record(&format!("{ROOT}/node"), Health::Stressed, "3 nodes"));
        for (counted, value) in [(Counted::Streams, 6), (Counted::Messages, 2)] {
            snapshot.record_count(Count {
                scope: format!("{ROOT}/node/gamma/daily-backlog"),
                counted,
                value,
                window_start_unix_nanos: 7,
                window_end_unix_nanos: 7,
                observed_unix_nanos: 7,
            });
        }
        snapshot
    }

    /// What the run configured: `alpha` receives, `beta` processes, `gamma` and `zeta`
    /// send.
    fn roster() -> Roster {
        Roster::parse("alpha=receive,beta=process,gamma=send,zeta=send").expect("a roster")
    }

    fn drawn() -> Topology {
        let hops = [
            hop("alpha", "receive", "beta", "process", 3),
            hop("beta", "process", "gamma", "send", 2),
        ];
        cluster_topology(&published(), &roster(), true, &hops, 9)
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
                ("node/alpha", "node", "cluster"),
                ("node/alpha/receive", "stage", "node/alpha"),
                ("node/alpha/receive/file", "endpoint", "node/alpha/receive"),
                ("node/alpha/receive/tcp", "endpoint", "node/alpha/receive"),
                ("node/beta", "node", "cluster"),
                ("node/beta/process", "stage", "node/beta"),
                ("node/gamma", "node", "cluster"),
                ("node/gamma/send", "stage", "node/gamma"),
                ("node/gamma/send/tcp", "endpoint", "node/gamma/send"),
                ("node/zeta", "node", "cluster"),
                ("node/zeta/send", "stage", "node/zeta"),
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
        assert_eq!(find(&topology, "node/alpha").state, Health::Fine);
        assert_eq!(find(&topology, "node/gamma").state, Health::Holding);
        let endpoint = find(&topology, "node/gamma/send/tcp");
        assert_eq!(
            (
                endpoint.label.as_str(),
                endpoint.scope.as_str(),
                endpoint.state.word()
            ),
            ("tcp", "xmip:///playground/node/gamma/send/tcp", "holding")
        );
        let idle = find(&topology, "node/zeta/send");
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
                    "node/alpha/receive",
                    "node/beta/process",
                    "send-receive",
                    "handoff",
                    3
                ),
                (
                    "node/beta/process",
                    "node/gamma/send",
                    "send-receive",
                    "handoff",
                    2
                ),
                (
                    "node/beta/process",
                    "node/zeta/send",
                    "send-receive",
                    "handoff",
                    0
                ),
                ("node/gamma", "shared", "publish-consume", "file", 0),
                ("node/gamma", "shared", "publish-consume", "file", 6),
            ]
        );
        assert_eq!(topology.links[0].state, Health::Fine);
        assert_eq!(
            topology.links[1].state,
            Health::Stressed,
            "the worst leaf involved"
        );
        assert_eq!(topology.links[1].origin, Origin::Both);
        assert!((topology.links[4].progress - 0.75).abs() < f64::EPSILON);

        let bare = cluster_topology(
            &Snapshot::new(),
            &Roster::of(&["node-01".into()]),
            false,
            &[],
            9,
        );
        let ids: Vec<&str> = bare.nodes.iter().map(|node| node.id.as_str()).collect();
        assert_eq!(
            ids,
            ["cluster", "node/node-01"],
            "no store until a test runs over it"
        );
        assert!(bare.links.is_empty());
    }

    /// The owner, 2026-09-25: *even when testing, the topology does not show
    /// configured traffic or its usage.* Every path the roster configures is
    /// drawn, handed over or not, and says which; a hop no configuration
    /// declares is observed; and the rate is the rise since the last round.
    #[test]
    fn a_configured_path_is_drawn_before_its_first_handoff_and_says_so() {
        let topology = drawn();
        let idle = topology
            .links
            .iter()
            .find(|link| link.to == "node/zeta/send")
            .expect("beta to zeta is configured and drawn");
        assert_eq!((idle.origin, idle.volume), (Origin::Configured, 0));
        assert!(
            idle.evidence
                .starts_with("configured beta to zeta; no handoff observed yet"),
            "{}",
            idle.evidence
        );

        let stray = [hop("gamma", "process", "alpha", "send", 1)];
        let observed = cluster_topology(&published(), &roster(), true, &stray, 9);
        let link = observed
            .links
            .iter()
            .find(|link| link.from == "node/gamma/process")
            .expect("a delivered hop is drawn");
        assert_eq!(link.origin, Origin::Observed);

        // Not relaying RoundTrip over stages: nothing is configured between
        // the nodes, and only what was delivered is drawn.
        let unrelayed = cluster_topology(&published(), &roster(), false, &[], 9);
        assert!(
            unrelayed
                .links
                .iter()
                .all(|link| link.protocol != "handoff")
        );

        let earlier = cluster_topology(
            &published(),
            &roster(),
            true,
            &[hop("alpha", "receive", "beta", "process", 1)],
            1_000_000_009,
        );
        let mut later = cluster_topology(
            &published(),
            &roster(),
            true,
            &[hop("alpha", "receive", "beta", "process", 5)],
            3_000_000_009,
        );
        later.rate_since(&earlier);
        assert!((later.links[0].rate - 2.0).abs() < f64::EPSILON);
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
