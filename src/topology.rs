//! The communication topology a roll publishes beside its snapshot: Xmip's
//! own communication, drawn from what the cluster configured and what its
//! nodes reported (ADR-0052, amendment 2026-09-14, ruling 3). Nothing here is
//! inferred from a socket — a node is in the picture because `-Nodes` named
//! it, a stage because the node declared it (ADR-0056) or reported on
//! it, and a link because the roster configures a handoff there or one was
//! delivered over it — configured, observed or both, said on the link.
//!
//! The cluster, its nodes, their stages and endpoints, and the Parties
//! outside them are drawn by `observe::topology::draw` and
//! `observe::topology::party`, the one drawing every publisher calls — a
//! running node draws itself through it too (ADR-0018, amendment
//! 2026-09-30). The Playground's Party is its own, `identity::PARTY`. What
//! only a roll has is drawn here: between the stages run the handoffs,
//! receive to process to send, one link per pair of stages configured to
//! exchange them or that exchanged any, its volume the hops and its rate
//! their rise per second since the roll's last publication
//! (`Topology::rate_since`); a link shows the worst leaf at either end. The
//! shared directory is drawn only when a node ran a test over it. The model
//! and its words are `observe::topology`'s, which writes and reads them
//! (open problem 25).

mod handoff;
mod shared;

use observe::Topology;
use observe::topology::{draw, party};

use crate::handoff::Hop;
use crate::identity::PARTY;
use crate::roster::Roster;
use crate::support::cluster_root;

/// The cluster's topology from what it was configured with, what its nodes
/// published this round and the handoffs they delivered: the cluster, one
/// node per name on the roster, each node's stages and their endpoints, the
/// handoff links — configured where `relayed` says the run hands `RoundTrip`
/// along the stages the roster declares, observed where a hop was
/// delivered — the Parties outside them, and the shared store with its links
/// when a node ran a test over it.
#[must_use]
pub fn cluster_topology(
    snapshot: &observe::Snapshot,
    roster: &Roster,
    relayed: bool,
    hops: &[Hop],
    now: i64,
) -> Topology {
    let names: Vec<&str> = roster.names();
    let root = cluster_root();
    let mut topology = Topology {
        source: format!("playground — cluster {}", draw::label(&root)),
        observed_unix_nanos: now,
        nodes: draw::members(snapshot, &root, &names),
        links: Vec::new(),
    };
    let configured = relayed.then_some(roster);
    topology
        .links
        .extend(handoff::handoff_links(snapshot, configured, hops));
    party::draw(snapshot, &root, &names, PARTY, &mut topology);
    shared::draw(snapshot, &names, &mut topology);
    topology
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::ROOT;
    use crate::report::roll_toml;
    use node::Capability;
    use observe::{
        Count, Counted, Health, HealthRecord, NodeKind, Origin, Publication, Snapshot, TopologyNode,
    };

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
    /// words `--role` takes.
    fn declares(roles: &str) -> String {
        Capability::parse(roles).expect("a declaration").evidence()
    }

    /// `alpha` received over tcp and file, `beta` processed, `gamma` sent over tcp
    /// with one pair stressed; `gamma` also ran the two shared-directory tests.
    /// `zeta` has only said what it can do and reported nothing yet.
    fn published() -> Snapshot {
        let mut snapshot = Snapshot::new();
        for (leaf, capability) in [
            ("alpha", "receiving"),
            ("beta", "processing"),
            ("gamma", "sending"),
            ("zeta", "sending"),
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
        Roster::parse("alpha=receiving,beta=processing,gamma=sending,zeta=sending")
            .expect("a roster")
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
                ("party/sending/party-x", "party", "cluster"),
                ("party/receiving/party-x", "party", "cluster"),
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
                (
                    "party/sending/party-x",
                    "node/alpha/receive",
                    "send-receive",
                    "2 transports",
                    0
                ),
                (
                    "node/gamma/send",
                    "party/receiving/party-x",
                    "send-receive",
                    "tcp",
                    0
                ),
                (
                    "node/zeta/send",
                    "party/receiving/party-x",
                    "send-receive",
                    "no transport reported",
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
        assert!((topology.links[7].progress - 0.75).abs() < f64::EPSILON);

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

    /// The owner, 2026-09-29: *Something is sending streams to a Xmip Node.
    /// A Xmip Node sends streams to somethings.* One box per Party and side,
    /// a link per stage facing it, carrying what the stage counted and the
    /// worst leaf beneath its endpoints, named by transport.
    #[test]
    fn a_party_sends_into_the_receive_stages_and_the_send_stages_deliver_to_one() {
        let mut snapshot = published();
        snapshot.record_health(record(
            &format!("{ROOT}/node/alpha/receive/file/text/authorization"),
            Health::Stressed,
            "the party is not permitted on this Receive Location",
        ));
        for (stage, counted, value) in [
            ("alpha/receive", Counted::Streams, 40),
            ("gamma/send", Counted::Messages, 31),
            ("gamma/send", Counted::Bytes, 9000),
        ] {
            snapshot.record_count(Count {
                scope: format!("{ROOT}/node/{stage}"),
                counted,
                value,
                window_start_unix_nanos: 7,
                window_end_unix_nanos: 7,
                observed_unix_nanos: 7,
            });
        }
        let topology = cluster_topology(&snapshot, &roster(), true, &[], 9);
        let link = |id: &str| {
            topology
                .links
                .iter()
                .find(|link| link.id == id)
                .unwrap_or_else(|| panic!("{id} is drawn"))
        };

        let sends = link("party/sending/party-x/alpha");
        assert_eq!(
            (sends.from.as_str(), sends.to.as_str()),
            ("party/sending/party-x", "node/alpha/receive")
        );
        assert_eq!((sends.volume, sends.state), (40, Health::Stressed));
        assert_eq!(sends.origin, Origin::Both);
        assert_eq!(
            sends.evidence,
            "party-x sends into alpha over 2 transports; worst over file: \
             the party is not permitted on this Receive Location"
        );

        let delivers = link("party/receiving/party-x/gamma");
        assert_eq!(
            (delivers.from.as_str(), delivers.to.as_str()),
            ("node/gamma/send", "party/receiving/party-x")
        );
        assert_eq!(
            (delivers.volume, delivers.state, delivers.protocol.as_str()),
            (31, Health::Stressed, "tcp"),
            "Messages at Send, not its bytes"
        );
        let idle = link("party/receiving/party-x/zeta");
        assert_eq!((idle.origin, idle.volume), (Origin::Configured, 0));
        assert_eq!(
            idle.evidence,
            "zeta delivers to party-x; no transport reported yet"
        );

        // A Party is Fine or Holding over the worst it faces (ADR-0041).
        let sender = find(&topology, "party/sending/party-x");
        assert_eq!(
            (sender.label.as_str(), sender.scope.as_str(), sender.state),
            (
                "party-x",
                "xmip:///playground/party/party-x",
                Health::Holding
            )
        );
        assert!(
            sender
                .evidence
                .starts_with("sends into alpha; worst at alpha over file: "),
            "{}",
            sender.evidence
        );
        let receiver = find(&topology, "party/receiving/party-x");
        assert_eq!(receiver.state, Health::Holding);
        assert!(
            receiver
                .evidence
                .starts_with("is delivered to by gamma, zeta; worst at gamma over tcp")
        );

        // No stage, no Party.
        let bare = cluster_topology(&Snapshot::new(), &Roster::of(&["n".into()]), false, &[], 9);
        assert!(bare.nodes.iter().all(|node| node.kind != NodeKind::Party));
    }

    #[test]
    fn the_topology_rides_the_snapshot_and_the_records_still_read_back() {
        let snapshot = published();
        let topology = drawn();

        let text = roll_toml(ROOT, &snapshot, Some(topology.clone()), None, "");
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
