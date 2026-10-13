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
//! 2026-09-30). The Playground's Parties are its configuration's, each on
//! its share of the transports on each side (`parties::Spread`; ADR-0028,
//! amendment 2026-10-13). What
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
use crate::parties::Spread;
use crate::roster::Roster;
use crate::support::cluster_root;

/// The cluster's topology from what it was configured with, what its nodes
/// published this round and the handoffs they delivered: the cluster, one
/// node per name on the roster, each node's stages and their endpoints, the
/// handoff links — configured where `relayed` says the run hands `RoundTrip`
/// along the stages the roster declares, observed where a hop was
/// delivered — the Parties outside them, each over its own transports as
/// `spread` says, and the shared store with its links when a node ran a
/// test over it.
#[must_use]
pub fn cluster_topology(
    snapshot: &observe::Snapshot,
    roster: &Roster,
    relayed: bool,
    hops: &[Hop],
    spread: &Spread,
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
    let sides: Vec<(&str, Vec<String>, Vec<String>)> = spread
        .parties()
        .all()
        .iter()
        .enumerate()
        .map(|(place, one)| {
            (
                one.name.as_str(),
                spread.sends(place),
                spread.receives(place),
            )
        })
        .collect();
    let parties: Vec<party::Party<'_>> = sides
        .iter()
        .map(|(name, sends, receives)| party::Party {
            name,
            sends: Some(sends),
            receives: Some(receives),
        })
        .collect();
    party::draw(snapshot, &root, &names, &parties, &mut topology);
    shared::draw(snapshot, &names, &mut topology);
    topology
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parties::Parties;
    use crate::report::roll_toml;
    use crate::support::{declaring, path, test_cluster};
    use node::Capability;
    use observe::{
        Count, Counted, Health, HealthRecord, NodeKind, Origin, Publication, Snapshot, TopologyNode,
    };

    /// The test cluster's receiving, processing and two sending nodes: the
    /// first sending node reports, the second has only declared.
    fn nodes() -> [String; 4] {
        let cluster = test_cluster();
        let [receiving, processing, _] = path(&cluster);
        let [sending, idle] = declaring(&cluster, "sending")[..] else {
            panic!("the test cluster has two sending nodes");
        };
        [receiving, processing, sending, idle].map(ToString::to_string)
    }

    /// The configuration's Parties over the two transports the snapshot
    /// reports, `file` then `tcp`.
    fn spread() -> Spread {
        Parties::configured().spread(&["file", "tcp"])
    }

    /// `text` with `<r>`, `<p>`, `<s>` and `<z>` the names of [`nodes`], in
    /// that order, `<root>` the cluster's scope, and `<in-file>`,
    /// `<in-tcp>`, `<out-file>` and `<out-tcp>` the Parties [`spread`] says
    /// send in over and are delivered to over each transport.
    fn named(text: &str) -> String {
        let [receiving, processing, sending, idle] = nodes();
        let spread = spread();
        let name = |place: usize| spread.parties().at(place).name.clone();
        text.replace("<in-file>", &name(spread.sender("file")))
            .replace("<in-tcp>", &name(spread.sender("tcp")))
            .replace("<out-file>", &name(spread.receiver("file")))
            .replace("<out-tcp>", &name(spread.receiver("tcp")))
            .replace("<root>", &cluster_root())
            .replace("<r>", &receiving)
            .replace("<p>", &processing)
            .replace("<s>", &sending)
            .replace("<z>", &idle)
    }

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

    /// `<r>` received over tcp and file, `<p>` processed, `<s>` sent over tcp
    /// with one pair stressed; `<s>` also ran the two shared-directory tests.
    /// `<z>` has only said what it can do and reported nothing yet.
    fn published() -> Snapshot {
        let mut snapshot = Snapshot::new();
        for (leaf, capability) in [
            ("<r>", "receiving"),
            ("<p>", "processing"),
            ("<s>", "sending"),
            ("<z>", "sending"),
        ] {
            let scope = named(&format!("<root>/node/{leaf}/capability"));
            snapshot.record_health(record(&scope, Health::Fine, &declares(capability)));
        }
        for (leaf, health, evidence) in [
            ("<r>/system-process", Health::Fine, "alive"),
            ("<r>/receive/tcp/json", Health::Fine, "3/3 rounds passed"),
            ("<r>/receive/tcp/json/identification", Health::Fine, "held"),
            ("<r>/receive/file/text", Health::Fine, "3/3 rounds passed"),
            ("<p>/system-process", Health::Fine, "alive"),
            ("<p>/process/tcp/json", Health::Fine, "3/3 rounds passed"),
            ("<s>/system-process", Health::Fine, "alive"),
            (
                "<s>/send/tcp/json",
                Health::Stressed,
                "2/3 rounds passed, 1 failed",
            ),
            ("<s>/exclusive-claim/file", Health::Fine, "one holder"),
            ("<s>/daily-backlog/drain", Health::Fine, "drained"),
        ] {
            let scope = named(&format!("<root>/node/{leaf}"));
            snapshot.record_health(record(&scope, health, evidence));
        }
        snapshot.record_health(record(&named("<root>/node"), Health::Stressed, "3 nodes"));
        for (counted, value) in [(Counted::Streams, 6), (Counted::Messages, 2)] {
            snapshot.record_count(Count {
                scope: named("<root>/node/<s>/daily-backlog"),
                counted,
                value,
                window_start_unix_nanos: 7,
                window_end_unix_nanos: 7,
                observed_unix_nanos: 7,
            });
        }
        snapshot
    }

    /// What the run configured: `<r>` receives, `<p>` processes, `<s>` and
    /// `<z>` send.
    fn roster() -> Roster {
        Roster::parse(&named(
            "<r>=receiving,<p>=processing,<s>=sending,<z>=sending",
        ))
        .expect("a roster")
    }

    fn drawn() -> Topology {
        let [receiving, processing, sending, _] = nodes();
        let hops = [
            hop(&receiving, "receive", &processing, "process", 3),
            hop(&processing, "process", &sending, "send", 2),
        ];
        cluster_topology(&published(), &roster(), true, &hops, &spread(), 9)
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
        // The Parties are the drawing's last but the store, and are held by
        // their own test below.
        let shape: Vec<(String, &str, String)> = topology
            .nodes
            .iter()
            .filter(|node| node.kind != NodeKind::Party)
            .map(|node| (node.id.clone(), node.kind.word(), node.parent.clone()))
            .collect();
        let expected: Vec<(String, &str, String)> = [
            ("cluster", "cluster", ""),
            ("node/<r>", "node", "cluster"),
            ("node/<r>/receive", "stage", "node/<r>"),
            ("node/<r>/receive/file", "endpoint", "node/<r>/receive"),
            ("node/<r>/receive/tcp", "endpoint", "node/<r>/receive"),
            ("node/<p>", "node", "cluster"),
            ("node/<p>/process", "stage", "node/<p>"),
            ("node/<s>", "node", "cluster"),
            ("node/<s>/send", "stage", "node/<s>"),
            ("node/<s>/send/tcp", "endpoint", "node/<s>/send"),
            ("node/<z>", "node", "cluster"),
            ("node/<z>/send", "stage", "node/<z>"),
            ("shared", "location", "cluster"),
        ]
        .into_iter()
        .map(|(id, kind, parent)| (named(id), kind, named(parent)))
        .collect();
        assert_eq!(shape, expected);

        let cluster = find(&topology, "cluster");
        let root = cluster_root();
        assert_eq!(
            (
                cluster.label.as_str(),
                cluster.scope.as_str(),
                cluster.state.word()
            ),
            (test_cluster().name.as_str(), root.as_str(), "holding")
        );
        assert!((cluster.activity - 0.75).abs() < f64::EPSILON);
        // ADR-0041: a parent is Fine or Holding, never its leaf's mood.
        assert_eq!(find(&topology, &named("node/<r>")).state, Health::Fine);
        assert_eq!(find(&topology, &named("node/<s>")).state, Health::Holding);
        let endpoint = find(&topology, &named("node/<s>/send/tcp"));
        assert_eq!(
            (
                endpoint.label.as_str(),
                endpoint.scope.as_str(),
                endpoint.state.word()
            ),
            ("tcp", named("<root>/node/<s>/send/tcp").as_str(), "holding")
        );
        let idle = find(&topology, &named("node/<z>/send"));
        assert_eq!(
            (idle.origin.word(), idle.state.word()),
            ("configured", "working")
        );
    }

    #[test]
    fn handoffs_link_the_stages_and_the_store_is_linked_only_by_who_ran_over_it() {
        let topology = drawn();
        let links: Vec<(String, String, &str, &str, u64)> = topology
            .links
            .iter()
            .filter(|link| !link.id.starts_with("party/"))
            .map(|link| {
                (
                    link.from.clone(),
                    link.to.clone(),
                    link.pattern.word(),
                    link.protocol.as_str(),
                    link.volume,
                )
            })
            .collect();
        let expected: Vec<(String, String, &str, &str, u64)> = [
            (
                "node/<r>/receive",
                "node/<p>/process",
                "send-receive",
                "handoff",
                3,
            ),
            (
                "node/<p>/process",
                "node/<s>/send",
                "send-receive",
                "handoff",
                2,
            ),
            (
                "node/<p>/process",
                "node/<z>/send",
                "send-receive",
                "handoff",
                0,
            ),
            ("node/<s>", "shared", "publish-consume", "file", 0),
            ("node/<s>", "shared", "publish-consume", "file", 6),
        ]
        .into_iter()
        .map(|(from, to, pattern, protocol, volume)| {
            (named(from), named(to), pattern, protocol, volume)
        })
        .collect();
        assert_eq!(links, expected);
        assert_eq!(topology.links[0].state, Health::Fine);
        assert_eq!(
            topology.links[1].state,
            Health::Stressed,
            "the worst leaf involved"
        );
        assert_eq!(topology.links[1].origin, Origin::Both);
        let store = topology
            .links
            .last()
            .expect("the store's links are drawn last");
        assert!((store.progress - 0.75).abs() < f64::EPSILON);

        let [only, ..] = nodes();
        let bare = cluster_topology(
            &Snapshot::new(),
            &Roster::of(&[only]),
            false,
            &[],
            &spread(),
            9,
        );
        let ids: Vec<&str> = bare.nodes.iter().map(|node| node.id.as_str()).collect();
        assert_eq!(
            ids,
            ["cluster", named("node/<r>").as_str()],
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
        let [receiving, processing, sending, _] = nodes();
        let topology = drawn();
        let idle = topology
            .links
            .iter()
            .find(|link| link.to == named("node/<z>/send"))
            .expect("the processing node to the idle one is configured and drawn");
        assert_eq!((idle.origin, idle.volume), (Origin::Configured, 0));
        assert!(
            idle.evidence
                .starts_with(&named("configured <p> to <z>; no handoff observed yet")),
            "{}",
            idle.evidence
        );

        let stray = [hop(&sending, "process", &receiving, "send", 1)];
        let observed = cluster_topology(&published(), &roster(), true, &stray, &spread(), 9);
        let link = observed
            .links
            .iter()
            .find(|link| link.from == named("node/<s>/process"))
            .expect("a delivered hop is drawn");
        assert_eq!(link.origin, Origin::Observed);

        // Not relaying RoundTrip over stages: nothing is configured between
        // the nodes, and only what was delivered is drawn.
        let unrelayed = cluster_topology(&published(), &roster(), false, &[], &spread(), 9);
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
            &[hop(&receiving, "receive", &processing, "process", 1)],
            &spread(),
            1_000_000_009,
        );
        let mut later = cluster_topology(
            &published(),
            &roster(),
            true,
            &[hop(&receiving, "receive", &processing, "process", 5)],
            &spread(),
            3_000_000_009,
        );
        later.rate_since(&earlier);
        assert!((later.links[0].rate - 2.0).abs() < f64::EPSILON);
    }

    /// The test's snapshot with a Party's receive leaf stressed and counts
    /// at each endpoint, drawn.
    fn faced() -> Topology {
        let mut snapshot = published();
        snapshot.record_health(record(
            &named("<root>/node/<r>/receive/file/text/authorization"),
            Health::Stressed,
            "the party is not permitted on this Receive Location",
        ));
        for (endpoint, counted, value) in [
            ("<r>/receive/file", Counted::Streams, 40),
            ("<r>/receive/tcp", Counted::Streams, 2),
            ("<s>/send/tcp", Counted::Messages, 31),
            ("<s>/send/tcp", Counted::Bytes, 9000),
        ] {
            snapshot.record_count(Count {
                scope: named(&format!("<root>/node/{endpoint}")),
                counted,
                value,
                window_start_unix_nanos: 7,
                window_end_unix_nanos: 7,
                observed_unix_nanos: 7,
            });
        }
        cluster_topology(&snapshot, &roster(), true, &[], &spread(), 9)
    }

    fn link_in<'t>(topology: &'t Topology, id: &str) -> &'t observe::TopologyLink {
        topology
            .links
            .iter()
            .find(|link| link.id == id)
            .unwrap_or_else(|| panic!("{id} is drawn"))
    }

    /// The owner, 2026-09-29: *Something is sending streams to a Xmip Node.
    /// A Xmip Node sends streams to somethings.* And 2026-10-13: *You have
    /// not Implementet more Parties than Party-X.* One box per Party and
    /// side, a link per stage facing it over the Party's own transports,
    /// carrying what the stage counted over them and the worst leaf beneath
    /// them, named by transport.
    #[test]
    fn each_party_sends_into_the_receive_stages_over_its_own_transports() {
        let topology = faced();
        let link = |id: &str| link_in(&topology, id);

        let sends = link(&named("party/sending/<in-file>/<r>"));
        assert_eq!(
            (sends.from.clone(), sends.to.clone()),
            (named("party/sending/<in-file>"), named("node/<r>/receive"))
        );
        assert_eq!((sends.volume, sends.state), (40, Health::Stressed));
        assert_eq!(
            (sends.origin, sends.protocol.as_str()),
            (Origin::Both, "file")
        );
        assert_eq!(
            sends.evidence,
            named(
                "<in-file> sends into <r> over file; worst over file: \
                 the party is not permitted on this Receive Location"
            )
        );
        // Another Party on the same stage is not judged by what it does not
        // send over.
        let other = link(&named("party/sending/<in-tcp>/<r>"));
        assert_eq!(
            (other.volume, other.state, other.protocol.as_str()),
            (2, Health::Fine, "tcp")
        );

        // A Party is Fine or Holding over the worst it faces (ADR-0041).
        let sender = find(&topology, &named("party/sending/<in-file>"));
        assert_eq!(
            (sender.label.clone(), sender.scope.clone(), sender.state),
            (
                named("<in-file>"),
                named("<root>/party/<in-file>"),
                Health::Holding
            )
        );
        assert!(
            sender
                .evidence
                .starts_with(&named("sends into <r>; worst at <r> over file: ")),
            "{}",
            sender.evidence
        );
    }

    #[test]
    fn each_party_is_delivered_to_over_its_own_transports() {
        let topology = faced();
        let link = |id: &str| link_in(&topology, id);

        let delivers = link(&named("party/receiving/<out-tcp>/<s>"));
        assert_eq!(
            (delivers.from.clone(), delivers.to.clone()),
            (named("node/<s>/send"), named("party/receiving/<out-tcp>"))
        );
        assert_eq!(
            (delivers.volume, delivers.state, delivers.protocol.as_str()),
            (31, Health::Stressed, "tcp"),
            "Messages at Send, not its bytes"
        );
        let idle = link(&named("party/receiving/<out-tcp>/<z>"));
        assert_eq!((idle.origin, idle.volume), (Origin::Configured, 0));
        assert_eq!(
            idle.evidence,
            named("<z> delivers to <out-tcp>; no transport reported yet")
        );
        assert!(
            topology
                .links
                .iter()
                .all(|link| link.id != named("party/receiving/<out-file>/<s>")),
            "a send stage holding none of a Party's transports does not face it"
        );

        let receiver = find(&topology, &named("party/receiving/<out-tcp>"));
        assert_eq!(receiver.state, Health::Holding);
        assert!(
            receiver
                .evidence
                .starts_with(&named("is delivered to by <s>, <z>; worst at <s> over tcp"))
        );

        // No stage, no Party.
        let [only, ..] = nodes();
        let bare = cluster_topology(
            &Snapshot::new(),
            &Roster::of(&[only]),
            false,
            &[],
            &spread(),
            9,
        );
        assert!(bare.nodes.iter().all(|node| node.kind != NodeKind::Party));
    }

    #[test]
    fn the_topology_rides_the_snapshot_and_the_records_still_read_back() {
        let snapshot = published();
        let topology = drawn();
        let root = cluster_root();

        let text = roll_toml(&root, &snapshot, Some(topology.clone()), None, "");
        assert!(text.contains("[[topology.nodes]]"));
        assert!(text.contains("[[topology.links]]"));
        let parsed: toml::Value = text.parse().expect("valid TOML");
        assert_eq!(
            parsed["topology"]["nodes"].as_array().map(Vec::len),
            Some(topology.nodes.len())
        );

        let back = Publication::read(&text).expect("reads back");
        assert_eq!(back.records.len(), snapshot.health(&root).len());
        assert_eq!(back.topology, Some(topology));
    }
}
