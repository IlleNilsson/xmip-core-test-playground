//! The Parties outside the cluster and the links to them (ADR-0019; ADR-0052,
//! amendment 2026-09-29). The owner, 2026-09-29: *Something is sending
//! streams to a Xmip Node. A Xmip Node sends streams to somethings.* One
//! Party is drawn for every side it is on: the one that sends into a receive
//! stage, linked to each receive stage it sends into, and the one a send stage
//! delivers to, linked from each send stage that delivers to it — never one
//! box per transport far end. A link carries what its stage counted and the
//! worst leaf of the endpoints beneath the stage, and says which transport
//! that was. In the Playground both sides are its one Party, [`PARTY`], whose
//! test peers are the far end of every transport.

use node::Stage;
use observe::{
    Counted, HealthRecord, NodeKind, Origin, Pattern, Snapshot, Topology, TopologyLink,
    TopologyNode,
};

use super::{CLUSTER, drawn, mood, node_id, worst};
use crate::identity::PARTY;
use crate::support::cluster_root;

/// Which side of the cluster a Party is on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Side {
    /// It sends into the cluster's receive stages.
    Sending,
    /// The cluster's send stages deliver to it.
    Receiving,
}

impl Side {
    /// The stage of the message path that faces a Party on this side.
    const fn stage(self) -> Stage {
        match self {
            Side::Sending => Stage::Receive,
            Side::Receiving => Stage::Send,
        }
    }

    /// The segment of the Party's id that tells its two boxes apart.
    const fn word(self) -> &'static str {
        match self {
            Side::Sending => "sending",
            Side::Receiving => "receiving",
        }
    }
}

/// The worst leaf beneath one stage's endpoints, the transport it is under,
/// and how many endpoints there are.
struct Facing {
    transports: usize,
    worst: Option<(String, HealthRecord)>,
}

/// Add the Party on each side any node's stage faces, with a link to or from
/// each such stage; nothing on a side no stage faces. Call it once the
/// stages and their endpoints are drawn.
pub(super) fn draw(snapshot: &Snapshot, names: &[&str], topology: &mut Topology) {
    for side in [Side::Sending, Side::Receiving] {
        let mut facing = Vec::new();
        let mut links = Vec::new();
        for name in names {
            if let Some((stage, link)) = link(snapshot, topology, name, side) {
                facing.push((*name, stage));
                links.push(link);
            }
        }
        if links.is_empty() {
            continue;
        }
        topology.nodes.push(party(side, &facing));
        topology.links.extend(links);
    }
}

/// The id of the Party's box on `side`.
fn party_id(side: Side) -> String {
    format!("party/{}/{PARTY}", side.word())
}

/// The endpoints the stage `stage_id` holds on the drawn topology, and the
/// worst leaf beneath them in the snapshot's own order, worst first (ADR-0041:
/// the worst record, not a rollup).
fn facing(snapshot: &Snapshot, topology: &Topology, stage_id: &str) -> Facing {
    let endpoints: Vec<&TopologyNode> = topology
        .nodes
        .iter()
        .filter(|node| node.parent == stage_id && node.kind == NodeKind::Endpoint)
        .collect();
    let worst = endpoints
        .iter()
        .filter_map(|endpoint| {
            worst(snapshot, &endpoint.scope).map(|record| (endpoint.label.clone(), record))
        })
        .min_by(|(_, a), (_, b)| a.standing().cmp(&b.standing()));
    Facing {
        transports: endpoints.len(),
        worst,
    }
}

/// The link between the Party on `side` and the node `name`'s stage that
/// faces it, where that stage is drawn.
fn link(
    snapshot: &Snapshot,
    topology: &Topology,
    name: &str,
    side: Side,
) -> Option<(Facing, TopologyLink)> {
    let stage = side.stage();
    let stage_id = format!("{}/{}", node_id(name), stage.name());
    if !topology.nodes.iter().any(|node| node.id == stage_id) {
        return None;
    }
    let facing = facing(snapshot, topology, &stage_id);
    let scope = format!("{}/node/{name}/{}", cluster_root(), stage.name());
    let volume = snapshot
        .measure(&scope, Counted::at(stage))
        .map_or(0, |count| count.value);
    let (health, said) = mood(facing.worst.as_ref().map(|(_, record)| record));
    let (from, to, ends) = match side {
        Side::Sending => (
            party_id(side),
            stage_id,
            format!("{PARTY} sends into {name}"),
        ),
        Side::Receiving => (
            stage_id,
            party_id(side),
            format!("{name} delivers to {PARTY}"),
        ),
    };
    let link = TopologyLink {
        id: format!("party/{}/{PARTY}/{name}", side.word()),
        from,
        to,
        pattern: Pattern::SendReceive,
        // The roster configures the stage and the Playground's Location
        // accepts or presents the Party; a transport reported is observed.
        origin: if facing.transports == 0 {
            Origin::Configured
        } else {
            Origin::Both
        },
        protocol: transports(&facing),
        state: health,
        volume,
        rate: 0.0,
        latency_ms: 0.0,
        progress: 0.0,
        attempts: 0,
        evidence: facing.worst.as_ref().map_or_else(
            || format!("{ends}; no transport reported yet"),
            |(transport, _)| {
                let over = transports(&facing);
                format!("{ends} over {over}; worst over {transport}: {said}")
            },
        ),
    };
    Some((facing, link))
}

/// The Party's box on `side`: Fine or Holding over the worst leaf of every
/// stage it faces (ADR-0041), and what that leaf said.
fn party(side: Side, facing: &[(&str, Facing)]) -> TopologyNode {
    let worse = facing
        .iter()
        .filter_map(|(name, facing)| {
            facing
                .worst
                .as_ref()
                .map(|(transport, record)| (*name, transport, record))
        })
        .min_by(|(_, _, a), (_, _, b)| a.standing().cmp(&b.standing()));
    let scope = format!("{}/party/{PARTY}", cluster_root());
    let (state, said) = drawn(worse.map(|(_, _, record)| record), &scope);
    let names: Vec<&str> = facing.iter().map(|(name, _)| *name).collect();
    let verb = match side {
        Side::Sending => "sends into",
        Side::Receiving => "is delivered to by",
    };
    let evidence = worse.map_or_else(
        || format!("{verb} {}; no transport reported yet", names.join(", ")),
        |(name, transport, _)| {
            format!(
                "{verb} {}; worst at {name} over {transport}: {said}",
                names.join(", ")
            )
        },
    );
    TopologyNode {
        id: party_id(side),
        parent: CLUSTER.to_string(),
        label: PARTY.to_string(),
        kind: NodeKind::Party,
        scope,
        state,
        origin: if worse.is_some() {
            Origin::Both
        } else {
            Origin::Configured
        },
        load: 0.0,
        activity: 0.0,
        evidence,
    }
}

/// What a link runs over: the one transport, or how many.
fn transports(facing: &Facing) -> String {
    match (facing.transports, &facing.worst) {
        (1, Some((transport, _))) => transport.clone(),
        (0, _) => "no transport reported".to_string(),
        (1, None) => "1 transport".to_string(),
        (count, _) => format!("{count} transports"),
    }
}
