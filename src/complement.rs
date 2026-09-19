//! The full complement: the nodes a level brings when nobody named any.
//!
//! The owner, 2026-09-19: *Omitted -Stress, Omitted -Nodes means bring it
//! all.* An omitted selector is not a cautious default; it is the most the rig
//! can give (ADR-0059, amendment of that day). For `-Nodes` the most a level
//! can give is its own count, [`Stress::nodes`], already scaled to the
//! machine's headroom — and a count alone is not a complement, because a
//! cluster of nodes that declare nothing leaves `RoundTrip` running whole in
//! the roll while the message path between processes, the thing the nodes are
//! there for, never runs.
//!
//! So the complement is dealt: the level's nodes, `node-01` up, taking
//! receive, process and send in message-path order and round again, so every
//! stage is covered and no two counts differ by more than one. Twenty nodes
//! are seven receiving, seven processing and six sending. Nothing is read out
//! of a name here either (ADR-0056) — the deal is by position in the list, and
//! the capability each node gets is what it is told.
//!
//! **A level too small for the path.** Below three nodes the deal cannot cover
//! receive, process and send, and a roster with two of the three declared is
//! refused by [`Roster::refusal`] — an omitted `-Nodes` would then refuse
//! itself, which is no answer at all. Below three, every node of the
//! complement declares no stage instead: each runs whole tests itself, as a
//! node that declares nothing always has, and the roll runs `RoundTrip`
//! whole. [`covers_the_path`] is how a surface knows which of the two it got,
//! so it can say so rather than leave it to be noticed.

use crate::capability::Capability;
use crate::roster::Roster;
use crate::stress::Stress;
use crate::verdict::Stage;

/// The complement `stress` brings: [`Stress::nodes`] nodes, dealt over the
/// message path.
#[must_use]
pub fn full(stress: Stress) -> Roster {
    of_count(stress.nodes())
}

/// The complement of `count` nodes: `node-01` up, dealt receive, process,
/// send and round again; every node declaring no stage when there are too few
/// to cover the path.
#[must_use]
pub fn of_count(count: usize) -> Roster {
    let names: Vec<String> = (1..=count).map(|at| format!("node-{at:02}")).collect();
    let roster = Roster::of(&names);
    if count < Stage::ALL.len() {
        return roster;
    }
    names.iter().enumerate().fold(roster, |dealt, (at, name)| {
        dealt.declared(name, Capability::of(&[Stage::ALL[at % Stage::ALL.len()]]))
    })
}

/// Whether this roster covers the whole message path — every stage declared
/// somewhere, so `RoundTrip` runs between the node processes.
#[must_use]
pub fn covers_the_path(roster: &Roster) -> bool {
    Stage::ALL
        .into_iter()
        .all(|stage| !roster.with(stage).is_empty())
}

/// What a roster is, in one line: how many nodes, and how many of them serve
/// each stage — or that none declares a stage and the roll runs whole tests
/// itself. The roll's first line, and what a surface says an operator got.
#[must_use]
pub fn describe(roster: &Roster) -> String {
    let count = roster.names().len();
    if count == 0 {
        return "no nodes".to_string();
    }
    let many = if count == 1 { "node" } else { "nodes" };
    if !roster.serves_any_stage() {
        return format!("{count} {many}, none declaring a stage: whole tests in each");
    }
    let dealt: Vec<String> = Stage::ALL
        .into_iter()
        .map(|stage| format!("{} {}", roster.with(stage).len(), stage.name()))
        .collect();
    format!("{count} {many}, {}", dealt.join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_deal_covers_the_path_and_spreads_evenly() {
        let three = of_count(3);
        assert_eq!(three.text(), "node-01=receive,node-02=process,node-03=send");
        assert!(covers_the_path(&three));
        assert_eq!(three.refusal(), None);

        let twenty = of_count(20);
        assert_eq!(twenty.names().len(), 20);
        assert_eq!(twenty.with(Stage::Receive).len(), 7);
        assert_eq!(twenty.with(Stage::Process).len(), 7);
        assert_eq!(twenty.with(Stage::Send).len(), 6);
        assert_eq!(describe(&twenty), "20 nodes, 7 receive, 7 process, 6 send");
    }

    #[test]
    fn too_few_for_the_path_declare_nothing_rather_than_refuse_themselves() {
        for count in [1, 2] {
            let small = of_count(count);
            assert_eq!(small.names().len(), count);
            assert!(!small.serves_any_stage(), "{count} cannot cover the path");
            assert!(!covers_the_path(&small));
            // The point of declaring nothing: the roster refuses nobody.
            assert_eq!(small.refusal(), None);
        }
        let one = describe(&of_count(1));
        assert_eq!(one, "1 node, none declaring a stage: whole tests in each");
        assert_eq!(describe(&of_count(0)), "no nodes");
        assert!(of_count(0).is_empty());
    }

    #[test]
    fn every_level_brings_its_own_count_and_brutal_brings_the_most() {
        for stress in [
            Stress::Calm,
            Stress::Realistic,
            Stress::Harsh,
            Stress::Brutal,
        ] {
            assert_eq!(full(stress).names().len(), stress.nodes());
        }
        assert!(Stress::Brutal.nodes() >= Stress::Calm.nodes());
        assert_eq!(full(Stress::Realistic).text(), of_count(3).text());
    }
}
