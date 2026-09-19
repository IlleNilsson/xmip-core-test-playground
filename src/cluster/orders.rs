//! What a cluster starts its nodes on: who they are, how hard they run, the
//! tests they are told, which of them may assume the internet, and for how
//! many rounds.
//!
//! One value rather than seven arguments, so what the cluster process was told
//! at the door (ADR-0055) is handed on to its nodes unchanged and nothing is
//! decided twice.

use crate::role::Roster;
use crate::scenario::{ROUND_TRIP, drives};
use crate::stress::Stress;
use crate::switch::Switches;

/// The orders a cluster spawns its nodes under.
#[derive(Clone, Debug)]
pub struct Orders {
    /// How hard every node runs.
    pub stress: Stress,
    /// Every node's name, in the order they were named; the letter is the
    /// role, and each node is told the whole list so it finds the others.
    pub names: Vec<String>,
    /// The nodes that may assume the internet (ADR-0045). `None` leaves each
    /// node to `XMIP_PLAYGROUND_ONLINE_NODES` and `XMIP_ONLINE`.
    pub online: Option<Vec<String>>,
    /// The scenarios every node is told to run; empty means every one.
    pub scenarios: Vec<String>,
    /// Rounds a node runs before it exits on its own; `0` runs until stopped.
    pub rounds: u64,
}

impl Orders {
    /// Orders for the nodes named: every scenario, online left to the
    /// environment.
    #[must_use]
    pub fn of(stress: Stress, names: &[String], rounds: u64) -> Self {
        Self {
            stress,
            names: names.to_vec(),
            online: None,
            scenarios: Vec::new(),
            rounds,
        }
    }

    /// The same for `count` nodes numbered `node-01` up — a level's own,
    /// where nobody named them.
    #[must_use]
    pub fn numbered(stress: Stress, count: usize, rounds: u64) -> Self {
        let names: Vec<String> = (1..=count)
            .map(|index| format!("node-{index:02}"))
            .collect();
        Self::of(stress, &names, rounds)
    }

    /// The same driving only the scenarios named (the owner, 2026-09-19:
    /// nodes run the test that was named). None named is every one.
    #[must_use]
    pub fn driving(mut self, scenarios: &[String]) -> Self {
        self.scenarios = scenarios.to_vec();
        self
    }

    /// The same with the online nodes named outright rather than read from
    /// the environment.
    #[must_use]
    pub fn with_online(mut self, online: Option<Vec<String>>) -> Self {
        self.online = online;
        self
    }

    /// Whether the node called `name` may assume the internet.
    #[must_use]
    pub fn is_online(&self, name: &str) -> bool {
        self.online.as_deref().map_or_else(
            || Switches::for_node(name).online,
            |named| named.iter().any(|one| one.eq_ignore_ascii_case(name)),
        )
    }

    /// The switches the node called `name` is started with.
    #[must_use]
    pub fn switches(&self, name: &str) -> Switches {
        Switches {
            online: self.is_online(name),
        }
    }

    /// Why these orders cannot be carried out, if they cannot: no node at
    /// all, an online node that is no node of the cluster, or a `RoundTrip`
    /// whose roster is missing a role. Asked before a process is spawned
    /// (ADR-0055 clause 2).
    #[must_use]
    pub fn refusal(&self) -> Option<String> {
        if self.names.is_empty() {
            return Some(
                "REFUSED: a cluster supervises nodes and none was named; \
                 --nodes R1,P1,S1 names them."
                    .to_string(),
            );
        }
        let strangers: Vec<&str> = self
            .online
            .iter()
            .flatten()
            .map(String::as_str)
            .filter(|one| !self.names.iter().any(|name| name.eq_ignore_ascii_case(one)))
            .collect();
        if !strangers.is_empty() {
            return Some(format!(
                "REFUSED: --online names {}, which --nodes does not; the nodes are {}.",
                strangers.join(", "),
                self.names.join(", ")
            ));
        }
        drives(&self.scenarios, ROUND_TRIP)
            .then(|| Roster::of(&self.names).refusal())
            .flatten()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(ToString::to_string).collect()
    }

    #[test]
    fn the_online_nodes_are_the_ones_named_and_nobody_else() {
        let orders = Orders::of(Stress::Calm, &names(&["R1", "P1", "S1"]), 0)
            .with_online(Some(names(&["r1"])));
        assert!(orders.is_online("R1"), "named, whatever the case");
        assert!(!orders.is_online("P1"));
        assert_eq!(orders.switches("R1").word(), "online");
        assert_eq!(orders.switches("S1").word(), "offline");
        assert_eq!(orders.refusal(), None);
    }

    #[test]
    fn numbered_orders_name_the_level_s_own_nodes() {
        let orders = Orders::numbered(Stress::Realistic, 3, 5);
        assert_eq!(orders.names, ["node-01", "node-02", "node-03"]);
        assert_eq!(orders.rounds, 5);
        assert!(orders.scenarios.is_empty());
        assert_eq!(orders.refusal(), None, "no role nodes, nothing to refuse");
    }

    #[test]
    fn no_node_a_stranger_online_and_a_missing_role_are_each_refused() {
        let none = Orders::of(Stress::Calm, &[], 0)
            .refusal()
            .expect("no nodes");
        assert!(
            none.starts_with("REFUSED") && none.contains("--nodes"),
            "{none}"
        );

        let stranger = Orders::of(Stress::Calm, &names(&["R1", "P1", "S1"]), 0)
            .with_online(Some(names(&["Q9"])))
            .refusal()
            .expect("Q9 is no node");
        assert!(
            stranger.contains("Q9") && stranger.contains("R1, P1, S1"),
            "{stranger}"
        );

        let missing = Orders::of(Stress::Calm, &names(&["R1", "S1"]), 0)
            .refusal()
            .expect("no P");
        assert!(missing.ends_with("name no P."), "{missing}");

        // Not RoundTrip: the roster is nobody's business.
        assert_eq!(
            Orders::of(Stress::Calm, &names(&["R1", "S1"]), 0)
                .driving(&names(&["filing"]))
                .refusal(),
            None
        );
    }
}
