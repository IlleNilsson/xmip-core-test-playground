//! What a cluster starts its nodes on: who they are and what each declared it
//! can do, how hard they run, the tests they are told, which of them may
//! assume the internet, and for how many rounds.
//!
//! One value rather than seven arguments, so what the cluster process was told
//! at the door (ADR-0055) is handed on to its nodes unchanged and nothing is
//! decided twice — least of all a node's capability, which is declared and
//! never inferred (ADR-0056).

use crate::capability::Capability;
use crate::roster::Roster;
use crate::scenario::{ROUND_TRIP, drives};
use crate::stress::Stress;
use crate::switch::node_is_online;

/// The orders a cluster spawns its nodes under.
#[derive(Clone, Debug)]
pub struct Orders {
    /// How hard every node runs.
    pub stress: Stress,
    /// Every node, in the order they were named, with the capability each was
    /// declared with; each node is told the whole roster so it finds the
    /// others without asking anyone.
    pub roster: Roster,
    /// The nodes that may assume the internet (ADR-0045). `None` leaves each
    /// node to `XMIP_PLAYGROUND_ONLINE_NODES` and `XMIP_ONLINE`.
    pub online: Option<Vec<String>>,
    /// The scenarios every node is told to run; empty means every one.
    pub scenarios: Vec<String>,
    /// Rounds a node runs before it exits on its own; `0` runs until stopped.
    pub rounds: u64,
}

impl Orders {
    /// Orders for the nodes in `roster`: every scenario, online left to the
    /// environment.
    #[must_use]
    pub fn of(stress: Stress, roster: Roster, rounds: u64) -> Self {
        Self {
            stress,
            roster,
            online: None,
            scenarios: Vec::new(),
            rounds,
        }
    }

    /// The same for `count` nodes numbered `node-01` up, none of them
    /// declaring a stage — a level's own, where nobody named them.
    #[must_use]
    pub fn numbered(stress: Stress, count: usize, rounds: u64) -> Self {
        let names: Vec<String> = (1..=count)
            .map(|index| format!("node-{index:02}"))
            .collect();
        Self::of(stress, Roster::of(&names), rounds)
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

    /// Every node's name, in the order they were named.
    #[must_use]
    pub fn names(&self) -> Vec<&str> {
        self.roster.names()
    }

    /// Whether the node called `name` may assume the internet.
    #[must_use]
    pub fn is_online(&self, name: &str) -> bool {
        self.online.as_deref().map_or_else(
            || node_is_online(name),
            |named| named.iter().any(|one| one.eq_ignore_ascii_case(name)),
        )
    }

    /// The whole capability the node called `name` is started with: what it
    /// was declared with, and whether it may assume the internet.
    #[must_use]
    pub fn capability(&self, name: &str) -> Capability {
        self.roster
            .capability(name)
            .with_online(self.is_online(name))
    }

    /// Why these orders cannot be carried out, if they cannot: no node at
    /// all, an online node that is no node of the cluster, or a `RoundTrip`
    /// whose roster leaves a capability of the path undeclared. Asked before a
    /// process is spawned (ADR-0055 clause 2).
    #[must_use]
    pub fn refusal(&self) -> Option<String> {
        if self.roster.is_empty() {
            return Some(
                "REFUSED: a cluster supervises nodes and none was named; \
                 --nodes R1=receive,P1=process,S1=send names them."
                    .to_string(),
            );
        }
        let names = self.names();
        let strangers: Vec<&str> = self
            .online
            .iter()
            .flatten()
            .map(String::as_str)
            .filter(|one| !names.iter().any(|name| name.eq_ignore_ascii_case(one)))
            .collect();
        if !strangers.is_empty() {
            return Some(format!(
                "REFUSED: --online names {}, which --nodes does not; the nodes are {}.",
                strangers.join(", "),
                names.join(", ")
            ));
        }
        drives(&self.scenarios, ROUND_TRIP)
            .then(|| self.roster.refusal())
            .flatten()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::verdict::Stage;

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(ToString::to_string).collect()
    }

    fn roster(text: &str) -> Roster {
        Roster::parse(text).expect("a well-formed roster")
    }

    #[test]
    fn the_online_nodes_are_the_ones_named_and_nobody_else() {
        let orders = Orders::of(Stress::Calm, roster("R1=receive,P1=process,S1=send"), 0)
            .with_online(Some(names(&["r1"])));
        assert!(orders.is_online("R1"), "named, whatever the case");
        assert!(!orders.is_online("P1"));
        assert_eq!(orders.capability("R1").word(), "online");
        assert_eq!(orders.capability("S1").word(), "offline");
        assert!(orders.capability("S1").can(Stage::Send));
        assert_eq!(orders.refusal(), None);
    }

    #[test]
    fn numbered_orders_name_the_level_s_own_nodes_declaring_nothing() {
        let orders = Orders::numbered(Stress::Realistic, 3, 5);
        assert_eq!(orders.names(), ["node-01", "node-02", "node-03"]);
        assert_eq!(orders.rounds, 5);
        assert!(orders.scenarios.is_empty());
        assert!(orders.capability("node-01").declares_no_stage());
        assert_eq!(
            orders.refusal(),
            None,
            "no stage declared, nothing to refuse"
        );
    }

    #[test]
    fn no_node_a_stranger_online_and_a_missing_capability_are_each_refused() {
        let none = Orders::of(Stress::Calm, Roster::default(), 0)
            .refusal()
            .expect("no nodes");
        assert!(
            none.starts_with("REFUSED") && none.contains("--nodes"),
            "{none}"
        );

        let stranger = Orders::of(Stress::Calm, roster("R1=receive,P1=process,S1=send"), 0)
            .with_online(Some(names(&["Q9"])))
            .refusal()
            .expect("Q9 is no node");
        assert!(
            stranger.contains("Q9") && stranger.contains("R1, P1, S1"),
            "{stranger}"
        );

        let missing = Orders::of(Stress::Calm, roster("R1=receive,S1=send"), 0)
            .refusal()
            .expect("nobody processes");
        assert!(missing.ends_with("no node declares process."), "{missing}");

        // Not RoundTrip: the roster is nobody's business.
        assert_eq!(
            Orders::of(Stress::Calm, roster("R1=receive,S1=send"), 0)
                .driving(&names(&["filing"]))
                .refusal(),
            None
        );
    }
}
