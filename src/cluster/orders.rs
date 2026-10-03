//! What a cluster starts its nodes on: who they are and what each declared it
//! can do, how hard they run, the tests they are told, which of them may
//! assume the internet, and for how many rounds.
//!
//! One value rather than seven arguments, so what the cluster process was told
//! at the door (ADR-0055) is handed on to its nodes unchanged and nothing is
//! decided twice — least of all a node's capability, which is declared and
//! never inferred (ADR-0056).

use node::Capability;

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
    /// The cluster these nodes belong to, which each node's image is named
    /// for — `xmip-playground-<cluster>-node-<node>` (ADR-0053, amendment
    /// 2026-09-20). Empty where nobody said, and then nothing is linked and a
    /// node keeps the binary's own name.
    pub cluster: String,
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
            cluster: String::new(),
        }
    }

    /// The same for the cluster called `cluster`, so every node it spawns runs
    /// an image named for it (ADR-0053, amendment 2026-09-20).
    #[must_use]
    pub fn in_cluster(mut self, cluster: &str) -> Self {
        self.cluster = cluster.to_string();
        self
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
    /// all, a node whose name no file can carry, an online node that is no
    /// node of the cluster, or a `RoundTrip` whose roster leaves a capability
    /// of the path undeclared. Asked before a process is spawned (ADR-0055
    /// clause 2).
    #[must_use]
    pub fn refusal(&self) -> Option<String> {
        if self.roster.is_empty() {
            return Some(
                "REFUSED: a cluster supervises nodes and none was named; \
                 --nodes <node>=receiving,<node>=processing,<node>=sending names them."
                    .to_string(),
            );
        }
        let names = self.names();
        // A node's name becomes a file name and the last word of its process
        // name (ADR-0053, amendment 2026-09-20), so its shape is checked
        // before a process is spawned rather than mangled into something that
        // works. Nothing but the shape: no word is reserved, because the name
        // carries a node marker of its own.
        if let Some(refusal) = names.iter().find_map(|name| crate::image::refusal(name)) {
            return Some(refusal);
        }
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
    use crate::support::{path, path_roster, roster_text, test_cluster};
    use node::Stage;

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(ToString::to_string).collect()
    }

    fn roster(text: &str) -> Roster {
        Roster::parse(text).expect("a well-formed roster")
    }

    #[test]
    fn the_online_nodes_are_the_ones_named_and_nobody_else() {
        let cluster = test_cluster();
        let [receiving, processing, sending] = path(&cluster);
        // The receiving node's name in another case than its own.
        let recased = if receiving == receiving.to_uppercase() {
            receiving.to_lowercase()
        } else {
            receiving.to_uppercase()
        };
        let orders = Orders::of(Stress::Calm, roster(&path_roster(&cluster)), 0)
            .with_online(Some(vec![recased]));
        assert!(orders.is_online(receiving), "named, whatever the case");
        assert!(!orders.is_online(processing));
        assert_eq!(orders.capability(receiving).word(), "online");
        assert_eq!(orders.capability(sending).word(), "offline");
        assert!(orders.capability(sending).can(Stage::Send));
        assert_eq!(orders.refusal(), None);
    }

    #[test]
    fn orders_of_nodes_declaring_nothing_refuse_nothing() {
        let cluster = test_cluster();
        let names: Vec<String> = cluster.nodes.iter().map(|node| node.name.clone()).collect();
        let orders = Orders::of(Stress::Realistic, Roster::of(&names), 5);
        assert_eq!(orders.names(), names);
        assert_eq!(orders.rounds, 5);
        assert!(orders.scenarios.is_empty());
        assert!(orders.capability(&names[0]).declares_no_stage());
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

        let cluster = test_cluster();
        let [receiving, processing, sending] = path(&cluster);
        let absent = cluster.absent();
        let stranger = Orders::of(Stress::Calm, roster(&path_roster(&cluster)), 0)
            .with_online(Some(vec![absent.clone()]))
            .refusal()
            .expect("the absent name is no node");
        let nodes = format!("{receiving}, {processing}, {sending}");
        assert!(
            stranger.contains(&absent) && stranger.contains(&nodes),
            "{stranger}"
        );

        let ends = roster_text(&[(receiving, "receiving"), (sending, "sending")]);
        let missing = Orders::of(Stress::Calm, roster(&ends), 0)
            .refusal()
            .expect("nobody processes");
        assert!(
            missing.ends_with("no node declares processing."),
            "{missing}"
        );

        // Not RoundTrip: the roster is nobody's business.
        assert_eq!(
            Orders::of(Stress::Calm, roster(&ends), 0)
                .driving(&names(&["filing"]))
                .refusal(),
            None
        );
    }

    /// A node's name is its image's name (ADR-0053, amendment 2026-09-20), so
    /// one no file can be called is refused before a process is spawned — and
    /// one the tree's own words happen to spell is not, because the name
    /// carries a node marker (the owner, 2026-09-20).
    #[test]
    fn a_node_whose_name_cannot_be_an_image_is_refused_before_anything_spawns() {
        let cluster = test_cluster();
        let [receiving, _, sending] = path(&cluster);
        let unfit = roster_text(&[(receiving, "receiving"), ("9lives", "processing")]);
        let wrong = Orders::of(Stress::Calm, roster(&unfit), 0)
            .refusal()
            .expect("no file is called 9lives here");
        assert!(
            wrong.starts_with("REFUSED") && wrong.contains("file name"),
            "{wrong}"
        );

        // The tree's own words, `roll` and `cluster`, as nodes' names.
        let words = roster_text(&[
            ("roll", "receiving"),
            ("cluster", "processing"),
            (sending, "sending"),
        ]);
        assert_eq!(
            Orders::of(Stress::Calm, roster(&words), 0).refusal(),
            None,
            "a node is a node, whatever it is called"
        );
    }
}
