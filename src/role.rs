//! The role a node's name gives it, and the roster of a cluster's nodes.
//!
//! The owner's shape, 2026-09-19: *I would like to see cluster, nodes,
//! receive, process, send.* The letter is the role — a node whose name starts
//! with `R` receives, `P` processes, `S` sends, in either case, the rest of
//! the name letters and digits (`R1`, `p2`, `Send3`). Any other name
//! (`node-01`, `left`) has no role and runs whole tests itself, as every node
//! did before roles.
//!
//! The [`Roster`] is every node of the cluster by role, the same list in the
//! roll and in every node, so each decides alone and all agree: which pairs of
//! the matrix an `R` node receives (its index modulo the `R` count), and which
//! `P` or `S` node a pair is handed to (a stable hash of the pair over the
//! nodes of that role).

use crate::verdict::{Contract, Stage};

/// What a node does on the message path, from its name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Role {
    /// Takes the Stream in through the transport and hands it on.
    Receive,
    /// Holds the content contract over what was received and hands it on.
    Process,
    /// Sends out through the transport and closes the verdict.
    Send,
    /// No role: the node runs whole tests itself.
    Whole,
}

impl Role {
    /// The three roles of the message path, in its order.
    pub const PATH: [Role; 3] = [Role::Receive, Role::Process, Role::Send];

    /// The role `name` gives a node: by its first letter when the whole name
    /// is letters and digits, else none.
    #[must_use]
    pub fn of(name: &str) -> Self {
        if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric()) {
            return Self::Whole;
        }
        match name.chars().next().map(|c| c.to_ascii_uppercase()) {
            Some('R') => Self::Receive,
            Some('P') => Self::Process,
            Some('S') => Self::Send,
            _ => Self::Whole,
        }
    }

    /// The letter a name starts with to have this role; empty for none.
    #[must_use]
    pub const fn letter(self) -> &'static str {
        match self {
            Self::Receive => "R",
            Self::Process => "P",
            Self::Send => "S",
            Self::Whole => "",
        }
    }

    /// The stage of the message path this role runs, if it has one.
    #[must_use]
    pub const fn stage(self) -> Option<Stage> {
        match self {
            Self::Receive => Some(Stage::Receive),
            Self::Process => Some(Stage::Process),
            Self::Send => Some(Stage::Send),
            Self::Whole => None,
        }
    }

    /// The role a handoff goes to next: `R` to `P`, `P` to `S`.
    #[must_use]
    pub const fn next(self) -> Option<Self> {
        match self {
            Self::Receive => Some(Self::Process),
            Self::Process => Some(Self::Send),
            Self::Send | Self::Whole => None,
        }
    }
}

/// Every node of a cluster, by role, in the order they were named.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Roster {
    names: Vec<String>,
}

impl Roster {
    /// The roster of the nodes named.
    #[must_use]
    pub fn of(names: &[String]) -> Self {
        Self {
            names: names.to_vec(),
        }
    }

    /// The nodes with `role`, in the order they were named.
    #[must_use]
    pub fn with(&self, role: Role) -> Vec<&str> {
        self.names
            .iter()
            .map(String::as_str)
            .filter(|name| Role::of(name) == role)
            .collect()
    }

    /// Whether any node has a role on the message path.
    #[must_use]
    pub fn has_roles(&self) -> bool {
        self.names.iter().any(|name| Role::of(name) != Role::Whole)
    }

    /// Why `RoundTrip` cannot run over these role nodes, if it cannot: the
    /// path needs all three roles, and the message names the ones missing.
    /// `None` when there are no role nodes at all, or every role is there.
    #[must_use]
    pub fn refusal(&self) -> Option<String> {
        if !self.has_roles() {
            return None;
        }
        let missing: Vec<&str> = Role::PATH
            .into_iter()
            .filter(|role| self.with(*role).is_empty())
            .map(Role::letter)
            .collect();
        if missing.is_empty() {
            return None;
        }
        Some(format!(
            "REFUSED. RoundTrip over role nodes needs at least one R, one P and one S node; \
             the nodes name no {}.",
            missing.join(" and no ")
        ))
    }

    /// Whether the `R` node `name` receives the pair at `index` of the
    /// matrix: a stable partition, the index modulo the `R` count.
    #[must_use]
    pub fn receives(&self, name: &str, index: usize) -> bool {
        let receivers = self.with(Role::Receive);
        receivers
            .iter()
            .position(|one| *one == name)
            .is_some_and(|at| index % receivers.len() == at)
    }

    /// The node of `role` a pair is handed to: a stable hash of the role and
    /// the pair over the nodes of that role, so every sender picks the same
    /// one.
    #[must_use]
    pub fn target(&self, role: Role, transport: &str, contract: Contract) -> Option<&str> {
        let nodes = self.with(role);
        if nodes.is_empty() {
            return None;
        }
        // Salted with the role, so a pair's index at one role says nothing
        // of its index at the next.
        let hash = fnv1a(&format!(
            "{}/{transport}/{}",
            role.letter(),
            contract.name()
        ));
        let at = usize::try_from(hash % nodes.len() as u64).unwrap_or(0);
        Some(nodes[at])
    }
}

/// FNV-1a over a string, then mixed: stable across processes, platforms and
/// builds, which `std`'s hasher does not promise. The mix matters — FNV's
/// lowest bit is only the parity of the bytes', so unmixed and taken modulo
/// two, a pair's `P` node decided its `S` node (seen 2026-09-19).
fn fnv1a(text: &str) -> u64 {
    let hash = text.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
    });
    let mixed = (hash ^ (hash >> 33)).wrapping_mul(0xff51_afd7_ed55_8ccd);
    mixed ^ (mixed >> 33)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schedule::CONTRACTS;

    fn roster(names: &[&str]) -> Roster {
        Roster::of(&names.iter().map(ToString::to_string).collect::<Vec<_>>())
    }

    #[test]
    fn the_letter_is_the_role_and_other_names_have_none() {
        for (name, role) in [
            ("R1", Role::Receive),
            ("r2", Role::Receive),
            ("P1", Role::Process),
            ("Send3", Role::Send),
            ("node-01", Role::Whole),
            ("left", Role::Whole),
            ("R-1", Role::Whole),
            ("", Role::Whole),
        ] {
            assert_eq!(Role::of(name), role, "{name}");
        }
    }

    #[test]
    fn a_missing_role_is_refused_by_name_and_no_roles_is_no_refusal() {
        assert_eq!(roster(&["node-01", "node-02"]).refusal(), None);
        assert_eq!(roster(&["R1", "P1", "S1", "node-01"]).refusal(), None);
        let refusal = roster(&["R1", "R2", "S1"]).refusal().expect("no P");
        assert!(refusal.starts_with("REFUSED"), "{refusal}");
        assert!(refusal.ends_with("name no P."), "{refusal}");
        let refusal = roster(&["R1"]).refusal().expect("no P, no S");
        assert!(refusal.ends_with("name no P and no S."), "{refusal}");
    }

    #[test]
    fn every_pair_has_one_receiver_and_one_stable_target_per_role() {
        let roster = roster(&["R1", "R2", "P1", "P2", "S1", "S2", "node-01"]);
        for index in 0..40 {
            let takers = ["R1", "R2", "P1", "node-01"]
                .into_iter()
                .filter(|name| roster.receives(name, index))
                .count();
            assert_eq!(takers, 1, "pair {index} has exactly one receiver");
        }
        let mut seen = std::collections::BTreeSet::new();
        for contract in CONTRACTS {
            let first = roster.target(Role::Process, "tcp", contract);
            assert_eq!(first, roster.target(Role::Process, "tcp", contract));
            assert!(first.is_some_and(|name| Role::of(name) == Role::Process));
            seen.extend(first);
        }
        assert_eq!(seen.len(), 2, "the pairs spread over both P nodes");
        let crossed = CONTRACTS.into_iter().any(|contract| {
            let process = roster.target(Role::Process, "tcp", contract);
            let send = roster.target(Role::Send, "tcp", contract);
            process == Some("P1") && send == Some("S2")
        });
        let straight = CONTRACTS.into_iter().any(|contract| {
            let process = roster.target(Role::Process, "tcp", contract);
            let send = roster.target(Role::Send, "tcp", contract);
            process == Some("P1") && send == Some("S1")
        });
        assert!(
            crossed && straight,
            "a pair's P node does not decide its S node"
        );
        assert_eq!(
            roster.target(Role::Whole, "tcp", Contract::Json),
            Some("node-01")
        );
        assert_eq!(
            Roster::default().target(Role::Send, "tcp", Contract::Json),
            None
        );
    }
}
