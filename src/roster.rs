//! Every node of a cluster and the capability each declared: the roster.
//!
//! The owner's shape, 2026-09-19: *I would like to see cluster, nodes,
//! receive, process, send.* Which node serves which stage comes from what the
//! node **declared** (`node::Capability`, ADR-0056), never from what it is
//! called. A node that declares no stage runs whole tests itself, as every
//! node did before capabilities.
//!
//! The roster is the same list in the roll, in the cluster and in every node,
//! so each decides alone and all agree: which pairs of the matrix a receiving
//! node takes (its index modulo the count of nodes that can receive), and
//! which node a pair is handed to next (a stable hash of the stage and the
//! pair over the nodes that declared it).
//!
//! A roster is written as the `--nodes` flag takes it: `name`, or
//! `name=capability`, or `name=capability+capability`, comma separated —
//! `R1=receive,P1=process,S1=send`, or `node-01,node-02` where neither
//! declares a stage.

use node::{Capability, Stage};

use crate::verdict::Contract;

/// Every node of a cluster, in the order they were named, with what each
/// declared it can do.
///
/// A node's online capability is only as good as what the builder of the
/// roster knew: a cluster fills it in per node, and a roster parsed from
/// `--nodes` leaves every node offline, which is ADR-0045's default anyway.
/// Nothing on the message path reads another node's online capability.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Roster {
    nodes: Vec<(String, Capability)>,
}

impl Roster {
    /// The roster of the nodes named, none of them declaring a stage.
    #[must_use]
    pub fn of(names: &[String]) -> Self {
        Self {
            nodes: names
                .iter()
                .map(|name| (name.clone(), Capability::none()))
                .collect(),
        }
    }

    /// The roster a `--nodes` value says: `name[=capability[+capability]]`,
    /// comma separated.
    ///
    /// # Errors
    ///
    /// When a word is no capability, as [`Capability::parse`] refuses it.
    pub fn parse(raw: &str) -> Result<Self, String> {
        let mut nodes = Vec::new();
        for entry in raw.split(',').map(str::trim) {
            if entry.is_empty() {
                continue;
            }
            let (name, capability) = Capability::from_entry(entry);
            nodes.push((name.to_string(), capability?));
        }
        Ok(Self { nodes })
    }

    /// The nodes named, each declaring what `declared` gives it — the same
    /// `name=capability` list, from which a node it does not name declares
    /// nothing.
    ///
    /// # Errors
    ///
    /// When a word is no capability, or when `declared` names a node that is
    /// no node of the cluster (ADR-0055: both sides named).
    pub fn declaring(names: &[String], declared: &str) -> Result<Self, String> {
        let said = Self::parse(declared)?;
        let strangers: Vec<&str> = said
            .names()
            .into_iter()
            .filter(|one| !names.iter().any(|name| name.eq_ignore_ascii_case(one)))
            .collect();
        if !strangers.is_empty() {
            return Err(format!(
                "REFUSED: a capability was given to {}, which is no node of this cluster; \
                 the nodes are {}.",
                strangers.join(", "),
                names.join(", ")
            ));
        }
        Ok(Self {
            nodes: names
                .iter()
                .map(|name| (name.clone(), said.capability(name)))
                .collect(),
        })
    }

    /// The roster as `--nodes` takes it back.
    #[must_use]
    pub fn text(&self) -> String {
        self.nodes
            .iter()
            .map(|(name, capability)| capability.entry(name))
            .collect::<Vec<String>>()
            .join(",")
    }

    /// Every node's name, in the order they were named.
    #[must_use]
    pub fn names(&self) -> Vec<&str> {
        self.nodes.iter().map(|(name, _)| name.as_str()).collect()
    }

    /// Whether no node was named at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// What the node called `name` declared; nothing when it is no node here.
    #[must_use]
    pub fn capability(&self, name: &str) -> Capability {
        self.nodes
            .iter()
            .find(|(one, _)| one.eq_ignore_ascii_case(name))
            .map_or_else(Capability::none, |(_, capability)| capability.clone())
    }

    /// The same roster with `name`'s declaration replaced by `capability` —
    /// how a node makes its own `--can` the last word on itself.
    #[must_use]
    pub fn declared(mut self, name: &str, capability: Capability) -> Self {
        match self
            .nodes
            .iter_mut()
            .find(|(one, _)| one.eq_ignore_ascii_case(name))
        {
            Some(entry) => entry.1 = capability,
            None => self.nodes.push((name.to_string(), capability)),
        }
        self
    }

    /// The nodes that declared they can serve `stage`, in the order they were
    /// named.
    #[must_use]
    pub fn with(&self, stage: Stage) -> Vec<&str> {
        self.nodes
            .iter()
            .filter(|(_, capability)| capability.can(stage))
            .map(|(name, _)| name.as_str())
            .collect()
    }

    /// Whether any node declared a stage of the message path.
    #[must_use]
    pub fn serves_any_stage(&self) -> bool {
        self.nodes
            .iter()
            .any(|(_, capability)| !capability.declares_no_stage())
    }

    /// Why `RoundTrip` cannot run across these nodes, if it cannot: the path
    /// needs receive, process and send declared somewhere, and the message
    /// names the capability nobody declared — not a letter, because a name is
    /// no criterion (ADR-0056). `None` when no node declares a stage at all,
    /// or every stage is covered.
    #[must_use]
    pub fn refusal(&self) -> Option<String> {
        if !self.serves_any_stage() {
            return None;
        }
        let missing: Vec<&str> = Stage::ALL
            .into_iter()
            .filter(|stage| self.with(*stage).is_empty())
            .map(Stage::name)
            .collect();
        if missing.is_empty() {
            return None;
        }
        Some(format!(
            "REFUSED. RoundTrip across nodes needs the receive, process and send capability \
             declared; no node declares {}.",
            missing.join(" or ")
        ))
    }

    /// Whether the receiving node `name` takes the pair at `index` of the
    /// matrix: a stable partition, the index modulo the count that receive.
    #[must_use]
    pub fn receives(&self, name: &str, index: usize) -> bool {
        let receivers = self.with(Stage::Receive);
        receivers
            .iter()
            .position(|one| *one == name)
            .is_some_and(|at| index % receivers.len() == at)
    }

    /// The node a pair is handed to for `stage`: a stable hash of the stage
    /// and the pair over the nodes that declared it, so every sender picks the
    /// same one.
    #[must_use]
    pub fn target(&self, stage: Stage, transport: &str, contract: Contract) -> Option<&str> {
        let nodes = self.with(stage);
        if nodes.is_empty() {
            return None;
        }
        // Salted with the stage, so a pair's index at one stage says nothing
        // of its index at the next.
        let hash = fnv1a(&format!("{}/{transport}/{}", stage.name(), contract.name()));
        let at = usize::try_from(hash % nodes.len() as u64).unwrap_or(0);
        Some(nodes[at])
    }
}

/// FNV-1a over a string, then mixed: stable across processes, platforms and
/// builds, which `std`'s hasher does not promise. The mix matters — FNV's
/// lowest bit is only the parity of the bytes', so unmixed and taken modulo
/// two, a pair's process node decided its send node (seen 2026-09-19).
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

    fn roster(text: &str) -> Roster {
        Roster::parse(text).expect("a well-formed roster")
    }

    #[test]
    fn a_node_serves_what_it_declared_and_a_name_decides_nothing() {
        let declared = roster("R1=receive,P1=process,S1=send,node-01");
        assert_eq!(declared.with(Stage::Receive), ["R1"]);
        assert_eq!(declared.with(Stage::Send), ["S1"]);
        assert!(declared.capability("node-01").declares_no_stage());
        assert_eq!(declared.names(), ["R1", "P1", "S1", "node-01"]);

        // The same three stages under names that say nothing at all.
        let anonymous = roster("alpha=receive,beta=process,gamma=send");
        assert_eq!(anonymous.with(Stage::Receive), ["alpha"]);
        assert_eq!(anonymous.with(Stage::Process), ["beta"]);
        assert_eq!(anonymous.with(Stage::Send), ["gamma"]);
        assert_eq!(anonymous.refusal(), None);

        // And a name that looks like a role decides nothing on its own.
        let misleading = roster("R1=send,P1,S1=receive+process");
        assert_eq!(misleading.with(Stage::Receive), ["S1"]);
        assert_eq!(misleading.with(Stage::Process), ["S1"]);
        assert_eq!(misleading.with(Stage::Send), ["R1"]);
        assert!(misleading.capability("P1").declares_no_stage());
        assert_eq!(misleading.refusal(), None);
    }

    #[test]
    fn a_missing_capability_is_refused_by_capability_and_no_stage_is_no_refusal() {
        assert_eq!(Roster::of(&["node-01".to_string()]).refusal(), None);
        assert_eq!(roster("R1=receive,P1=process,S1=send,x").refusal(), None);
        let refusal = roster("R1=receive,R2=receive,S1=send")
            .refusal()
            .expect("nobody processes");
        assert!(refusal.starts_with("REFUSED"), "{refusal}");
        assert!(refusal.ends_with("no node declares process."), "{refusal}");
        let refusal = roster("R1=receive").refusal().expect("two missing");
        assert!(refusal.ends_with("declares process or send."), "{refusal}");
    }

    #[test]
    fn a_roster_is_written_read_back_and_a_node_has_the_last_word_on_itself() {
        let text = "R1=receive,P1=process+send,node-01";
        assert_eq!(roster(text).text(), text);
        assert_eq!(Roster::of(&["a".to_string(), "b".into()]).text(), "a,b");

        let names = ["alpha".to_string(), "beta".into()];
        let declaring = Roster::declaring(&names, "alpha=receive").expect("alpha is a node");
        assert_eq!(declaring.with(Stage::Receive), ["alpha"]);
        assert!(declaring.capability("beta").declares_no_stage());

        let stranger = Roster::declaring(&names, "gamma=send").expect_err("gamma is no node");
        assert!(
            stranger.contains("gamma") && stranger.contains("alpha, beta"),
            "{stranger}"
        );
        assert!(Roster::parse("alpha=relay").is_err());

        let own = roster("alpha,beta=send")
            .declared("alpha", Capability::parse("receive").expect("receive"));
        assert_eq!(own.with(Stage::Receive), ["alpha"]);
        assert_eq!(own.names(), ["alpha", "beta"], "no node is added twice");
        assert!(Roster::default().is_empty());
    }

    #[test]
    fn every_pair_has_one_receiver_and_one_stable_target_per_stage() {
        let roster = roster("R1=receive,R2=receive,P1=process,P2=process,S1=send,S2=send,n1");
        for index in 0..40 {
            let takers = ["R1", "R2", "P1", "n1"]
                .into_iter()
                .filter(|name| roster.receives(name, index))
                .count();
            assert_eq!(takers, 1, "pair {index} has exactly one receiver");
        }
        let mut seen = std::collections::BTreeSet::new();
        for contract in CONTRACTS {
            let first = roster.target(Stage::Process, "tcp", contract);
            assert_eq!(first, roster.target(Stage::Process, "tcp", contract));
            assert!(first.is_some_and(|name| roster.capability(name).can(Stage::Process)));
            seen.extend(first);
        }
        assert_eq!(seen.len(), 2, "the pairs spread over both process nodes");
        let crossed = CONTRACTS.into_iter().any(|contract| {
            let process = roster.target(Stage::Process, "tcp", contract);
            let send = roster.target(Stage::Send, "tcp", contract);
            process == Some("P1") && send == Some("S2")
        });
        let straight = CONTRACTS.into_iter().any(|contract| {
            let process = roster.target(Stage::Process, "tcp", contract);
            let send = roster.target(Stage::Send, "tcp", contract);
            process == Some("P1") && send == Some("S1")
        });
        assert!(
            crossed && straight,
            "a pair's process node does not decide its send node"
        );
        assert_eq!(
            Roster::default().target(Stage::Send, "tcp", Contract::Json),
            None
        );
    }
}
