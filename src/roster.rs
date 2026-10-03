//! Every node of a cluster and the roles each declared: the roster.
//!
//! The owner's shape, 2026-09-19: *I would like to see cluster, nodes,
//! receive, process, send.* Which node serves which stage comes from the
//! roles the node **declared** (`node::Capability`, ADR-0056, amendment
//! 2026-10-01), never from what it is called. A node whose roles serve no
//! stage runs whole tests itself, as every node did before capabilities.
//!
//! The roster is the same list in the roll, in the cluster and in every node,
//! so each decides alone and all agree: which pairs of the matrix a receiving
//! node takes (its index modulo the count of nodes that can receive), and
//! which node a pair is handed to next (a stable hash of the stage and the
//! pair over the nodes serving it — or the handing node itself when it is
//! executing, whose Journey takes no process hop).
//!
//! A roster is written as the `--nodes` flag takes it: `name`, or
//! `name=role`, or `name=role+role`, comma separated —
//! `<node>=receiving,<node>=processing,<node>=sending`, or `<node>,<node>`
//! where neither declares a role.

use node::{Capability, NodeRole, Stage};

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

    /// The roster a `--nodes` value says: `name[=role[+role]]`, comma
    /// separated.
    ///
    /// # Errors
    ///
    /// When a word is no role, as [`Capability::parse`] refuses it.
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

    /// The roster a cluster's `xmip.toml` declares: the nodes under its
    /// `[nodes]`, in the order of their names, each declaring the roles its
    /// `roles` says (`configure::cluster::roles`) and nothing where it says
    /// none. The one place a roll's nodes come from (ADR-0056, amendment
    /// 2026-10-03: names are configuration).
    ///
    /// # Errors
    ///
    /// When the text is not a cluster's file, or a word is no role, as
    /// [`Capability::parse`] refuses it.
    pub fn of_cluster(cluster: &str) -> Result<Self, String> {
        let entries: Vec<String> = configure::cluster::roles(cluster)?
            .into_iter()
            .map(|(name, roles)| {
                if roles.is_empty() {
                    name
                } else {
                    format!("{name}={}", roles.join("+"))
                }
            })
            .collect();
        Self::parse(&entries.join(","))
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
    /// how a node makes its own `--role` the last word on itself.
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

    /// Whether these nodes cover the whole message path — every stage
    /// declared somewhere, so `RoundTrip` runs between the node processes.
    #[must_use]
    pub fn covers_the_path(&self) -> bool {
        Stage::ALL
            .into_iter()
            .all(|stage| !self.with(stage).is_empty())
    }

    /// What the roster is, in one line: how many nodes, and how many of them
    /// serve each stage — or that none declares a stage and the roll runs
    /// whole tests itself. The roll's first line, and what a surface says an
    /// operator got.
    #[must_use]
    pub fn describe(&self) -> String {
        let count = self.nodes.len();
        if count == 0 {
            return "no nodes".to_string();
        }
        let many = if count == 1 { "node" } else { "nodes" };
        if !self.serves_any_stage() {
            return format!("{count} {many}, none serving a stage: whole tests in each");
        }
        let dealt: Vec<String> = Stage::ALL
            .into_iter()
            .map(|stage| {
                let role = NodeRole::serving(stage);
                format!("{} {}", self.with(stage).len(), role.name())
            })
            .collect();
        format!("{count} {many}, {}", dealt.join(", "))
    }

    /// Why `RoundTrip` cannot run across these nodes, if it cannot: the path
    /// needs receiving, processing and sending declared somewhere — executing
    /// is all three — and the message names the role nobody declared — not a
    /// letter, because a name is no criterion (ADR-0056). `None` when no node
    /// serves a stage at all, or every stage is covered.
    #[must_use]
    pub fn refusal(&self) -> Option<String> {
        if !self.serves_any_stage() {
            return None;
        }
        let missing: Vec<&str> = Stage::ALL
            .into_iter()
            .filter(|stage| self.with(*stage).is_empty())
            .map(|stage| NodeRole::serving(stage).name())
            .collect();
        if missing.is_empty() {
            return None;
        }
        Some(format!(
            "REFUSED. RoundTrip across nodes needs the receiving, processing and sending \
             roles declared, or executing; no node declares {}.",
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

    /// The node `from` hands a pair on to for `stage`: `from` itself when it
    /// is executing, so its Journey takes no process hop (ADR-0018 clause
    /// 10a; the owner, 2026-10-01: *Executing would be used for Low
    /// Latency*); otherwise a stable hash of the stage and the pair over the
    /// nodes serving it, so every sender picks the same one.
    #[must_use]
    pub fn target<'a>(
        &'a self,
        from: &'a str,
        stage: Stage,
        transport: &str,
        contract: Contract,
    ) -> Option<&'a str> {
        if self.capability(from).executes() {
            return Some(from);
        }
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
    use crate::support::test_cluster;

    fn roster(text: &str) -> Roster {
        Roster::parse(text).expect("a well-formed roster")
    }

    /// `N` names that say nothing: the test cluster's nodes', in the order
    /// of their names, and past its last each again with a number.
    fn names<const N: usize>() -> [String; N] {
        let cluster = test_cluster();
        let count = cluster.nodes.len();
        std::array::from_fn(|place| {
            let name = &cluster.nodes[place % count].name;
            match place / count {
                0 => name.clone(),
                again => format!("{name}-{again}"),
            }
        })
    }

    #[test]
    fn a_node_serves_what_it_declared_and_a_name_decides_nothing() {
        let [a, b, c, bare] = names();
        let declared = roster(&format!("{a}=receiving,{b}=processing,{c}=sending,{bare}"));
        assert_eq!(declared.with(Stage::Receive), [a.as_str()]);
        assert_eq!(declared.with(Stage::Send), [c.as_str()]);
        assert!(declared.capability(&bare).declares_no_stage());
        assert_eq!(declared.names(), [&a, &b, &c, &bare].map(String::as_str));

        assert_eq!(declared.refusal(), None);

        // And neither a name nor what its node declares in the cluster's
        // file decides anything here: each node serves what the roster
        // starts it with, which is not what the test cluster gives it.
        let cluster = test_cluster();
        let receiver = cluster.with_role("receiving").name.as_str();
        let processor = cluster.with_role("processing").name.as_str();
        let sender = cluster.with_role("sending").name.as_str();
        let misleading = roster(&format!(
            "{receiver}=sending,{processor},{sender}=receiving+processing"
        ));
        assert_eq!(misleading.with(Stage::Receive), [sender]);
        assert_eq!(misleading.with(Stage::Process), [sender]);
        assert_eq!(misleading.with(Stage::Send), [receiver]);
        assert!(misleading.capability(processor).declares_no_stage());
        assert_eq!(misleading.refusal(), None);
    }

    #[test]
    fn a_missing_capability_is_refused_by_capability_and_no_stage_is_no_refusal() {
        let [a, b, c, bare] = names();
        assert_eq!(Roster::of(std::slice::from_ref(&bare)).refusal(), None);
        assert_eq!(
            roster(&format!("{a}=receiving,{b}=processing,{c}=sending,{bare}")).refusal(),
            None
        );
        let refusal = roster(&format!("{a}=receiving,{b}=receiving,{c}=sending"))
            .refusal()
            .expect("nobody processes");
        assert!(refusal.starts_with("REFUSED"), "{refusal}");
        assert!(
            refusal.ends_with("no node declares processing."),
            "{refusal}"
        );
        let refusal = roster(&format!("{a}=receiving"))
            .refusal()
            .expect("two missing");
        assert!(
            refusal.ends_with("declares processing or sending."),
            "{refusal}"
        );
        assert_eq!(
            roster(&format!("{a}=executing,{b}=receiving")).refusal(),
            None
        );
    }

    #[test]
    fn a_roster_is_written_read_back_and_a_node_has_the_last_word_on_itself() {
        let [a, b, c, bare] = names();
        let text = format!("{a}=receiving,{b}=processing+sending,{bare}");
        assert_eq!(roster(&text).text(), text);
        let two = [a.clone(), b.clone()];
        assert_eq!(Roster::of(&two).text(), format!("{a},{b}"));

        assert!(Roster::parse(&format!("{a}=relay")).is_err());
        assert!(Roster::parse(&format!("{c}=relay")).is_err());

        let own = roster(&format!("{a},{b}=sending"))
            .declared(&a, Capability::parse("receiving").expect("receiving"));
        assert_eq!(own.with(Stage::Receive), [a.as_str()]);
        assert_eq!(
            own.names(),
            [a.as_str(), b.as_str()],
            "no node is added twice"
        );
        assert!(Roster::default().is_empty());
    }

    #[test]
    fn every_pair_has_one_receiver_and_one_stable_target_per_stage() {
        let [in_a, in_b, process_a, process_b, out_a, out_b, bare] = names();
        let roster = roster(&format!(
            "{in_a}=receiving,{in_b}=receiving,{process_a}=processing,\
             {process_b}=processing,{out_a}=sending,{out_b}=sending,{bare}"
        ));
        for index in 0..40 {
            let takers = [&in_a, &in_b, &process_a, &bare]
                .into_iter()
                .filter(|name| roster.receives(name, index))
                .count();
            assert_eq!(takers, 1, "pair {index} has exactly one receiver");
        }
        let mut seen = std::collections::BTreeSet::new();
        for contract in CONTRACTS {
            let first = roster.target(&in_a, Stage::Process, "tcp", contract);
            assert_eq!(first, roster.target(&in_b, Stage::Process, "tcp", contract));
            assert!(first.is_some_and(|name| roster.capability(name).can(Stage::Process)));
            seen.extend(first);
        }
        assert_eq!(seen.len(), 2, "the pairs spread over both process nodes");
        let reaches = |sender: &str| {
            CONTRACTS.into_iter().any(|contract| {
                let process = roster.target(&in_a, Stage::Process, "tcp", contract);
                let send = roster.target(&process_a, Stage::Send, "tcp", contract);
                process == Some(process_a.as_str()) && send == Some(sender)
            })
        };
        assert!(
            reaches(&out_a) && reaches(&out_b),
            "a pair's process node does not decide its send node"
        );
        assert_eq!(
            Roster::default().target(&in_a, Stage::Send, "tcp", Contract::Json),
            None
        );
    }

    #[test]
    fn an_executing_node_keeps_its_journey_and_the_rest_hand_on() {
        let [e, r, p, s] = names();
        let roster = roster(&format!(
            "{e}=executing,{r}=receiving,{p}=processing,{s}=sending"
        ));
        for contract in CONTRACTS {
            for stage in [Stage::Process, Stage::Send] {
                assert_eq!(roster.target(&e, stage, "tcp", contract), Some(e.as_str()));
            }
            let next = roster.target(&r, Stage::Process, "tcp", contract);
            assert!(next.is_some_and(|name| roster.capability(name).can(Stage::Process)));
        }
        assert_eq!(roster.with(Stage::Receive), [e.as_str(), r.as_str()]);
    }

    #[test]
    fn the_roster_is_the_cluster_file_s_nodes_with_the_roles_each_declares() {
        let cluster = test_cluster();
        let read = Roster::of_cluster(&cluster.text).expect("the test cluster reads");
        let every: Vec<&str> = cluster
            .nodes
            .iter()
            .map(|node| node.name.as_str())
            .collect();
        assert_eq!(read.names(), every);
        for node in &cluster.nodes {
            let declared = node.roles.join(",");
            let expected = Capability::parse(&declared).expect("the test cluster's roles");
            assert_eq!(read.capability(&node.name), expected, "{}", node.name);
        }
        assert!(read.covers_the_path());

        let [a, b] = names();
        let bare = Roster::of_cluster(&format!("[nodes.{a}]\n\n[nodes.{b}]\n")).expect("reads");
        assert!(!bare.serves_any_stage() && !bare.covers_the_path());
        assert_eq!(bare.refusal(), None);
        let unknown = format!("[nodes.{a}]\nroles = \"relay\"\n");
        assert!(Roster::of_cluster(&unknown).is_err(), "no role is no role");
        assert!(
            Roster::of_cluster("[service]\n")
                .expect("no nodes")
                .is_empty()
        );
    }

    #[test]
    fn a_roster_says_how_many_nodes_and_what_each_stage_has() {
        let [r, p, s, bare] = names();
        let path = roster(&format!("{r}=receiving,{p}=processing,{s}=sending,{bare}"));
        assert_eq!(
            path.describe(),
            "4 nodes, 1 receiving, 1 processing, 1 sending"
        );
        assert_eq!(
            roster(&r).describe(),
            "1 node, none serving a stage: whole tests in each"
        );
        assert_eq!(Roster::default().describe(), "no nodes");
    }
}
