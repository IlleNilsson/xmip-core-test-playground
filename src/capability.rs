//! What a node declares it can do, and nothing inferred from what it is
//! called. ADR-0056.
//!
//! ADR-0056: *a node declares its capabilities, and work is placed on a node
//! whose capabilities satisfy what the work requires.* A name is not a
//! criterion. On 2026-09-19 this rig read a node's stage out of the first
//! letter of its name; the owner: *I know, so why do you break it!* The estate
//! already held the model — ADR-0009, what a node does is its configuration;
//! ADR-0022, placement must satisfy node capability; `deployment-model.md`,
//! *any capable node may resume work if it can satisfy the required
//! capabilities* — and this file is that model restored.
//!
//! Two of ADR-0056's four kinds are modelled here:
//!
//!   - **Feature capability** — which stages of the message path the node can
//!     serve: `receive`, `process`, `send`. A node may declare more than one
//!     (ADR-0052, amended 2026-09-19: a node carries the roles suitable for
//!     its purpose); a node that declares none runs whole tests itself.
//!   - **Online capability** — whether a route off this machine may be
//!     assumed (ADR-0045). The switch `switch.rs` reads and `--online`
//!     carries, folded in here rather than left as a second notion of the
//!     same thing. What `--online` means and how it is published is unchanged.
//!
//! **Authentication capability and runtime capability are not modelled in this
//! rig.** The Playground verifies no credential and isolates no identity
//! context, so either would be a word with nothing behind it. ADR-0056 names
//! all four; this file deliberately carries two.
//!
//! **The words are the node crate's.** `node::Stage::declared` is the one
//! parse of a declared stage list — lowercase exactly, any other word refused —
//! and every reading here goes through it (open problem 25, row i: *`node`
//! parses, `cluster` places*).

use node::Stage;

/// What a node that declares no stage publishes in place of the words.
const NO_STAGE: &str = "no stage of the message path";

/// What one node declared it can do.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Capability {
    /// Online capability: a route off this machine may be assumed.
    online: bool,
    /// Feature capability: the stages of the message path this node serves,
    /// in message-path order, each at most once.
    features: Vec<Stage>,
}

impl Capability {
    /// A node that declares nothing: no stage of the message path, offline.
    #[must_use]
    pub const fn none() -> Self {
        Self {
            online: false,
            features: Vec::new(),
        }
    }

    /// The capability declaring these stages, in message-path order.
    #[must_use]
    pub fn of(stages: &[Stage]) -> Self {
        Self {
            online: false,
            features: Stage::ALL
                .into_iter()
                .filter(|stage| stages.contains(stage))
                .collect(),
        }
    }

    /// The capability a `--can` value declares, read by
    /// [`Stage::declared`]: the stage words, separated by commas or by `+`,
    /// lowercase exactly. Empty declares nothing.
    ///
    /// # Errors
    ///
    /// When a word is no capability: REFUSED, naming the word and the values
    /// it would take (ADR-0055).
    pub fn parse(raw: &str) -> Result<Self, String> {
        Stage::declared(raw).map(|stages| Self::of(&stages))
    }

    /// The same capability with its online capability said (ADR-0045).
    #[must_use]
    pub fn with_online(mut self, online: bool) -> Self {
        self.online = online;
        self
    }

    /// Whether a route off this machine may be assumed.
    #[must_use]
    pub const fn is_online(&self) -> bool {
        self.online
    }

    /// The stages of the message path this node serves, in path order.
    #[must_use]
    pub fn features(&self) -> &[Stage] {
        &self.features
    }

    /// Whether this node can serve `stage`.
    #[must_use]
    pub fn can(&self, stage: Stage) -> bool {
        self.features.contains(&stage)
    }

    /// Whether this node declares no stage of the message path — it runs
    /// whole tests itself, as every node did before capabilities.
    #[must_use]
    pub fn declares_no_stage(&self) -> bool {
        self.features.is_empty()
    }

    /// The feature capability as `--can` takes it: `receive,process`, or the
    /// empty string when none is declared.
    #[must_use]
    pub fn words(&self) -> String {
        self.features
            .iter()
            .map(|stage| stage.name())
            .collect::<Vec<&str>>()
            .join(",")
    }

    /// The word a health record carries for the online capability.
    #[must_use]
    pub const fn word(&self) -> &'static str {
        if self.online { "online" } else { "offline" }
    }

    /// The capability as the node binary's flags: what it may serve, and
    /// whether it may assume the internet.
    #[must_use]
    pub fn flags(&self) -> Vec<String> {
        vec![
            "--can".to_string(),
            self.words(),
            "--online".to_string(),
            self.online.to_string(),
        ]
    }

    /// What the node's own capability record says, so a surface reads a node's
    /// capabilities from the snapshot and never from its name.
    #[must_use]
    pub fn evidence(&self) -> String {
        let declared = if self.declares_no_stage() {
            NO_STAGE.to_string()
        } else {
            self.words()
        };
        format!(
            "declares {declared}; {}; authentication and runtime capability \
             are not modelled in this rig",
            self.word()
        )
    }

    /// The capability an evidence line says, read back by a surface drawing a
    /// node's stages, through the same parse as `--can`. A line that is no
    /// declaration at all declares nothing.
    ///
    /// # Errors
    ///
    /// When the declaration names a word that is no capability: REFUSED, as
    /// [`Stage::declared`] says it — never read as the words that were known.
    pub fn from_evidence(evidence: &str) -> Result<Self, String> {
        let said = evidence
            .strip_prefix("declares ")
            .and_then(|rest| rest.split(';').next())
            .unwrap_or_default();
        let stages = if said == NO_STAGE {
            Vec::new()
        } else {
            Stage::declared(said)?
        };
        Ok(Self::of(&stages).with_online(evidence.contains("; online;")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_node_declares_stages_by_name_and_an_unknown_word_is_refused() {
        let one = Capability::parse("receive").expect("receive is a capability");
        assert_eq!(one.features(), [Stage::Receive]);
        assert!(one.can(Stage::Receive) && !one.can(Stage::Send));
        assert!(!one.declares_no_stage());

        let two = Capability::parse(" send + receive ,, ").expect("both, in path order");
        assert_eq!(two.features(), [Stage::Receive, Stage::Send]);
        assert_eq!(two.words(), "receive,send");
        let cased = Capability::parse("Send + RECEIVE").expect_err("lowercase only");
        assert!(cased.contains("called Send, RECEIVE;"), "{cased}");

        assert_eq!(Capability::parse(""), Ok(Capability::none()));
        assert!(Capability::none().declares_no_stage());
        assert_eq!(Capability::none().words(), "");

        let refusal = Capability::parse("receive,relay").expect_err("relay is no capability");
        assert!(refusal.starts_with("REFUSED"), "{refusal}");
        assert!(
            refusal.contains("relay") && refusal.contains("process"),
            "{refusal}"
        );
    }

    #[test]
    fn the_online_capability_rides_along_and_the_flags_say_both() {
        let capability = Capability::of(&[Stage::Process]).with_online(true);
        assert!(capability.is_online());
        assert_eq!(capability.word(), "online");
        assert_eq!(capability.flags(), ["--can", "process", "--online", "true"]);
        assert_eq!(Capability::none().word(), "offline");
        assert_eq!(
            Capability::none().flags(),
            ["--can", "", "--online", "false"]
        );
    }

    #[test]
    fn what_a_node_publishes_reads_back_as_what_it_declared() {
        for capability in [
            Capability::none(),
            Capability::of(&[Stage::Receive]).with_online(true),
            Capability::of(&[Stage::Receive, Stage::Process, Stage::Send]),
        ] {
            let evidence = capability.evidence();
            assert_eq!(
                Capability::from_evidence(&evidence),
                Ok(capability),
                "{evidence}"
            );
        }
        assert!(
            Capability::of(&[Stage::Send])
                .evidence()
                .contains("not modelled in this rig"),
            "the two kinds this rig leaves out are said, not silent"
        );
        assert_eq!(Capability::from_evidence("alive"), Ok(Capability::none()));
    }

    #[test]
    fn a_published_declaration_reads_by_the_same_rule_as_the_flag() {
        // Lowercase exactly, as `--can` takes it; any other case is refused.
        assert_eq!(
            Capability::from_evidence("declares receive,send; offline; x"),
            Ok(Capability::of(&[Stage::Receive, Stage::Send]))
        );
        let cased = Capability::from_evidence("declares Receive,SEND; offline; x")
            .expect_err("lowercase only");
        assert!(cased.contains("called Receive, SEND;"), "{cased}");
        // An unknown word is refused, never read as the words that were known.
        let refusal = Capability::from_evidence("declares receive,relay; online; x")
            .expect_err("relay is no capability");
        assert!(
            refusal.starts_with("REFUSED") && refusal.contains("relay"),
            "{refusal}"
        );
    }
}
