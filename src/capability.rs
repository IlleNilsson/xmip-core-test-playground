//! What a node declares it can do, as the node binary's flags.
//!
//! The declaration is `node::Capability` (ADR-0056): what a node declares,
//! the evidence it publishes and the entry a run lists it by are written and
//! read there and nowhere else (open problem 25). What stays here is the
//! Playground's own: how the cluster tells a node process what it was started
//! with, `--can` and `--online`, which `bin/node.rs` reads back through
//! `Capability::parse`.

use node::Capability;

/// The capability as the node binary's flags: what it may serve, and whether
/// it may assume the internet.
#[must_use]
pub fn flags(capability: &Capability) -> Vec<String> {
    vec![
        "--can".to_string(),
        capability.words(),
        "--online".to_string(),
        capability.is_online().to_string(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use node::Stage;

    #[test]
    fn the_flags_say_both_capabilities() {
        let capability = Capability::of(&[Stage::Process]).with_online(true);
        assert_eq!(flags(&capability), ["--can", "process", "--online", "true"]);
        assert_eq!(
            flags(&Capability::none()),
            ["--can", "", "--online", "false"]
        );
        assert_eq!(
            Capability::parse(&flags(&capability)[1]).map(|read| read.with_online(true)),
            Ok(capability),
            "the node reads its --can back by the one parse"
        );
    }
}
