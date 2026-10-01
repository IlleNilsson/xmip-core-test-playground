//! What a node declares, as the node binary's flags.
//!
//! The declaration is `node::Capability` (ADR-0056): the node's roles, the
//! evidence it publishes and the entry a run lists it by are written and
//! read there and nowhere else (open problem 25). What stays here is the
//! Playground's own: how the cluster tells a node process what it was started
//! with, `--role` and `--online`, which `bin/node.rs` reads back through
//! `Capability::parse`.

use node::Capability;

/// The declaration as the node binary's flags: its roles, and whether it may
/// assume the internet.
#[must_use]
pub fn flags(capability: &Capability) -> Vec<String> {
    vec![
        "--role".to_string(),
        capability.words(),
        "--online".to_string(),
        capability.is_online().to_string(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use node::NodeRole;

    #[test]
    fn the_flags_say_the_roles_and_the_online_capability() {
        let capability = Capability::of(&[NodeRole::Processing]).with_online(true);
        assert_eq!(
            flags(&capability),
            ["--role", "processing", "--online", "true"]
        );
        assert_eq!(
            flags(&Capability::none()),
            ["--role", "", "--online", "false"]
        );
        assert_eq!(
            Capability::parse(&flags(&capability)[1]).map(|read| read.with_online(true)),
            Ok(capability),
            "the node reads its --role back by the one parse"
        );
    }
}
