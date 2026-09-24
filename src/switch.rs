//! Where the online capability is read from the environment (ADR-0045): the
//! variables, and the rule that turns them into whether one node may assume a
//! route to the internet.
//!
//! The capability itself lives in `capability.rs` — online capability is one
//! of ADR-0056's four kinds, not a notion of its own, and what a node carries
//! is one [`Capability`](node::Capability). This file stayed for
//! the environment it reads.
//!
//! False unless set. Nothing in the estate reaches out at runtime, so at
//! every stress level the default is that no emulated node may; the switch
//! exists so the first test that genuinely needs the internet reads it and
//! stays silent without it, rather than bringing the suite online with it.
//! `XMIP_ONLINE=true` is how an operator or a roll says the world is there.

/// Whether the cluster's node called `name` may assume the internet: online
/// when `XMIP_PLAYGROUND_ONLINE_NODES` names it, else — the variable unset —
/// whatever `XMIP_ONLINE` says for every node.
#[must_use]
pub fn node_is_online(name: &str) -> bool {
    node_online(name, online_nodes().as_deref(), online())
}

/// Whether this process may assume the internet: `XMIP_ONLINE=true`.
#[must_use]
pub fn online() -> bool {
    std::env::var("XMIP_ONLINE").is_ok_and(|raw| parse(&raw) == Some(true))
}

/// The cluster's nodes that may assume the internet, by name:
/// `XMIP_PLAYGROUND_ONLINE_NODES`, comma separated; `None` when unset. Set and
/// empty means none of them.
#[must_use]
pub fn online_nodes() -> Option<Vec<String>> {
    std::env::var("XMIP_PLAYGROUND_ONLINE_NODES")
        .ok()
        .map(|raw| names(&raw))
}

/// A comma-separated list of node names, trimmed, empties dropped.
#[must_use]
pub fn names(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .collect()
}

/// The rule behind [`node_is_online`], with the environment already read: a
/// list names the online nodes, case-insensitively; no list leaves it to `all`.
#[must_use]
pub fn node_online(name: &str, online: Option<&[String]>, all: bool) -> bool {
    online.map_or(all, |online| {
        online.iter().any(|one| one.eq_ignore_ascii_case(name))
    })
}

/// The switch a word means: `true`, `false`, `yes`, `no`, `on`, `off`.
#[must_use]
pub fn parse(raw: &str) -> Option<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "yes" | "on" | "1" => Some(true),
        "false" | "no" | "off" | "0" => Some(false),
        _ => None,
    }
}

/// For a test that needs the internet: `true` when it may run, and when it
/// may not, the line to print in its place so a reader sees it was skipped
/// on purpose, not lost.
#[must_use]
pub fn needs_internet(what: &str) -> bool {
    if online() {
        return true;
    }
    println!("skipped offline: {what} needs the internet; set XMIP_ONLINE=true to run it");
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_switch_is_off_unless_said_and_reads_the_usual_words() {
        assert_eq!(parse("TRUE"), Some(true));
        assert_eq!(parse("off"), Some(false));
        assert_eq!(parse("maybe"), None);
    }

    #[test]
    fn a_list_names_the_online_nodes_and_no_list_defers_to_all() {
        let online = names(" R1, p1 ,, ");
        assert_eq!(online, ["R1", "p1"]);
        assert!(node_online("R1", Some(&online), false));
        assert!(node_online("p1", Some(&online), false));
        assert!(!node_online("S1", Some(&online), true));
        assert!(!node_online("R1", Some(&[]), true));
        assert!(node_online("S1", None, true));
        assert!(!node_online("S1", None, false));
    }

    #[test]
    fn a_test_that_needs_the_internet_reads_the_switch() {
        // The suite runs offline; this proves the gate says so and yields.
        if needs_internet("this very test") {
            assert!(online());
        } else {
            assert!(!online());
        }
    }
}
