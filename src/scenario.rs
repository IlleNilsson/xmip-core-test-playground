//! The scenarios a roll and its nodes can drive, by the names
//! `XMIP_PLAYGROUND_SCENARIOS` and the node's `--scenarios` use.
//!
//! One list for both binaries, so the roll and a node cannot disagree on what
//! a name means. A name neither knows is REFUSED, never dropped: until
//! 2026-09-19 the roll said so on stderr and carried on, and a node ran the
//! same two tests whatever was named (the owner: *nodes run the test that was
//! named*).

/// Every scenario, in the order the board lists them.
/// Written out, not built from the constants below: `test/XmipTest.Test.ps1`
/// reads this list as text and holds `Start-XmipTest`'s test names against it.
pub const SCENARIOS: [&str; 7] = [
    "round-trip",
    "low-latency",
    "heavy-load",
    "retention",
    "filing",
    "exclusive-claim",
    "daily-backlog",
];

/// The scenario a cluster's nodes hand from one stage to the next.
pub const ROUND_TRIP: &str = "round-trip";
/// Exclusive pickup over the cluster's shared directory.
pub const EXCLUSIVE_CLAIM: &str = "exclusive-claim";
/// The backlog drained over the cluster's shared directory.
pub const DAILY_BACKLOG: &str = "daily-backlog";

/// The scenarios named in a comma-separated list: empty when the list is
/// absent or names nothing, which means every scenario. Names are trimmed and
/// lowered.
///
/// # Errors
///
/// When a name is not a scenario: the message says REFUSED, names every
/// stranger and lists the scenarios there are.
pub fn chosen(raw: Option<&str>) -> Result<Vec<String>, String> {
    let (known, unknown): (Vec<String>, Vec<String>) = raw
        .unwrap_or_default()
        .split(',')
        .map(|name| name.trim().to_ascii_lowercase())
        .filter(|name| !name.is_empty())
        .partition(|name| SCENARIOS.contains(&name.as_str()));
    if unknown.is_empty() {
        Ok(known)
    } else {
        Err(format!(
            "REFUSED: no scenario is named {}; the scenarios are {}",
            unknown.join(", "),
            SCENARIOS.join(", ")
        ))
    }
}

/// Whether the chosen list drives the named scenario: every one when nothing
/// was chosen.
#[must_use]
pub fn drives(chosen: &[String], scenario: &str) -> bool {
    chosen.is_empty() || chosen.iter().any(|name| name == scenario)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nothing_chosen_drives_every_scenario() {
        for raw in [None, Some(""), Some(" , ")] {
            assert_eq!(chosen(raw), Ok(Vec::new()));
        }
        for scenario in SCENARIOS {
            assert!(drives(&[], scenario));
        }
        for named in [ROUND_TRIP, EXCLUSIVE_CLAIM, DAILY_BACKLOG] {
            assert!(SCENARIOS.contains(&named), "{named} is a scenario");
        }
    }

    #[test]
    fn a_list_drives_only_what_it_names() {
        let picked = chosen(Some(" Round-Trip, heavy-load ")).expect("both are scenarios");
        assert_eq!(picked, ["round-trip", "heavy-load"]);
        assert!(drives(&picked, ROUND_TRIP));
        assert!(drives(&picked, "heavy-load"));
        assert!(!drives(&picked, "low-latency"));
    }

    #[test]
    fn an_unknown_name_is_refused_with_the_names_there_are() {
        let refusal = chosen(Some("round-trip,typo")).expect_err("typo is no scenario");
        assert!(refusal.starts_with("REFUSED"), "{refusal}");
        assert!(refusal.contains("typo"), "{refusal}");
        assert!(refusal.contains("daily-backlog"), "{refusal}");
    }
}
