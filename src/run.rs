//! What a run was started with, published beside the snapshot under `[run]`.
//!
//! The owner, 2026-09-19: the run says what it was started with. A surface
//! showed a cluster and its leaves and nothing of which tests were named,
//! which nodes, which of them online, or how hard — so a board could not be
//! told from the one before it. The roll writes this once per publication; a
//! reader that does not know the table skips it.

use serde::{Deserialize, Serialize};

use crate::scenario::SCENARIOS;
use crate::stress::Stress;
use crate::switch::Switches;

/// The choices behind a roll, in the words `Start-XmipTest` takes them.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Run {
    /// The cluster's name.
    pub cluster: String,
    /// The tests that run, by the names a person asks for them; every one
    /// when none was named.
    pub tests: Vec<String>,
    /// The nodes spawned, one process each; empty when there are none.
    pub nodes: Vec<String>,
    /// The nodes among them that may assume the internet (ADR-0045).
    pub online: Vec<String>,
    /// The stress level's name.
    pub stress: String,
}

impl Run {
    /// The run of `cluster` driving the `chosen` scenarios (none chosen is
    /// every one) over `nodes` at `stress`; which nodes are online is read
    /// the way the nodes themselves are told.
    #[must_use]
    pub fn of(cluster: &str, chosen: &[String], nodes: &[String], stress: Stress) -> Self {
        let scenarios: Vec<&str> = if chosen.is_empty() {
            SCENARIOS.to_vec()
        } else {
            chosen.iter().map(String::as_str).collect()
        };
        Self {
            cluster: cluster.to_string(),
            tests: scenarios.into_iter().map(test_name).collect(),
            nodes: nodes.to_vec(),
            online: nodes
                .iter()
                .filter(|name| Switches::for_node(name).online)
                .cloned()
                .collect(),
            stress: stress.name().to_string(),
        }
    }
}

/// The test a person knows for a scenario: `round-trip` is `RoundTrip`, the
/// map `Xmip/New-XmipPlaygroundEnvironment.ps1` keeps the other way round.
fn test_name(scenario: &str) -> String {
    scenario
        .split('-')
        .map(|word| {
            let mut letters = word.chars();
            letters.next().map_or_else(String::new, |first| {
                first.to_ascii_uppercase().to_string() + letters.as_str()
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_run_names_its_tests_the_way_a_person_asks_for_them() {
        let nodes = ["R1".to_string(), "P1".to_string()];
        let run = Run::of("C1", &["round-trip".to_string()], &nodes, Stress::Harsh);
        assert_eq!(run.tests, ["RoundTrip"]);
        assert_eq!(run.nodes, ["R1", "P1"]);
        assert_eq!((run.cluster.as_str(), run.stress.as_str()), ("C1", "harsh"));

        let every = Run::of("C1", &[], &[], Stress::Calm);
        assert_eq!(
            every.tests,
            [
                "RoundTrip",
                "LowLatency",
                "HeavyLoad",
                "Retention",
                "Filing",
                "ExclusiveClaim",
                "DailyBacklog"
            ]
        );
    }
}
