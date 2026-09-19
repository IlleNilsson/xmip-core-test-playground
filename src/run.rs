//! What a run was started with, published beside the snapshot under `[run]`.
//!
//! The owner, 2026-09-19: the run says what it was started with. A surface
//! showed a cluster and its leaves and nothing of which tests were named,
//! which nodes, which of them online, or how hard — so a board could not be
//! told from the one before it. The roll writes this once per publication; a
//! reader that does not know the table skips it.

use serde::{Deserialize, Serialize};

use crate::roster::Roster;
use crate::scenario::SCENARIOS;
use crate::stress::Stress;

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
    /// What each node was started with, in the words `--can` takes:
    /// `R1=receive`. A node that declared nothing is listed by name alone, so
    /// a reader sees that it runs whole tests itself (ADR-0056).
    pub capabilities: Vec<String>,
    /// The nodes among them that may assume the internet (ADR-0045).
    pub online: Vec<String>,
    /// The stress level's name.
    pub stress: String,
}

impl Run {
    /// The run of `cluster` driving the `chosen` scenarios (none chosen is
    /// every one) over the nodes of `roster` at `stress`; the roster says what
    /// each node was started with, including its online capability.
    #[must_use]
    pub fn of(cluster: &str, chosen: &[String], roster: &Roster, stress: Stress) -> Self {
        let scenarios: Vec<&str> = if chosen.is_empty() {
            SCENARIOS.to_vec()
        } else {
            chosen.iter().map(String::as_str).collect()
        };
        let names = roster.names();
        Self {
            cluster: cluster.to_string(),
            tests: scenarios.into_iter().map(test_name).collect(),
            nodes: names.iter().map(ToString::to_string).collect(),
            capabilities: names
                .iter()
                .map(|name| {
                    let capability = roster.capability(name);
                    if capability.declares_no_stage() {
                        (*name).to_string()
                    } else {
                        format!("{name}={}", capability.words().replace(',', "+"))
                    }
                })
                .collect(),
            online: names
                .iter()
                .filter(|name| roster.capability(name).is_online())
                .map(|name| (*name).to_string())
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
    fn a_run_names_its_tests_and_what_each_node_was_started_with() {
        let roster = Roster::parse("R1=receive,P1=process+send,n1")
            .expect("a well-formed roster")
            .declared(
                "R1",
                crate::Capability::parse("receive")
                    .expect("receive")
                    .with_online(true),
            );
        let run = Run::of("C1", &["round-trip".to_string()], &roster, Stress::Harsh);
        assert_eq!(run.tests, ["RoundTrip"]);
        assert_eq!(run.nodes, ["R1", "P1", "n1"]);
        assert_eq!(run.capabilities, ["R1=receive", "P1=process+send", "n1"]);
        assert_eq!(run.online, ["R1"]);
        assert_eq!((run.cluster.as_str(), run.stress.as_str()), ("C1", "harsh"));

        let every = Run::of("C1", &[], &Roster::default(), Stress::Calm);
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
