//! What a roll was started with, published beside the snapshot under `[run]`
//! (the owner, 2026-09-19: the run says what it was started with). The roll
//! fills it once per publication; its shape is `observe::Run`'s.

use observe::Run;

use crate::roster::Roster;
use crate::scenario::SCENARIOS;
use crate::stress::Stress;

/// The run of `cluster` driving the `chosen` scenarios (none chosen is every
/// one) over the nodes of `roster` at `stress`: the tests by the names a
/// person asks for them, and what each node was started with, including its
/// online capability. The shape is `observe::Run`'s; this only fills it.
#[must_use]
pub fn started(cluster: &str, chosen: &[String], roster: &Roster, stress: Stress) -> Run {
    let scenarios: Vec<&str> = if chosen.is_empty() {
        SCENARIOS.to_vec()
    } else {
        chosen.iter().map(String::as_str).collect()
    };
    let names = roster.names();
    Run {
        cluster: cluster.to_string(),
        tests: scenarios.into_iter().map(test_name).collect(),
        nodes: names.iter().map(ToString::to_string).collect(),
        capabilities: names
            .iter()
            .map(|name| roster.capability(name).entry(name))
            .collect(),
        online: names
            .iter()
            .filter(|name| roster.capability(name).is_online())
            .map(|name| (*name).to_string())
            .collect(),
        stress: stress.name().to_string(),
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
                node::Capability::parse("receive")
                    .expect("receive")
                    .with_online(true),
            );
        let run = started("C1", &["round-trip".to_string()], &roster, Stress::Harsh);
        assert_eq!(run.tests, ["RoundTrip"]);
        assert_eq!(run.nodes, ["R1", "P1", "n1"]);
        assert_eq!(run.capabilities, ["R1=receive", "P1=process+send", "n1"]);
        assert_eq!(run.online, ["R1"]);
        assert_eq!((run.cluster.as_str(), run.stress.as_str()), ("C1", "harsh"));

        let every = started("C1", &[], &Roster::default(), Stress::Calm);
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
