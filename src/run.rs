//! What a roll was started with, published beside the snapshot under `[run]`
//! (the owner, 2026-09-19: the run says what it was started with). The roll
//! fills it once per publication; its shape is `observe::Run`'s.

use observe::Run;

use crate::parties::Spread;
use crate::real::RealNode;
use crate::roster::Roster;
use crate::scenario::SCENARIOS;
use crate::stress::Stress;

/// The run of `cluster` driving the `chosen` scenarios (none chosen is every
/// one) over the nodes of `roster` at `stress`, those named in `real` on
/// machines of their own and the rest simulated: the tests by the names a
/// person asks for them, and what each node was started with, including its
/// online capability. The shape is `observe::Run`'s; this only fills it.
#[must_use]
pub fn started(
    cluster: &str,
    chosen: &[String],
    roster: &Roster,
    stress: Stress,
    real: &[String],
) -> Run {
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
        roles: names
            .iter()
            .map(|name| roster.capability(name).entry(name))
            .collect(),
        online: names
            .iter()
            .filter(|name| roster.capability(name).is_online())
            .map(|name| (*name).to_string())
            .collect(),
        stress: stress.name().to_string(),
        hidden: crate::environment::hidden(),
        real: real.to_vec(),
    }
}

/// The line that says which nodes are real and where each answered, and
/// that the rest are simulated; none where every node is simulated, as on
/// one computer (ADR-0028, amendment 2026-10-21).
#[must_use]
pub fn found(real: &[RealNode]) -> Option<String> {
    if real.is_empty() {
        return None;
    }
    let named: Vec<String> = real
        .iter()
        .map(|node| format!("{} at {}", node.name, node.address))
        .collect();
    Some(format!(
        "  real, each read where it answers — on a machine of its own, or started here as \
         xmip-service: {}; the rest simulated",
        named.join(", ")
    ))
}

/// The first lines of a run: the level, the cluster and the roster its
/// cluster file declares, and the Parties it trades with, each with how
/// many transports it sends in over and is delivered to over (ADR-0028,
/// amendment 2026-10-13). Where the nodes declare no stage, that is said
/// here rather than left to be noticed. The board clears a live terminal
/// every round; a redirected log keeps these lines, and the `[run]` table of
/// every snapshot carries the same answer of the nodes.
#[must_use]
pub fn announced(cluster: &str, stress: Stress, roster: &Roster, spread: &Spread) -> Vec<String> {
    let mut lines = vec![format!(
        "roll at {} as cluster {cluster}: {}",
        stress.name(),
        roster.describe()
    )];
    if !roster.is_empty() {
        lines.push(format!("  roster: {}", roster.text()));
    }
    let parties: Vec<String> = spread
        .parties()
        .all()
        .iter()
        .enumerate()
        .map(|(place, party)| {
            let (sends, receives) = (spread.sends(place).len(), spread.receives(place).len());
            format!("{} ({sends} in, {receives} out)", party.name)
        })
        .collect();
    lines.push(format!(
        "  parties, by the transports each sends in and is delivered to over: {}",
        parties.join(", ")
    ));
    lines
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
    use crate::support::{path, test_cluster};

    #[test]
    fn a_run_names_its_tests_and_what_each_node_was_started_with() {
        let cluster = test_cluster();
        // The third declares nothing: a bare name in the roster.
        let [first, second, bare] = path(&cluster);
        let roles = [
            format!("{first}=receiving"),
            format!("{second}=processing+sending"),
            bare.to_string(),
        ];
        let roster = Roster::parse(&roles.join(","))
            .expect("a well-formed roster")
            .declared(
                first,
                node::Capability::parse("receiving")
                    .expect("receiving")
                    .with_online(true),
            );
        let c = cluster.name.as_str();
        let run = started(
            c,
            &["round-trip".to_string()],
            &roster,
            Stress::Harsh,
            &[first.to_string()],
        );
        assert_eq!(run.real, [first], "the real nodes are said apart");
        assert_eq!(run.tests, ["RoundTrip"]);
        assert_eq!(run.nodes, [first, second, bare]);
        assert_eq!(run.roles, roles);
        assert_eq!(run.online, [first]);
        assert_eq!((run.cluster.as_str(), run.stress.as_str()), (c, "harsh"));

        let every = started(c, &[], &Roster::default(), Stress::Calm, &[]);
        assert!(
            every.real.is_empty() && found(&[]).is_none(),
            "all simulated"
        );
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
