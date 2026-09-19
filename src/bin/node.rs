//! One emulated node: a System Process the cluster spawns (ADR-0028 clause 2).
//!
//! ```text
//! node --name <name> --shared <dir> --stress <level> --rounds <n> --snapshot <path>
//!      [--interval-ms <ms>] [--online true|false]
//!      [--nodes <name,name,...>] [--scenarios <scenario,scenario,...>]
//! ```
//!
//! **It runs the tests that were named** (the owner, 2026-09-19) — its part
//! of the `--scenarios` it is given, every one when none is given. A name
//! that is no scenario is REFUSED with exit code 2 and the scenarios there
//! are; it is never dropped.
//!
//!   - **`RoundTrip`**, when its name gives it a role — `R` receives, `P`
//!     processes, `S` sends — and `--nodes` names a cluster with all three:
//!     its one stage of the message path, handing each pair on to the next
//!     node through the inboxes under `<shared>/handoff/` (`relay.rs`). A
//!     node with no role runs no part of it; the roll runs it whole.
//!   - **`ExclusiveClaim`** and **`DailyBacklog`**, over a directory the whole
//!     cluster shares — `<shared>/exclusive-claim` and
//!     `<shared>/daily-backlog` — so exclusive pickup and backlog draining are
//!     contended by real processes, not threads: the property ADR-0024's
//!     claim exists to prove (`create_new`, `O_EXCL`, across processes).
//!
//! Each round it publishes its own snapshot, under
//! `xmip:///<cluster>/node/<name>/...`, atomically to `<path>`; the cluster
//! merges every node's file and adds the rollup the surface owes (ADR-0027
//! decision 8).
//!
//! It exits after `<n>` rounds — `0` means until stopped — or as soon as
//! `<shared>/stop` appears, checked between rounds. Another node deleting or
//! claiming what this one was about to take is a lost race and a normal
//! outcome; nothing here treats it as an error. The stress level sets the
//! injected fault rates (none at `calm`), and the interval is the pause
//! between rounds, a quarter of a second unless given. `--online` gates what
//! is outside the cluster only: an offline node takes handoffs like any other.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use observe::Snapshot;
use xmip_test_playground::cluster::merge;
use xmip_test_playground::scenario::{self, DAILY_BACKLOG, EXCLUSIVE_CLAIM, ROUND_TRIP, drives};
use xmip_test_playground::{
    DailyBacklog, ExclusiveClaim, Relay, Roster, Stress, Switches, cluster_root, node_toml,
    write_atomic,
};

/// What the command line said.
struct Arguments {
    name: String,
    shared: PathBuf,
    stress: Stress,
    rounds: u64,
    snapshot: PathBuf,
    interval: Duration,
    /// ADR-0045: whether this node may assume the internet. Published in
    /// its own health record, so the cluster's board shows it per node.
    online: bool,
    /// Every node of the cluster, this one among them; empty when not told.
    nodes: Vec<String>,
    /// The scenarios to run this node's part of; empty means every one.
    scenarios: Vec<String>,
}

fn main() -> ExitCode {
    let arguments = match parse(std::env::args().skip(1)) {
        Ok(arguments) => arguments,
        Err(problem) => {
            eprintln!("node: {problem}");
            eprintln!(
                "usage: node --name <name> --shared <dir> --stress <level> --rounds <n> \
                 --snapshot <path> [--interval-ms <ms>] [--online true|false] \
                 [--nodes <names>] [--scenarios <scenarios>]"
            );
            return ExitCode::from(2);
        }
    };

    let node = format!("{}/node/{}", cluster_root(), arguments.name);

    // What this process says of itself while it runs (ADR-0053).
    let _declared = ::node::Declaration::new("xmip-playground-node", &node, ::node::Purpose::Test)
        .declare()
        .map_err(|error| eprintln!("node {}: could not declare itself: {error}", arguments.name));

    let shared = &arguments.shared;
    let chosen = &arguments.scenarios;
    let mut relay = drives(chosen, ROUND_TRIP)
        .then(|| {
            let work = shared.join("work").join(&arguments.name);
            let roster = Roster::of(&arguments.nodes);
            Relay::new(&arguments.name, node.clone(), &roster, shared, &work)
        })
        .flatten()
        .map(|relay| relay.at(arguments.stress));
    let mut exclusive_claim = drives(chosen, EXCLUSIVE_CLAIM).then(|| {
        let scope = format!("{node}/{EXCLUSIVE_CLAIM}");
        ExclusiveClaim::shared(scope, shared.join(EXCLUSIVE_CLAIM)).at(arguments.stress)
    });
    let mut daily_backlog = drives(chosen, DAILY_BACKLOG).then(|| {
        DailyBacklog::shared(
            format!("{node}/{DAILY_BACKLOG}"),
            shared.join(DAILY_BACKLOG),
        )
    });
    let stop = shared.join("stop");

    let mut round = 0;
    while arguments.rounds == 0 || round < arguments.rounds {
        if stop.exists() {
            break;
        }
        round += 1;

        let mut snapshot = Snapshot::new();
        if let Some(relay) = relay.as_mut() {
            merge(&mut snapshot, &relay.tick());
        }
        if let Some(exclusive_claim) = exclusive_claim.as_mut() {
            merge(&mut snapshot, &exclusive_claim.tick());
        }
        if let Some(daily_backlog) = daily_backlog.as_mut() {
            merge(&mut snapshot, &daily_backlog.tick());
        }
        snapshot.record_health(switch_record(&node, arguments.online));

        let hops = relay
            .as_ref()
            .map_or_else(Vec::new, |relay| relay.hops().links().cloned().collect());
        let text = node_toml(&node, &snapshot, hops);
        if let Err(error) = write_atomic(&arguments.snapshot, &text) {
            eprintln!(
                "node {}: could not publish to {}: {error}",
                arguments.name,
                arguments.snapshot.display()
            );
        }

        if !arguments.interval.is_zero() && (arguments.rounds == 0 || round < arguments.rounds) {
            std::thread::sleep(arguments.interval);
        }
    }

    ExitCode::SUCCESS
}

/// `--flag value` pairs, every required one present and well-formed.
fn parse(args: impl Iterator<Item = String>) -> Result<Arguments, String> {
    let mut name = None;
    let mut shared = None;
    let mut stress = None;
    let mut rounds = None;
    let mut snapshot = None;
    let mut interval = Duration::from_millis(250);
    let mut online = false;
    let mut nodes = Vec::new();
    let mut scenarios = Vec::new();

    let mut args = args.peekable();
    while let Some(flag) = args.next() {
        let value = args.next().ok_or_else(|| format!("{flag} needs a value"))?;
        match flag.as_str() {
            "--name" => name = Some(value),
            "--shared" => shared = Some(PathBuf::from(value)),
            "--stress" => {
                stress = Some(Stress::parse(&value).ok_or(format!("unknown stress {value}"))?);
            }
            "--rounds" => rounds = Some(number(&flag, &value)?),
            "--snapshot" => snapshot = Some(PathBuf::from(value)),
            "--interval-ms" => interval = Duration::from_millis(number(&flag, &value)?),
            "--online" => {
                online = xmip_test_playground::switch::parse(&value)
                    .ok_or(format!("--online wants true or false, not {value}"))?;
            }
            "--nodes" => nodes = xmip_test_playground::switch::names(&value),
            "--scenarios" => scenarios = scenario::chosen(Some(&value))?,
            other => return Err(format!("unknown flag {other}")),
        }
    }

    Ok(Arguments {
        name: name.ok_or("--name is required")?,
        shared: shared.ok_or("--shared is required")?,
        stress: stress.ok_or("--stress is required")?,
        rounds: rounds.ok_or("--rounds is required")?,
        snapshot: snapshot.ok_or("--snapshot is required")?,
        interval,
        online,
        nodes,
        scenarios,
    })
}

/// The node's `online` switch as one health record under its scope:
/// always fine, its evidence the word, so the cluster's board shows which
/// emulated nodes may assume the internet (ADR-0045, none by default).
fn switch_record(node: &str, online: bool) -> observe::HealthRecord {
    let switches = Switches { online };
    observe::HealthRecord {
        scope: format!("{node}/switch"),
        health: observe::Health::Fine,
        severity: 0,
        evidence: format!("{}; the estate's tests run offline", switches.word()),
        observed_unix_nanos: xmip_test_playground::now_unix_nanos(),
    }
}

fn number(flag: &str, value: &str) -> Result<u64, String> {
    value
        .parse()
        .map_err(|_| format!("{flag} wants a number, not {value}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arguments(extra: &[&str]) -> Result<Arguments, String> {
        let required = [
            "--name",
            "R1",
            "--shared",
            "s",
            "--stress",
            "calm",
            "--rounds",
            "1",
            "--snapshot",
            "r1.toml",
        ];
        parse(required.iter().chain(extra).map(ToString::to_string))
    }

    #[test]
    fn the_scenarios_and_the_nodes_are_read_and_absent_means_every_one() {
        let bare = arguments(&[]).expect("the required flags suffice");
        assert!(bare.scenarios.is_empty() && bare.nodes.is_empty());

        let told = arguments(&["--scenarios", "Round-Trip", "--nodes", "R1, P1,S1"])
            .expect("both are well formed");
        assert_eq!(told.scenarios, ["round-trip"]);
        assert_eq!(told.nodes, ["R1", "P1", "S1"]);
        assert!(
            arguments(&["--scenarios", ""])
                .expect("empty is every one")
                .scenarios
                .is_empty()
        );
    }

    #[test]
    fn an_unknown_scenario_is_refused_with_the_names_there_are() {
        let refusal = arguments(&["--scenarios", "round-trip,pingpong"])
            .err()
            .expect("pingpong is no scenario");
        assert!(refusal.starts_with("REFUSED"), "{refusal}");
        assert!(
            refusal.contains("pingpong") && refusal.contains("exclusive-claim"),
            "{refusal}"
        );
    }
}
