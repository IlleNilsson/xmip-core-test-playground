//! One emulated node: a System Process the cluster spawns (ADR-0028 clause 2).
//!
//! ```text
//! node --name <name> --shared <dir> --stress <level> --rounds <n> --snapshot <path>
//!      [--interval-ms <ms>] [--online true|false] [--can receive,process]
//!      [--nodes <name[=capability],...>] [--scenarios <scenario,...>]
//! ```
//!
//! **A node declares what it can do** (ADR-0056). `--can` says which stages of
//! the message path this node serves — `receive`, `process`, `send`, or more
//! than one, in lowercase exactly (`Send` is no capability; the owner,
//! 2026-09-24) — and `--nodes` says the same for every node of the cluster, so
//! this one finds the others without asking. Nothing is read out of a name: on
//! 2026-09-19 the rig took a node's stage from its first letter and the owner
//! said *I know, so why do you break it!* A word that is no capability is
//! REFUSED with exit code 2 and the words there are (ADR-0055).
//!
//! **It runs the tests that were named** (the owner, 2026-09-19) — its part
//! of the `--scenarios` it is given, every one when none is given. A name
//! that is no scenario is REFUSED with exit code 2 and the scenarios there
//! are; it is never dropped.
//!
//!   - **`RoundTrip`**, for each stage it declared, when `--nodes` covers the
//!     whole path: that stage of the message path, handing each pair on to a
//!     node that declared the next through the inboxes under
//!     `<shared>/handoff/` (`relay.rs`). A node that declared no stage runs no
//!     part of it; the roll runs it whole.
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
//! between rounds, a quarter of a second unless given. Online capability gates
//! what is outside the cluster only: an offline node takes handoffs like any
//! other.
//!
//! Each round it publishes what it declared at `<node>/capability`, beside the
//! `switch` record, so a surface reads a node's capabilities from the snapshot
//! rather than from its name.
//!
//! **It audits** (ADR-0062): `start` with its scope, capability, stress and
//! scenarios, `stop` when its rounds are done or it was told to stop, a
//! refused argument as the failure to `start`, a failed publish as a
//! failure, and every panic as `unhandled` — into `XMIP_AUDIT_DIRECTORY`,
//! else the operating system's log (`process_audit.rs`).

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use node::Capability;
use observe::Snapshot;
use xaudit::program_audit::ProgramAudit;
use xmip_core_test_playground::cluster::merge;
use xmip_core_test_playground::scenario::{
    self, DAILY_BACKLOG, EXCLUSIVE_CLAIM, ROUND_TRIP, drives,
};
use xmip_core_test_playground::{
    DailyBacklog, ExclusiveClaim, Relay, Roster, Stress, cluster_root, node_toml, write_atomic,
};
use xmip_core_test_playground::{image, process_audit};

/// What the command line said.
struct Arguments {
    name: String,
    shared: PathBuf,
    stress: Stress,
    rounds: u64,
    snapshot: PathBuf,
    interval: Duration,
    /// What this node declared it can do (ADR-0056): the stages of the
    /// message path from `--can`, and from `--online` whether it may assume
    /// the internet (ADR-0045). Published in its own health records, so the
    /// cluster's board shows both per node.
    capability: Capability,
    /// Every node of the cluster and what each declared, this one among them;
    /// empty when not told.
    roster: Roster,
    /// The scenarios to run this node's part of; empty means every one.
    scenarios: Vec<String>,
}

const USAGE: &str = "usage: node --name <name> --shared <dir> --stress <level> --rounds <n> \
     --snapshot <path> [--interval-ms <ms>] [--online true|false] \
     [--can receive,process] [--nodes <name[=capability],...>] \
     [--scenarios <scenarios>]";

fn main() -> ExitCode {
    // The name is the image's own — xmip-playground-<cluster>-node-<node>
    // where a cluster named it — so the declaration, the audit and the
    // process list agree (ADR-0053, amendment 2026-09-20).
    let called = image::this_process("xmip-playground-node");
    let audit = ProgramAudit::new(&called, None);
    audit.watch_panics();

    let arguments = match parse(std::env::args().skip(1)) {
        Ok(arguments) => arguments,
        Err(problem) => {
            process_audit::fail(&audit, "start", &format!("node: {problem}"));
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };

    let node = format!("{}/node/{}", cluster_root(), arguments.name);

    // What this process says of itself while it runs (ADR-0053).
    let _declared = ::node::Declaration::new(called, &node, ::node::Purpose::Test)
        .declare()
        .map_err(|error| {
            let problem = format!("node {}: could not declare itself: {error}", arguments.name);
            process_audit::fail(&audit, "declare", &problem);
        });
    say_started(&audit, &node, &arguments);

    let shared = &arguments.shared;
    let chosen = &arguments.scenarios;
    let mut relays = relays(&arguments, &node);
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
        for relay in &mut relays {
            let ticked = relay.tick();
            merge(&mut snapshot, &ticked);
        }
        if let Some(exclusive_claim) = exclusive_claim.as_mut() {
            merge(&mut snapshot, &exclusive_claim.tick());
        }
        if let Some(daily_backlog) = daily_backlog.as_mut() {
            merge(&mut snapshot, &daily_backlog.tick());
        }
        snapshot.record_health(record(
            format!("{node}/switch"),
            format!(
                "{}; the estate's tests run offline",
                arguments.capability.word()
            ),
        ));
        snapshot.record_health(record(
            observe::capability::scope(&node),
            arguments.capability.evidence(),
        ));

        let hops = relays
            .iter()
            .flat_map(|relay| relay.hops().links().cloned())
            .collect();
        let text = node_toml(&node, &snapshot, hops);
        if let Err(error) = write_atomic(&arguments.snapshot, &text) {
            let problem = format!(
                "node {}: could not publish to {}: {error}",
                arguments.name,
                arguments.snapshot.display()
            );
            process_audit::fail(&audit, "publish", &problem);
        }

        if !arguments.interval.is_zero() && (arguments.rounds == 0 || round < arguments.rounds) {
            std::thread::sleep(arguments.interval);
        }
    }

    process_audit::stop(&audit, &[("node", &node), ("rounds", &round.to_string())]);
    ExitCode::SUCCESS
}

/// The node's `start` record: its scope and everything it was told.
fn say_started(audit: &ProgramAudit, node: &str, arguments: &Arguments) {
    process_audit::start(
        audit,
        &[
            ("node", node),
            ("capability", &arguments.capability.words()),
            ("online", arguments.capability.word()),
            ("stress", arguments.stress.name()),
            ("scenarios", &arguments.scenarios.join(",")),
            ("rounds", &arguments.rounds.to_string()),
            ("shared", &arguments.shared.display().to_string()),
            ("snapshot", &arguments.snapshot.display().to_string()),
        ],
    );
}

/// One relay per stage this node declared, when `RoundTrip` was chosen. This
/// node's own `--can` is the last word on itself; the roster says what the
/// others declared, so it knows who can take the next stage.
fn relays(arguments: &Arguments, node: &str) -> Vec<Relay> {
    if !drives(&arguments.scenarios, ROUND_TRIP) {
        return Vec::new();
    }
    let roster = arguments
        .roster
        .clone()
        .declared(&arguments.name, arguments.capability.clone());
    let shared = &arguments.shared;
    let work = shared.join("work").join(&arguments.name);
    arguments
        .capability
        .features()
        .iter()
        .filter_map(|stage| {
            Relay::new(
                &arguments.name,
                node.to_string(),
                *stage,
                &roster,
                shared,
                &work,
            )
            .map(|relay| relay.at(arguments.stress))
        })
        .collect()
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
    let mut can = Capability::none();
    let mut roster = Roster::default();
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
                online = xmip_core_test_playground::switch::parse(&value)
                    .ok_or(format!("--online wants true or false, not {value}"))?;
            }
            "--can" => can = Capability::parse(&value)?,
            "--nodes" => roster = Roster::parse(&value)?,
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
        capability: can.with_online(online),
        roster,
        scenarios,
    })
}

/// One health record of the node's own, always fine: what it declared, and
/// the online capability's word. The cluster's board reads both per node
/// (ADR-0045, ADR-0056), and the topology takes a node's stages from the
/// capability record rather than from its name.
fn record(scope: String, evidence: String) -> observe::HealthRecord {
    observe::HealthRecord {
        scope,
        health: observe::Health::Fine,
        severity: 0,
        evidence,
        observed_unix_nanos: xmip_core_test_playground::now_unix_nanos(),
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
            "alpha",
            "--shared",
            "s",
            "--stress",
            "calm",
            "--rounds",
            "1",
            "--snapshot",
            "alpha.toml",
        ];
        parse(required.iter().chain(extra).map(ToString::to_string))
    }

    #[test]
    fn the_scenarios_and_the_roster_are_read_and_absent_means_every_one() {
        let bare = arguments(&[]).expect("the required flags suffice");
        assert!(bare.scenarios.is_empty() && bare.roster.is_empty());
        assert!(
            bare.capability.declares_no_stage(),
            "a node told nothing declares nothing and runs whole tests"
        );

        let told = arguments(&[
            "--scenarios",
            "Round-Trip",
            "--nodes",
            "alpha=receive, beta=process,gamma=send",
            "--can",
            "receive",
        ])
        .expect("all three are well formed");
        assert_eq!(told.scenarios, ["round-trip"]);
        assert_eq!(told.roster.names(), ["alpha", "beta", "gamma"]);
        assert_eq!(told.capability.words(), "receive");
        assert!(
            arguments(&["--scenarios", ""])
                .expect("empty is every one")
                .scenarios
                .is_empty()
        );
    }

    #[test]
    fn an_unknown_scenario_or_capability_is_refused_with_the_names_there_are() {
        let refusal = arguments(&["--scenarios", "round-trip,pingpong"])
            .err()
            .expect("pingpong is no scenario");
        assert!(refusal.starts_with("REFUSED"), "{refusal}");
        assert!(
            refusal.contains("pingpong") && refusal.contains("exclusive-claim"),
            "{refusal}"
        );

        for extra in [["--can", "relay"], ["--nodes", "alpha=relay"]] {
            let refusal = arguments(&extra)
                .err()
                .unwrap_or_else(|| panic!("{extra:?}"));
            assert!(refusal.starts_with("REFUSED"), "{refusal}");
            assert!(
                refusal.contains("relay") && refusal.contains("receive, process, send"),
                "{refusal}"
            );
        }
    }
}
