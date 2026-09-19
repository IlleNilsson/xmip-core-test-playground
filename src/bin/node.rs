//! One emulated node: a System Process the fleet spawns (ADR-0028 clause 2).
//!
//! ```text
//! node --name <name> --shared <dir> --stress <level> --rounds <n> --snapshot <path>
//!      [--interval-ms <ms>] [--online true|false]
//! ```
//!
//! It runs, in-process, the **`ExclusiveClaim`** and **`DailyBacklog`** tests over a directory
//! the whole fleet shares — `<shared>/exclusive-claim` and `<shared>/daily-backlog` — so exclusive
//! pickup and backlog draining are contended by real processes, not threads:
//! the property ADR-0024's claim exists to prove (`create_new`, `O_EXCL`,
//! across processes). Each round it publishes its own snapshot, under
//! `xmip:///playground/node/<name>/...`, atomically to `<path>`; the fleet
//! merges every node's file and adds the cluster rollup the surface owes
//! (ADR-0027 decision 8).
//!
//! It exits after `<n>` rounds — `0` means until stopped — or as soon as
//! `<shared>/stop` appears, checked between rounds. Another node deleting or
//! claiming what this one was about to take is a lost race and a normal
//! outcome; nothing here treats it as an error. The stress level sets the
//! claim's injected breach rate (none at `calm`), and the interval is the pause
//! between rounds, a quarter of a second unless given.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use observe::Snapshot;
use xmip_test_playground::fleet::merge;
use xmip_test_playground::{
    DailyBacklog, ExclusiveClaim, Stress, Switches, cluster_root, to_toml, write_atomic,
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
    /// its own health record, so a fleet's board shows it per node.
    online: bool,
}

fn main() -> ExitCode {
    let arguments = match parse(std::env::args().skip(1)) {
        Ok(arguments) => arguments,
        Err(problem) => {
            eprintln!("node: {problem}");
            eprintln!(
                "usage: node --name <name> --shared <dir> --stress <level> --rounds <n> \
                 --snapshot <path> [--interval-ms <ms>]"
            );
            return ExitCode::from(2);
        }
    };

    let node = format!("{}/node/{}", cluster_root(), arguments.name);

    // What this process says of itself while it runs (ADR-0053).
    let _declared = ::node::Declaration::new("xmip-playground-node", &node, ::node::Purpose::Test)
        .declare()
        .map_err(|error| eprintln!("node {}: could not declare itself: {error}", arguments.name));
    let mut exclusive_claim = ExclusiveClaim::shared(
        format!("{node}/exclusive-claim"),
        arguments.shared.join("exclusive-claim"),
    )
    .at(arguments.stress);
    let mut daily_backlog = DailyBacklog::shared(
        format!("{node}/daily-backlog"),
        arguments.shared.join("daily-backlog"),
    );
    let stop = arguments.shared.join("stop");

    let mut round = 0;
    while arguments.rounds == 0 || round < arguments.rounds {
        if stop.exists() {
            break;
        }
        round += 1;

        let mut snapshot = Snapshot::new();
        merge(&mut snapshot, &exclusive_claim.tick());
        merge(&mut snapshot, &daily_backlog.tick());
        snapshot.record_health(switch_record(&node, arguments.online));

        if let Err(error) = write_atomic(&arguments.snapshot, &to_toml(&node, &snapshot)) {
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
    })
}

/// The node's `online` switch as one health record under its scope:
/// always fine, its evidence the word, so the fleet's board shows which
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
