//! Roll the playground: run every scenario continuously.
//!
//! `cargo run` ticks all four scenarios on an interval and redraws the combined
//! board each round — the tests as ADR-0028 means them, over time and never
//! stopping:
//!
//!   - **`RoundTrip`** — every transport by every contract round-trips and holds
//!     its contract; the message-path stages, with injected faults.
//!   - **`LowLatency`** — the same pairs, timed against a latency budget (p50/p99).
//!   - **`HeavyLoad`** — a megabyte per pair; does it arrive whole and validate.
//!   - **Retention** — retention and archiving: retain, then archive by age
//!     (Xmip does not delete, ADR-0040).
//!   - **Filing** — every archive technology by every contract: file a probe
//!     item through the real store and restore it whole.
//!   - **`ExclusiveClaim`** — exclusive pickup: one holder per item under contention,
//!     per execution style (sequential, parallel, concurrent).
//!   - **`DailyBacklog`** — drain a backlog as fast as possible; tweak, add a node.
//!
//! Each publishes under its own subtree of `xmip:///playground`, merged into one
//! snapshot so the rollup covers all four and an operator drills scenario →
//! detail → the failing leaf.
//!
//! Pass a number to run that many rounds and stop; omit it to roll until
//! interrupted. `XMIP_PLAYGROUND_SCENARIOS` names the scenarios to drive, comma
//! separated (`round-trip,heavy-load`); unset, every one rolls, so nothing changed
//! quietly. Two time limits bound any roll (ADR-0028): a maximum wall-clock
//! time, `XMIP_PLAYGROUND_MAX_SECONDS`, and a factor on time,
//! `XMIP_PLAYGROUND_TIME_FACTOR`, which stretches a **simulated clock** — `1.0`
//! mimics real time, retracted below one runs simulated time faster, so a long
//! horizon plays out in a short run (three simulated years in fifteen real
//! minutes is `MAX_SECONDS=900` with `TIME_FACTOR≈9.5e-6`). The round cadence
//! stays real; the factor stretches simulated time, which the Retention test ages on.
//!
//! When stdout is a terminal the board is redrawn in place; when it is piped,
//! one summary line per round is appended. After every tick the snapshot,
//! history and activity are written to the TOML files the monitoring GUI reads,
//! overridable with `XMIP_PLAYGROUND_SNAPSHOT`, `_HISTORY`, `_ACTIVITY`.
//!
//! **The fleet.** When `XMIP_PLAYGROUND_NODES` is set — a count, or empty for
//! the level's own — or `XMIP_PLAYGROUND_STRESS` is `harsh` or `brutal`, the
//! roll spawns a fleet of node processes beside the in-process scenarios and
//! merges their snapshot each round (ADR-0028 clause 2). The board shows the
//! fleet's rollup row, and a node's leaf only when it is not fine. Unset, no
//! process is spawned and the roll is what it was. `XMIP_PLAYGROUND_NODE_NAMES`
//! names the nodes instead, comma separated, one process each, at any level;
//! `XMIP_PLAYGROUND_ONLINE_NODES` names the ones among them that may assume the
//! internet (ADR-0045); unset, every node reads `XMIP_ONLINE`.

use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::time::Duration;

use observe::{Health, History, Snapshot};
use xmip_test_playground::Headroom;
use xmip_test_playground::fleet::{Fleet, merge, node_binary};
use xmip_test_playground::{
    Budget, DailyBacklog, ExclusiveClaim, FaultPlan, Filing, HeavyLoad, LowLatency, Retention,
    Schedule, Stress, activity_toml, cluster_name, cluster_root, fleet_topology, history_toml,
    now_unix_nanos, to_toml_with, write_atomic,
};

fn main() {
    let (cluster, root, base) = this_cluster();
    let root = root.as_str();

    // What this process says of itself while it runs (ADR-0053): the roll is
    // the cluster it emulates, and everything the Playground runs is test.
    let _declared = ::node::Declaration::new("xmip-playground-roll", root, ::node::Purpose::Test)
        .declare()
        .map_err(|error| eprintln!("roll: could not declare itself: {error}"));
    let stress = Stress::from_env();
    let chosen = chosen(std::env::var("XMIP_PLAYGROUND_SCENARIOS").ok().as_deref());
    let mut fleet = spawn_fleet(stress, &base);

    // Each scenario under its own subtree, each with faults or pressure on, so
    // the board is realistic rather than uniformly green. `file` stays clean in
    // every one.
    // Bounded rounds: a slice of the matrix per round, rotating, so a round
    // lands in seconds and the counters an operator watches keep moving.
    let slice = stress.workers() * 16;
    let mut round_trip = Schedule::new(format!("{root}/round-trip"), base.join("round-trip"))
        .with_faults(FaultPlan::realistic())
        .pairs_per_round(slice);
    let mut low_latency = LowLatency::new(format!("{root}/low-latency"), base.join("low-latency"))
        .under_pressure()
        .pairs_per_round(slice);
    let mut heavy_load = HeavyLoad::new(format!("{root}/heavy-load"), base.join("heavy-load"))
        .under_pressure()
        .with_bytes(load_bytes())
        .pairs_per_round(stress.workers() * 8);
    let mut retention = Retention::new(format!("{root}/retention")).under_pressure();
    let mut filing = Filing::new(format!("{root}/filing"), base.join("filing")).under_pressure();
    let mut exclusive_claim = ExclusiveClaim::new(
        format!("{root}/exclusive-claim"),
        base.join("exclusive-claim"),
    )
    .under_pressure();
    let mut daily_backlog =
        DailyBacklog::new(format!("{root}/daily-backlog"), base.join("daily-backlog"));

    // An hour of history at one point a second: enough to watch a shift, bounded
    // so a week-long run does not grow. ADR-0029.
    let mut history = History::with_capacity(3600);

    let (snapshot_path, history_path, activity_path) = publish_paths(&cluster);
    let limit: Option<u64> = std::env::args().nth(1).and_then(|arg| arg.parse().ok());
    let live = std::io::stdout().is_terminal();
    let real = Duration::from_millis(1000);
    let budget = Budget::new(max_seconds(), time_factor());

    if !live {
        println!("publishing snapshots to {}", snapshot_path.display());
    }

    let mut round: u64 = 0;
    loop {
        round += 1;

        // What everyone else is using, measured now: the levels size this
        // round's pairs to half of what is left (ADR-0028, 2026-09-11). The
        // fleet was sized the same way when it was spawned.
        let headroom = Headroom::refresh();

        let mut snapshot = Snapshot::new();
        if drives(&chosen, "round-trip") {
            merge(&mut snapshot, &round_trip.tick());
        }
        if drives(&chosen, "low-latency") {
            merge(&mut snapshot, &low_latency.tick());
        }
        if drives(&chosen, "heavy-load") {
            merge(&mut snapshot, &heavy_load.tick());
        }
        if drives(&chosen, "retention") {
            merge(&mut snapshot, &retention.tick(budget.simulated_elapsed()));
        }
        if drives(&chosen, "filing") {
            merge(&mut snapshot, &filing.tick());
        }
        if drives(&chosen, "exclusive-claim") {
            merge(&mut snapshot, &exclusive_claim.tick());
        }
        if drives(&chosen, "daily-backlog") {
            merge(&mut snapshot, &daily_backlog.tick());
        }
        if let Some(fleet) = fleet.as_mut() {
            merge(&mut snapshot, &fleet.tick());
        }

        history.record(&snapshot);

        let topology = fleet
            .as_ref()
            .map(|fleet| fleet_topology(&snapshot, fleet.names(), now_unix_nanos()));
        write(
            &snapshot_path,
            &to_toml_with(root, &snapshot, topology),
            "snapshot",
        );
        write(&history_path, &history_toml(root, &history), "history");
        write(
            &activity_path,
            &activity_toml(root, round_trip.activity()),
            "activity",
        );

        if live {
            redraw(root, round, &snapshot);
            println!("  publishing to {}", snapshot_path.display());
        } else {
            summarise(root, round, &snapshot);
        }
        println!("  headroom: {}", headroom.describe());

        if limit.is_some_and(|limit| round >= limit) || budget.expired() {
            break;
        }
        std::thread::sleep(real);
    }

    if let Some(mut fleet) = fleet {
        fleet.stop();
    }
    std::fs::remove_dir_all(&base).ok();
}

/// The fleet a roll wants, if any: `XMIP_PLAYGROUND_NODE_NAMES` names its nodes
/// outright; else `XMIP_PLAYGROUND_NODES` names a count (or, empty, the level's
/// own; `0` means no fleet at any level), and `harsh` or `brutal` spawn one
/// unasked. A fleet that cannot start is said so and the roll goes on without it.
fn spawn_fleet(stress: Stress, base: &Path) -> Option<Fleet> {
    let listed = std::env::var("XMIP_PLAYGROUND_NODE_NAMES")
        .ok()
        .map(|raw| xmip_test_playground::switch::names(&raw));
    let count = std::env::var("XMIP_PLAYGROUND_NODES").ok();
    if listed.is_none() && count.is_none() && stress < Stress::Harsh {
        return None;
    }
    let names = listed.unwrap_or_else(|| {
        let count = count
            .and_then(|raw| raw.trim().parse::<usize>().ok())
            .unwrap_or_else(|| stress.nodes());
        (1..=count)
            .map(|index| format!("node-{index:02}"))
            .collect()
    });
    if names.is_empty() {
        return None;
    }
    let shared = base.join("fleet/shared");
    let snapshots = base.join("fleet/snapshots");
    let spawned = node_binary()
        .and_then(|binary| Fleet::spawn_named(&binary, stress, &names, &shared, &snapshots, 0));
    match spawned {
        Ok(fleet) => Some(fleet),
        Err(error) => {
            eprintln!("no fleet: {error}");
            None
        }
    }
}

/// The scenarios named in `XMIP_PLAYGROUND_SCENARIOS`: empty when the variable is
/// unset or names nothing, meaning every scenario. Names are trimmed and lowered;
/// one the roll does not know is said on stderr and dropped, so a typo loses one
/// scenario visibly rather than the whole roll silently.
fn chosen(raw: Option<&str>) -> Vec<String> {
    let (known, unknown): (Vec<String>, Vec<String>) = raw
        .unwrap_or_default()
        .split(',')
        .map(|name| name.trim().to_ascii_lowercase())
        .filter(|name| !name.is_empty())
        .partition(|name| SCENARIOS.contains(&name.as_str()));
    for name in unknown {
        eprintln!(
            "no scenario named {name}; the scenarios are {}",
            SCENARIOS.join(", ")
        );
    }
    known
}

/// Every scenario a roll can drive, by the name `XMIP_PLAYGROUND_SCENARIOS` uses.
const SCENARIOS: [&str; 7] = [
    "round-trip",
    "low-latency",
    "heavy-load",
    "retention",
    "filing",
    "exclusive-claim",
    "daily-backlog",
];

/// Whether this roll drives the named scenario: every one when nothing was chosen.
fn drives(chosen: &[String], scenario: &str) -> bool {
    chosen.is_empty() || chosen.iter().any(|name| name == scenario)
}

/// A publish path: the environment override, or the well-known temp file the GUI
/// defaults to as well. The variable is external, so it keeps the prefix.
/// One cluster per roll (ADR-0028): its name, its scope root, and its own
/// scratch directory, emptied now, so a second cluster beside it neither wipes
/// nor shares this one's directories.
fn this_cluster() -> (String, String, PathBuf) {
    let Some(cluster) = cluster_name() else {
        eprintln!(
            "REFUSED: a roll is a cluster and the owner names it; set XMIP_PLAYGROUND_CLUSTER \
             (Start-XmipTest -Cluster <name>). A test spawns nodes, never a cluster."
        );
        std::process::exit(2);
    };
    let base = std::env::temp_dir().join("playground").join(&cluster);
    std::fs::remove_dir_all(&base).ok();
    (cluster, cluster_root(), base)
}

/// Where the roll publishes its snapshot, history and activity: the three
/// variables the cmdlet sets, else `<cluster>-snapshot.toml` and kin in the
/// temp directory.
fn publish_paths(cluster: &str) -> (PathBuf, PathBuf, PathBuf) {
    (
        env_path(
            "XMIP_PLAYGROUND_SNAPSHOT",
            &format!("{cluster}-snapshot.toml"),
        ),
        env_path(
            "XMIP_PLAYGROUND_HISTORY",
            &format!("{cluster}-history.toml"),
        ),
        env_path(
            "XMIP_PLAYGROUND_ACTIVITY",
            &format!("{cluster}-activity.toml"),
        ),
    )
}

fn env_path(variable: &str, default: &str) -> PathBuf {
    std::env::var_os(variable).map_or_else(|| std::env::temp_dir().join(default), PathBuf::from)
}

/// The `HeavyLoad` payload size: `XMIP_PLAYGROUND_LOAD_BYTES` if set — a plain number or
/// a human size like `512mb` or `2gb` — else a megabyte. The variable is
/// external, so it keeps the prefix. Note the memory: peak is roughly twice this
/// per pair, so a gigabyte wants a few free.
fn load_bytes() -> usize {
    let Some(raw) = std::env::var("XMIP_PLAYGROUND_LOAD_BYTES").ok() else {
        return 1024 * 1024;
    };
    let text = raw.trim().to_lowercase();
    let (number, unit) = text
        .find(|c: char| c.is_alphabetic())
        .map_or((text.as_str(), ""), |at| text.split_at(at));
    let scale: usize = match unit {
        "gb" | "g" => 1024 * 1024 * 1024,
        "mb" | "m" => 1024 * 1024,
        "kb" | "k" => 1024,
        _ => 1,
    };
    number
        .trim()
        .parse::<usize>()
        .map_or(1024 * 1024, |value| value.saturating_mul(scale))
}

/// The maximum wall-clock time to roll: `XMIP_PLAYGROUND_MAX_SECONDS` if set,
/// else no ceiling. The variable is external, so it keeps the prefix.
fn max_seconds() -> Option<Duration> {
    std::env::var("XMIP_PLAYGROUND_MAX_SECONDS")
        .ok()
        .and_then(|raw| raw.trim().parse::<f64>().ok())
        .filter(|seconds| *seconds > 0.0)
        .map(Duration::from_secs_f64)
}

/// The factor on time: `XMIP_PLAYGROUND_TIME_FACTOR` if set, else `1.0` (real
/// time). Below one runs faster than real time, above one slower. The variable
/// is external, so it keeps the prefix.
fn time_factor() -> f64 {
    std::env::var("XMIP_PLAYGROUND_TIME_FACTOR")
        .ok()
        .and_then(|raw| raw.trim().parse::<f64>().ok())
        .unwrap_or(1.0)
}

fn write(path: &Path, contents: &str, what: &str) {
    if let Err(error) = write_atomic(path, contents) {
        eprintln!("could not write the {what} to {}: {error}", path.display());
    }
}

/// The full board, cleared and reprinted in place — a live terminal view.
fn redraw(node: &str, round: u64, snapshot: &Snapshot) {
    print!("\x1b[2J\x1b[H");
    println!("Xmip Playground — rolling every scenario   (round {round})");
    println!("{:-<86}", "");

    for record in pairs(node, snapshot) {
        let leaf = record
            .scope
            .strip_prefix(&format!("{node}/"))
            .unwrap_or(&record.scope);
        println!(
            "  {:<44} {:<7} sev {:>3}   {}",
            leaf,
            word(record.health),
            record.severity,
            record.evidence
        );
    }

    println!("{:-<86}", "");
    println!(
        "  rollup at {node}: {}",
        word(snapshot.worst(node).unwrap_or(Health::Fine))
    );
    println!("\n  ctrl-c to stop");
}

/// One line per round, for a piped run: the rollup, and the worst leaf when it is
/// not green.
fn summarise(node: &str, round: u64, snapshot: &Snapshot) {
    let worst = snapshot.worst(node).map_or("NONE", word);
    let count = pairs(node, snapshot).len();

    let trouble = pairs(node, snapshot)
        .into_iter()
        .find(|record| record.health != Health::Fine)
        .map_or_else(String::new, |record| {
            format!("  — worst {}: {}", record.scope, record.evidence)
        });

    println!("round {round:>4}: {worst}  ({count} leaves){trouble}");
}

/// The rows the board shows: every leaf, except that a fleet node's leaves
/// appear only when not fine — the fleet's own row always does, and an
/// operator drills into a node from there.
fn pairs(node: &str, snapshot: &Snapshot) -> Vec<observe::HealthRecord> {
    let nodes = format!("{node}/node/");
    let mut records = snapshot.health(node);
    records.retain(|record| !record.scope.starts_with(&nodes) || record.health != Health::Fine);
    records.sort_by(|left, right| left.scope.cmp(&right.scope));
    records
}

fn word(health: Health) -> &'static str {
    match health {
        Health::Fine => "FINE",
        Health::Paused => "PAUSED",
        Health::Working => "WORKING",
        Health::Stressed => "STRESSED",
        Health::Exhausted => "EXHAUSTED",
        Health::Holding => "HOLDING",
        Health::Done => "DONE",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nothing_chosen_drives_every_scenario() {
        assert!(chosen(None).is_empty());
        assert!(chosen(Some("")).is_empty());
        assert!(chosen(Some(" , ")).is_empty());
        for scenario in SCENARIOS {
            assert!(drives(&[], scenario));
        }
    }

    #[test]
    fn a_list_drives_only_what_it_names() {
        let picked = chosen(Some(" Round-Trip, heavy-load "));
        assert_eq!(picked, ["round-trip", "heavy-load"]);
        assert!(drives(&picked, "round-trip"));
        assert!(drives(&picked, "heavy-load"));
        assert!(!drives(&picked, "low-latency"));
    }

    #[test]
    fn an_unknown_name_is_dropped_and_the_rest_kept() {
        let picked = chosen(Some("round-trip,typo"));
        assert_eq!(picked, ["round-trip"]);
        assert!(!drives(&picked, "typo"));
        assert!(!drives(&picked, "daily-backlog"));
    }
}
