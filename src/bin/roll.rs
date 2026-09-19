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
//! overridable with `XMIP_PLAYGROUND_SNAPSHOT`, `_HISTORY`, `_ACTIVITY`. Every
//! variable is read in one place, `environment.rs`.
//!
//! **The cluster is a process too** (the owner, 2026-09-19). When
//! `XMIP_PLAYGROUND_NODES` is set — a count, or empty for the level's own — or
//! `XMIP_PLAYGROUND_STRESS` is `harsh` or `brutal`, the roll spawns exactly
//! one `xmip-playground-cluster` process, and that process spawns and
//! supervises one `xmip-playground-node` per node (ADR-0028 clause 2). The
//! roll merges the one file the cluster publishes into its own snapshot each
//! round; the board shows the nodes' rollup row, and a node's leaf only when
//! it is not fine. Unset, no process is spawned and the roll is what it was.
//! `XMIP_PLAYGROUND_NODE_NAMES` names the nodes instead, comma separated, one
//! process each, at any level; `XMIP_PLAYGROUND_ONLINE_NODES` names the ones
//! among them that may assume the internet (ADR-0045); unset, every node
//! reads `XMIP_ONLINE`.
//!
//! **The letter is the role** (the owner, 2026-09-19). A node named `R…`
//! receives, `P…` processes, `S…` sends; every node is told the scenarios
//! that were named and runs its part of them. With role nodes, `RoundTrip`
//! is theirs — each pair handed `R` to `P` to `S` between the processes — and
//! the roll does not also run it in-process; a role missing among them is
//! REFUSED at the start. A node with any other name (`node-01`) has no role
//! and runs the shared-directory tests whole, as every node did before.

use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::time::Duration;

use observe::{Activity, Health, History, Snapshot};
use xmip_test_playground::Headroom;
use xmip_test_playground::cluster::{Orders, Spawned, cluster_binary, merge};
use xmip_test_playground::environment::{
    self, load_bytes, max_seconds, node_names, publish_paths, time_factor,
};
use xmip_test_playground::scenario::{ROUND_TRIP, drives};
use xmip_test_playground::{
    Budget, DailyBacklog, ExclusiveClaim, FaultPlan, Filing, HeavyLoad, LowLatency, Retention,
    Roster, Run, Schedule, Stress, Topology, activity_toml, cluster_name, cluster_root,
    cluster_topology, history_toml, now_unix_nanos, to_toml_run, write_atomic,
};

fn main() {
    let (cluster, root, base) = this_cluster();
    let root = root.as_str();

    // What this process says of itself while it runs (ADR-0053): the roll is
    // the test over the cluster it starts, and everything the Playground runs
    // is test.
    let _declared = ::node::Declaration::new("xmip-playground-roll", root, ::node::Purpose::Test)
        .declare()
        .map_err(|error| eprintln!("roll: could not declare itself: {error}"));
    let stress = Stress::from_env();
    let chosen = chosen_or_refuse();
    let names = node_names(stress);
    let relayed = relayed_or_refuse(&chosen, &names);
    let run = Run::of(&cluster, &chosen, &names, stress);

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

    let publication = Publication::of(&cluster, run);

    // One cluster per roll (ADR-0028), and since 2026-09-19 a process of its
    // own: it spawns and supervises the nodes and publishes beside this
    // roll's snapshot, which merges that one file each round.
    let cluster_path = publication.beside(&format!("{cluster}-cluster.toml"));
    let mut spawned = spawn_cluster(&cluster, stress, &names, &chosen, &base, &cluster_path);

    let limit: Option<u64> = std::env::args().nth(1).and_then(|arg| arg.parse().ok());
    let live = std::io::stdout().is_terminal();
    let real = Duration::from_millis(1000);
    let budget = Budget::new(max_seconds(), time_factor());

    if !live {
        println!("publishing snapshots to {}", publication.snapshot.display());
    }

    let mut round: u64 = 0;
    loop {
        round += 1;

        // What everyone else is using, measured now: the levels size this
        // round's pairs to half of what is left (ADR-0028, 2026-09-11). The
        // level's own count of nodes was sized the same way.
        let headroom = Headroom::refresh();

        let mut snapshot = Snapshot::new();
        // With role nodes the message path is theirs, between processes.
        if drives(&chosen, ROUND_TRIP) && !relayed {
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
        // The cluster's own file, and from it what the topology draws.
        let topology = spawned.as_mut().map(|spawned| {
            merge(&mut snapshot, spawned.tick());
            let names = names.iter().map(String::as_str);
            cluster_topology(&snapshot, names, spawned.hops(), now_unix_nanos())
        });

        history.record(&snapshot);
        publication.round(root, &snapshot, topology, &history, round_trip.activity());

        if live {
            redraw(root, round, &snapshot);
            println!("  publishing to {}", publication.snapshot.display());
        } else {
            summarise(root, round, &snapshot);
        }
        println!("  headroom: {}", headroom.describe());

        if limit.is_some_and(|limit| round >= limit) || budget.expired() {
            break;
        }
        std::thread::sleep(real);
    }

    // The cluster is asked to leave before this process does, so it stops its
    // own nodes on the way out and nothing is orphaned.
    if let Some(mut spawned) = spawned {
        spawned.stop();
    }
    std::fs::remove_dir_all(&base).ok();
    std::fs::remove_file(&cluster_path).ok();
}

/// The three files a round publishes, and the run header every snapshot
/// carries. The paths are the cmdlet's or the roll's own defaults, decided
/// once (`environment.rs`); the header says what the run was started with.
struct Publication {
    snapshot: PathBuf,
    history: PathBuf,
    activity: PathBuf,
    run: Run,
}

impl Publication {
    /// Where this cluster publishes, with the run it publishes under.
    fn of(cluster: &str, run: Run) -> Self {
        let (snapshot, history, activity) = publish_paths(cluster);
        Self {
            snapshot,
            history,
            activity,
            run,
        }
    }

    /// A file called `name` beside the snapshot — where the cluster process
    /// publishes its own.
    fn beside(&self, name: &str) -> PathBuf {
        self.snapshot.with_file_name(name)
    }

    /// One round's snapshot, history and activity, each written atomically.
    fn round(
        &self,
        root: &str,
        snapshot: &Snapshot,
        topology: Option<Topology>,
        history: &History,
        activity: &Activity,
    ) {
        let text = to_toml_run(root, snapshot, topology, Some(self.run.clone()));
        write(&self.snapshot, &text, "snapshot");
        write(&self.history, &history_toml(root, history), "history");
        write(&self.activity, &activity_toml(root, activity), "activity");
    }
}

/// The scenarios named in `XMIP_PLAYGROUND_SCENARIOS`: none named is every
/// one. A name that is no scenario is REFUSED and the roll does not start —
/// until 2026-09-19 it was said on stderr and dropped, and the roll carried on
/// as something nobody asked for.
fn chosen_or_refuse() -> Vec<String> {
    environment::scenarios().unwrap_or_else(|refusal| {
        eprintln!("{refusal}");
        std::process::exit(2);
    })
}

/// Whether `RoundTrip` runs over role nodes rather than in this process: it
/// was chosen, and nodes are named by role. A role missing among them is
/// REFUSED before anything is spawned, naming the role.
fn relayed_or_refuse(chosen: &[String], names: &[String]) -> bool {
    let roster = Roster::of(names);
    if !drives(chosen, ROUND_TRIP) || !roster.has_roles() {
        return false;
    }
    if let Some(refusal) = roster.refusal() {
        eprintln!("{refusal}");
        std::process::exit(2);
    }
    true
}

/// One cluster process, told the nodes to spawn and the scenarios chosen,
/// over the shared directory it will own. Nothing is spawned when no node was
/// named; a cluster that cannot start is said so and the roll goes on without
/// one, as it did when the nodes were its own.
fn spawn_cluster(
    cluster: &str,
    stress: Stress,
    names: &[String],
    chosen: &[String],
    base: &Path,
    path: &Path,
) -> Option<Spawned> {
    if names.is_empty() {
        return None;
    }
    let shared = base.join("shared");
    let orders = Orders::of(stress, names, 0).driving(chosen);
    let started = cluster_binary()
        .and_then(|binary| Spawned::start(&binary, cluster, &orders, &shared, path));
    match started {
        Ok(spawned) => Some(spawned),
        Err(error) => {
            eprintln!("no cluster: {error}");
            None
        }
    }
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

/// The rows the board shows: every leaf, except that a node's leaves appear
/// only when not fine — the nodes' rollup row always does, and an operator
/// drills into a node from there.
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

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(ToString::to_string).collect()
    }

    #[test]
    fn round_trip_is_the_role_nodes_when_there_are_any_and_it_was_chosen() {
        let roles = names(&["R1", "P1", "S1"]);
        assert!(relayed_or_refuse(&[], &roles));
        assert!(relayed_or_refuse(&names(&["round-trip"]), &roles));
        assert!(!relayed_or_refuse(&names(&["heavy-load"]), &roles));
        assert!(!relayed_or_refuse(&[], &names(&["node-01", "node-02"])));
        assert!(!relayed_or_refuse(&[], &[]));
        // A role missing is no refusal when RoundTrip was not chosen.
        assert!(!relayed_or_refuse(&names(&["filing"]), &names(&["R1"])));
    }
}
