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
//! **The cluster is a process too** (the owner, 2026-09-19). The roll spawns
//! exactly one `xmip-playground-cluster` process, and that process spawns and
//! supervises one `xmip-playground-node` per node (ADR-0028 clause 2). The
//! roll merges the one file the cluster publishes into its own snapshot each
//! round; the board shows the nodes' rollup row, and a node's leaf only when
//! it is not fine. `XMIP_PLAYGROUND_NODE_NAMES` names the nodes, comma
//! separated, one process each, at any level; `XMIP_PLAYGROUND_NODES` names a
//! count instead, numbered `node-01` up, of which `0` is none at all;
//! `XMIP_PLAYGROUND_ONLINE_NODES` names the ones among them that may assume
//! the internet (ADR-0045); unset, every node reads `XMIP_ONLINE`.
//!
//! **Told neither, the level brings its full complement** — `complement.rs`,
//! and the owner's rule of 2026-09-19 that an omitted selector means the most
//! the rig can give (ADR-0059). `roll --roster <level>` prints that
//! complement and starts nothing, which is how `Start-XmipTest` resolves an
//! omitted `-Nodes` at its own door.
//!
//! **A node declares what it can do** (ADR-0056).
//! `XMIP_PLAYGROUND_NODE_CAPABILITIES` says which stages of the message path
//! each node serves — `R1=receive,P1=process+send` — and every node runs its
//! part of the scenarios named. Where a node declares a stage, `RoundTrip` is
//! the nodes' — each pair handed receive to process to send between the
//! processes — and the roll does not also run it in-process; a stage no node
//! declares is REFUSED at the start, naming the capability. A node that
//! declares nothing runs the shared-directory tests whole, as before. A
//! node's name decides none of this.
//!
//! **It audits** (ADR-0062): `start` once the roll knows what it is, `stop`
//! when its rounds are done, every refusal and failed write as a failure, and
//! every panic as `unhandled` — into `XMIP_AUDIT_DIRECTORY`, else the
//! operating system's log (`process_audit.rs`).

use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::time::Duration;

use observe::{History, Run};
use xaudit::program_audit::ProgramAudit;
use xmip_core_test_playground::cluster::{Orders, Spawned, cluster_binary, merge};
use xmip_core_test_playground::environment::{self, max_seconds, time_factor};
use xmip_core_test_playground::in_process::InProcess;
use xmip_core_test_playground::publication::Publication;
use xmip_core_test_playground::scenario::{ROUND_TRIP, drives};
use xmip_core_test_playground::{
    Budget, Headroom, Roster, Stress, cluster_name, cluster_root, cluster_topology, complement,
    now_unix_nanos, record_round, redraw, started, summarise,
};
use xmip_core_test_playground::{image, process_audit};

fn main() {
    // The name is the image's own — xmip-playground-<cluster>-roll where
    // Start-XmipTest linked one — so the declaration and the audit say what
    // Get-Process says (ADR-0053, amendment 2026-09-20).
    let called = image::this_process("xmip-playground-roll");
    let audit = ProgramAudit::new(&called, None);
    audit.watch_panics();

    // Asked what a level brings, this process answers and starts nothing:
    // `Start-XmipTest` asks before it spawns, so an omitted `-Nodes` is
    // resolved once, at the operator's door, by the rig that owns the numbers.
    if std::env::args().nth(1).as_deref() == Some("--roster") {
        println!(
            "{}",
            or_refuse(&audit, the_complement(std::env::args().nth(2).as_deref()))
        );
        return;
    }

    let (cluster, root, base) = or_refuse(&audit, this_cluster());
    let root = root.as_str();

    // What this process says of itself while it runs (ADR-0053): the roll is
    // the test over the cluster it starts, and everything the Playground runs
    // is test.
    let _declared = ::node::Declaration::new(called, root, ::node::Purpose::Test)
        .declare()
        .map_err(|error| {
            let problem = format!("roll: could not declare itself: {error}");
            process_audit::fail(&audit, "declare", &problem);
        });
    let stress = Stress::from_env();
    let chosen = or_refuse(&audit, environment::scenarios());
    let roster = or_refuse(&audit, environment::roster(stress));
    let relayed = or_refuse(&audit, relayed(&chosen, &roster));
    let run = started(&cluster, &chosen, &roster, stress);

    announce(&cluster, stress, &roster);

    // Every scenario this process runs itself (`in_process.rs`).
    let mut in_process = InProcess::new(root, &base, stress, chosen.clone(), relayed);

    // An hour of history at one point a second: enough to watch a shift, bounded
    // so a week-long run does not grow. ADR-0029.
    let mut history = History::with_capacity(3600);

    let limit: Option<u64> = std::env::args().nth(1).and_then(|arg| arg.parse().ok());
    let publication = Publication::of(&cluster, run.clone(), audit.clone());
    say_started(&audit, &run, limit, &publication.snapshot);

    // One cluster per roll (ADR-0028), and since 2026-09-19 a process of its
    // own: it spawns and supervises the nodes and publishes beside this
    // roll's snapshot, which merges that one file each round.
    let cluster_path = publication.beside(&format!("{cluster}-cluster.toml"));
    let orders = Orders::of(stress, roster.clone(), 0).driving(&chosen);
    let mut spawned = spawn_cluster(&audit, &cluster, &orders, &base, &cluster_path);

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

        let mut snapshot = in_process.tick(budget.simulated_elapsed());
        // The cluster's own file, and from it what the topology draws.
        let topology = spawned.as_mut().map(|spawned| {
            merge(&mut snapshot, spawned.tick());
            let named = roster.names().into_iter();
            cluster_topology(&snapshot, named, spawned.hops(), now_unix_nanos())
        });

        // Every scope's series, and the rollup at the root that the history
        // file is written from: a scenario counts beneath the node, so a roll
        // that recorded only the snapshot published `points = []` for as long
        // as the Playground has had a history (curve.rs, 2026-09-19).
        record_round(&mut history, root, &snapshot);
        publication.round(root, &snapshot, topology, &history, in_process.activity());

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
    // The images the cluster and its nodes ran under go with them. This
    // process still holds its own, so Stop-XmipTest takes the rest, and the
    // next roll on this cluster clears the directory before it starts.
    image::clear();
    let rounds = round.to_string();
    process_audit::stop(&audit, &[("cluster", &cluster), ("rounds", &rounds)]);
}

/// The roll's `start` record: what the run was started with, as its `[run]`
/// header says it, how many rounds, and where it publishes.
fn say_started(audit: &ProgramAudit, run: &Run, limit: Option<u64>, snapshot: &Path) {
    let rounds = limit.map_or_else(|| "until stopped".to_string(), |limit| limit.to_string());
    process_audit::start(
        audit,
        &[
            ("cluster", &run.cluster),
            ("stress", &run.stress),
            ("tests", &run.tests.join(",")),
            ("nodes", &run.capabilities.join(",")),
            ("online", &run.online.join(",")),
            ("rounds", &rounds),
            ("snapshot", &snapshot.display().to_string()),
        ],
    );
}

/// What the environment told this roll, or REFUSED: a value it cannot read is
/// said, audited as the failure to `start`, and the roll does not start —
/// until 2026-09-19 an unknown scenario was said on stderr and dropped, and
/// the roll carried on as something nobody asked for. A capability that is
/// no capability is refused the same way.
fn or_refuse<T>(audit: &ProgramAudit, told: Result<T, String>) -> T {
    told.unwrap_or_else(|refusal| {
        process_audit::fail(audit, "start", &refusal);
        std::process::exit(2);
    })
}

/// The first lines of a run: the level, the cluster and the roster it
/// resolved to. A run nobody gave switches to is still told from the one
/// before it (ADR-0059, amendment 2026-09-19), and where an omitted `-Nodes`
/// brought a complement too small for the message path, that is said here
/// rather than left to be noticed. The board clears a live terminal every
/// round; a redirected log keeps these lines, and the `[run]` table of every
/// snapshot carries the same answer.
fn announce(cluster: &str, stress: Stress, roster: &Roster) {
    println!(
        "roll at {} as cluster {cluster}: {}",
        stress.name(),
        complement::describe(roster)
    );
    if !roster.is_empty() {
        println!("  roster: {}", roster.text());
    }
}

/// What a level brings when nobody names nodes, printed as `--nodes` takes it
/// back: `node-01=receive,node-02=process,…`. The level is the argument and
/// the environment is not read, so the answer is the level's alone; an unknown
/// one is REFUSED naming the four (ADR-0055). `Start-XmipTest` asks this, sets
/// the names it gets, and records them, so the operator's door and the roll
/// agree on one roster and the numbers stay in `stress.rs` alone.
fn the_complement(level: Option<&str>) -> Result<String, String> {
    let Some(stress) = level.and_then(Stress::parse) else {
        return Err(format!(
            "REFUSED: --roster takes a stress level; '{}' is none. The levels are {}.",
            level.unwrap_or_default(),
            Stress::NAMES.join(", ")
        ));
    };
    Ok(complement::full(stress).text())
}

/// Whether `RoundTrip` runs across the cluster's nodes rather than in this
/// process: it was chosen, and some node declared a stage of the path. A stage
/// no node declares is REFUSED before anything is spawned, naming the
/// capability that went undeclared (ADR-0056).
fn relayed(chosen: &[String], roster: &Roster) -> Result<bool, String> {
    if !drives(chosen, ROUND_TRIP) || !roster.serves_any_stage() {
        return Ok(false);
    }
    roster.refusal().map_or(Ok(true), Err)
}

/// One cluster process, told the nodes to spawn and the scenarios chosen,
/// over the shared directory it will own. Nothing is spawned when no node was
/// named; a cluster that cannot start is said so, audited as the failure to
/// `spawn-cluster`, and the roll goes on without one, as it did when the
/// nodes were its own.
fn spawn_cluster(
    audit: &ProgramAudit,
    cluster: &str,
    orders: &Orders,
    base: &Path,
    path: &Path,
) -> Option<Spawned> {
    if orders.roster.is_empty() {
        return None;
    }
    let shared = base.join("shared");
    let started =
        cluster_binary().and_then(|binary| Spawned::start(&binary, cluster, orders, &shared, path));
    match started {
        Ok(spawned) => Some(spawned),
        Err(error) => {
            process_audit::fail(audit, "spawn-cluster", &format!("no cluster: {error}"));
            None
        }
    }
}

/// One cluster per roll (ADR-0028): its name, its scope root, and its own
/// scratch directory, emptied now, so a second cluster beside it neither wipes
/// nor shares this one's directories. REFUSED when the owner named none.
fn this_cluster() -> Result<(String, String, PathBuf), String> {
    let Some(cluster) = cluster_name() else {
        return Err(
            "REFUSED: a roll is a cluster and the owner names it; set XMIP_PLAYGROUND_CLUSTER \
             (Start-XmipTest -Cluster <name>). A test spawns nodes, never a cluster."
                .to_string(),
        );
    };
    let base = std::env::temp_dir().join("playground").join(&cluster);
    std::fs::remove_dir_all(&base).ok();
    Ok((cluster, cluster_root(), base))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(ToString::to_string).collect()
    }

    fn roster(text: &str) -> Roster {
        Roster::parse(text).expect("a well-formed roster")
    }

    #[test]
    fn round_trip_is_the_nodes_when_any_declares_a_stage_and_it_was_chosen() {
        let path = roster("R1=receive,P1=process,S1=send");
        assert_eq!(relayed(&[], &path), Ok(true));
        assert_eq!(relayed(&names(&["round-trip"]), &path), Ok(true));
        assert_eq!(relayed(&names(&["heavy-load"]), &path), Ok(false));
        assert_eq!(relayed(&[], &roster("node-01,node-02")), Ok(false));
        assert_eq!(relayed(&[], &Roster::default()), Ok(false));
        // A capability missing is no refusal when RoundTrip was not chosen.
        assert_eq!(
            relayed(&names(&["filing"]), &roster("R1=receive")),
            Ok(false)
        );
        let refused = relayed(&[], &roster("R1=receive")).expect_err("no node processes");
        assert!(refused.starts_with("REFUSED"), "{refused}");
    }

    #[test]
    fn an_unknown_level_is_refused_naming_the_levels() {
        let refused = the_complement(Some("gentle")).expect_err("gentle is no level");
        assert!(
            refused.contains("gentle") && refused.contains("brutal"),
            "{refused}"
        );
        assert!(the_complement(Some("calm")).is_ok());
    }
}
