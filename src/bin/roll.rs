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
//! Each publishes under its own subtree of `xmip:///<cluster>`, merged into one
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
//! named for the cluster in `XMIP_PLAYGROUND_AREA` (the temp directory unset);
//! `roll --publication <cluster> <area>` prints where they are and starts
//! nothing. Every variable is read in one place, `environment.rs`.
//!
//! **The cluster is a process too** (the owner, 2026-09-19). The roll spawns
//! exactly one `xmip-playground-cluster` process, and that process spawns and
//! supervises one `xmip-playground-node` per node (ADR-0028 clause 2). The
//! roll merges the one file the cluster publishes into its own snapshot each
//! round; the board shows the nodes' rollup row, and a node's leaf only when
//! it is not fine.
//!
//! **The nodes are the cluster's `xmip.toml`** (ADR-0056, amendment
//! 2026-10-03): `XMIP_TEST_CLUSTER` names the file, and every node under its
//! `[nodes]` is one process, declaring the roles its `roles` says
//! (`environment::roster`). `Start-XmipTest` writes the run's file from
//! `-Cluster`, `-Nodes` and `-NodeRole`, or names the test cluster's where
//! `-Nodes` was not given; nothing is counted or numbered.
//! `XMIP_PLAYGROUND_ONLINE_NODES` names the ones among them that may assume
//! the internet (ADR-0045); unset, every node reads `XMIP_ONLINE`.
//!
//! **A node declares its roles** (ADR-0056, amendment 2026-10-01) —
//! `receiving`, `processing+sending`, or `executing` for all
//! three in one process — and every node runs its part of the scenarios
//! named. Where a node's roles serve a stage, `RoundTrip` is the nodes' —
//! each pair handed receive to process to send between the processes, or
//! kept in an executing node's one process — and the roll does not also run
//! it in-process; a stage no node serves is REFUSED at the start, naming the
//! role. A node serving no stage runs the shared-directory tests whole, as
//! before. A node's name decides none of this.
//!
//! **A real node is used where its name resolves** (ADR-0028, amendments
//! 2026-09-19 and 2026-10-21): the names the operator typed,
//! `XMIP_PLAYGROUND_NAMED`, and the cluster's, are resolved, and one that
//! answers as that node of this cluster over its operate listener, asked
//! with the operator certificate `XMIP_PLAYGROUND_CERTIFICATE` names, is
//! real — never spawned, configured or stopped, its publication read each
//! round; one that resolves and does not is REFUSED by name. The cluster
//! process is given only the simulated nodes, and `[run] real` and the
//! first lines say which are which (`real.rs`).
//!
//! **Every other node is started as `xmip-service`** where the run was
//! given one, `XMIP_PLAYGROUND_SERVICE`, and one of those nodes declares the
//! Storage role (ADR-0028, amendment 2026-10-22): the run gives each its
//! slice, its certificate, its addresses on loopback and its bindings
//! (`served.rs`), reads it as a real node, and drives it through its Receive
//! Locations each round (`carried.rs`); simulated where not.
//!
//! **A run with real nodes drives them through the Parties' far end**
//! (ADR-0028, amendment 2026-10-22): the rig serves SFTP on this computer
//! (`far_end.rs`), each Location the real nodes' environment placed on them
//! a user of its own there (`xmip-service --slice --bind`), drops into the
//! Receive Locations' and is told of what the Send Ports put
//! (`carried.rs`); no node is started as `xmip-service` beside them.
//!
//! **It audits** (ADR-0062): `start` once the roll knows what it is, `stop`
//! when its rounds are done, every refusal and failed write as a failure, and
//! every panic as `unhandled` — into `XMIP_AUDIT_DIRECTORY`, else the
//! operating system's log (`process_audit.rs`).

use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::time::Duration;

use observe::now_unix_nanos;
use observe::{History, Run};
use xaudit::program_audit::ProgramAudit;
use xmip_core_test_playground::carried::Carried;
use xmip_core_test_playground::cluster::{Orders, Spawned, cluster_binary, merge};
use xmip_core_test_playground::drop_box::Dropping;
use xmip_core_test_playground::environment::{self, max_seconds, time_factor};
use xmip_core_test_playground::far_end::FarEnd;
use xmip_core_test_playground::in_process::InProcess;
use xmip_core_test_playground::operator_orders;
use xmip_core_test_playground::publication::Publication;
use xmip_core_test_playground::real::{self, RealNode};
use xmip_core_test_playground::run::{announced, found};
use xmip_core_test_playground::scenario::{ROUND_TRIP, drives};
use xmip_core_test_playground::served::{self, Served};
use xmip_core_test_playground::{
    Budget, Headroom, Parties, Roster, Spread, Stress, cluster_name, cluster_root,
    cluster_topology, record_round, redraw, started, summarize,
};
use xmip_core_test_playground::{image, process_audit};

fn main() {
    // The name is the image's own — xmip-playground-<cluster>-roll where
    // Start-XmipTest linked one — so the declaration and the audit say what
    // Get-Process says (ADR-0053, amendment 2026-09-20).
    let called = image::this_process("xmip-playground-roll");
    let audit = ProgramAudit::new(&called, None);
    audit.watch_panics();

    if answered_publication(&audit) {
        return;
    }

    let (cluster, root, base) = or_refuse(&audit, this_cluster());
    let root = root.as_str();
    let _declared = declared(&audit, called, root);
    let stress = Stress::from_env();
    let chosen = or_refuse(&audit, environment::scenarios());
    let Nodes {
        real: real_nodes,
        names: real_names,
        roster,
        simulated,
        served,
        mut carried,
        far_end,
    } = or_refuse(&audit, nodes(&cluster, &base));
    let relayed = or_refuse(&audit, relayed(&chosen, &simulated));
    let run = started(&cluster, &chosen, &roster, stress, &real_names);

    let spread = Parties::configured().over_every_transport();
    let mut first = announced(&cluster, stress, &roster, &spread);
    first.extend(found(&real_nodes));
    first.extend(far_end.as_ref().map(FarEnd::said));
    for line in first {
        println!("{line}");
    }

    // Every scenario this process runs itself (`in_process.rs`).
    let mut in_process = InProcess::new(root, &base, stress, chosen.clone(), relayed);
    // The Parties' bursts into the drop box the receiving nodes share.
    let mut dropping = relayed.then(|| Dropping::new(&shared(&base)));

    // An hour of history at one point a second: enough to watch a shift, bounded
    // so a week-long run does not grow. ADR-0029.
    let mut history = History::with_capacity(3600);

    let limit: Option<u64> = std::env::args().nth(1).and_then(|arg| arg.parse().ok());
    // The nodes take an operator's orders under the directory they share,
    // and every snapshot says where (ADR-0065, amendment 2026-09-29; ADR-0013,
    // amendment 2026-09-30).
    let publication = Publication::of(&cluster, run.clone(), audit.clone())
        .taking_orders(&operator_orders::orders(&shared(&base)));
    say_started(&audit, (&run, &spread), limit, &publication.snapshot);

    // One cluster per roll (ADR-0028), and since 2026-09-19 a process of its
    // own: it spawns and supervises the nodes and publishes beside this
    // roll's snapshot, which merges that one file each round.
    let cluster_path = publication.beside(&format!("{cluster}-cluster.toml"));
    let orders = Orders::of(stress, simulated.clone(), 0).driving(&chosen);
    let mut spawned = spawn_cluster(&audit, &cluster, &orders, &base, &cluster_path);

    let live = std::io::stdout().is_terminal();
    let real = Duration::from_millis(1000);
    let budget = Budget::new(max_seconds(), time_factor());

    if !live {
        println!("publishing snapshots to {}", publication.snapshot.display());
    }

    let mut round: u64 = 0;
    let mut drawn_before = None;
    loop {
        round += 1;

        // What everyone else is using, measured now: the levels size this
        // round's pairs to half of what is left (ADR-0028, 2026-09-11). The
        // level's own count of nodes was sized the same way.
        let headroom = Headroom::refresh();

        let mut snapshot = in_process.tick(budget.simulated_elapsed());
        // The cluster's own file, and what the real nodes and those started
        // as xmip-service published, from both of which the topology draws.
        if let Some(spawned) = spawned.as_mut() {
            merge(&mut snapshot, spawned.tick());
        }
        real::published_into(&real_nodes, root, &mut snapshot, now_unix_nanos());
        let drawing = (spawned.as_ref(), !real_nodes.is_empty());
        let topology = drawn(
            &snapshot,
            drawing,
            (&roster, relayed, &spread),
            drawn_before.as_ref(),
        );
        drawn_before.clone_from(&topology);
        if let Some(carried) = carried.as_mut() {
            carried.round(root, &mut snapshot, now_unix_nanos());
        }

        // Every scope's series, and the rollup at the root that the history
        // file is written from: a scenario counts beneath the node, so a roll
        // that recorded only the snapshot published `points = []` for as long
        // as the Playground has had a history (curve.rs, 2026-09-19).
        if let Some(dropping) = dropping.as_mut() {
            dropping.round(root, &mut snapshot, now_unix_nanos());
        }
        record_round(&mut history, root, &snapshot);
        publication.round(root, &snapshot, topology, &history, in_process.activity());

        if live {
            redraw(root, round, &snapshot);
            if let Some(line) = found(&real_nodes) {
                println!("{line}");
            }
            println!("  publishing to {}", publication.snapshot.display());
        } else {
            summarize(root, round, &snapshot);
        }
        println!("  headroom: {}", headroom.describe());

        if limit.is_some_and(|limit| round >= limit) || budget.expired() {
            break;
        }
        std::thread::sleep(real);
    }

    let ran = Ran {
        round,
        dropping,
        carried,
    };
    leave(
        &audit,
        &cluster,
        &ran,
        (spawned, served),
        (&base, &cluster_path),
    );
}

/// What a roll did, said as it stops.
struct Ran {
    round: u64,
    dropping: Option<Dropping>,
    carried: Option<Carried>,
}

/// The roll of `cluster` leaving, having `ran`: the cluster process asked
/// to leave first, so it stops its own nodes on the way out and nothing is
/// orphaned; every file dropped into the nodes the run started given its
/// time to arrive, then those nodes drained before what they keep goes
/// with the run's `base`, and the cluster's file at `path`; the images go
/// with them, and the stop is audited.
fn leave(
    audit: &ProgramAudit,
    cluster: &str,
    ran: &Ran,
    (spawned, served): (Option<Spawned>, Option<Served>),
    (base, path): (&Path, &Path),
) {
    if let Some(mut spawned) = spawned {
        spawned.stop();
    }
    if let Some(carried) = &ran.carried {
        carried.settle();
    }
    if let Some(served) = served {
        served.stop();
    }
    std::fs::remove_dir_all(base).ok();
    std::fs::remove_file(path).ok();
    // This process still holds its own image, so Stop-XmipTest takes the
    // rest, and the next roll on this cluster clears the directory first.
    image::clear();
    let rounds = ran.round.to_string();
    let taken = ran
        .dropping
        .as_ref()
        .map(Dropping::words)
        .unwrap_or_default();
    let carried = ran.carried.as_ref().map(Carried::words).unwrap_or_default();
    process_audit::stop(
        audit,
        &[
            ("cluster", cluster),
            ("rounds", &rounds),
            ("drop-box", &taken),
            ("carried", &carried),
        ],
    );
}

/// The run's nodes, sorted: the real ones — a typed name that resolves and
/// answers, and every other node started as `xmip-service` where the run
/// was given one, driven through its Receive Locations — their names, the
/// roster, and the simulated nodes alone, which are all the cluster process
/// spawns.
struct Nodes {
    real: Vec<RealNode>,
    names: Vec<String>,
    roster: Roster,
    simulated: Roster,
    served: Option<Served>,
    carried: Option<Carried>,
    /// What the real nodes fetch from and put on, kept served for the run.
    far_end: Option<FarEnd>,
}

/// The nodes of the run of `cluster` beneath `base`, as [`Nodes`] sorts
/// them.
///
/// # Errors
///
/// REFUSED, as [`real::sorted`] and [`serve`] say.
fn nodes(cluster: &str, base: &Path) -> Result<Nodes, String> {
    let real::Sorted {
        mut real,
        mut names,
        roster,
        simulated,
    } = environment::roster().and_then(|all| real::sorted(cluster, all))?;
    // Nodes on machines of their own: driven through the Parties' far end
    // their environment bound them to, and none started here, which could
    // reach no Storage node of theirs (ADR-0028, amendment 2026-10-22).
    if !real.is_empty() {
        let arrived = Carried::arrivals();
        let far = FarEnd::serve(&names, &arrived)?;
        let carried = far
            .as_ref()
            .map(|far| Carried::new(&far.bindings, arrived))
            .transpose()?;
        return Ok(Nodes {
            real,
            names,
            roster,
            simulated,
            served: None,
            carried,
            far_end: far,
        });
    }
    let (served, started, simulated) = serve(cluster, &roster, simulated, base)?;
    names.extend(started.iter().map(|node| node.name.clone()));
    real.extend(started);
    let carried = served
        .as_ref()
        .map(|served| Carried::new(&served.bindings, Carried::arrivals()))
        .transpose()?;
    Ok(Nodes {
        real,
        names,
        roster,
        simulated,
        served,
        carried,
        far_end: None,
    })
}

/// The topology of this round's `snapshot`, where a cluster process ran
/// (`spawned`) or nodes real or started as xmip-service published
/// (`real`): a run with no cluster process too, whose every node is one of
/// those. The handoffs are the cluster process's alone. Each link's rate is
/// its rise since the round `before` (ADR-0052, amendment 2026-09-25).
fn drawn(
    snapshot: &observe::Snapshot,
    (spawned, real): (Option<&Spawned>, bool),
    (roster, relayed, spread): (&Roster, bool, &Spread),
    before: Option<&observe::Topology>,
) -> Option<observe::Topology> {
    (spawned.is_some() || real).then(|| {
        let hops = spawned.map_or(&[][..], Spawned::hops);
        let handed = relayed && spawned.is_some();
        let now = now_unix_nanos();
        let mut drawn = cluster_topology(snapshot, roster, handed, hops, spread, now);
        if let Some(before) = before {
            drawn.rate_since(before);
        }
        drawn
    })
}

/// Every node of `simulated` started as the `xmip-service` the run was
/// given (`environment::service`), and the real nodes the rig reads them
/// as; none, and every one still simulated, where it was given none or none
/// of them declares the Storage role, which the rest would reach.
///
/// # Errors
///
/// REFUSED where a node does not start, as [`Served::start`] says.
fn serve(
    cluster: &str,
    roster: &Roster,
    simulated: Roster,
    base: &Path,
) -> Result<(Option<Served>, Vec<RealNode>, Roster), String> {
    let Some(service) = environment::service() else {
        return Ok((None, Vec::new(), simulated));
    };
    let names: Vec<String> = simulated.names().into_iter().map(str::to_string).collect();
    if names.is_empty() || !served::stores(&names)? {
        return Ok((None, Vec::new(), simulated));
    }
    let (served, real) = Served::start((cluster, roster), &names, (base, &service))?;
    Ok((Some(served), real, simulated.without(&names)))
}

/// Asked where a cluster publishes, it answers the same way: the names are
/// decided here (`environment::publish_paths`), and `Start-XmipTest`
/// records what it is told rather than naming the files again. Whether it
/// was asked, and answered.
fn answered_publication(audit: &ProgramAudit) -> bool {
    if std::env::args().nth(1).as_deref() != Some("--publication") {
        return false;
    }
    let told: Vec<String> = std::env::args().skip(2).collect();
    println!("{}", or_refuse(audit, the_publication(&told)));
    true
}

/// What this process says of itself while it runs (ADR-0053): the roll is
/// the test over the cluster it starts, and everything the Playground runs
/// is test. Every record from here carries the same location, so a reader
/// knows whose it is (ADR-0062, amendment 2026-09-29). The declaration
/// stays while what this answers is held.
fn declared(audit: &ProgramAudit, called: String, root: &str) -> impl Sized {
    audit.locate(root);
    let declaration = ::node::Declaration::new(called, root, ::node::Purpose::Test);
    process_audit::declared(audit, declaration)
        .and_then(|declaration| declaration.declare().map_err(|error| error.to_string()))
        .map_err(|error| {
            let problem = format!("roll: could not declare itself: {error}");
            process_audit::fail(audit, "declare", &problem);
        })
}

/// The roll's `start` record: what the run was started with, as its `[run]`
/// header says it, the Parties it trades with, how many rounds, and where it
/// publishes.
fn say_started(audit: &ProgramAudit, ran: (&Run, &Spread), limit: Option<u64>, snapshot: &Path) {
    let (run, spread) = ran;
    let rounds = limit.map_or_else(|| "until stopped".to_string(), |limit| limit.to_string());
    process_audit::start(
        audit,
        &[
            ("cluster", &run.cluster),
            ("stress", &run.stress),
            ("tests", &run.tests.join(",")),
            ("nodes", &run.roles.join(",")),
            ("online", &run.online.join(",")),
            ("real", &run.real.join(",")),
            ("parties", &spread.parties().names().join(",")),
            ("rounds", &rounds),
            ("snapshot", &snapshot.display().to_string()),
        ],
    );
}

/// What the environment told this roll, or REFUSED: a value it cannot read is
/// said, audited as the failure to `start`, and the roll does not start —
/// until 2026-09-19 an unknown scenario was said on stderr and dropped, and
/// the roll carried on as something nobody asked for. A word that is no
/// role is refused the same way.
fn or_refuse<T>(audit: &ProgramAudit, told: Result<T, String>) -> T {
    told.unwrap_or_else(|refusal| {
        process_audit::fail(audit, "start", &refusal);
        std::process::exit(2);
    })
}

/// Where the roll of a cluster publishes in an area, one `name=path` line
/// each for the snapshot, the history and the activity. The cluster and the
/// area are the arguments and the environment is not read.
fn the_publication(told: &[String]) -> Result<String, String> {
    let [cluster, area] = told else {
        return Err("REFUSED: --publication takes a cluster and an area.".to_string());
    };
    let (snapshot, history, activity) = environment::publish_paths(cluster, Path::new(area));
    Ok(format!(
        "snapshot={}\nhistory={}\nactivity={}",
        snapshot.display(),
        history.display(),
        activity.display()
    ))
}

/// Whether `RoundTrip` runs across the cluster's nodes rather than in this
/// process: it was chosen, and some node's roles serve a stage of the path.
/// A stage no node serves is REFUSED before anything is spawned, naming the
/// role that went undeclared (ADR-0056).
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
    let shared = shared(base);
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

/// The directory the cluster owns and its nodes share, beneath the roll's.
fn shared(base: &Path) -> PathBuf {
    base.join("shared")
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
    use configure::fixture::test_cluster;

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(ToString::to_string).collect()
    }

    fn roster(text: &str) -> Roster {
        Roster::parse(text).expect("a well-formed roster")
    }

    #[test]
    fn round_trip_is_the_nodes_when_any_declares_a_stage_and_it_was_chosen() {
        let cluster = test_cluster();
        let [receiving, processing, sending] =
            ["receiving", "processing", "sending"].map(|role| &cluster.with_role(role).name);
        let path = roster(&format!(
            "{receiving}=receiving,{processing}=processing,{sending}=sending"
        ));
        assert_eq!(relayed(&[], &path), Ok(true));
        assert_eq!(relayed(&names(&["round-trip"]), &path), Ok(true));
        assert_eq!(relayed(&names(&["heavy-load"]), &path), Ok(false));
        let bare = roster(&format!("{receiving},{processing}"));
        assert_eq!(relayed(&[], &bare), Ok(false));
        assert_eq!(relayed(&[], &Roster::default()), Ok(false));
        // A capability missing is no refusal when RoundTrip was not chosen.
        let receives = roster(&format!("{receiving}=receiving"));
        assert_eq!(relayed(&names(&["filing"]), &receives), Ok(false));
        let refused = relayed(&[], &receives).expect_err("no node processes");
        assert!(refused.starts_with("REFUSED"), "{refused}");
    }

    #[test]
    fn the_publication_names_the_cluster_s_files_in_the_area() {
        let cluster = test_cluster().name;
        let said = the_publication(&names(&[&cluster, "area"])).expect("a cluster and an area");
        let area = Path::new("area");
        assert_eq!(
            said,
            format!(
                "snapshot={}\nhistory={}\nactivity={}",
                area.join(format!("{cluster}-snapshot.toml")).display(),
                area.join(format!("{cluster}-history.toml")).display(),
                area.join(format!("{cluster}-activity.toml")).display()
            )
        );
        let refused = the_publication(&names(&[&cluster])).expect_err("no area");
        assert!(refused.starts_with("REFUSED"), "{refused}");
    }
}
