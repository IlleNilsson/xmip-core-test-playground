//! The cluster: its nodes as processes, spawned and watched.
//!
//! ADR-0028 clause 2: nodes run as System Processes, and a process that hangs
//! is killed and restarted like any other Host Service. Until 2026-09-09 no
//! scenario spawned one; the owner's requirement — *about 10-40 processes
//! emulating nodes* — is this. A [`Cluster`] starts one copy of the `node`
//! binary per node beside the current executable, each told the tests that
//! were named and every node's name, all over one shared directory, so the
//! contention and the handoffs are between processes, and each publishing its
//! own snapshot file. 2026-09-19, the owner: what the Playground spawns is a
//! cluster and its nodes; the word this file was named for until then is
//! retired.
//!
//! 2026-09-19, the owner again: *even clusters have to be spawned as processes
//! during tests.* This is that process's library. `xmip-playground-cluster`
//! (`src/bin/cluster.rs`) is a [`Cluster`] and nothing else; the roll holds it
//! from outside as a [`Spawned`] and reads the one file it publishes.
//!
//! [`Cluster::tick`] is the surface's half of ADR-0027 decision 8: a node
//! answers for itself, and the cluster view is assembled by whoever asks each
//! node. Every node's latest file is read and merged — scopes are disjoint per
//! node — and the cluster adds what no node can say about itself: a health
//! record per node (alive, exited with its code, or hung) and the rollup at
//! `xmip:///<cluster>/node`, worst of all. A node that stops beating is hung:
//! it is killed and restarted, and the restart is recorded as a fault — a
//! yellow that stays for the cluster's life — never silently. Liveness is
//! beats, never rounds (`liveness.rs`, the owner 2026-09-26): a node beats
//! within milliseconds of its start and every tenth of a second while it
//! works, a node that has not beaten is starting for ten seconds and hung
//! after, and one that has is hung after ten seconds without a beat. A
//! brutal first round that runs for minutes is a node at work, not a hang
//! (2026-09-25: judging by rounds restarted every node into the same wait,
//! over and over).

mod binary;
mod liveness;
mod member;
mod orders;
mod rollup;
mod spawned;

use std::io;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use observe::Snapshot;

#[cfg(test)]
pub(crate) use binary::built_cluster_binary;
#[cfg(test)]
pub(crate) use binary::built_node_binary;
pub use binary::{cluster_binary, node_binary};
pub(crate) use liveness::Liveness;
use member::Member;
pub use orders::Orders;
pub use rollup::merge;
use rollup::rollup;
pub use spawned::Spawned;

use crate::handoff::Hop;
use crate::stress::Stress;
use crate::support::now_unix_nanos;

/// The fixture root this crate's tests publish under. A roll is a cluster the
/// owner named; a test spawns nodes, never a cluster (ADR-0052, 2026-09-14).
pub const ROOT: &str = "xmip:///playground";
/// How long a tick waits for every live node to publish something new before
/// it judges with what it has.
const GRACE: Duration = Duration::from_secs(2);
/// How long `stop` waits for the nodes to leave on their own.
const STOP_WAIT: Duration = Duration::from_secs(5);

/// The node processes, and what the cluster knows about each.
pub struct Cluster {
    binary: PathBuf,
    shared: PathBuf,
    /// Who the nodes are and what they were told — the one value the cluster
    /// hands on to every node it starts.
    orders: Orders,
    /// How a node shows it is alive, and when its silence is a hang.
    liveness: Liveness,
    nodes: Vec<Member>,
    /// The cluster's view as last assembled, rebuilt only when something in
    /// it changed: a node's round read, or what the cluster says of a
    /// node's process.
    view: Snapshot,
    /// What the cluster last said of each node's process.
    said: Vec<(observe::Health, String)>,
    /// Whether the last tick changed the view.
    changed: bool,
}

impl Cluster {
    /// Spawn one node process per name in `orders` over `shared`, each
    /// publishing to `snapshots/<name>.toml` and each running an image named
    /// for the cluster the orders are for and for itself.
    ///
    /// # Errors
    ///
    /// When a process cannot be started.
    pub fn spawn(
        binary: &Path,
        orders: &Orders,
        shared: &Path,
        snapshots: &Path,
    ) -> io::Result<Self> {
        Self::judged(binary, orders, shared, snapshots, Liveness::OWNERS)
    }

    /// [`Cluster::spawn`], judging liveness by `liveness`.
    fn judged(
        binary: &Path,
        orders: &Orders,
        shared: &Path,
        snapshots: &Path,
        liveness: Liveness,
    ) -> io::Result<Self> {
        std::fs::create_dir_all(shared)?;
        std::fs::create_dir_all(snapshots)?;
        std::fs::remove_file(shared.join("stop")).ok();

        let mut cluster = Self {
            binary: binary.to_path_buf(),
            shared: shared.to_path_buf(),
            orders: orders.clone(),
            liveness,
            nodes: Vec::new(),
            view: Snapshot::new(),
            said: Vec::new(),
            changed: false,
        };
        for name in orders.names().iter().map(ToString::to_string) {
            let path = snapshots.join(format!("{name}.toml"));
            let child = cluster.start(&name, &path)?;
            cluster.nodes.push(Member::started(name, child, path));
        }
        Ok(cluster)
    }

    /// Start the node called `name` — the orders say what it is declared with
    /// (ADR-0056), whether it may assume the internet, and what tests it is
    /// told to run. Nothing about it is worked out from its name.
    fn start(&self, name: &str, path: &Path) -> io::Result<Child> {
        /// How often a node ticks. A quarter second where a test wants
        /// contention now; two seconds in a brutal roll, where forty nodes
        /// ticking four times a second burned five cores between them
        /// (2026-09-11) and the budget is half of what is free.
        fn node_interval_ms(stress: Stress) -> u64 {
            match stress {
                Stress::Calm | Stress::Realistic => 250,
                Stress::Harsh => 1_000,
                Stress::Brutal => 2_000,
            }
        }

        let orders = &self.orders;
        // One image per node, so the operating system's list says which node
        // of which cluster each row is (ADR-0053, amendment 2026-09-20).
        let image = crate::image::of_node(&self.binary, &orders.cluster, name)?;
        Command::new(image)
            .args(["--name", name, "--stress", orders.stress.name()])
            .args(["--rounds", &orders.rounds.to_string()])
            .args([
                "--interval-ms",
                &node_interval_ms(orders.stress).to_string(),
            ])
            // The node beats as often as this cluster judges it by.
            .args(["--beat-ms", &self.liveness.beat.as_millis().to_string()])
            .args(["--nodes", &orders.roster.text()])
            // None named is every scenario, which is what no flag means.
            .args(
                (!orders.scenarios.is_empty())
                    .then(|| ["--scenarios".to_string(), orders.scenarios.join(",")])
                    .into_iter()
                    .flatten(),
            )
            .args(crate::capability::flags(&orders.capability(name)))
            .arg("--shared")
            .arg(&self.shared)
            .arg("--snapshot")
            .arg(path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .spawn()
    }

    /// One round of the surface: wait, within a grace period, for every live
    /// node to publish anew; restart the hung; and, where anything changed,
    /// merge what each published and add the per-node health and the rollup.
    /// [`Cluster::changed`] says whether it did; an unchanged view is not
    /// assembled, or published, again.
    pub fn tick(&mut self) -> &Snapshot {
        let started = Instant::now();
        for node in &mut self.nodes {
            node.fresh = false;
        }
        loop {
            self.read_all();
            let all_fresh = self.nodes.iter().all(|node| node.fresh || !node.alive());
            if all_fresh || started.elapsed() > GRACE {
                break;
            }
            std::thread::sleep(Duration::from_millis(25));
        }

        for index in 0..self.nodes.len() {
            self.judge_process(index);
        }

        let now = now_unix_nanos();
        let records: Vec<_> = self
            .nodes
            .iter()
            .map(|node| node.process_record(now))
            .collect();
        let said: Vec<_> = records
            .iter()
            .map(|record| (record.health, record.evidence.clone()))
            .collect();
        self.changed = said != self.said || self.nodes.iter().any(|node| node.fresh);
        if self.changed {
            let mut view = Snapshot::new();
            for (node, record) in self.nodes.iter().zip(records) {
                merge(&mut view, &node.published);
                view.record_health(record);
            }
            view.record_health(rollup(&view, self.nodes.len(), now));
            self.view = view;
            self.said = said;
        }
        &self.view
    }

    /// The view as the last [`Cluster::tick`] left it.
    #[must_use]
    pub const fn view(&self) -> &Snapshot {
        &self.view
    }

    /// Whether the last [`Cluster::tick`] changed the view — what the
    /// cluster process publishes on, and only on.
    #[must_use]
    pub const fn changed(&self) -> bool {
        self.changed
    }

    /// Read every node's file.
    fn read_all(&mut self) {
        for node in &mut self.nodes {
            node.read();
        }
    }

    /// Reap an exit, and restart a node that stopped beating.
    fn judge_process(&mut self, index: usize) {
        let node = &mut self.nodes[index];
        node.reap();
        // The beat as it is now: reading every publication can take the
        // cluster longer than a node may stay silent, and that is not the
        // node's silence.
        node.listen();
        if node.hung(&self.liveness) {
            node.kill();
            node.restarts += 1;
            let (name, path) = (node.name.clone(), node.path.clone());
            match self.start(&name, &path) {
                Ok(child) => self.nodes[index].restarted(child),
                Err(error) => eprintln!("cluster: could not restart {name}: {error}"),
            }
        }
    }

    /// Ask every node to leave — the stop file — wait for them, and kill what
    /// remains after the wait.
    pub fn stop(&mut self) {
        std::fs::write(self.shared.join("stop"), b"stop").ok();
        let started = Instant::now();
        while self.alive() > 0 && started.elapsed() < STOP_WAIT {
            for node in &mut self.nodes {
                node.reap();
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        for node in &mut self.nodes {
            node.kill();
        }
    }

    /// How many node processes are still running.
    #[must_use]
    pub fn alive(&self) -> usize {
        self.nodes.iter().filter(|node| node.alive()).count()
    }

    /// The nodes' names, `node-01` up.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.nodes.iter().map(|node| node.name.as_str())
    }

    /// How many restarts the cluster has recorded, over every node.
    #[must_use]
    pub fn restarts(&self) -> u32 {
        self.nodes.iter().map(|node| node.restarts).sum()
    }

    /// The handoffs every node says it delivered, per link — what the
    /// topology draws between the nodes.
    #[must_use]
    pub fn hops(&self) -> Vec<Hop> {
        self.nodes
            .iter()
            .flat_map(|node| node.hops.iter().cloned())
            .collect()
    }
}

impl Drop for Cluster {
    /// A failing test leaves no orphans.
    fn drop(&mut self) {
        for node in &mut self.nodes {
            node.kill();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::heartbeat::{self, Beat};
    use crate::support::scratch;
    use observe::{Health, HealthRecord};

    fn spawn(stress: Stress, count: usize, rounds: u64) -> (PathBuf, Cluster) {
        judged(stress, count, rounds, Liveness::OWNERS)
    }

    fn judged(stress: Stress, count: usize, rounds: u64, liveness: Liveness) -> (PathBuf, Cluster) {
        let dir = scratch("cluster");
        let cluster = Cluster::judged(
            &built_node_binary(),
            &Orders::numbered(stress, count, rounds),
            &dir.join("shared"),
            &dir.join("snapshots"),
            liveness,
        )
        .expect("the cluster spawns");
        (dir, cluster)
    }

    /// Every node's snapshot arrives, the rollup exists, no claim across
    /// processes was double-picked, and stop leaves no child running.
    #[test]
    fn a_realistic_cluster_publishes_rolls_up_and_stops_clean() {
        let (dir, mut cluster) = spawn(Stress::Calm, Stress::Realistic.nodes(), 0);
        assert_eq!(cluster.alive(), 3);

        let mut snapshot = cluster.tick().clone();
        snapshot = merge_into(snapshot, cluster.tick());

        for name in ["node-01", "node-02", "node-03"] {
            let claim = format!("{ROOT}/node/{name}/exclusive-claim/file");
            let verdicts = snapshot.health(&claim);
            assert_eq!(verdicts.len(), 3, "{name}: three styles published");
            for record in verdicts {
                assert!(
                    !record.evidence.contains("holders at once"),
                    "{}: {}",
                    record.scope,
                    record.evidence
                );
            }
            assert!(
                snapshot
                    .worst(&format!("{ROOT}/node/{name}/daily-backlog"))
                    .is_some(),
                "{name}: the DailyBacklog drain published"
            );
            assert_eq!(
                snapshot.worst(&format!("{ROOT}/node/{name}/system-process")),
                Some(Health::Fine),
                "{name} is alive"
            );
        }
        let rollup = snapshot_record(&snapshot, &format!("{ROOT}/node"));
        assert!(
            rollup.evidence.starts_with("3 nodes"),
            "{}",
            rollup.evidence
        );

        cluster.stop();
        assert_eq!(cluster.alive(), 0, "stop leaves no child running");
        assert_eq!(cluster.restarts(), 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_node_that_exits_is_reported_with_its_code_and_the_cluster_stays_up() {
        let (dir, mut cluster) = spawn(Stress::Calm, 1, 1);
        let mut last = cluster.tick().clone();
        for _ in 0..8 {
            last = cluster.tick().clone();
            if cluster.alive() == 0 {
                break;
            }
        }
        let process = snapshot_record(&last, &format!("{ROOT}/node/node-01/system-process"));
        assert_eq!(process.health, Health::Fine, "{}", process.evidence);
        assert!(
            process.evidence.starts_with("exited 0"),
            "{}",
            process.evidence
        );
        cluster.stop();
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Liveness quick enough for a test: a beat every tenth of a second,
    /// three missed, and `starting` to show it is alive.
    fn quick(starting: Duration) -> Liveness {
        Liveness {
            beat: Duration::from_millis(100),
            missed: 3,
            starting,
        }
    }

    /// The owner, 2026-09-26: a node that stops beating is hung, and the
    /// restart is a yellow.
    #[test]
    fn a_node_that_stops_beating_is_restarted_and_the_restart_is_a_yellow() {
        let (dir, mut cluster) = judged(Stress::Calm, 1, 0, quick(Duration::from_secs(5)));
        // Once it has beaten, point the cluster at a copy of the node's file
        // with a beat beside it that is never written again; the real node is
        // then "hung".
        cluster.tick();
        let frozen = dir.join("frozen.toml");
        std::fs::copy(&cluster.nodes[0].path, &frozen).expect("a frozen copy");
        cluster.nodes[0].path = frozen;
        let started = Instant::now();
        let mut last = cluster.tick().clone();
        while cluster.restarts() == 0 && started.elapsed() < Duration::from_secs(10) {
            last = cluster.tick().clone();
        }
        assert_eq!(
            cluster.restarts(),
            1,
            "silent past its missed beats is hung"
        );
        let process = snapshot_record(&last, &format!("{ROOT}/node/node-01/system-process"));
        assert_eq!(process.health, Health::Stressed, "{}", process.evidence);
        assert!(process.evidence.contains("hung"), "{}", process.evidence);
        let rollup = snapshot_record(&last, &format!("{ROOT}/node"));
        assert_ne!(rollup.health, Health::Fine, "the rollup carries the fault");
        cluster.stop();
        assert_eq!(cluster.alive(), 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The owner, 2026-09-26: a node that beats is alive while its first
    /// round runs, however long past `starting` and past the missed beats
    /// that round takes, and it is said to be starting.
    #[test]
    fn a_node_that_beats_but_has_not_finished_a_round_is_never_restarted() {
        let (dir, mut cluster) = judged(Stress::Calm, 1, 0, quick(Duration::from_secs(1)));
        // A publication nobody writes and a beat only this test writes:
        // beats, and never a finished round.
        let beating = dir.join("beating.toml");
        cluster.nodes[0].path.clone_from(&beating);
        std::fs::create_dir_all(&dir).expect("the scratch directory");
        let scope = format!("{ROOT}/node/node-01");
        let beater = std::thread::spawn(move || {
            let started = Instant::now();
            while started.elapsed() < Duration::from_secs(4) {
                let beat = Beat {
                    unix_nanos: now_unix_nanos(),
                    rounds: 0,
                };
                let text = toml::to_string(&beat).expect("a beat serialises");
                std::fs::write(heartbeat::beside(&beating), text).ok();
                std::thread::sleep(Duration::from_millis(50));
            }
        });
        let started = Instant::now();
        let mut last = Snapshot::new();
        while started.elapsed() < Duration::from_secs(3) {
            last = cluster.tick().clone();
        }
        beater.join().expect("the beats were written");
        assert_eq!(cluster.restarts(), 0, "a beating node is not hung");
        assert_eq!(cluster.alive(), 1);
        let process = snapshot_record(&last, &format!("{scope}/system-process"));
        assert!(
            process.evidence.starts_with("starting: beating"),
            "{}",
            process.evidence
        );
        cluster.stop();
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The owner, 2026-09-26: a node that never beats is starting until its
    /// allowance is over — the owner's ten seconds, here five — and then it
    /// is restarted.
    #[test]
    fn a_node_that_never_beats_is_restarted_once_starting_is_over() {
        let (dir, mut cluster) = judged(Stress::Calm, 1, 0, quick(Duration::from_secs(5)));
        // A file the node never writes: as far as the cluster can tell, it
        // has not beaten once.
        cluster.nodes[0].path = dir.join("never-written.toml");
        let process = format!("{ROOT}/node/node-01/system-process");
        cluster.tick();
        let last = cluster.tick().clone();
        assert_eq!(cluster.restarts(), 0, "starting, within its allowance");
        let record = snapshot_record(&last, &process);
        assert!(
            record.evidence.starts_with("starting, no beat"),
            "{}",
            record.evidence
        );
        cluster.tick();
        cluster.tick();
        assert_eq!(cluster.restarts(), 1, "silent past starting is hung");
        cluster.stop();
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The owner, 2026-09-26: more than a millisecond, apart from load, is
    /// wrong. A pass over idle nodes reads each one's beat — a few bytes —
    /// and neither reads nor parses a publication whose round has not moved;
    /// on 2026-09-26 the pass re-read every node's whole file and one pass
    /// over nineteen brutal nodes took longer than ten seconds.
    #[test]
    fn a_pass_over_idle_nodes_reads_their_beats_and_parses_nothing() {
        const NODES: usize = 4;
        const PASSES: u32 = 100;
        let (dir, mut cluster) = spawn(Stress::Calm, NODES, 0);
        std::fs::create_dir_all(&dir).expect("the scratch directory");
        // A publication the size a brutal node writes, and a beat saying its
        // first round is done, each only this test writes.
        let mut published = Snapshot::new();
        for leaf in 0..2_000 {
            published.record_health(HealthRecord {
                scope: format!("{ROOT}/node/idle/exclusive-claim/file/{leaf}"),
                health: Health::Fine,
                severity: 0,
                evidence: "one holder at a time".to_string(),
                observed_unix_nanos: 7,
            });
        }
        let text = crate::report::node_toml(&format!("{ROOT}/node/idle"), &published, Vec::new());
        let beat = toml::to_string(&Beat {
            unix_nanos: 7,
            rounds: 1,
        })
        .expect("a beat serialises");
        for (index, node) in cluster.nodes.iter_mut().enumerate() {
            node.path = dir.join(format!("idle-{index}.toml"));
            std::fs::write(&node.path, &text).expect("a publication");
            std::fs::write(heartbeat::beside(&node.path), &beat).expect("a beat");
        }
        cluster.read_all();
        assert!(cluster.nodes.iter().all(|node| node.parses == 1));

        let started = Instant::now();
        for _ in 0..PASSES {
            cluster.read_all();
        }
        let per_node = started.elapsed() / (PASSES * u32::try_from(NODES).expect("few"));
        assert!(
            cluster.nodes.iter().all(|node| node.parses == 1),
            "an idle node's publication is not parsed again"
        );
        assert!(
            per_node < Duration::from_millis(1),
            "a pass cost {per_node:?} per idle node"
        );
        cluster.stop();
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The owner's shape, 2026-09-19: nodes run the test that was named, and
    /// a node serves the stages it declared. `RoundTrip` alone over three
    /// nodes whose **names say nothing** is three processes handing each pair
    /// on, each publishing its own stage, the hops recorded per link — and
    /// neither shared-directory test runs.
    #[test]
    fn declared_nodes_run_the_named_test_and_hand_each_pair_along_the_path() {
        let dir = scratch("cluster-declared");
        let roster = crate::Roster::parse("alpha=receive,beta=process,gamma=send")
            .expect("a well-formed roster");
        let orders = Orders::of(Stress::Calm, roster, 0).driving(&["round-trip".to_string()]);
        let mut cluster = Cluster::spawn(
            &built_node_binary(),
            &orders,
            &dir.join("shared"),
            &dir.join("snapshots"),
        )
        .expect("the cluster spawns");

        let mut snapshot = cluster.tick().clone();
        for _ in 0..40 {
            snapshot = merge_into(snapshot, cluster.tick());
            if !snapshot
                .health(&format!("{ROOT}/node/gamma/send"))
                .is_empty()
            {
                break;
            }
        }
        for (name, stage) in [("alpha", "receive"), ("beta", "process"), ("gamma", "send")] {
            let leaves = snapshot.health(&format!("{ROOT}/node/{name}/{stage}"));
            assert!(!leaves.is_empty(), "{name} published its {stage} stage");
            for other in [
                "receive",
                "process",
                "send",
                "exclusive-claim",
                "daily-backlog",
            ] {
                let foreign = snapshot.health(&format!("{ROOT}/node/{name}/{other}"));
                assert!(other == stage || foreign.is_empty(), "{name} ran {other}");
            }
        }
        let links: Vec<(String, String)> = cluster
            .hops()
            .into_iter()
            .inspect(|hop| assert!(hop.count > 0))
            .map(|hop| (hop.from, hop.to))
            .collect();
        assert!(
            links.contains(&("alpha".to_string(), "beta".to_string())),
            "{links:?}"
        );
        assert!(
            links.contains(&("beta".to_string(), "gamma".to_string())),
            "{links:?}"
        );

        cluster.stop();
        assert_eq!(cluster.alive(), 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The owner, 2026-09-26: *if something takes more than a millisecond,
    /// apart from load, something is wrong.* A node's first beat is the first
    /// thing it does, so it lands within milliseconds of the process being
    /// asked to start — measured from the spawn, which counts the operating
    /// system's own start-up, so the bound is generous; it is not seconds.
    #[test]
    fn a_node_beats_within_milliseconds_of_its_start() {
        let dir = scratch("cluster-first-beat");
        let snapshot = dir.join("alpha.toml");
        let binary = built_node_binary();
        // Once refused first: the operating system's scan of an image it has
        // not run before is the machine's load, not the node's start.
        Command::new(&binary).args(["--refused", "x"]).output().ok();
        let asked = now_unix_nanos();
        let mut child = Command::new(binary)
            .args(["--name", "alpha", "--stress", "calm", "--rounds", "0"])
            .args(["--beat-ms", "60000"])
            .arg("--shared")
            .arg(dir.join("shared"))
            .arg("--snapshot")
            .arg(&snapshot)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .spawn()
            .expect("the node binary runs");
        let waiting = Instant::now();
        let beat = loop {
            let text = std::fs::read_to_string(heartbeat::beside(&snapshot)).ok();
            if let Some(beat) = text.as_deref().and_then(Beat::read) {
                break beat;
            }
            assert!(waiting.elapsed() < Duration::from_secs(10), "no beat");
            std::thread::sleep(Duration::from_millis(1));
        };
        std::fs::write(dir.join("shared").join("stop"), b"stop").ok();
        child.kill().ok();
        child.wait().ok();
        let took = Duration::from_nanos((beat.unix_nanos - asked).unsigned_abs());
        assert_eq!(beat.rounds, 0, "before its first round");
        assert!(
            took < Duration::from_secs(1),
            "the first beat took {took:?}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_node_told_an_unknown_scenario_is_refused_with_exit_code_two() {
        let dir = scratch("cluster-refused");
        let output = Command::new(built_node_binary())
            .args(["--name", "alpha", "--stress", "calm", "--rounds", "1"])
            .args(["--can", "receive"])
            .args(["--scenarios", "round-trip,pingpong"])
            .arg("--shared")
            .arg(dir.join("shared"))
            .arg("--snapshot")
            .arg(dir.join("alpha.toml"))
            .output()
            .expect("the node binary runs");
        assert_eq!(output.status.code(), Some(2));
        let said = String::from_utf8_lossy(&output.stderr);
        assert!(said.contains("REFUSED"), "{said}");
        assert!(
            said.contains("pingpong") && said.contains("daily-backlog"),
            "{said}"
        );
        assert!(!dir.join("alpha.toml").exists(), "nothing ran");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Forty processes, the owner's ceiling. For the runner, not the gate.
    #[test]
    #[ignore = "forty processes; run on purpose"]
    fn brutal_cluster_of_forty() {
        let (dir, mut cluster) = spawn(Stress::Brutal, Stress::Brutal.nodes(), 0);
        let mut snapshot = cluster.tick().clone();
        for _ in 0..4 {
            snapshot = merge_into(snapshot, cluster.tick());
        }
        let published = cluster
            .names()
            .filter(|name| {
                snapshot
                    .worst(&format!("{ROOT}/node/{name}/exclusive-claim"))
                    .is_some()
            })
            .count();
        assert_eq!(published, 40, "every one of forty nodes published");
        cluster.stop();
        assert_eq!(cluster.alive(), 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    fn merge_into(mut into: Snapshot, from: &Snapshot) -> Snapshot {
        merge(&mut into, from);
        into
    }

    fn snapshot_record(snapshot: &Snapshot, scope: &str) -> HealthRecord {
        snapshot
            .health(scope)
            .into_iter()
            .find(|record| record.scope == scope)
            .unwrap_or_else(|| panic!("{scope} is recorded"))
    }
}
