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
//! `xmip:///<cluster>/node`, worst of all. A node whose snapshot has not
//! changed for longer than three rounds is hung: it is killed and restarted,
//! and the restart is recorded as a fault — a yellow that stays for the
//! cluster's life — never silently. A node that has not published once is
//! starting, not hung, until it has had two minutes for its first round
//! (2026-09-25: a brutal roll's first round outlasted three, and killing it
//! for that restarted every node into the same wait, over and over).

mod binary;
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
/// Rounds a node may stay silent before it is hung.
const SILENT_ROUNDS: u32 = 3;
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
    nodes: Vec<Member>,
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
        std::fs::create_dir_all(shared)?;
        std::fs::create_dir_all(snapshots)?;
        std::fs::remove_file(shared.join("stop")).ok();

        let mut cluster = Self {
            binary: binary.to_path_buf(),
            shared: shared.to_path_buf(),
            orders: orders.clone(),
            nodes: Vec::new(),
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
    /// node to publish anew; merge what each published; restart the hung; and
    /// add the per-node health and the rollup.
    pub fn tick(&mut self) -> Snapshot {
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
        let mut snapshot = Snapshot::new();
        for node in &self.nodes {
            merge(&mut snapshot, &node.published);
            snapshot.record_health(node.process_record(now));
        }
        snapshot.record_health(rollup(&snapshot, self.nodes.len(), now));
        snapshot
    }

    /// Read every node's file.
    fn read_all(&mut self) {
        for node in &mut self.nodes {
            node.read();
        }
    }

    /// Reap an exit, count silence, and restart a node silent too long.
    fn judge_process(&mut self, index: usize) {
        let node = &mut self.nodes[index];
        node.reap();
        if node.fresh {
            node.silent = 0;
        } else {
            node.silent += 1;
        }
        if node.hung() {
            node.kill();
            node.restarts += 1;
            node.silent = 0;
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
    use crate::support::scratch;
    use observe::{Health, HealthRecord};

    fn spawn(stress: Stress, count: usize, rounds: u64) -> (PathBuf, Cluster) {
        let dir = scratch("cluster");
        let cluster = Cluster::spawn(
            &built_node_binary(),
            &Orders::numbered(stress, count, rounds),
            &dir.join("shared"),
            &dir.join("snapshots"),
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

        let mut snapshot = cluster.tick();
        snapshot = merge_into(snapshot, &cluster.tick());

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
        let mut last = cluster.tick();
        for _ in 0..8 {
            last = cluster.tick();
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

    #[test]
    fn a_silent_node_is_restarted_and_the_restart_is_a_yellow() {
        let (dir, mut cluster) = spawn(Stress::Calm, 1, 0);
        // Freeze the node's file at what it first published by pointing the
        // cluster at a copy it will never update; the real node is then "hung".
        cluster.tick();
        let frozen = dir.join("frozen.toml");
        std::fs::copy(&cluster.nodes[0].path, &frozen).expect("a frozen copy");
        cluster.nodes[0].path = frozen;
        let mut last = cluster.tick();
        for _ in 0..=SILENT_ROUNDS {
            last = cluster.tick();
        }
        assert_eq!(cluster.restarts(), 1, "silent past three rounds is hung");
        let process = snapshot_record(&last, &format!("{ROOT}/node/node-01/system-process"));
        assert_eq!(process.health, Health::Stressed, "{}", process.evidence);
        assert!(process.evidence.contains("hung"), "{}", process.evidence);
        let rollup = snapshot_record(&last, &format!("{ROOT}/node"));
        assert_ne!(rollup.health, Health::Fine, "the rollup carries the fault");
        cluster.stop();
        assert_eq!(cluster.alive(), 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// 2026-09-25: a node slow over its first round is starting, not hung.
    /// One that has published nothing is left to finish that round however
    /// many rounds it takes, and said to be starting.
    #[test]
    fn a_node_that_has_not_published_yet_is_starting_and_not_restarted() {
        let (dir, mut cluster) = spawn(Stress::Calm, 1, 0);
        // A file the node never writes: it has, as far as the cluster can
        // tell, not published once.
        cluster.nodes[0].path = dir.join("never-written.toml");
        let mut last = cluster.tick();
        for _ in 0..=SILENT_ROUNDS + 1 {
            last = cluster.tick();
        }
        assert_eq!(cluster.restarts(), 0, "a starting node is not hung");
        assert_eq!(cluster.alive(), 1);
        let process = snapshot_record(&last, &format!("{ROOT}/node/node-01/system-process"));
        assert!(
            process.evidence.starts_with("starting"),
            "{}",
            process.evidence
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

        let mut snapshot = cluster.tick();
        for _ in 0..40 {
            snapshot = merge_into(snapshot, &cluster.tick());
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
        let mut snapshot = cluster.tick();
        for _ in 0..4 {
            snapshot = merge_into(snapshot, &cluster.tick());
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
