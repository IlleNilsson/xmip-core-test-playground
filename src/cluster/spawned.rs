//! The cluster process a roll spawns: started, watched, read and stopped.
//!
//! 2026-09-19, the owner: *even clusters have to be spawned as processes
//! during tests.* The roll is the test — it chooses the scenarios, sets the
//! stress, judges and draws the board — the cluster is a System Process the
//! roll starts (ADR-0053), and the nodes are System Processes the cluster
//! starts. Each declares itself, so `Get-XmipProcess` shows the tree.
//!
//! What the roll knows of its cluster it reads from the file the cluster
//! publishes, which is the same bridge a cluster reads its nodes over
//! (ADR-0027 decision 8): a node answers for itself, and whoever asks
//! assembles the view. The roll merges that file into the snapshot it
//! publishes, so `<Cluster>-snapshot.toml` keeps its path and its shape.

use std::io;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use observe::Snapshot;

use super::Orders;
use crate::handoff::Hop;
use crate::report::node_from_toml;

/// How long a tick waits for the cluster to publish something new before it
/// goes on with what it has.
const GRACE: Duration = Duration::from_secs(2);
/// How long `stop` waits for the cluster to leave on its own. Longer than the
/// cluster's own wait for its nodes, because it stops them before it goes.
const STOP_WAIT: Duration = Duration::from_secs(10);

/// One cluster process, as the roll that spawned it holds it.
pub struct Spawned {
    child: Option<Child>,
    /// The directory the cluster shares with its nodes; `stop` in it is how
    /// the whole tree is asked to leave.
    shared: PathBuf,
    /// The file the cluster publishes its own snapshot to.
    path: PathBuf,
    text: String,
    published: Snapshot,
    hops: Vec<Hop>,
}

impl Spawned {
    /// Start one cluster process called `cluster` over `shared`, publishing to
    /// `path`, spawning the nodes `orders` names.
    ///
    /// The cluster's scope root is `xmip:///<cluster>`, which it reads from
    /// `XMIP_PLAYGROUND_CLUSTER` as its nodes do, so it is set on the child
    /// here and inherited all the way down.
    ///
    /// # Errors
    ///
    /// When the process cannot be started.
    pub fn start(
        binary: &Path,
        cluster: &str,
        orders: &Orders,
        shared: &Path,
        path: &Path,
    ) -> io::Result<Self> {
        std::fs::create_dir_all(shared)?;
        let mut command = Command::new(binary);
        command
            .env("XMIP_PLAYGROUND_CLUSTER", cluster)
            .args(["--name", cluster, "--stress", orders.stress.name()])
            // Each node with what it was declared with, never its name alone.
            .args(["--nodes", &orders.roster.text()])
            // The cluster rolls until the roll stops it; its nodes run its
            // rounds, which is what `orders` carries.
            .args(["--rounds", "0"]);
        if let Some(online) = orders.online.as_ref() {
            command.args(["--online", &online.join(",")]);
        }
        if !orders.scenarios.is_empty() {
            command.args(["--scenarios", &orders.scenarios.join(",")]);
        }
        let child = command
            .arg("--shared")
            .arg(shared)
            .arg("--snapshot")
            .arg(path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .spawn()?;
        Ok(Self {
            child: Some(child),
            shared: shared.to_path_buf(),
            path: path.to_path_buf(),
            text: String::new(),
            published: Snapshot::new(),
            hops: Vec::new(),
        })
    }

    /// One round of the roll's half: wait, within a grace period, for the
    /// cluster to publish anew, and answer with what it last published.
    pub fn tick(&mut self) -> &Snapshot {
        let started = Instant::now();
        loop {
            self.reap();
            if self.read() || !self.alive() || started.elapsed() > GRACE {
                break;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        &self.published
    }

    /// The handoffs the cluster's nodes delivered, per link — what the
    /// topology draws between them.
    #[must_use]
    pub fn hops(&self) -> &[Hop] {
        &self.hops
    }

    /// Whether the cluster process is still running.
    #[must_use]
    pub const fn alive(&self) -> bool {
        self.child.is_some()
    }

    /// Ask the cluster to leave — the stop file the whole tree watches — wait
    /// for it to stop its own nodes and go, and end it if it stays.
    pub fn stop(&mut self) {
        if !self.alive() {
            return;
        }
        std::fs::write(self.shared.join("stop"), b"stop").ok();
        let started = Instant::now();
        while self.alive() && started.elapsed() < STOP_WAIT {
            self.reap();
            std::thread::sleep(Duration::from_millis(25));
        }
        if let Some(mut child) = self.child.take() {
            child.kill().ok();
            child.wait().ok();
        }
    }

    /// Read the cluster's file; a changed one is parsed. True when this read
    /// brought something new.
    fn read(&mut self) -> bool {
        let Ok(text) = std::fs::read_to_string(&self.path) else {
            return false;
        };
        if text == self.text {
            return false;
        }
        let Ok((snapshot, hops)) = node_from_toml(&text) else {
            return false;
        };
        self.published = snapshot;
        self.hops = hops;
        self.text = text;
        true
    }

    fn reap(&mut self) {
        if let Some(child) = self.child.as_mut()
            && let Ok(Some(_)) = child.try_wait()
        {
            self.child = None;
        }
    }
}

impl Drop for Spawned {
    /// A roll that ends any other way leaves no cluster behind, and no nodes
    /// under it: the cluster is asked first, so it stops its own.
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::built_cluster_binary;
    use crate::stress::Stress;
    use crate::support::scratch;

    /// The tree the owner asked for: the test spawns a cluster, the cluster
    /// spawns its nodes, each publishes, and the roll's half reads one file.
    #[test]
    fn a_spawned_cluster_publishes_its_nodes_and_stops_them_with_itself() {
        let dir = scratch("spawned");
        let roster =
            crate::Roster::parse("R1=receive,P1=process,S1=send").expect("a well-formed roster");
        let orders = Orders::of(Stress::Calm, roster, 0)
            .driving(&["round-trip".to_string()])
            .with_online(Some(vec!["R1".to_string()]));
        let path = dir.join("Zt-cluster.toml");
        let mut cluster = Spawned::start(
            &built_cluster_binary(),
            "Zt",
            &orders,
            &dir.join("shared"),
            &path,
        )
        .expect("the cluster process starts");

        let root = "xmip:///Zt";
        let mut published = false;
        for _ in 0..40 {
            let snapshot = cluster.tick();
            published = !snapshot.health(&format!("{root}/node/S1/send")).is_empty();
            if published {
                break;
            }
        }
        assert!(published, "S1 published its send stage through the cluster");

        let snapshot = cluster.tick();
        for name in ["R1", "P1", "S1"] {
            let process = format!("{root}/node/{name}/system-process");
            assert!(
                !snapshot.health(&process).is_empty(),
                "the cluster says whether {name} is alive"
            );
        }
        assert!(
            !snapshot.health(&format!("{root}/node")).is_empty(),
            "the cluster adds the rollup its surface owes"
        );
        let links: Vec<(&str, &str)> = cluster
            .hops()
            .iter()
            .map(|hop| (hop.from.as_str(), hop.to.as_str()))
            .collect();
        assert!(links.contains(&("R1", "P1")), "{links:?}");
        assert!(links.contains(&("P1", "S1")), "{links:?}");

        cluster.stop();
        assert!(!cluster.alive(), "the cluster left when it was asked");
        std::fs::remove_dir_all(&dir).ok();
    }
}
