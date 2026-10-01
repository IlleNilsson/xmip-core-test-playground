//! One node of the cluster as the cluster holds it: the process, the file it
//! publishes to, and what was last read from it.

use std::path::PathBuf;
use std::process::{Child, ExitStatus};
use std::time::Instant;

use observe::topology::draw::{ALIVE, system_process};
use observe::{Health, HealthRecord, Snapshot};

use super::Liveness;
use crate::handoff::Hop;
use crate::heartbeat::{self, Beat};
use crate::report::node_from_toml;
use crate::support::cluster_root;

/// One node process and its published state.
pub(super) struct Member {
    pub(super) name: String,
    pub(super) child: Option<Child>,
    pub(super) exit: Option<ExitStatus>,
    pub(super) path: PathBuf,
    pub(super) published: Snapshot,
    /// The handoffs the node says it delivered, per link.
    pub(super) hops: Vec<Hop>,
    pub(super) fresh: bool,
    pub(super) restarts: u32,
    /// When the running process was started, when the cluster last saw it
    /// beat — `None` until it has, since it started — the beat as last read,
    /// and how many rounds it said it had finished (ADR-0052, amendment
    /// 2026-09-26).
    since: Instant,
    beat: Option<Instant>,
    beat_text: String,
    rounds: u64,
    /// The round count the published snapshot was last read at: the file is
    /// read and parsed again only when the beat says another round is done
    /// (the owner, 2026-09-26: more than a millisecond is wrong).
    read_at: Option<u64>,
    /// How many times the published file was read and parsed.
    pub(super) parses: u64,
}

impl Member {
    /// A node just started as `child`, publishing to `path`.
    pub(super) fn started(name: String, child: Child, path: PathBuf) -> Self {
        Self {
            name,
            child: Some(child),
            exit: None,
            path,
            published: Snapshot::new(),
            hops: Vec::new(),
            fresh: false,
            restarts: 0,
            since: Instant::now(),
            beat: None,
            beat_text: String::new(),
            rounds: 0,
            read_at: None,
            parses: 0,
        }
    }

    pub(super) fn alive(&self) -> bool {
        self.child.is_some()
    }

    /// The node runs again as `child`, restarted: it has not beaten yet.
    pub(super) fn restarted(&mut self, child: Child) {
        self.child = Some(child);
        self.exit = None;
        self.since = Instant::now();
        self.beat = None;
        self.rounds = 0;
    }

    /// Alive as a process and hung by its beats: none since it started for
    /// longer than it may take to start, or none for the missed beats since
    /// the last one. A round, however long, is not asked about.
    pub(super) fn hung(&self, liveness: &Liveness) -> bool {
        self.alive() && liveness.hung(self.since.elapsed(), self.beat.map(|beat| beat.elapsed()))
    }

    /// Read the node's beat, and its published file only when the beat says
    /// a round was finished since the file was last read: then it is parsed
    /// once and marks the node fresh. An idle node costs one read of a few
    /// bytes.
    pub(super) fn read(&mut self) {
        self.listen();
        if self.rounds == 0 || self.read_at == Some(self.rounds) {
            return;
        }
        let Ok(text) = std::fs::read_to_string(&self.path) else {
            return;
        };
        if let Ok((snapshot, hops)) = node_from_toml(&text) {
            self.published = snapshot;
            self.hops = hops;
            self.read_at = Some(self.rounds);
            self.parses += 1;
            self.fresh = true;
        }
    }

    /// Read the node's beat alone: a few bytes, so the cluster asks again
    /// right before it judges, and its own time spent reading publications
    /// is never counted against a node. A changed beat — even one caught
    /// half-written — is a sign of life.
    pub(super) fn listen(&mut self) {
        if let Ok(text) = std::fs::read_to_string(heartbeat::beside(&self.path))
            && text != self.beat_text
        {
            if let Some(beat) = Beat::read(&text) {
                self.rounds = beat.rounds;
            }
            self.beat = Some(Instant::now());
            self.beat_text = text;
        }
    }

    pub(super) fn reap(&mut self) {
        if let Some(child) = self.child.as_mut()
            && let Ok(Some(status)) = child.try_wait()
        {
            self.exit = Some(status);
            self.child = None;
        }
    }

    pub(super) fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            child.kill().ok();
            self.exit = child.wait().ok();
        }
    }

    /// What the cluster knows about the process itself, which the node cannot
    /// say: alive, exited with its code, starting, or restarted after hanging.
    /// Published at `<node>/system-process` (the System Process the node is,
    /// ADR-0053) — until 2026-09-19 at `<node>/process`, which is a `P` node's
    /// stage of the message path and must not be shared with it.
    pub(super) fn process_record(&self, now: i64) -> HealthRecord {
        let restarted = format!(
            "restarted {} time(s) after it stopped beating (hung)",
            self.restarts
        );
        let (health, severity, evidence) = match (&self.exit, self.restarts) {
            (Some(status), _) if !status.success() => {
                (Health::Done, 90, format!("exited with {status}"))
            }
            (Some(_), 0) => (Health::Fine, 0, "exited 0 after its rounds".to_string()),
            (Some(_), _) => (Health::Stressed, 60, format!("exited 0; {restarted}")),
            (None, 0) if self.beat.is_none() => {
                (Health::Working, 20, "starting, no beat yet".to_string())
            }
            (None, 0) if self.rounds == 0 => (
                Health::Working,
                20,
                "starting: beating, its first round under way".to_string(),
            ),
            (None, 0) => (Health::Fine, 0, ALIVE.to_string()),
            (None, _) => (Health::Stressed, 60, format!("{ALIVE}; {restarted}")),
        };
        HealthRecord {
            scope: system_process(&format!("{}/node/{}", cluster_root(), self.name)),
            health,
            severity,
            evidence,
            observed_unix_nanos: now,
        }
    }
}
