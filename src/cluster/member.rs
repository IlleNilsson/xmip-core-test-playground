//! One node of the cluster as the cluster holds it: the process, the file it
//! publishes to, and what was last read from it.

use std::path::PathBuf;
use std::process::{Child, ExitStatus};

use observe::{Health, HealthRecord, Snapshot};

use super::SILENT_ROUNDS;
use crate::handoff::Hop;
use crate::report::node_from_toml;
use crate::support::cluster_root;

/// One node process and its published state.
pub(super) struct Member {
    pub(super) name: String,
    pub(super) child: Option<Child>,
    pub(super) exit: Option<ExitStatus>,
    pub(super) path: PathBuf,
    pub(super) text: String,
    pub(super) published: Snapshot,
    /// The handoffs the node says it delivered, per link.
    pub(super) hops: Vec<Hop>,
    pub(super) fresh: bool,
    pub(super) silent: u32,
    pub(super) restarts: u32,
}

impl Member {
    /// A node just started as `child`, publishing to `path`.
    pub(super) fn started(name: String, child: Child, path: PathBuf) -> Self {
        Self {
            name,
            child: Some(child),
            exit: None,
            path,
            text: String::new(),
            published: Snapshot::new(),
            hops: Vec::new(),
            fresh: false,
            silent: 0,
            restarts: 0,
        }
    }

    pub(super) fn alive(&self) -> bool {
        self.child.is_some()
    }

    /// Read the node's file; a changed one is parsed and marks the node fresh.
    pub(super) fn read(&mut self) {
        let Ok(text) = std::fs::read_to_string(&self.path) else {
            return;
        };
        if text == self.text {
            return;
        }
        if let Ok((snapshot, hops)) = node_from_toml(&text) {
            self.published = snapshot;
            self.hops = hops;
            self.text = text;
            self.fresh = true;
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
            "restarted {} time(s) after {SILENT_ROUNDS} silent rounds (hung)",
            self.restarts
        );
        let (health, severity, evidence) = match (&self.exit, self.restarts) {
            (Some(status), _) if !status.success() => {
                (Health::Done, 90, format!("exited with {status}"))
            }
            (Some(_), 0) => (Health::Fine, 0, "exited 0 after its rounds".to_string()),
            (Some(_), _) => (Health::Stressed, 60, format!("exited 0; {restarted}")),
            (None, 0) if self.text.is_empty() => {
                (Health::Working, 20, "starting, no snapshot yet".to_string())
            }
            (None, 0) => (Health::Fine, 0, "alive".to_string()),
            (None, _) => (Health::Stressed, 60, format!("alive; {restarted}")),
        };
        HealthRecord {
            scope: format!("{}/node/{}/system-process", cluster_root(), self.name),
            health,
            severity,
            evidence,
            observed_unix_nanos: now,
        }
    }
}
