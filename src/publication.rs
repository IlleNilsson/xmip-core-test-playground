//! [`Publication`]: the three files a roll publishes each round — snapshot,
//! history and activity — and the run header every snapshot carries. The
//! paths are the cmdlet's or the roll's own defaults, decided once
//! (`environment.rs`); the header says what the run was started with.
//!
//! A file that cannot be written costs the monitors that round and is
//! audited as the roll's failure to `publish` (ADR-0062), not only said on
//! stderr.

use std::path::{Path, PathBuf};

use observe::{Activity, History, Run, Snapshot, Topology};
use xaudit::program_audit::ProgramAudit;

use crate::environment::publish_paths;
use crate::process_audit;
use crate::report::{activity_toml, history_toml, roll_toml, write_atomic};

/// Where one roll publishes, and the run it publishes under.
pub struct Publication {
    /// The snapshot the prompt, the CLI and the GUI read.
    pub snapshot: PathBuf,
    /// The history the monitors draw curves from.
    pub history: PathBuf,
    /// The recent activity the monitors list.
    pub activity: PathBuf,
    run: Run,
    audit: ProgramAudit,
}

impl Publication {
    /// Where the roll of `cluster` publishes, with the run it publishes
    /// under, auditing a failed write into `audit`.
    #[must_use]
    pub fn of(cluster: &str, run: Run, audit: ProgramAudit) -> Self {
        let (snapshot, history, activity) = publish_paths(cluster);
        Self {
            snapshot,
            history,
            activity,
            run,
            audit,
        }
    }

    /// A file called `name` beside the snapshot — where the cluster process
    /// publishes its own.
    #[must_use]
    pub fn beside(&self, name: &str) -> PathBuf {
        self.snapshot.with_file_name(name)
    }

    /// One round's snapshot, history and activity, each written atomically.
    pub fn round(
        &self,
        root: &str,
        snapshot: &Snapshot,
        topology: Option<Topology>,
        history: &History,
        activity: &Activity,
    ) {
        let text = roll_toml(root, snapshot, topology, Some(self.run.clone()));
        self.write(&self.snapshot, &text, "snapshot");
        self.write(&self.history, &history_toml(root, history), "history");
        self.write(&self.activity, &activity_toml(root, activity), "activity");
    }

    fn write(&self, path: &Path, contents: &str, what: &str) {
        if let Err(error) = write_atomic(path, contents) {
            let problem = format!("could not write the {what} to {}: {error}", path.display());
            process_audit::fail(&self.audit, "publish", &problem);
        }
    }
}
