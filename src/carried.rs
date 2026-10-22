//! Carried: the rig drives the nodes the run started (`served`) through
//! their Receive Locations, and holds them to what Xmip promises — an
//! accepted Message is never lost — over the wire to Xmip Storage (ADR-0028,
//! amendment 2026-10-22). Each round it drops a file into each bound drop
//! folder, the next of the Playground's content contracts each time, under
//! a name its contract's name begins (`Contract::file_name`), which the
//! cluster's Subscriptions route by to a Send Port another node sends; the
//! file arrives in that Send Port's folder under its name. The receiving
//! node writes it to the Ledger on a Storage node, the sending node takes
//! its Journey from there: neither keeps Xmip Storage itself.
//!
//! What it publishes at `<cluster>/carried`: Fine while every file dropped
//! arrived, or is younger than [`GRACE`]; Stressed while one is older and
//! not yet [`LOST`]; Done, naming it, once one is — and how long a file took
//! from its drop to its arrival, the fastest and the median of the round's.
//! The folders are watched, never looked in on a timer: an arrival is told
//! by the file system.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use observe::{Health, HealthRecord, Snapshot};
use runtime::directory_watch::DirectoryWatch;

use crate::bindings::Placed;
use crate::schedule::CONTRACTS;
use crate::served::Binding;

/// How long a file may take before it is overdue: a bound on the test,
/// never on Xmip.
pub const GRACE: Duration = Duration::from_secs(5);

/// How long an overdue file may take before it is lost.
pub const LOST: Duration = Duration::from_secs(60);

/// Each file arrived, by name, and when it was first seen whole.
type Arrived = Arc<Mutex<BTreeMap<String, Instant>>>;

/// The drop folders, the files dropped into them, and what arrived.
pub struct Carried {
    drops: Vec<Binding>,
    dropped: BTreeMap<String, Instant>,
    arrived: Arrived,
    _watching: Vec<DirectoryWatch>,
    next: u64,
}

/// Every file whole in `folder` — none a send is still writing — recorded
/// where it was not, at `now`.
fn seen(folder: &Path, arrived: &Arrived, now: Instant) {
    let Ok(entries) = std::fs::read_dir(folder) else {
        return;
    };
    let mut arrived = arrived.lock().unwrap_or_else(PoisonError::into_inner);
    for entry in entries.filter_map(Result::ok) {
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.contains(".xmip-") {
            arrived.entry(name).or_insert(now);
        }
    }
}

impl Carried {
    /// Drive the bindings `bindings` the run gave the nodes it started:
    /// each drop folder and each Send Port's folder made, every Send Port's
    /// watched.
    ///
    /// # Errors
    ///
    /// A folder that cannot be made or watched, in words.
    pub fn new(bindings: &[Binding]) -> Result<Self, String> {
        let arrived: Arrived = Arc::default();
        let mut watching = Vec::new();
        for binding in bindings {
            std::fs::create_dir_all(&binding.folder)
                .map_err(|error| format!("{}: {error}", binding.folder.display()))?;
            if binding.placed == Placed::Send {
                let (folder, told) = (binding.folder.clone(), Arc::clone(&arrived));
                let watch = DirectoryWatch::spawn(&binding.folder, move || {
                    seen(&folder, &told, Instant::now());
                })
                .map_err(|error| format!("{}: {error}", binding.folder.display()))?;
                watching.push(watch);
            }
        }
        let drops = bindings
            .iter()
            .filter(|binding| binding.placed == Placed::Receive)
            .cloned()
            .collect();
        Ok(Self {
            drops,
            dropped: BTreeMap::new(),
            arrived,
            _watching: watching,
            next: 0,
        })
    }

    /// One round: a file dropped into each drop folder, written beside it
    /// and renamed in as a producer that means it whole drops one, and the
    /// judgment so far recorded at `<root>/carried`.
    pub fn round(&mut self, root: &str, snapshot: &mut Snapshot, now: i64) {
        let mut failed = Vec::new();
        for binding in &self.drops {
            let contract = CONTRACTS[usize::try_from(self.next).unwrap_or(0) % CONTRACTS.len()];
            let name = contract.file_name(self.next);
            self.next += 1;
            let beside = binding.folder.with_extension("beside");
            let written = std::fs::create_dir_all(&beside)
                .and_then(|()| std::fs::write(beside.join(&name), contract.payload()))
                .and_then(|()| std::fs::rename(beside.join(&name), binding.folder.join(&name)));
            match written {
                Ok(()) => {
                    self.dropped.insert(name, Instant::now());
                }
                Err(error) => failed.push(format!("{name} into {}: {error}", binding.name)),
            }
        }
        snapshot.record_health(self.record(root, &failed, now));
    }

    /// What was carried so far, and how long each took.
    fn record(&self, root: &str, failed: &[String], now: i64) -> HealthRecord {
        let arrived = self.arrived.lock().unwrap_or_else(PoisonError::into_inner);
        let at = Instant::now();
        let mut took: Vec<Duration> = Vec::new();
        let (mut overdue, mut lost) = (Vec::new(), Vec::new());
        for (name, dropped) in &self.dropped {
            match arrived.get(name) {
                Some(seen) => took.push(seen.saturating_duration_since(*dropped)),
                None if at.duration_since(*dropped) >= LOST => lost.push(name.as_str()),
                None if at.duration_since(*dropped) >= GRACE => overdue.push(name.as_str()),
                None => {}
            }
        }
        took.sort();
        let spoken = took.first().map_or_else(String::new, |fastest| {
            format!(
                "; from its drop to its arrival {fastest:?} the fastest, {:?} the median",
                took[took.len() / 2]
            )
        });
        let carried = format!(
            "{} of {} file(s) dropped carried, each through a receiving node, Xmip Storage \
             over the wire and a sending node{spoken}",
            took.len(),
            self.dropped.len()
        );
        let (health, evidence) = if !failed.is_empty() {
            (
                Health::Done,
                format!("not dropped: {}; {carried}", failed.join("; ")),
            )
        } else if !lost.is_empty() {
            let named = lost.iter().take(5).copied().collect::<Vec<_>>().join(", ");
            (
                Health::Done,
                format!("{} lost, {named}: {carried}", lost.len()),
            )
        } else if !overdue.is_empty() {
            (
                Health::Stressed,
                format!("{} overdue past {GRACE:?}: {carried}", overdue.len()),
            )
        } else {
            (Health::Fine, carried)
        };
        HealthRecord {
            scope: format!("{root}/carried"),
            health,
            severity: 0,
            evidence,
            observed_unix_nanos: now,
        }
    }

    /// Wait, as the run ends, for every file dropped to arrive, for at most
    /// [`LOST`]: what is not carried by then is lost. Asked each tenth of a
    /// second, the carrying being what it waits out.
    pub fn settle(&self) {
        let deadline = Instant::now() + LOST;
        while Instant::now() < deadline {
            let arrived = self.arrived.lock().unwrap_or_else(PoisonError::into_inner);
            if self.dropped.keys().all(|name| arrived.contains_key(name)) {
                return;
            }
            drop(arrived);
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// The judgment so far, in words.
    #[must_use]
    pub fn words(&self) -> String {
        let arrived = self.arrived.lock().unwrap_or_else(PoisonError::into_inner);
        let carried = self
            .dropped
            .keys()
            .filter(|name| arrived.contains_key(*name))
            .count();
        format!("{carried} of {} carried", self.dropped.len())
    }
}
