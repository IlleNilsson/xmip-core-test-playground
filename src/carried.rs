//! Carried: the rig drives nodes through their Receive Locations, and holds
//! them to what Xmip promises — an accepted Message is never lost — over the
//! wire to Xmip Storage (ADR-0028, amendments 2026-10-22). Each round it
//! drops a file into each Receive Location placed (`configure::placed`),
//! the next of the Playground's content contracts each time, under a name
//! its contract's name begins (`Contract::file_name`), which the cluster's
//! Subscriptions route by to a Send Port another node sends; the file
//! arrives where that Send Port is placed, under its name. The receiving
//! node writes it to the Ledger on a Storage node, the sending node takes
//! its Journey from there: neither keeps Xmip Storage itself.
//!
//! Where it drops and reads is a [`Spot`]: a folder of the run's for the
//! nodes it starts on this computer (`served`), and on the Parties' far
//! end for the nodes on machines of their own, each Location a user's
//! directory there (`far_end`) — the same placement either way.
//!
//! What it publishes at `<cluster>/carried`: Fine while every file dropped
//! arrived, or is younger than [`GRACE`]; Stressed while one is older and
//! not yet [`LOST`]; Done, naming it, once one is — and how long a file took
//! from its drop to its arrival, the fastest and the median of the round's.
//! Arrivals are told — by the file system for a folder, by the far end as a
//! client closes what it wrote — never looked for on a timer.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use configure::placed::{Placed, Placement};
use observe::{Health, HealthRecord, Snapshot};
use runtime::directory_watch::DirectoryWatch;
use transport_sftp::directory::{Shared, Told};

use crate::schedule::CONTRACTS;

/// How long a file may take before it is overdue: a bound on the test,
/// never on Xmip.
pub const GRACE: Duration = Duration::from_secs(5);

/// How long an overdue file may take before it is lost.
pub const LOST: Duration = Duration::from_secs(60);

/// Each file arrived, by name, and when it was first seen whole.
pub type Arrived = Arc<Mutex<BTreeMap<String, Instant>>>;

/// Where a placed Location takes its files from, or a Send Port puts them.
#[derive(Clone)]
pub enum Spot {
    /// A folder on this computer.
    Folder(PathBuf),
    /// A user's directory on the Parties' far end.
    FarEnd(Shared),
}

/// One Receive Location or Send Port placed, and its spot.
#[derive(Clone)]
pub struct Binding {
    pub placement: Placement,
    pub spot: Spot,
}

/// The drops, the files dropped into them, and what arrived.
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

/// What a far end tells of a file a Send Location finished writing: its
/// arrival, now.
#[must_use]
pub fn told(arrived: &Arrived) -> Told {
    let arrived = Arc::clone(arrived);
    Arc::new(move |name: &str| {
        arrived
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(name.to_string())
            .or_insert_with(Instant::now);
    })
}

impl Carried {
    /// Drive `bindings`: each folder made and each Send Port's watched; a
    /// far end's Send Port directories tell `arrived`, which they were made
    /// with ([`told`]).
    ///
    /// # Errors
    ///
    /// A folder that cannot be made or watched, in words.
    pub fn new(bindings: &[Binding], arrived: Arrived) -> Result<Self, String> {
        let mut watching = Vec::new();
        for binding in bindings {
            let Spot::Folder(folder) = &binding.spot else {
                continue;
            };
            std::fs::create_dir_all(folder)
                .map_err(|error| format!("{}: {error}", folder.display()))?;
            if binding.placement.placed == Placed::Send {
                let (seeing, telling) = (folder.clone(), Arc::clone(&arrived));
                let watch = DirectoryWatch::spawn(folder, move || {
                    seen(&seeing, &telling, Instant::now());
                })
                .map_err(|error| format!("{}: {error}", folder.display()))?;
                watching.push(watch);
            }
        }
        let drops = bindings
            .iter()
            .filter(|binding| binding.placement.placed == Placed::Receive)
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

    /// What arrives is recorded in: a new, empty record.
    #[must_use]
    pub fn arrivals() -> Arrived {
        Arc::default()
    }

    /// One round: a file dropped into each drop — into a folder written
    /// beside it and renamed in, as a producer that means it whole drops
    /// one; onto a far end whole at once — and the judgment so far recorded
    /// at `<root>/carried`.
    pub fn round(&mut self, root: &str, snapshot: &mut Snapshot, now: i64) {
        let mut failed = Vec::new();
        for binding in &self.drops {
            let contract = CONTRACTS[usize::try_from(self.next).unwrap_or(0) % CONTRACTS.len()];
            let name = contract.file_name(self.next);
            self.next += 1;
            let written = match &binding.spot {
                Spot::Folder(folder) => {
                    let beside = folder.with_extension("beside");
                    std::fs::create_dir_all(&beside)
                        .and_then(|()| std::fs::write(beside.join(&name), contract.payload()))
                        .and_then(|()| std::fs::rename(beside.join(&name), folder.join(&name)))
                        .map_err(|error| error.to_string())
                }
                Spot::FarEnd(directory) => {
                    directory
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .files
                        .insert(name.clone(), contract.payload());
                    Ok(())
                }
            };
            match written {
                Ok(()) => {
                    self.dropped.insert(name, Instant::now());
                }
                Err(error) => {
                    failed.push(format!("{name} into {}: {error}", binding.placement.name));
                }
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
