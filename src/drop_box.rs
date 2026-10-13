//! The Parties' drop box: bursts of files dropped into a Receive Location
//! every receiving node reads, each taken by exactly one of them (ADR-0024;
//! ADR-0028, amendment 2026-10-13).
//!
//! The owner, 2026-10-13: *induce multiple files for FILE and (S)FTP(S) so I
//! can see that it holds up*, and *only one Rn should win.* Every round the
//! roll drops a burst of [`BURST`] files for each Party into the Party's
//! own Receive Location, a directory under the cluster's shared one, through
//! the file transport's own send — written aside and renamed into place.
//! Every node that declared receive reads every Party's Location through the
//! file transport's own receive, whose claim is the rename (ADR-0024,
//! amendment 2026-09-26): whichever node's rename succeeds has the file, and
//! no other can. The winner accepts it — the file is consumed — and signs
//! it, `won/<party>/<file>/<node>`. The roll judges every file once its
//! [`GRACE`] has passed: one signature is a file taken once, two a file
//! taken twice, none a file never taken; and says which in words.
//!
//! What is dropped is the roll's record, `dropped/<party>/<file>`; what a
//! node took is its own count, `<node>/drop-box/<party>`, so the board
//! shows the split between the receiving nodes Location by Location.

use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use observe::{Count, Counted, Health, HealthRecord, Snapshot};
use transport::{Transport, Verdict};
use transport_file::FileTransport;

use crate::parties::Parties;

/// Files each Party drops at once, every round.
pub const BURST: usize = 8;

/// How long a dropped file may wait to be taken before it is judged.
pub const GRACE: Duration = Duration::from_secs(10);

/// How many files a judgement names before it counts the rest.
const NAMED: usize = 8;

/// The drop box under a cluster's shared directory.
#[derive(Clone, Debug)]
pub struct DropBox {
    root: PathBuf,
}

impl DropBox {
    /// The drop box of the cluster sharing `shared`.
    #[must_use]
    pub fn at(shared: &Path) -> Self {
        Self {
            root: shared.join("drop-box"),
        }
    }

    /// The Receive Location the Party called `party` drops into.
    #[must_use]
    pub fn location(&self, party: &str) -> PathBuf {
        self.root.join("in").join(party)
    }

    fn dropped(&self, party: &str) -> PathBuf {
        self.root.join("dropped").join(party)
    }

    fn won(&self, party: &str, file: &str) -> PathBuf {
        self.root.join("won").join(party).join(file)
    }

    /// Each Party drops a burst of [`BURST`] files in `round`, recorded as
    /// dropped before it is dropped.
    ///
    /// # Errors
    /// A file that could not be written or recorded, in words.
    pub fn drop_burst(&self, parties: &Parties, round: u64) -> Result<usize, String> {
        let mut dropped = 0;
        for name in parties.names() {
            let location = self.location(name);
            let record = self.dropped(name);
            for dir in [&location, &record] {
                std::fs::create_dir_all(dir).map_err(|error| error.to_string())?;
            }
            let into = FileTransport::new(&location);
            for one in 0..BURST {
                let file = format!("{round:010}-{one:02}.txt");
                std::fs::write(record.join(&file), b"").map_err(|error| error.to_string())?;
                let body = format!("{name} dropped {file}");
                into.send(&file, body.as_bytes())
                    .map_err(|error| format!("{name}: {file}: {error}"))?;
                dropped += 1;
            }
        }
        Ok(dropped)
    }
}

/// One receiving node reading every Party's Location: the file transport
/// of each, kept so its listing's stability check spans rounds, and what
/// the node has taken from each.
pub struct Taker {
    node: String,
    drop_box: DropBox,
    locations: Vec<(String, FileTransport, u64)>,
}

impl Taker {
    /// The node called `name` reading the drop box every Party drops into.
    #[must_use]
    pub fn new(name: &str, drop_box: DropBox, parties: &Parties) -> Self {
        let locations = parties
            .names()
            .into_iter()
            .map(|party| {
                let location = drop_box.location(party);
                (party.to_string(), FileTransport::new(location), 0)
            })
            .collect();
        Self {
            node: name.to_string(),
            drop_box,
            locations,
        }
    }

    /// Take what this node's claims win, accept and sign each, and record
    /// at `<node>/drop-box/<party>` the Streams it has taken from each.
    pub fn take(&mut self, node: &str, snapshot: &mut Snapshot, now: i64) {
        for (party, location, taken) in &mut self.locations {
            let Ok(arrived) = location.receive() else {
                continue;
            };
            for one in arrived {
                let (origin, mut body, acknowledgement) = one.into_parts();
                let file = origin.rsplit('/').next().unwrap_or_default().to_string();
                let mut bytes = Vec::new();
                let read = body.read_to_end(&mut bytes).is_ok();
                drop(body);
                let verdict = if read {
                    Verdict::Accepted
                } else {
                    Verdict::Failed
                };
                if acknowledgement.acknowledge(verdict).is_ok() && read {
                    let signed = self.drop_box.won(party, &file);
                    let _ = std::fs::create_dir_all(&signed);
                    let _ = std::fs::write(signed.join(&self.node), b"");
                    *taken += 1;
                }
            }
            snapshot.record_count(Count {
                scope: format!("{node}/drop-box/{party}"),
                counted: Counted::Streams,
                value: *taken,
                window_start_unix_nanos: now,
                window_end_unix_nanos: now,
                observed_unix_nanos: now,
            });
        }
    }
}

/// What the roll has judged of every file dropped so far.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Judged {
    pub files: u64,
    /// The files each receiving node took, by its name.
    pub by_node: BTreeMap<String, u64>,
    /// Each file taken more than once: `<party>/<file>` and who took it.
    pub twice: Vec<(String, Vec<String>)>,
    /// Each file nobody took within its grace.
    pub never: Vec<String>,
}

impl Judged {
    /// Judge every file dropped longer than [`GRACE`] before `now`, and
    /// clear its records.
    pub fn judge(&mut self, drop_box: &DropBox, parties: &Parties, now: SystemTime) {
        for party in parties.names() {
            let Ok(dropped) = std::fs::read_dir(drop_box.dropped(party)) else {
                continue;
            };
            for record in dropped.flatten() {
                let due = record
                    .metadata()
                    .and_then(|meta| meta.modified())
                    .is_ok_and(|at| now.duration_since(at).is_ok_and(|age| age >= GRACE));
                if !due {
                    continue;
                }
                let file = record.file_name().to_string_lossy().to_string();
                let signed = drop_box.won(party, &file);
                let mut winners: Vec<String> = std::fs::read_dir(&signed)
                    .map(|read| {
                        read.flatten()
                            .map(|one| one.file_name().to_string_lossy().to_string())
                            .collect()
                    })
                    .unwrap_or_default();
                winners.sort();
                self.files += 1;
                let named = format!("{party}/{file}");
                match winners.as_slice() {
                    [] => self.never.push(named),
                    [one] => *self.by_node.entry(one.clone()).or_default() += 1,
                    _ => self.twice.push((named, winners)),
                }
                let _ = std::fs::remove_file(record.path());
                let _ = std::fs::remove_dir_all(&signed);
            }
        }
    }

    /// The judgement in words.
    #[must_use]
    pub fn words(&self) -> String {
        let split: Vec<String> = self
            .by_node
            .iter()
            .map(|(node, files)| format!("{node} {files}"))
            .collect();
        let split = if split.is_empty() {
            "none yet".to_string()
        } else {
            split.join(", ")
        };
        if self.twice.is_empty() && self.never.is_empty() {
            return format!(
                "every file taken by exactly one receiving node: {} files, {split}",
                self.files
            );
        }
        let mut said = Vec::new();
        if !self.twice.is_empty() {
            let twice: Vec<String> = self
                .twice
                .iter()
                .map(|(file, by)| format!("{file} by {}", by.join(" and ")))
                .collect();
            said.push(format!(
                "{} taken twice: {}",
                self.twice.len(),
                listed(&twice)
            ));
        }
        if !self.never.is_empty() {
            said.push(format!(
                "{} never taken: {}",
                self.never.len(),
                listed(&self.never)
            ));
        }
        format!(
            "{} of {} files; once each: {split}",
            said.join("; "),
            self.files
        )
    }

    /// The judgement as the record at `<root>/drop-box`: Fine while every
    /// file was taken once, Done where one was not.
    #[must_use]
    pub fn record(&self, root: &str, now: i64) -> HealthRecord {
        let held = self.twice.is_empty() && self.never.is_empty();
        HealthRecord {
            scope: format!("{root}/drop-box"),
            health: if held { Health::Fine } else { Health::Done },
            severity: if held { 0 } else { 80 },
            evidence: self.words(),
            observed_unix_nanos: now,
        }
    }
}

/// The roll's side: every round a burst dropped for every Party, every
/// file past its grace judged, and the judgement recorded.
pub struct Dropping {
    drop_box: DropBox,
    judged: Judged,
    round: u64,
}

impl Dropping {
    /// The roll's drop box under the cluster's `shared` directory.
    #[must_use]
    pub fn new(shared: &Path) -> Self {
        Self {
            drop_box: DropBox::at(shared),
            judged: Judged::default(),
            round: 0,
        }
    }

    /// Drop this round's bursts, judge what is due, and record the
    /// judgement at `<root>/drop-box` — a drop that failed, in its words.
    pub fn round(&mut self, root: &str, snapshot: &mut Snapshot, now: i64) {
        self.round += 1;
        let parties = Parties::configured();
        let dropped = self.drop_box.drop_burst(parties, self.round);
        self.judged
            .judge(&self.drop_box, parties, SystemTime::now());
        let mut record = self.judged.record(root, now);
        if let Err(why) = dropped {
            record.health = Health::Done;
            record.evidence = format!("a burst was not dropped: {why}; {}", record.evidence);
        }
        snapshot.record_health(record);
    }

    /// The judgement so far, in words.
    #[must_use]
    pub fn words(&self) -> String {
        self.judged.words()
    }
}

/// `files`, the first [`NAMED`] of them, and how many more.
fn listed(files: &[String]) -> String {
    let shown = files
        .iter()
        .take(NAMED)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ");
    match files.len().saturating_sub(NAMED) {
        0 => shown,
        more => format!("{shown} and {more} more"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::support::{declaring, scratch, test_cluster};

    /// Two receiving nodes race for every Party's bursts, each on its own
    /// thread, until the drop box is drained; every file has one winner.
    #[test]
    fn two_receivers_racing_take_every_file_exactly_once() {
        let shared = scratch("drop-box");
        let drop_box = DropBox::at(&shared);
        let parties = Parties::configured();
        let cluster = test_cluster();
        let [first, second] = declaring(&cluster, "sending")[..] else {
            panic!("the test cluster has two nodes of a kind");
        };
        let mut dropped = 0;
        for round in 1..=3 {
            dropped += drop_box.drop_burst(parties, round).expect("dropped");
        }
        assert_eq!(dropped, 3 * BURST * parties.all().len());

        let race = |name: &str| {
            let mut taker = Taker::new(name, drop_box.clone(), parties);
            let mut snapshot = Snapshot::new();
            for _ in 0..20 {
                taker.take(name, &mut snapshot, 1);
            }
            snapshot
        };
        let (one, two) = std::thread::scope(|scope| {
            let one = scope.spawn(|| race(first));
            let two = scope.spawn(|| race(second));
            (one.join().expect("first"), two.join().expect("second"))
        });
        let took = |snapshot: &Snapshot, name: &str| {
            snapshot
                .measure(&format!("{name}/drop-box"), Counted::Streams)
                .map_or(0, |count| count.value)
        };
        assert_eq!(took(&one, first) + took(&two, second), dropped as u64);

        let mut judged = Judged::default();
        judged.judge(&drop_box, parties, SystemTime::now() + 2 * GRACE);
        assert_eq!(judged.files, dropped as u64);
        assert!(
            judged.twice.is_empty() && judged.never.is_empty(),
            "{judged:?}"
        );
        assert_eq!(judged.by_node.values().sum::<u64>(), dropped as u64);
        assert!(
            judged
                .words()
                .starts_with("every file taken by exactly one receiving node: "),
            "{}",
            judged.words()
        );
        let _ = std::fs::remove_dir_all(&shared);
    }

    #[test]
    fn a_file_taken_twice_or_never_is_named_in_words() {
        let cluster = test_cluster();
        let [first, second] = declaring(&cluster, "sending")[..] else {
            panic!("the test cluster has two nodes of a kind");
        };
        let judged = Judged {
            files: 3,
            by_node: BTreeMap::from([(first.to_string(), 1)]),
            twice: vec![("a/1".to_string(), vec![first.into(), second.into()])],
            never: vec!["a/2".to_string()],
        };
        assert_eq!(
            judged.words(),
            format!(
                "1 taken twice: a/1 by {first} and {second}; 1 never taken: a/2 of 3 files; \
                 once each: {first} 1"
            )
        );
        assert_eq!(judged.record("r", 1).health, Health::Done);
        let none = Judged::default();
        assert_eq!(none.record("r", 1).health, Health::Fine);
    }
}
