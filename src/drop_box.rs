//! The Parties' drop box: bursts of files dropped into Receive Locations
//! every receiving node reads, over file, FTP, FTPS and SFTP, each file
//! taken by exactly one of them (ADR-0024; ADR-0028, amendment 2026-10-13).
//!
//! The owner, 2026-10-13: *induce multiple files for FILE and (S)FTP(S) so I
//! can see that it holds up*, and *only one Rn should win.* Each Party has
//! one directory per protocol, shared by every receiving node: its own under
//! the drop box for file, and a server of its own for FTP, FTPS and SFTP,
//! which the roll stands up (`far_ends.rs`). Every so many rounds the roll
//! drops a burst of [`BURST`] files for each Party over each protocol,
//! through that transport's own send. Every node that declared receive
//! reads every Party's directories through the same transports' receive,
//! whose claim is the file family's turn and rename (ADR-0024, amendments
//! 2026-09-26 and 2026-10-13): whichever node's rename succeeds has the
//! file, and no other can. The winner accepts it — the file is consumed —
//! and signs it, `won/<protocol>/<party>/<file>/<node>`. The roll judges
//! every file once its [`GRACE`] has passed (`judged.rs`), per protocol.
//!
//! What is dropped is the roll's record, `dropped/<protocol>/<party>/<file>`;
//! what a node took is its own count, `<node>/drop-box/<protocol>/<party>`,
//! so the board shows the split between the receiving nodes Location by
//! Location.

mod far_ends;
mod judged;

use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use observe::{Count, Counted, Health, Snapshot};
use transport::{NodeLocation, Verdict};

pub use far_ends::{FarEnds, PROTOCOLS};
use far_ends::{Speaking, bursts_every, listed_every, transport};
pub use judged::Judged;

use crate::parties::Parties;

/// Files each Party drops at once, over each protocol.
pub const BURST: usize = 8;

/// How long a dropped file may wait to be taken before it is judged.
pub const GRACE: Duration = Duration::from_secs(10);

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

    /// The file Receive Location the Party called `party` drops into.
    #[must_use]
    pub fn location(&self, party: &str) -> PathBuf {
        self.root.join("in").join("file").join(party)
    }

    fn servers(&self) -> PathBuf {
        self.root.join("servers")
    }

    /// Where the `protocol` server of the Party called `party` says it
    /// listens.
    fn address(&self, protocol: &str, party: &str) -> PathBuf {
        self.servers().join(format!("{protocol}-{party}"))
    }

    /// The anchor an FTPS client trusts the servers' certificate by.
    fn anchor(&self) -> PathBuf {
        self.servers().join("anchor.pem")
    }

    fn dropped(&self, protocol: &str, party: &str) -> PathBuf {
        self.root.join("dropped").join(protocol).join(party)
    }

    fn won(&self, protocol: &str, party: &str, file: &str) -> PathBuf {
        self.root.join("won").join(protocol).join(party).join(file)
    }
}

/// The roll's side: the far ends, every so many rounds a burst dropped for
/// every Party over every protocol, every file past its grace judged, and
/// the judgement recorded.
pub struct Dropping {
    drop_box: DropBox,
    far_ends: Result<FarEnds, String>,
    /// Each Party's sending transport by protocol, kept so a server's
    /// connection carries every burst.
    senders: BTreeMap<(String, String), Speaking>,
    judged: BTreeMap<&'static str, Judged>,
    round: u64,
}

impl Dropping {
    /// The roll's drop box under the cluster's `shared` directory, and its
    /// far ends started.
    #[must_use]
    pub fn new(shared: &Path) -> Self {
        let drop_box = DropBox::at(shared);
        let far_ends = FarEnds::start(&drop_box, &Parties::configured().names());
        Self {
            drop_box,
            far_ends,
            senders: BTreeMap::new(),
            judged: PROTOCOLS.map(|one| (one, Judged::default())).into(),
            round: 0,
        }
    }

    /// Drop the bursts due in `round`, each recorded as dropped before it
    /// is dropped.
    ///
    /// # Errors
    /// A file that could not be sent or recorded, in words.
    pub fn drop_burst(&mut self, parties: &Parties, round: u64) -> Result<usize, String> {
        let mut dropped = 0;
        for protocol in PROTOCOLS {
            if !(round - 1).is_multiple_of(bursts_every(protocol)) {
                continue;
            }
            for name in parties.names() {
                let record = self.drop_box.dropped(protocol, name);
                std::fs::create_dir_all(&record).map_err(|error| error.to_string())?;
                std::fs::create_dir_all(self.drop_box.location(name))
                    .map_err(|error| error.to_string())?;
                let key = (protocol.to_string(), name.to_string());
                if !self.senders.contains_key(&key) {
                    let built = transport(&self.drop_box, protocol, name, None)
                        .ok_or_else(|| format!("{protocol} for {name}: no server"))??;
                    self.senders.insert(key.clone(), built);
                }
                let into = &self.senders[&key];
                for one in 0..BURST {
                    let file = format!("{round:010}-{one:02}.txt");
                    std::fs::write(record.join(&file), b"").map_err(|error| error.to_string())?;
                    let body = format!("{name} dropped {file} over {protocol}");
                    into.send(&file, body.as_bytes())
                        .map_err(|error| format!("{protocol}: {name}: {file}: {error}"))?;
                    dropped += 1;
                }
            }
        }
        Ok(dropped)
    }

    /// Drop this round's bursts, judge what is due, and record each
    /// protocol's judgement at `<root>/drop-box/<protocol>` — a drop that
    /// failed, in its words.
    pub fn round(&mut self, root: &str, snapshot: &mut Snapshot, now: i64) {
        self.round += 1;
        let parties = Parties::configured();
        let dropped = match &self.far_ends {
            Ok(_) => self.drop_burst(parties, self.round),
            Err(why) => Err(format!("the drop box's servers did not start: {why}")),
        };
        for (protocol, judged) in &mut self.judged {
            judged.judge(&self.drop_box, protocol, parties, SystemTime::now());
            let mut record = judged.record(root, protocol, now);
            if let Err(why) = &dropped {
                record.health = Health::Done;
                record.evidence = format!("a burst was not dropped: {why}; {}", record.evidence);
            }
            snapshot.record_health(record);
        }
    }

    /// The judgement so far, protocol by protocol, in words.
    #[must_use]
    pub fn words(&self) -> String {
        self.judged
            .iter()
            .map(|(protocol, judged)| format!("{protocol}: {}", judged.words()))
            .collect::<Vec<_>>()
            .join("; ")
    }
}

/// One Party's directory over one protocol, as a receiving node reads it.
struct Location {
    protocol: &'static str,
    party: String,
    /// Built once its far end says where it listens.
    transport: Option<Speaking>,
    taken: u64,
}

/// One receiving node reading every Party's directories: the transport of
/// each, kept so its listing's stability check spans rounds, and what the
/// node has taken from each.
pub struct Taker {
    name: String,
    node: NodeLocation,
    drop_box: DropBox,
    locations: Vec<Location>,
    round: u64,
}

impl Taker {
    /// The node called `name`, at `node`, reading the drop box every Party
    /// drops into.
    #[must_use]
    pub fn new(name: &str, node: &str, drop_box: DropBox, parties: &Parties) -> Self {
        let locations = PROTOCOLS
            .into_iter()
            .flat_map(|protocol| {
                parties.names().into_iter().map(move |party| Location {
                    protocol,
                    party: party.to_string(),
                    transport: None,
                    taken: 0,
                })
            })
            .collect();
        Self {
            name: name.to_string(),
            node: NodeLocation::new(node),
            drop_box,
            locations,
            round: 0,
        }
    }

    /// Take what this node's claims win, accept and sign each, and record
    /// at `<node>/drop-box/<protocol>/<party>` the Streams it has taken
    /// from each.
    pub fn take(&mut self, node: &str, snapshot: &mut Snapshot, now: i64) {
        self.round += 1;
        for location in &mut self.locations {
            if self.round.is_multiple_of(listed_every(location.protocol)) {
                take_from(location, (&self.drop_box, &self.node, &self.name));
            }
            snapshot.record_count(Count {
                scope: format!("{node}/drop-box/{}/{}", location.protocol, location.party),
                counted: Counted::Streams,
                value: location.taken,
                window_start_unix_nanos: now,
                window_end_unix_nanos: now,
                observed_unix_nanos: now,
            });
        }
    }
}

/// What `location` hands this node now, each accepted and signed as `name`.
fn take_from(location: &mut Location, taker: (&DropBox, &NodeLocation, &str)) {
    let (drop_box, node, name) = taker;
    if location.transport.is_none() {
        location.transport = transport(drop_box, location.protocol, &location.party, Some(node))
            .and_then(Result::ok);
    }
    let Some(Ok(arrived)) = location.transport.as_ref().map(|one| one.receive()) else {
        return;
    };
    for one in arrived {
        let (origin, mut body, acknowledgement) = one.into_parts();
        let file = origin
            .split('?')
            .next()
            .and_then(|path| path.rsplit('/').next())
            .unwrap_or_default()
            .to_string();
        let mut bytes = Vec::new();
        let read = body.read_to_end(&mut bytes).is_ok();
        drop(body);
        let verdict = if read {
            Verdict::Accepted
        } else {
            Verdict::Failed
        };
        if acknowledgement.acknowledge(verdict).is_ok() && read {
            let signed = drop_box.won(location.protocol, &location.party, &file);
            let _ = std::fs::create_dir_all(&signed);
            let _ = std::fs::write(signed.join(name), b"");
            location.taken += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::support::{cluster_root, declaring, scratch, test_cluster};

    /// Two receiving nodes race for every Party's bursts over every
    /// protocol, each on its own thread, until each is drained; every file
    /// has one winner.
    #[test]
    fn two_receivers_racing_take_every_file_exactly_once_over_every_protocol() {
        let shared = scratch("drop-box");
        let parties = Parties::configured();
        let mut dropping = Dropping::new(&shared);
        assert!(
            dropping.far_ends.is_ok(),
            "{:?}",
            dropping.far_ends.as_ref().err()
        );
        let cluster = test_cluster();
        let [first, second] = declaring(&cluster, "sending")[..] else {
            panic!("the test cluster has two nodes of a kind");
        };
        // The first round drops over every protocol, the second over file
        // alone: the servers' bursts are paced.
        let dropped = dropping.drop_burst(parties, 1).expect("dropped")
            + dropping.drop_burst(parties, 2).expect("dropped");
        let per_protocol = BURST * parties.all().len();
        assert_eq!(dropped, (PROTOCOLS.len() + 1) * per_protocol);
        assert!(bursts_every("ftp") > 1 && bursts_every("file") == 1);

        let drop_box = dropping.drop_box.clone();
        let race = |name: &str| {
            let node = format!("{}/node/{name}", cluster_root());
            let mut taker = Taker::new(name, &node, drop_box.clone(), parties);
            let mut snapshot = Snapshot::new();
            for _ in 0..24 {
                taker.take(&node, &mut snapshot, 1);
            }
            (node, snapshot)
        };
        let (one, two) = std::thread::scope(|scope| {
            let one = scope.spawn(|| race(first));
            let two = scope.spawn(|| race(second));
            (one.join().expect("first"), two.join().expect("second"))
        });
        let took = |(node, snapshot): &(String, Snapshot)| {
            snapshot
                .measure(&format!("{node}/drop-box"), Counted::Streams)
                .map_or(0, |count| count.value)
        };
        assert_eq!(took(&one) + took(&two), dropped as u64);

        let later = SystemTime::now() + 2 * GRACE;
        let mut files = 0;
        for protocol in PROTOCOLS {
            let mut judged = Judged::default();
            judged.judge(&drop_box, protocol, parties, later);
            assert!(judged.held(), "{protocol}: {}", judged.words());
            assert!(
                judged
                    .words()
                    .starts_with("every file taken by exactly one receiving node: "),
                "{}",
                judged.words()
            );
            files += judged.files;
        }
        assert_eq!(files, dropped as u64);
        drop(dropping);
        let _ = std::fs::remove_dir_all(&shared);
    }
}
