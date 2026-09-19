//! The handoff between node processes: a pair's bytes passed from the node
//! that received it to one that can process it and on to one that can send
//! it, through inboxes in the cluster's shared directory.
//!
//! The owner, 2026-09-19: the topology showed nothing *between receiving
//! nodes, processing nodes and sending nodes*, because nothing went between
//! them. This is what goes between them. Each node has one [`Inbox`] per stage
//! it declared, `<shared>/handoff/<node>/<stage>/`, so a node that declared
//! two stages does not drain its own handoffs at the wrong one. A sender
//! writes a [`Handoff`] under a temporary
//! name and renames it into place, so a reader never sees half a file; the
//! receiver claims one by renaming it to a name only it uses, so a file has
//! one holder however many look — the rename semantics ADR-0024's claim rests
//! on. Every delivered handoff is a hop, counted per link in [`Hops`] and
//! published, which is what the topology draws.
//!
//! This rehearses one option in the rig and rules nothing for the runtime
//! (`doc/planning/open-problems.md`, problem 17).

use std::collections::BTreeMap;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::schedule::CONTRACTS;
use crate::verdict::{Contract, Stage};

const READY: &str = "handoff";
const MAGIC: &str = "xmip-handoff-1";

/// One pair's bytes on their way to the next node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Handoff {
    pub transport: String,
    pub contract: Contract,
    /// The round the receiving node took it in: the pair's id with the two
    /// above, and what every later stage's faults are decided by.
    pub round: u64,
    /// The node that handed it on.
    pub from: String,
    pub bytes: Vec<u8>,
}

impl Handoff {
    /// One header line, then the bytes as they are.
    fn encode(&self) -> Vec<u8> {
        let mut out = format!(
            "{MAGIC} {} {} {} {} {}\n",
            self.transport,
            self.contract.name(),
            self.round,
            self.from,
            self.bytes.len()
        )
        .into_bytes();
        out.extend_from_slice(&self.bytes);
        out
    }

    fn decode(raw: &[u8]) -> Option<Self> {
        let end = raw.iter().position(|byte| *byte == b'\n')?;
        let header = std::str::from_utf8(&raw[..end]).ok()?;
        let mut words = header.split(' ');
        if words.next()? != MAGIC {
            return None;
        }
        let transport = words.next()?.to_string();
        let name = words.next()?;
        let contract = CONTRACTS.into_iter().find(|one| one.name() == name)?;
        let round = words.next()?.parse().ok()?;
        let from = words.next()?.to_string();
        let length: usize = words.next()?.parse().ok()?;
        let bytes = raw[end + 1..].to_vec();
        (bytes.len() == length).then_some(Self {
            transport,
            contract,
            round,
            from,
            bytes,
        })
    }
}

/// One node's inbox in the cluster's shared directory.
#[derive(Clone, Debug)]
pub struct Inbox {
    dir: PathBuf,
}

impl Inbox {
    /// The inbox `node` drains at `stage`, under `shared`:
    /// `<shared>/handoff/<node>/<stage>/`.
    #[must_use]
    pub fn of(shared: &Path, node: &str, stage: Stage) -> Self {
        Self {
            dir: shared.join("handoff").join(node).join(stage.name()),
        }
    }

    /// Put `handoff` in this inbox: written whole under a temporary name,
    /// then renamed into place. `sequence` is the sender's own count, which
    /// with its name makes the file's name unique.
    ///
    /// # Errors
    ///
    /// When the inbox cannot be created or the file written or renamed.
    pub fn deliver(&self, handoff: &Handoff, sequence: u64) -> io::Result<()> {
        std::fs::create_dir_all(&self.dir)?;
        let name = format!(
            "{:010}-{}-{sequence:010}-{}-{}",
            handoff.round,
            handoff.from,
            handoff.transport,
            handoff.contract.name()
        );
        let writing = self.dir.join(format!("{name}.writing"));
        let mut file = std::fs::File::create(&writing)?;
        file.write_all(&handoff.encode())?;
        drop(file);
        std::fs::rename(&writing, self.dir.join(format!("{name}.{READY}")))
    }

    /// Claim up to `limit` handoffs, oldest round first. A file is claimed by
    /// renaming it to this process's own name for it; a rename that fails is
    /// a race lost, not an error. A file that does not read back as a handoff
    /// is dropped and counted in the second value.
    #[must_use]
    pub fn claim(&self, limit: usize) -> (Vec<Handoff>, usize) {
        let Ok(entries) = std::fs::read_dir(&self.dir) else {
            return (Vec::new(), 0);
        };
        let mut ready: Vec<PathBuf> = entries
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| path.extension().is_some_and(|ext| ext == READY))
            .collect();
        ready.sort();

        let mut claimed = Vec::new();
        let mut unreadable = 0;
        for path in ready.into_iter().take(limit) {
            let mine = path.with_extension(format!("claimed-{}", std::process::id()));
            if std::fs::rename(&path, &mine).is_err() {
                continue;
            }
            match std::fs::read(&mine)
                .ok()
                .as_deref()
                .and_then(Handoff::decode)
            {
                Some(handoff) => claimed.push(handoff),
                None => unreadable += 1,
            }
            std::fs::remove_file(&mine).ok();
        }
        (claimed, unreadable)
    }

    /// How many handoffs wait unclaimed.
    #[must_use]
    pub fn waiting(&self) -> usize {
        std::fs::read_dir(&self.dir).map_or(0, |entries| {
            entries
                .flatten()
                .filter(|entry| entry.path().extension().is_some_and(|ext| ext == READY))
                .count()
        })
    }
}

/// The hops over one link: how many handoffs `from` delivered `to`, the stage
/// each end served, and when the last one was.
///
/// The stages are carried, not worked out later from the node names: the
/// topology draws this link between two stages and a name is no criterion
/// (ADR-0056). An older file without them reads back with neither, and such a
/// link is not drawn.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hop {
    pub from: String,
    /// The stage the handoff left, by name.
    #[serde(default)]
    pub from_stage: String,
    pub to: String,
    /// The stage it was delivered to, by name.
    #[serde(default)]
    pub to_stage: String,
    pub count: u64,
    pub last_unix_nanos: i64,
}

/// Every link a node has handed over, with its hops.
#[derive(Clone, Debug, Default)]
pub struct Hops {
    links: BTreeMap<(String, String), Hop>,
}

impl Hops {
    /// Record one delivered handoff from one node's stage to another's.
    pub fn record(&mut self, from: (&str, Stage), to: (&str, Stage), now: i64) {
        let ends = (
            format!("{}/{}", from.0, from.1.name()),
            format!("{}/{}", to.0, to.1.name()),
        );
        let hop = self.links.entry(ends).or_insert_with(|| Hop {
            from: from.0.to_string(),
            from_stage: from.1.name().to_string(),
            to: to.0.to_string(),
            to_stage: to.1.name().to_string(),
            count: 0,
            last_unix_nanos: now,
        });
        hop.count += 1;
        hop.last_unix_nanos = now;
    }

    /// The links, ordered by where they start and end.
    pub fn links(&self) -> impl Iterator<Item = &Hop> {
        self.links.values()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::support::scratch;

    fn handoff(round: u64, bytes: &[u8]) -> Handoff {
        Handoff {
            transport: "tcp".to_string(),
            contract: Contract::FixedWidth,
            round,
            from: "R1".to_string(),
            bytes: bytes.to_vec(),
        }
    }

    #[test]
    fn what_is_delivered_is_claimed_once_whole_and_oldest_first() {
        let shared = scratch("handoff");
        let inbox = Inbox::of(&shared, "P1", Stage::Process);
        let binary = [0x00, b'\n', 0xff, b' ', b'\n'];
        inbox.deliver(&handoff(2, b"second"), 1).expect("delivered");
        inbox.deliver(&handoff(1, &binary), 2).expect("delivered");
        inbox.deliver(&handoff(3, b""), 3).expect("delivered");
        assert_eq!(inbox.waiting(), 3);

        let (first, unreadable) = inbox.claim(2);
        assert_eq!(unreadable, 0);
        assert_eq!(first, [handoff(1, &binary), handoff(2, b"second")]);
        let (rest, _) = Inbox::of(&shared, "P1", Stage::Process).claim(8);
        assert_eq!(rest, [handoff(3, b"")], "a claimed handoff is gone");
        assert_eq!(inbox.claim(8), (Vec::new(), 0));
        assert_eq!(
            Inbox::of(&shared, "P2", Stage::Process).claim(8),
            (Vec::new(), 0)
        );
        assert_eq!(
            Inbox::of(&shared, "P1", Stage::Send).claim(8),
            (Vec::new(), 0),
            "one node's two stages do not share an inbox"
        );
        std::fs::remove_dir_all(&shared).ok();
    }

    #[test]
    fn a_half_written_file_is_never_claimed_and_a_torn_one_is_counted() {
        let shared = scratch("handoff-torn");
        let inbox = Inbox::of(&shared, "S1", Stage::Send);
        inbox.deliver(&handoff(1, b"whole"), 1).expect("delivered");
        let dir = shared.join("handoff/S1/send");
        std::fs::write(dir.join("0000000002-R1-x.writing"), b"half").expect("written");
        std::fs::write(dir.join("0000000003-R1-x.handoff"), b"not a handoff").expect("written");

        let (claimed, unreadable) = inbox.claim(8);
        assert_eq!(claimed, [handoff(1, b"whole")]);
        assert_eq!(unreadable, 1);
        assert!(dir.join("0000000002-R1-x.writing").exists());
        std::fs::remove_dir_all(&shared).ok();
    }

    #[test]
    fn hops_count_per_link_with_the_stages_at_each_end_and_the_last_time() {
        let mut hops = Hops::default();
        hops.record(("R1", Stage::Receive), ("P1", Stage::Process), 5);
        hops.record(("R1", Stage::Receive), ("P2", Stage::Process), 6);
        hops.record(("R1", Stage::Receive), ("P1", Stage::Process), 9);
        let links: Vec<_> = hops.links().cloned().collect();
        assert_eq!(links.len(), 2);
        assert_eq!(
            (
                links[0].to.as_str(),
                links[0].count,
                links[0].last_unix_nanos
            ),
            ("P1", 2, 9)
        );
        assert_eq!(
            (links[0].from_stage.as_str(), links[0].to_stage.as_str()),
            ("receive", "process"),
            "the link says which stages it runs between"
        );
        assert_eq!((links[1].to.as_str(), links[1].count), ("P2", 1));
    }
}
