//! Writing a snapshot and its history where a monitoring surface can read them.
//!
//! The playground and the readers (the GUI, the CLI) are separate processes;
//! the bridge between them is a file. It is written **atomically** — a temp file
//! flushed to the device and renamed over the target — so a reader never
//! catches a half-written file, or an empty one, even after a week of ticks.
//!
//! **The snapshot's shape is not this file's.** It is `observe::Publication`'s,
//! which writes it and reads it, and which a surface reads through the
//! runtime's library (open problem 25): this file names the publisher, adds
//! the handoffs a node's own file carries for the cluster. The history and
//! the activity are `observe::Curve`'s and `observe::Recent`'s the same way.
//!
//! **TOML, not JSON.** On disk the estate is TOML — the owner's rule, the same
//! reason `architecture.json` was deleted for `architecture.toml`; JSON is
//! reserved for what lives in memory or on the wire.

use std::io::{self, Write};
use std::path::Path;

use observe::{Activity, Curve, History, Publication, Recent, Run, Snapshot, Topology};
use serde::{Deserialize, Serialize};

use crate::handoff::Hop;

/// The handoffs a node delivered, per link, beside its publication in its
/// own file; a roll writes none — it draws them as the topology's links.
#[derive(Default, Serialize, Deserialize)]
struct Handoffs {
    #[serde(default)]
    hops: Vec<Hop>,
}

/// What the Playground calls itself as the publisher at `node`.
fn source(node: &str) -> String {
    format!("playground — {node}")
}

/// A roll's snapshot: the records beneath `node`, every kind summed at
/// `node`, and — when the roll has them — the communication topology under
/// `[topology]` and what the run was started with under `[run]`.
#[must_use]
pub fn roll_toml(
    node: &str,
    snapshot: &Snapshot,
    topology: Option<Topology>,
    run: Option<Run>,
) -> String {
    Publication::of(&source(node), node, snapshot)
        .with_topology(topology)
        .with_run(run)
        .to_toml()
}

/// A node's own file: its records, every count at the scope it was recorded
/// at — two tests on one node count the same kinds, and the cluster must
/// tell them apart — and the handoffs it delivered, per link.
#[must_use]
pub fn node_toml(node: &str, snapshot: &Snapshot, hops: Vec<Hop>) -> String {
    let mut text = Publication::whole(&source(node), node, snapshot).to_toml();
    if !hops.is_empty() {
        text.push('\n');
        text.push_str(&toml::to_string(&Handoffs { hops }).unwrap_or_default());
    }
    text
}

/// The snapshot a node published and the handoffs it delivered, read back
/// from the file [`node_toml`] wrote — the other half of the bridge, for a
/// cluster assembling itself from the files its nodes wrote (ADR-0027
/// decision 8).
///
/// # Errors
///
/// When the text is not a publication, in the reader's words.
pub fn node_from_toml(text: &str) -> Result<(Snapshot, Vec<Hop>), String> {
    let publication = Publication::read(text)?;
    let handoffs: Handoffs = toml::from_str(text).map_err(|error| error.to_string())?;
    Ok((publication.snapshot(), handoffs.hops))
}

/// The node's throughput over time as the file `Get-XmipHistory` reads:
/// `observe::Curve`, its own series at its exact scope, oldest first.
/// ADR-0029.
#[must_use]
pub fn history_toml(node: &str, history: &History) -> String {
    Curve::of(node, history).to_toml()
}

/// The recent individual items beneath `node`, newest first, as
/// `observe::Recent` writes them. ADR-0032.
#[must_use]
pub fn activity_toml(node: &str, activity: &Activity) -> String {
    Recent::of(node, activity).to_toml()
}

/// Write `contents` to `path` atomically: a sibling temp file, flushed to the
/// device, then a rename over the target. A reader either sees the previous
/// file or this one, never a torn write — and never an empty one: the rename
/// is journaled and the data is not, so a hard stop between the write and the
/// flush left a snapshot, a history and an activity file of the right length
/// and nothing but zeros in them, and the prompt said unavailable for two
/// days (2026-09-16).
///
/// # Errors
///
/// Where the parent could not be created, or the file could not be written,
/// flushed or renamed.
pub fn write_atomic(path: &Path, contents: &str) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Its own temporary file: two processes publishing to one path — which
    // Start-XmipTest now refuses, and a roll started by hand can still do —
    // must not write into each other's half-finished file.
    let temp = path.with_extension(format!("toml.writing-{}", std::process::id()));
    let mut file = std::fs::File::create(&temp)?;
    file.write_all(contents.as_bytes())?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&temp, path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Schedule;
    use observe::Counted;

    #[test]
    fn a_ticked_schedule_serialises_to_toml_with_records_and_counts() {
        let dir = std::env::temp_dir().join("xmip-report-test");
        std::fs::remove_dir_all(&dir).ok();
        let mut schedule =
            Schedule::new("xmip:///playground", &dir).over(crate::support::three(&dir));
        let snapshot = schedule.tick();

        let text = roll_toml("xmip:///playground", &snapshot, None, None);
        let parsed: toml::Value = text.parse().expect("valid TOML");

        assert_eq!(parsed["node"].as_str(), Some("xmip:///playground"));
        assert!(!parsed["records"].as_array().expect("records").is_empty());
        assert!(!parsed["counts"].as_array().expect("counts").is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_written_snapshot_reads_back_whole() {
        let dir = std::env::temp_dir().join("xmip-report-readback-test");
        std::fs::remove_dir_all(&dir).ok();
        let mut schedule =
            Schedule::new("xmip:///playground", &dir).over(crate::support::three(&dir));
        let written = schedule.tick();

        let (read, hops) = node_from_toml(&roll_toml("xmip:///playground", &written, None, None))
            .expect("reads back");
        assert!(hops.is_empty(), "a roll writes no handoffs");

        let before: Vec<_> = written.health("xmip:///playground");
        let after: Vec<_> = read.health("xmip:///playground");
        assert_eq!(before, after, "every record survives the round trip");
        assert_eq!(
            read.measure("xmip:///playground", Counted::Bytes)
                .map(|count| count.value),
            written
                .measure("xmip:///playground", Counted::Bytes)
                .map(|count| count.value),
            "the node's counts survive at the node's scope"
        );
        assert!(node_from_toml("not = [toml").is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_nodes_file_carries_its_handoffs_beside_its_publication() {
        let hop = Hop {
            from: "alpha".to_string(),
            from_stage: "receive".to_string(),
            to: "beta".to_string(),
            to_stage: "process".to_string(),
            count: 3,
            last_unix_nanos: 7,
        };
        let mut snapshot = Snapshot::new();
        snapshot.record_health(observe::HealthRecord {
            scope: "xmip:///alpha/receive/tcp".to_string(),
            health: observe::Health::Fine,
            severity: 0,
            evidence: "3/3".to_string(),
            observed_unix_nanos: 7,
        });

        let text = node_toml("xmip:///alpha", &snapshot, vec![hop.clone()]);
        let (read, hops) = node_from_toml(&text).expect("reads back");

        assert_eq!(hops, [hop]);
        assert_eq!(
            read.health("xmip:///alpha"),
            snapshot.health("xmip:///alpha")
        );
    }

    #[test]
    fn history_serialises_to_toml_points() {
        let dir = std::env::temp_dir().join("xmip-report-history-test");
        std::fs::remove_dir_all(&dir).ok();
        let mut schedule =
            Schedule::new("xmip:///playground", &dir).over(crate::support::three(&dir));
        let mut history = History::default();
        history.record(&schedule.tick());
        history.record(&schedule.tick());

        let text = history_toml("xmip:///playground", &history);
        let parsed: toml::Value = text.parse().expect("valid TOML");

        let points = parsed["points"].as_array().expect("points");
        assert!(!points.is_empty());
        assert!(
            points
                .iter()
                .any(|p| p["counted"].as_str() == Some("bytes"))
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn an_atomic_write_lands_the_contents() {
        let path = std::env::temp_dir().join("xmip-report-atomic/snapshot.toml");
        std::fs::remove_dir_all(path.parent().expect("parent")).ok();

        write_atomic(&path, "node = \"x\"\n").expect("write");

        assert_eq!(
            std::fs::read_to_string(&path).expect("read"),
            "node = \"x\"\n"
        );
        std::fs::remove_dir_all(path.parent().expect("parent")).ok();
    }
}
