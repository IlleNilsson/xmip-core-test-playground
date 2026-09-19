//! Writing a snapshot and its history where a monitoring surface can read them.
//!
//! The playground and the readers (the GUI, the CLI) are separate processes;
//! the bridge between them is a file. It is written **atomically** — a temp file
//! flushed to the device and renamed over the target — so a reader never
//! catches a half-written file, or an empty one, even after a week of ticks.
//!
//! **TOML, not JSON.** On disk the estate is TOML — the owner's rule, the same
//! reason `architecture.json` was deleted for `architecture.toml`; JSON is
//! reserved for what lives in memory or on the wire. These files persist, so
//! they are TOML. (Content that happens to be JSON, like a probe's payload, is
//! a different thing entirely — that is data being carried, not a file the
//! estate configures itself from.)

use std::io::{self, Write};
use std::path::Path;

use observe::{Activity, Count, Counted, Health, HealthRecord, History, ItemKind, Snapshot};
use serde::{Deserialize, Serialize};

use crate::handoff::Hop;
use crate::run::Run;
use crate::topology::Topology;

#[derive(Serialize, Deserialize)]
struct SnapshotReport {
    source: String,
    node: String,
    records: Vec<RecordReport>,
    counts: Vec<CountReport>,
    /// What the run was started with, when a roll says (2026-09-19); a node
    /// writes none, and a reader that does not know the table skips it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    run: Option<Run>,
    /// The handoffs a role node delivered, per link; a roll writes none —
    /// it draws them as the topology's links.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    hops: Vec<Hop>,
    /// The cluster's communication, when a roll has one to publish (ADR-0052,
    /// amendment 2026-09-14); a node writes none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    topology: Option<Topology>,
}

#[derive(Serialize, Deserialize)]
struct RecordReport {
    scope: String,
    state: String,
    severity: u8,
    evidence: String,
    observed_unix_nanos: i64,
}

#[derive(Serialize, Deserialize)]
struct CountReport {
    counted: String,
    value: u64,
    /// Where the count was recorded, in a node's own file; empty in a roll's,
    /// whose counts are the sums at its root, as the surfaces read them.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    scope: String,
}

#[derive(Serialize)]
struct HistoryReport {
    node: String,
    points: Vec<PointReport>,
}

#[derive(Serialize)]
struct ActivityReport {
    node: String,
    items: Vec<ItemReport>,
}

#[derive(Serialize)]
struct ItemReport {
    kind: String,
    scope: String,
    id: String,
    bytes: u64,
    detail: String,
    observed_unix_nanos: i64,
}

#[derive(Serialize)]
struct PointReport {
    counted: String,
    observed_unix_nanos: i64,
    value: u64,
}

/// The snapshot beneath `node` as the TOML the GUI's file surface reads: health
/// records and the node's throughput counts.
#[must_use]
pub fn to_toml(node: &str, snapshot: &Snapshot) -> String {
    to_toml_with(node, snapshot, None)
}

/// [`to_toml`] with the communication topology a roll publishes beside the
/// snapshot: the cluster's nodes and links, under `[topology]`.
#[must_use]
pub fn to_toml_with(node: &str, snapshot: &Snapshot, topology: Option<Topology>) -> String {
    to_toml_run(node, snapshot, topology, None)
}

/// [`to_toml_with`] with what the run was started with, under `[run]`.
#[must_use]
pub fn to_toml_run(
    node: &str,
    snapshot: &Snapshot,
    topology: Option<Topology>,
    run: Option<Run>,
) -> String {
    let counts = COUNTED
        .into_iter()
        .filter_map(|counted| {
            snapshot.measure(node, counted).map(|count| CountReport {
                counted: counted_name(counted).to_string(),
                value: count.value,
                scope: String::new(),
            })
        })
        .collect();
    let report = SnapshotReport {
        source: format!("playground — {node}"),
        node: node.to_string(),
        records: records(node, snapshot),
        counts,
        run,
        hops: Vec::new(),
        topology,
    };
    toml::to_string(&report).unwrap_or_default()
}

/// A node's own file: its records, every count at the scope it was recorded
/// at — two tests on one node count the same kinds, and the cluster must
/// tell them apart — and the handoffs it delivered, per link.
#[must_use]
pub fn node_toml(node: &str, snapshot: &Snapshot, hops: Vec<Hop>) -> String {
    let counts = snapshot
        .all_counts()
        .map(|count| CountReport {
            counted: counted_name(count.counted).to_string(),
            value: count.value,
            scope: count.scope.clone(),
        })
        .collect();
    let report = SnapshotReport {
        source: format!("playground — {node}"),
        node: node.to_string(),
        records: records(node, snapshot),
        counts,
        run: None,
        hops,
        topology: None,
    };
    toml::to_string(&report).unwrap_or_default()
}

/// Every kind a snapshot file carries, in the order it lists them.
const COUNTED: [Counted; 6] = [
    Counted::Streams,
    Counted::Messages,
    Counted::Journeys,
    Counted::Bytes,
    Counted::Retrying,
    Counted::Failed,
];

fn records(node: &str, snapshot: &Snapshot) -> Vec<RecordReport> {
    snapshot
        .health(node)
        .into_iter()
        .map(|record| RecordReport {
            scope: record.scope,
            state: state(record.health).to_string(),
            severity: record.severity,
            evidence: record.evidence,
            observed_unix_nanos: record.observed_unix_nanos,
        })
        .collect()
}

/// The snapshot a node published, read back from the TOML [`to_toml`] wrote —
/// the other half of the bridge, for a surface assembling a cluster from the
/// files its nodes wrote (ADR-0027 decision 8). The counts come back at the
/// node's own scope, dated by the newest record; a mood or a counted kind the
/// reader does not know is skipped rather than guessed at.
///
/// # Errors
///
/// When the text is not the TOML this module writes.
pub fn from_toml(text: &str) -> Result<Snapshot, toml::de::Error> {
    node_from_toml(text).map(|(snapshot, _)| snapshot)
}

/// [`from_toml`] with the handoffs the node delivered, per link.
///
/// # Errors
///
/// When the text is not the TOML this module writes.
pub fn node_from_toml(text: &str) -> Result<(Snapshot, Vec<Hop>), toml::de::Error> {
    let report: SnapshotReport = toml::from_str(text)?;
    let mut snapshot = Snapshot::new();
    let newest = report
        .records
        .iter()
        .map(|record| record.observed_unix_nanos)
        .max()
        .unwrap_or(0);

    for record in report.records {
        if let Some(health) = health_named(&record.state) {
            snapshot.record_health(HealthRecord {
                scope: record.scope,
                health,
                severity: record.severity,
                evidence: record.evidence,
                observed_unix_nanos: record.observed_unix_nanos,
            });
        }
    }

    for count in report.counts {
        if let Some(counted) = counted_named(&count.counted) {
            let scope = if count.scope.is_empty() {
                report.node.clone()
            } else {
                count.scope
            };
            snapshot.record_count(Count {
                scope,
                counted,
                value: count.value,
                window_start_unix_nanos: newest,
                window_end_unix_nanos: newest,
                observed_unix_nanos: newest,
            });
        }
    }

    Ok((snapshot, report.hops))
}

/// The node's throughput over time as the TOML the history cmdlet and UI read:
/// one point per counted kind per tick, oldest first. ADR-0029.
#[must_use]
pub fn history_toml(node: &str, history: &History) -> String {
    let mut points = Vec::new();

    for counted in [Counted::Streams, Counted::Messages, Counted::Bytes] {
        for point in history.count_series(node, counted) {
            points.push(PointReport {
                counted: counted_name(counted).to_string(),
                observed_unix_nanos: point.observed_unix_nanos,
                value: point.value,
            });
        }
    }

    let report = HistoryReport {
        node: node.to_string(),
        points,
    };

    toml::to_string(&report).unwrap_or_default()
}

/// The recent individual items beneath `node` as the TOML the item view reads:
/// the Streams, Messages and Journeys of the last rounds, newest first. ADR-0032.
#[must_use]
pub fn activity_toml(node: &str, activity: &Activity) -> String {
    let items = activity
        .recent(node, None, 400)
        .into_iter()
        .map(|item| ItemReport {
            kind: kind_name(item.kind).to_string(),
            scope: item.scope,
            id: item.id,
            bytes: item.bytes,
            detail: item.detail,
            observed_unix_nanos: item.observed_unix_nanos,
        })
        .collect();

    let report = ActivityReport {
        node: node.to_string(),
        items,
    };

    toml::to_string(&report).unwrap_or_default()
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

pub(crate) const fn state(health: Health) -> &'static str {
    // The mood, not a colour — the surface reading this paints it (ADR-0041).
    match health {
        Health::Fine => "fine",
        Health::Paused => "paused",
        Health::Working => "working",
        Health::Stressed => "stressed",
        Health::Exhausted => "exhausted",
        Health::Holding => "holding",
        Health::Done => "done",
    }
}

fn health_named(state: &str) -> Option<Health> {
    [
        Health::Fine,
        Health::Paused,
        Health::Working,
        Health::Stressed,
        Health::Exhausted,
        Health::Holding,
        Health::Done,
    ]
    .into_iter()
    .find(|health| self::state(*health) == state)
}

fn counted_named(name: &str) -> Option<Counted> {
    COUNTED
        .into_iter()
        .find(|counted| counted_name(*counted) == name)
}

const fn kind_name(kind: ItemKind) -> &'static str {
    match kind {
        ItemKind::Stream => "stream",
        ItemKind::Message => "message",
        ItemKind::Journey => "journey",
    }
}

const fn counted_name(counted: Counted) -> &'static str {
    match counted {
        Counted::Streams => "streams",
        Counted::Messages => "messages",
        Counted::Journeys => "journeys",
        Counted::Bytes => "bytes",
        Counted::Retrying => "retrying",
        Counted::Failed => "failed",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Schedule;

    #[test]
    fn a_ticked_schedule_serialises_to_toml_with_records_and_counts() {
        let dir = std::env::temp_dir().join("xmip-report-test");
        std::fs::remove_dir_all(&dir).ok();
        let mut schedule = Schedule::new("xmip:///playground", &dir);
        let snapshot = schedule.tick();

        let text = to_toml("xmip:///playground", &snapshot);
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
        let mut schedule = Schedule::new("xmip:///playground", &dir);
        let written = schedule.tick();

        let read = from_toml(&to_toml("xmip:///playground", &written)).expect("reads back");

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
        assert!(from_toml("not = [toml").is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn history_serialises_to_toml_points() {
        let dir = std::env::temp_dir().join("xmip-report-history-test");
        std::fs::remove_dir_all(&dir).ok();
        let mut schedule = Schedule::new("xmip:///playground", &dir);
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
