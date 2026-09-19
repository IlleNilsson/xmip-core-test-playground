//! What a round puts into the history a surface draws. ADR-0029.
//!
//! A history point is a counted measurement over time, and the measurement a
//! node publishes is the rollup at its own scope — the sum
//! [`Snapshot::measure`] writes into the snapshot file. [`History`] keeps a
//! series per scope exactly as it was recorded, and a scenario records
//! beneath the node, never at it: `round-trip`, `heavy-load` and the rest
//! each publish under their own subtree.
//!
//! So a roll that recorded only its snapshot left `count_series(node, …)`
//! with nothing to answer for the node itself, and every history file the
//! Playground ever published held `points = []` — the CLI's history, the
//! ABI's series and the GUI's curve all drew nothing from it, and nothing
//! asserted otherwise (found 2026-09-19).
//!
//! A round is therefore recorded twice: the snapshot as it stands, which
//! keeps every scope's own series, and the node's rollup beside it, which is
//! the curve the file carries.

use observe::{History, Snapshot};

use crate::report::COUNTED;

/// Record one round of `snapshot` in `history`: every scope's own series, and
/// the rollup at `node` that [`crate::history_toml`] writes the file from.
pub fn record_round(history: &mut History, node: &str, snapshot: &Snapshot) {
    history.record(snapshot);
    history.record(&rollup(node, snapshot));
}

/// The node's own counts at this instant and nothing else: one
/// [`Snapshot::measure`] per kind, at the node's scope — a snapshot made to be
/// recorded, never published, so the sums are not written twice into the
/// snapshot file that already rolls them up.
fn rollup(node: &str, snapshot: &Snapshot) -> Snapshot {
    let mut rolled = Snapshot::new();

    for counted in COUNTED {
        if let Some(count) = snapshot.measure(node, counted) {
            rolled.record_count(count);
        }
    }

    rolled
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Schedule, history_toml};

    /// A scenario as a roll composes it: publishing under its own subtree of
    /// the node, never at the node, which is what `roll.rs` does.
    fn scenario(node: &str, name: &str) -> (Schedule, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("xmip-curve-test-{name}"));
        std::fs::remove_dir_all(&dir).ok();
        (Schedule::new(format!("{node}/{name}"), &dir), dir)
    }

    #[test]
    fn a_node_s_history_holds_the_points_its_scenarios_counted() {
        let node = "xmip:///Y1";
        let (mut schedule, dir) = scenario(node, "round-trip");
        let mut history = History::default();

        for _ in 0..2 {
            let snapshot = schedule.tick();
            record_round(&mut history, node, &snapshot);
        }

        let parsed: toml::Value = history_toml(node, &history).parse().expect("valid TOML");
        let points = parsed["points"].as_array().expect("points");

        assert!(!points.is_empty(), "a roll's history file holds points");
        for kind in ["streams", "messages", "bytes"] {
            assert!(
                points.iter().any(|p| p["counted"].as_str() == Some(kind)),
                "the curve holds {kind}: {points:?}"
            );
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn the_snapshot_alone_holds_none_of_them() {
        // The defect of 2026-09-19, kept as a test: the scenarios' counts are
        // recorded beneath the node, and a series is read at one exact scope,
        // so recording the snapshot alone leaves the node's own curve empty.
        let node = "xmip:///Y1";
        let (mut schedule, dir) = scenario(node, "heavy-load");
        let mut history = History::default();

        history.record(&schedule.tick());

        let parsed: toml::Value = history_toml(node, &history).parse().expect("valid TOML");
        assert!(parsed["points"].as_array().expect("points").is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn the_point_is_the_rollup_the_snapshot_file_publishes() {
        let node = "xmip:///Y1";
        let (mut schedule, dir) = scenario(node, "filing");
        let mut history = History::default();
        let snapshot = schedule.tick();

        record_round(&mut history, node, &snapshot);

        let measured = snapshot
            .measure(node, observe::Counted::Bytes)
            .map(|count| count.value);
        let recorded = history
            .count_series(node, observe::Counted::Bytes)
            .last()
            .map(|count| count.value);

        assert_eq!(recorded, measured, "the curve is what the snapshot says");
        std::fs::remove_dir_all(&dir).ok();
    }
}
