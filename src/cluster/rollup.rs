//! The cluster's rollup of its nodes, and the merge that assembles a cluster
//! view from what each node published (ADR-0027 decision 8).

use observe::{Health, HealthRecord, Snapshot};

use crate::support::cluster_root;

/// The rollup the surface owes, at `<cluster>/node`: the worst leaf across
/// every node, and which node carries it. It sits on the branch the nodes
/// hang under, so the scope tree shows it as that branch's own record and
/// never as a node beside them. One point of severity below the leaf it
/// names, so the way to the problem ends at the leaf and not here.
pub(super) fn rollup(snapshot: &Snapshot, count: usize, now: i64) -> HealthRecord {
    let nodes = format!("{}/node", cluster_root());
    let records = snapshot.health(&nodes);
    let worst = records.first();
    let fine = records
        .iter()
        .filter(|record| record.health == Health::Fine)
        .count();
    let (health, severity, evidence) = match worst {
        Some(record) if record.health != Health::Fine => (
            record.health,
            record.severity.saturating_sub(1),
            format!(
                "{count} nodes, {fine} of {} leaves fine; worst {}: {}",
                records.len(),
                record.scope.trim_start_matches(&format!("{nodes}/")),
                record.evidence
            ),
        ),
        Some(_) => (
            Health::Fine,
            0,
            format!("{count} nodes, all {} leaves fine", records.len()),
        ),
        None => (
            Health::Working,
            20,
            format!("{count} nodes, nothing published yet"),
        ),
    };
    HealthRecord {
        scope: nodes,
        health,
        severity,
        evidence,
        observed_unix_nanos: now,
    }
}

/// Copy every health record and count from one snapshot into another. Scopes
/// are disjoint per scenario and per node, so nothing collides.
pub fn merge(into: &mut Snapshot, from: &Snapshot) {
    for record in from.health_records() {
        into.record_health(record.clone());
    }
    for count in from.all_counts() {
        into.record_count(count.clone());
    }
}
