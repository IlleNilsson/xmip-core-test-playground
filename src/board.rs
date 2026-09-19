//! The board a roll draws: every leaf of the snapshot, as a person reads it.
//!
//! Two views of the same round, chosen by where standard output goes. A live
//! terminal is cleared and reprinted in place, so the board stands still and
//! the numbers move. A redirected one — `Start-XmipTest` redirects the roll's
//! output to `roll-<start time>.log` — is appended to, one line per round,
//! because a screen-clearing escape in a log file is noise a reader has to
//! step over.
//!
//! A node's leaves are left out of both while the node is fine: a brutal roll
//! is twenty node processes and seven tests, and the rollup row is what an
//! operator watches. A node that is not fine appears in full, which is the
//! moment its leaves are worth the room.

use observe::{Health, HealthRecord, Snapshot};

/// The full board, cleared and reprinted in place — a live terminal view.
pub fn redraw(node: &str, round: u64, snapshot: &Snapshot) {
    print!("\x1b[2J\x1b[H");
    println!("Xmip Playground — rolling every scenario   (round {round})");
    println!("{:-<86}", "");

    for record in pairs(node, snapshot) {
        let leaf = record
            .scope
            .strip_prefix(&format!("{node}/"))
            .unwrap_or(&record.scope);
        println!(
            "  {:<44} {:<7} sev {:>3}   {}",
            leaf,
            word(record.health),
            record.severity,
            record.evidence
        );
    }

    println!("{:-<86}", "");
    println!(
        "  rollup at {node}: {}",
        word(snapshot.worst(node).unwrap_or(Health::Fine))
    );
    println!("\n  ctrl-c to stop");
}

/// One line per round, for a piped run: the rollup, and the worst leaf when it
/// is not green.
pub fn summarise(node: &str, round: u64, snapshot: &Snapshot) {
    let worst = snapshot.worst(node).map_or("NONE", word);
    let count = pairs(node, snapshot).len();

    let trouble = pairs(node, snapshot)
        .into_iter()
        .find(|record| record.health != Health::Fine)
        .map_or_else(String::new, |record| {
            format!("  — worst {}: {}", record.scope, record.evidence)
        });

    println!("round {round:>4}: {worst}  ({count} leaves){trouble}");
}

/// The rows the board shows: every leaf, except that a node's leaves appear
/// only when not fine — the nodes' rollup row always does, and an operator
/// drills into a node from there.
#[must_use]
pub fn pairs(node: &str, snapshot: &Snapshot) -> Vec<HealthRecord> {
    let nodes = format!("{node}/node/");
    let mut records = snapshot.health(node);
    records.retain(|record| !record.scope.starts_with(&nodes) || record.health != Health::Fine);
    records.sort_by(|left, right| left.scope.cmp(&right.scope));
    records
}

/// The word a health carries on the board. The estate says its outcomes in
/// words and never in colour alone.
#[must_use]
pub const fn word(health: Health) -> &'static str {
    match health {
        Health::Fine => "FINE",
        Health::Paused => "PAUSED",
        Health::Working => "WORKING",
        Health::Stressed => "STRESSED",
        Health::Exhausted => "EXHAUSTED",
        Health::Holding => "HOLDING",
        Health::Done => "DONE",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn recorded(scope: &str, health: Health) -> HealthRecord {
        HealthRecord {
            scope: scope.to_string(),
            health,
            severity: 0,
            evidence: "recorded".to_string(),
            observed_unix_nanos: 1,
        }
    }

    #[test]
    fn a_healthy_node_keeps_its_leaves_off_the_board() {
        let mut snapshot = Snapshot::new();
        let leaf = "xmip:///C1/round-trip/tcp/json";
        snapshot.record_health(recorded(leaf, Health::Fine));
        snapshot.record_health(recorded("xmip:///C1/node/node-02/leaf", Health::Fine));

        let shown = pairs("xmip:///C1", &snapshot);
        let scopes: Vec<&str> = shown.iter().map(|record| record.scope.as_str()).collect();

        assert!(scopes.contains(&leaf));
        assert!(
            !scopes.contains(&"xmip:///C1/node/node-02/leaf"),
            "a fine node's leaves stay off the board: {scopes:?}"
        );
    }

    #[test]
    fn a_node_that_is_not_fine_appears_in_full() {
        let mut snapshot = Snapshot::new();
        snapshot.record_health(recorded("xmip:///C1/node/node-02/leaf", Health::Stressed));

        let shown = pairs("xmip:///C1", &snapshot);

        assert_eq!(shown.len(), 1, "the leaf that is not fine is shown");
        assert_eq!(word(Health::Stressed), "STRESSED");
        assert_eq!(word(Health::Fine), "FINE");
    }
}
