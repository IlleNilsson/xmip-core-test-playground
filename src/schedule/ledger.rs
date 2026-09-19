//! The ledger: every pair's record over time, the throughput, and the recent
//! items — what a run of `RoundTrip` verdicts accumulates into, and the one
//! way it is published.
//!
//! It was the second half of [`Schedule::tick`](crate::Schedule::tick) until
//! 2026-09-19. A node judges the stages it declared of the same test and must
//! publish
//! it the same way — the same tally, the same standing between a pair's
//! turns, the same counts — so the accumulation lives here, used by the
//! schedule and by the [`Relay`](crate::relay::Relay) alike, and neither
//! copies the other.

use std::collections::{BTreeMap, BTreeSet};

use observe::{Activity, Count, Counted, Item, ItemKind, Snapshot};

use super::tally::{Tally, over_time};
use crate::verdict::{Outcome, Stage, Verdict};

/// What the verdicts published under one node scope have added up to.
pub struct Ledger {
    node: String,
    tallies: BTreeMap<String, Tally>,
    activity: Activity,
    item_seq: u64,
    streams: u64,
    messages: u64,
    journeys: u64,
    moved_bytes: u64,
}

impl Ledger {
    /// An empty ledger publishing under `node`.
    #[must_use]
    pub fn new(node: impl Into<String>) -> Self {
        Self {
            node: node.into(),
            tallies: BTreeMap::new(),
            activity: Activity::with_capacity(2048),
            item_seq: 0,
            streams: 0,
            messages: 0,
            journeys: 0,
            moved_bytes: 0,
        }
    }

    /// The scope this ledger publishes under.
    #[must_use]
    pub fn node(&self) -> &str {
        &self.node
    }

    /// Fold one verdict into its pair's tally and publish the pair's health
    /// over time into `snapshot`.
    pub fn fold(&mut self, verdict: &Verdict, snapshot: &mut Snapshot, now: i64) {
        // Throughput and the activity feed count the transport verdict, not
        // the identity children: a Stream is received once, not once per
        // identity step. The identity points still fold into health, so an
        // operator drills to the step that failed.
        let is_transport = verdict.point.is_none();

        if is_transport && matches!(verdict.outcome, Outcome::Delivered) {
            match verdict.stage {
                Stage::Receive => self.streams += 1,
                Stage::Process => self.journeys += 1,
                Stage::Send => {
                    self.messages += 1;
                    self.moved_bytes += verdict.bytes;
                }
            }
        }

        let scope = verdict.scope(&self.node);
        let tally = self.tallies.entry(scope.clone()).or_default();
        tally.fold(&verdict.outcome);
        snapshot.record_health(over_time(&scope, tally, now));

        if is_transport {
            self.item_seq += 1;
            self.activity.record(Item {
                kind: item_kind(verdict.stage),
                scope,
                id: format!("{:08}", self.item_seq),
                bytes: verdict.bytes,
                detail: detail(&verdict.outcome),
                observed_unix_nanos: now,
            });
        }
    }

    /// Close a round: the pairs that waited keep their standing on the board,
    /// and the cumulative throughput is published at the node scope — Streams
    /// in at Receive, Journeys through Process, Messages out at Send, and the
    /// Bytes that moved. These are what the operator's stage cards count.
    pub fn close(&self, snapshot: &mut Snapshot, now: i64) {
        let published: BTreeSet<String> = snapshot
            .health_records()
            .map(|record| record.scope.clone())
            .collect();
        for (scope, tally) in &self.tallies {
            if !published.contains(scope) {
                snapshot.record_health(over_time(scope, tally, now));
            }
        }

        for (counted, value) in [
            (Counted::Streams, self.streams),
            (Counted::Journeys, self.journeys),
            (Counted::Messages, self.messages),
            (Counted::Bytes, self.moved_bytes),
        ] {
            snapshot.record_count(Count {
                scope: self.node.clone(),
                counted,
                value,
                window_start_unix_nanos: now,
                window_end_unix_nanos: now,
                observed_unix_nanos: now,
            });
        }
    }

    /// The recent individual items — the Streams, Messages and Journeys of the
    /// last rounds — for the surface that lists what actually flowed. ADR-0032.
    #[must_use]
    pub fn activity(&self) -> &Activity {
        &self.activity
    }

    /// The tally for one pair's scope.
    #[must_use]
    pub fn tally(&self, scope: &str) -> Option<&Tally> {
        self.tallies.get(scope)
    }
}

/// Which kind of item a stage produces: Receive a Stream in, Process a Journey
/// through, Send a Message out.
const fn item_kind(stage: Stage) -> ItemKind {
    match stage {
        Stage::Receive => ItemKind::Stream,
        Stage::Process => ItemKind::Journey,
        Stage::Send => ItemKind::Message,
    }
}

/// The item's detail line: what became of it.
fn detail(outcome: &Outcome) -> String {
    match outcome {
        Outcome::Delivered => "delivered".to_string(),
        Outcome::OneSided(why) | Outcome::Failed(why) => why.clone(),
    }
}
