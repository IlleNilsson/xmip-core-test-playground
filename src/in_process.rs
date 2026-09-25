//! [`InProcess`]: the scenarios a roll runs in its own process, each under
//! its own subtree of the cluster's root and each with faults or pressure on,
//! so the board is realistic rather than uniformly green. `file` stays clean
//! in every one.
//!
//! Bounded rounds: a slice of the matrix per round, rotating, so a round
//! lands in seconds and the counters an operator watches keep moving. Where
//! nodes declare stages of the message path, `RoundTrip` is theirs, between
//! processes, and is not also run here (ADR-0056).

use std::path::Path;
use std::time::Duration;

use observe::{Activity, Snapshot};

use crate::cluster::merge;
use crate::environment::load_bytes;
use crate::scenario::{DAILY_BACKLOG, EXCLUSIVE_CLAIM, ROUND_TRIP, drives};
use crate::{
    DailyBacklog, ExclusiveClaim, FaultPlan, Filing, HeavyLoad, LowLatency, Retention, Schedule,
    Stress,
};

/// Every scenario a roll may run itself, and which of them it was told to.
pub struct InProcess {
    chosen: Vec<String>,
    relayed: bool,
    round_trip: Schedule,
    low_latency: LowLatency,
    heavy_load: HeavyLoad,
    retention: Retention,
    filing: Filing,
    exclusive_claim: ExclusiveClaim,
    daily_backlog: DailyBacklog,
}

impl InProcess {
    /// The scenarios under `root`, each over its own directory in `base`,
    /// sized for `stress`. `chosen` names the ones to run (empty is every
    /// one); `relayed` says the nodes run `RoundTrip`.
    #[must_use]
    pub fn new(
        root: &str,
        base: &Path,
        stress: Stress,
        chosen: Vec<String>,
        relayed: bool,
    ) -> Self {
        let slice = stress.workers() * 16;
        let scope = |name: &str| format!("{root}/{name}");
        Self {
            chosen,
            relayed,
            round_trip: Schedule::new(scope(ROUND_TRIP), base.join(ROUND_TRIP))
                .with_faults(FaultPlan::realistic())
                .pairs_per_round(slice),
            low_latency: LowLatency::new(scope("low-latency"), base.join("low-latency"))
                .under_pressure()
                .pairs_per_round(slice),
            heavy_load: HeavyLoad::new(scope("heavy-load"), base.join("heavy-load"))
                .under_pressure()
                .with_bytes(load_bytes())
                .pairs_per_round(stress.workers() * 8),
            retention: Retention::new(scope("retention")).under_pressure(),
            filing: Filing::new(scope("filing"), base.join("filing")).under_pressure(),
            exclusive_claim: ExclusiveClaim::new(
                scope(EXCLUSIVE_CLAIM),
                base.join(EXCLUSIVE_CLAIM),
            )
            .under_pressure(),
            daily_backlog: DailyBacklog::new(scope(DAILY_BACKLOG), base.join(DAILY_BACKLOG)),
        }
    }

    /// One round of every chosen scenario, merged into one snapshot;
    /// Retention ages on `simulated` time.
    pub fn tick(&mut self, simulated: Duration) -> Snapshot {
        let chosen = &self.chosen;
        let mut snapshot = Snapshot::new();
        if drives(chosen, ROUND_TRIP) && !self.relayed {
            merge(&mut snapshot, &self.round_trip.tick());
        }
        if drives(chosen, "low-latency") {
            merge(&mut snapshot, &self.low_latency.tick());
        }
        if drives(chosen, "heavy-load") {
            merge(&mut snapshot, &self.heavy_load.tick());
        }
        if drives(chosen, "retention") {
            merge(&mut snapshot, &self.retention.tick(simulated));
        }
        if drives(chosen, "filing") {
            merge(&mut snapshot, &self.filing.tick());
        }
        if drives(chosen, EXCLUSIVE_CLAIM) {
            merge(&mut snapshot, &self.exclusive_claim.tick());
        }
        if drives(chosen, DAILY_BACKLOG) {
            merge(&mut snapshot, &self.daily_backlog.tick());
        }
        snapshot
    }

    /// What `RoundTrip` did recently, for the activity file.
    #[must_use]
    pub fn activity(&self) -> &Activity {
        self.round_trip.activity()
    }
}
