//! The Schedule: what drives the `RoundTrip` test, and how it accumulates.
//!
//! The owner's shape, 2026-09-05: the `RoundTrip` test is an **integration test
//! over time** across the message path — Receive, Process, Send. A Schedule
//! ticks; each tick runs one round over every transport, for every contract, and
//! expands it into a verdict per stage. Loopback itself never fails, so the
//! Schedule injects transport and content faults — transport, addressing and
//! contract — on all three stages from a [`FaultPlan`], while the identity
//! pipeline faults its own steps. What it publishes is not the last round but
//! the record over time: how many rounds passed, and whether the pair fails now.
//!
//! The running thread is the caller's — the crate gives the tick, so a test
//! drives many rounds without waiting on a clock. At a [`Stress`] level the
//! tick drives several pairs at once from as many threads as the level says,
//! sends the level's payload for the round, and injects the level's faults;
//! without one it runs as it always has — one pair at a time, the probe, and
//! whatever faults were set.

pub mod ledger;
pub mod tally;
pub(crate) mod workers;

use node::Stage;
use observe::{Activity, Snapshot};

use crate::fault::FaultPlan;
use crate::identity::IdentityFaults;
use crate::identity::verdicts::{receive_verdicts, send_verdict};
use crate::round_trip::{round_trip, round_trip_with};
use crate::roundtrip::{RoundTrip, all_transports};
use crate::stress::{self, Stress};
use crate::support::now_unix_nanos;
use crate::verdict::{Contract, Outcome, Verdict};

pub use ledger::Ledger;
pub use tally::Tally;
use workers::drive_selected;
pub(crate) use workers::{all_pairs, drive_each, drive_pairs, slice};

/// Every contract the playground exercises today: the three local shapes and
/// every contract technology the estate has landed. ADR-0028's matrix is every
/// transport by every one of these.
pub const CONTRACTS: [Contract; 20] = [
    Contract::Bytes,
    Contract::Text,
    Contract::Json,
    Contract::Xml,
    Contract::Html,
    Contract::Csv,
    Contract::FixedWidth,
    Contract::Edifact,
    Contract::Regex,
    Contract::Schematron,
    Contract::Hl7v2,
    Contract::Fhir,
    Contract::X12,
    Contract::Avro,
    Contract::GraphqlSchema,
    Contract::Protobuf,
    Contract::Wsdl,
    Contract::OpenApi,
    Contract::AsyncApi,
    Contract::Sql,
];

/// A scheduled exercise of the estate's transports over the message path. Holds
/// one [`RoundTrip`] adapter per transport and a [`FaultPlan`], and on each tick
/// runs the scenario over every adapter by every contract, expands it across
/// Receive, Process and Send, and publishes what it found.
pub struct Schedule {
    transports: Vec<Box<dyn RoundTrip>>,
    faults: FaultPlan,
    identity_faults: IdentityFaults,
    /// The level, when one was set: its payload sizes and its workers.
    /// `None` is the schedule as it ran before the axis existed.
    stress: Option<Stress>,
    round: u64,
    /// Every pair's record over time, the throughput and the recent items.
    ledger: Ledger,
    /// How many pairs one round drives, when bounded; the rest wait their
    /// turn and keep their standing on the board. `None` drives every pair
    /// every round, as a test wants.
    per_round: Option<usize>,
    /// Where the next bounded round starts in the matrix.
    cursor: usize,
}

impl Schedule {
    /// A schedule publishing under `node`, running the `RoundTrip` test over every
    /// wired transport with no injected faults. `file_dir` is where the file
    /// transport round-trips.
    #[must_use]
    pub fn new(node: impl Into<String>, file_dir: impl Into<std::path::PathBuf>) -> Self {
        let transports = all_transports(file_dir);

        Self {
            ledger: Ledger::new(node),
            transports,
            faults: FaultPlan::none(),
            identity_faults: IdentityFaults::none(),
            stress: None,
            round: 0,
            per_round: None,
            cursor: 0,
        }
    }

    /// The same schedule, injecting `faults`. The runner uses
    /// [`FaultPlan::realistic`]; the tests use the fault-free default.
    #[must_use]
    pub fn with_faults(mut self, faults: FaultPlan) -> Self {
        // One switch turns on both: an empty plan runs the identity pipeline
        // clean, a realistic plan faults it too, so the runner's single
        // `with_faults(FaultPlan::realistic())` gets transport and identity
        // faults together.
        self.identity_faults = if faults.is_empty() {
            IdentityFaults::none()
        } else {
            IdentityFaults::realistic()
        };
        self.faults = faults;
        self
    }

    /// The same schedule at a [`Stress`] level: the realistic faults scaled
    /// to it (none at `Calm`, identity faults included above it), the level's
    /// payload for each round — validation still runs over what arrived — and
    /// its pairs driven from the level's workers at once.
    #[must_use]
    pub fn at(self, stress: Stress) -> Self {
        let faults = if stress == Stress::Calm {
            FaultPlan::none()
        } else {
            FaultPlan::realistic().at(stress)
        };
        let mut leveled = self.with_faults(faults);
        leveled.stress = Some(stress);
        leveled
    }

    /// Drive these transports rather than every one. A test that judges the
    /// rollup over forty rounds needs three; the runner drives all.
    #[must_use]
    pub fn over(mut self, transports: Vec<Box<dyn RoundTrip>>) -> Self {
        self.transports = transports;
        self
    }

    /// How many pairs a tick drives, and from how many threads at once.
    #[must_use]
    pub fn shape(&self) -> (usize, usize) {
        let pairs = self.transports.len() * CONTRACTS.len();
        (pairs, self.stress.map_or(1, Stress::workers))
    }

    /// Run every pair once, expand across the stages, fold each into its tally,
    /// and return the snapshot to publish. One tick; call it on a schedule.
    pub fn tick(&mut self) -> Snapshot {
        self.round += 1;
        let now = now_unix_nanos();
        let mut snapshot = Snapshot::new();

        for verdict in self.run_once(now) {
            self.ledger.fold(&verdict, &mut snapshot, now);
        }
        if let Some(per_round) = self.per_round {
            let matrix = (self.transports.len() * CONTRACTS.len()).max(1);
            self.cursor = (self.cursor + per_round.min(matrix)) % matrix;
        }

        // The pairs that waited this round keep their standing on the board.
        self.ledger.close(&mut snapshot, now);
        snapshot
    }

    /// The recent individual items — the Streams, Messages and Journeys of the
    /// last rounds — for the surface that lists what actually flowed. ADR-0032.
    #[must_use]
    pub fn activity(&self) -> &Activity {
        self.ledger.activity()
    }

    /// The verdicts of one round: every transport by every contract, each
    /// expanded across Receive, Process and Send with its faults injected.
    /// At a level the pairs run from its workers at once; the verdicts come
    /// back in the pairs' original order whichever thread reached them.
    #[must_use]
    pub fn run_once(&self, now: i64) -> Vec<Verdict> {
        let size = self.stress.map(|level| level.size_for(self.round));
        let (_, workers) = self.shape();
        let pairs = slice(&all_pairs(&self.transports), self.cursor, self.per_round);
        drive_selected(&self.transports, &pairs, workers, |transport, contract| {
            self.judge(transport, contract, size, now)
        })
        .into_iter()
        .flatten()
        .collect()
    }

    /// Drive at most `pairs` pairs a round, the matrix rotating under it, so
    /// a round lands while an operator watches and the counters move every
    /// few seconds rather than once a minute (2026-09-11). Every pair keeps
    /// its standing on the board between its turns.
    #[must_use]
    pub fn pairs_per_round(mut self, pairs: usize) -> Self {
        self.per_round = Some(pairs.max(1));
        self
    }

    /// One pair's verdicts this round: the real exchange — the probe, or the
    /// level's payload at `size` — expanded across the three stages with the
    /// round's faults, then the identity steps.
    fn judge(
        &self,
        transport: &dyn RoundTrip,
        contract: Contract,
        size: Option<usize>,
        now: i64,
    ) -> Vec<Verdict> {
        let name = transport.transport();
        let (base, bytes) = match size {
            Some(size) => round_trip_with(transport, contract, &stress::payload(contract, size)),
            None => round_trip(transport, contract),
        };

        let mut verdicts = Vec::with_capacity(Stage::ALL.len() + 4);
        for stage in Stage::ALL {
            let (outcome, stage_bytes) =
                match self.faults.fault_for(stage, name, contract, self.round) {
                    Some(fault) => (Outcome::Failed(fault.evidence()), 0),
                    None => stage_outcome(stage, &base, bytes),
                };

            verdicts.push(Verdict {
                stage,
                transport: name.to_string(),
                contract,
                outcome,
                bytes: stage_bytes,
                point: None,
                observed_unix_nanos: now,
            });
        }

        let identity = &self.identity_faults;
        verdicts.extend(receive_verdicts(identity, name, contract, self.round, now));
        verdicts.push(send_verdict(identity, name, contract, self.round, now));
        verdicts
    }

    /// The tally for one pair's scope, for a caller that wants the numbers
    /// rather than the health.
    #[must_use]
    pub fn tally(&self, scope: &str) -> Option<&Tally> {
        self.ledger.tally(scope)
    }
}

/// One stage's outcome from the base result of the real exchange. Loopback
/// almost always delivers, so this is mostly Delivered; a genuine failure is
/// attributed to the stage it belongs to — a contract failure to Process, a
/// transport failure to Receive. Bytes are counted once, at Send.
fn stage_outcome(stage: Stage, base: &Outcome, bytes: u64) -> (Outcome, u64) {
    match base {
        Outcome::Delivered => match stage {
            Stage::Send => (Outcome::Delivered, bytes),
            _ => (Outcome::Delivered, 0),
        },
        Outcome::OneSided(why) => (Outcome::OneSided(why.clone()), 0),
        Outcome::Failed(why) => {
            let owns = if why.starts_with("contract not held") {
                stage == Stage::Process
            } else {
                stage == Stage::Receive
            };
            if owns {
                (Outcome::Failed(why.clone()), 0)
            } else if stage == Stage::Send {
                (Outcome::Delivered, bytes)
            } else {
                (Outcome::Delivered, 0)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;
    use crate::roundtrip::{FileRoundTrip, TIMEOUT, TcpRoundTrip, UdpRoundTrip};
    use crate::storm::violations;
    use crate::support::scratch;
    use observe::{Counted, Health};

    const NODE: &str = "xmip:///playground";

    fn sample(dir: &std::path::Path) -> Vec<Box<dyn RoundTrip>> {
        vec![
            Box::new(FileRoundTrip::new(dir)),
            Box::new(TcpRoundTrip),
            Box::new(UdpRoundTrip),
        ]
    }

    /// Drive `schedule` for `rounds` under the storm's invariants: a tick
    /// within `pairs * TIMEOUT * 3 / workers`, a reason on every verdict not
    /// delivered, a rollup that tells the truth. Returns the last snapshot.
    fn stress_rounds(schedule: &mut Schedule, rounds: u64) -> Snapshot {
        let (pairs, workers) = schedule.shape();
        let budget = TIMEOUT * 3 * u32::try_from(pairs).expect("few pairs")
            / u32::try_from(workers).expect("few workers");
        let mut snapshot = Snapshot::new();
        for round in 1..=rounds {
            let started = Instant::now();
            snapshot = schedule.tick();
            let took = started.elapsed();
            assert!(
                took <= budget,
                "round {round} took {took:?}, budget {budget:?}"
            );
            let lying = violations(&snapshot, NODE);
            assert!(lying.is_empty(), "round {round}: {}", lying.join("; "));
        }
        snapshot
    }

    #[test]
    fn harsh_faults_sizes_and_workers_keep_the_invariants_and_leave_file_fine() {
        let dir = scratch("harsh");
        let mut schedule = Schedule::new(NODE, &dir)
            .at(Stress::Harsh)
            .over(sample(&dir));
        // Four workers at harsh, or fewer within the headroom (ADR-0028, 2026-09-11).
        assert_eq!(
            schedule.shape(),
            (3 * CONTRACTS.len(), Stress::Harsh.workers())
        );

        let snapshot = stress_rounds(&mut schedule, Stress::Harsh.rounds());

        assert_eq!(
            snapshot.worst(NODE),
            Some(Health::Holding),
            "harsh faults surface"
        );
        let file: Vec<_> = snapshot
            .health(&format!("{NODE}/receive/file"))
            .into_iter()
            .filter(|r| r.health != Health::Fine)
            .collect();
        assert!(
            file.is_empty(),
            "file's transport path carries no fault at any level: {file:?}"
        );
        // A datagram cannot carry sixteen bits plus one; that round is red
        // with the transport's reason, not a hang and not a blank.
        let udp = snapshot.health(&format!("{NODE}/receive/udp/bytes"));
        assert!(
            udp.iter()
                .any(|r| r.health != Health::Fine && !r.evidence.is_empty()),
            "{udp:?}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn the_verdicts_come_back_in_pair_order_whatever_thread_reached_them() {
        let dir = scratch("order");
        let schedule = Schedule::new(NODE, &dir)
            .at(Stress::Harsh)
            .over(sample(&dir));
        let verdicts = schedule.run_once(1);
        let expected: Vec<(String, Contract)> = ["file", "tcp", "udp"]
            .into_iter()
            .flat_map(|t| CONTRACTS.iter().map(move |&c| (t.to_string(), c)))
            .collect();
        let seen: Vec<(String, Contract)> = verdicts
            .iter()
            .filter(|v| v.stage == Stage::Receive && v.point.is_none())
            .map(|v| (v.transport.clone(), v.contract))
            .collect();
        assert_eq!(seen, expected);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    #[ignore = "brutal: the whole matrix at every core, for the runner"]
    fn brutal_schedule_over_every_transport() {
        let dir = scratch("brutal");
        let mut schedule = Schedule::new(NODE, &dir).at(Stress::Brutal);
        stress_rounds(&mut schedule, Stress::Brutal.rounds());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_tick_reports_every_pair_across_the_three_stages() {
        let dir = scratch("tick");
        let mut schedule = Schedule::new("xmip:///playground", &dir);

        let snapshot = schedule.tick();

        // file carries no fault — transport or identity — so every record under
        // every stage of every file pair is green. Receive now carries the
        // transport verdict plus three identity steps per contract; Send carries
        // the transport verdict plus the identity presentation.
        for (stage, per_contract) in [("receive", 4), ("process", 1), ("send", 2)] {
            let records = snapshot.health(&format!("xmip:///playground/{stage}/file"));
            assert_eq!(
                records.len(),
                CONTRACTS.len() * per_contract,
                "{per_contract} record(s) per contract at {stage}"
            );
            assert!(records.iter().all(|r| r.health == Health::Fine));
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_fault_free_schedule_has_no_red_and_says_why_it_is_not_all_green() {
        let dir = scratch("rollup");
        let mut schedule = Schedule::new("xmip:///playground", &dir);

        let snapshot = schedule.tick();
        // Nothing is broken. What is not green is a transport that declares
        // it cannot carry a probe as it is — a queue that takes UTF-8 text,
        // an OS object this machine lacks — judged one-sided, yellow, with
        // the reason (ADR-0028 clause 5, ADR-0051).
        let red: Vec<_> = snapshot
            .health("xmip:///playground")
            .into_iter()
            .filter(|record| record.health == Health::Done)
            .map(|record| format!("{}: {}", record.scope, record.evidence))
            .collect();
        assert!(
            red.is_empty(),
            "no pair fails without a fault:
{}",
            red.join(
                "
"
            )
        );
        // A yellow leaf rolls up as Holding (ADR-0041); red never appears.
        assert_ne!(snapshot.worst("xmip:///playground"), Some(Health::Done));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn injected_faults_turn_pairs_done_but_leave_file_fine() {
        let dir = scratch("faults");
        let mut schedule = Schedule::new("xmip:///playground", &dir)
            .with_faults(FaultPlan::realistic())
            .over(vec![
                Box::new(crate::roundtrip::FileRoundTrip::new(&dir)),
                Box::new(crate::roundtrip::TcpRoundTrip),
                Box::new(crate::roundtrip::UdpRoundTrip),
            ]);

        let mut snapshot = schedule.tick();
        for _ in 0..40 {
            snapshot = schedule.tick();
        }

        assert_eq!(
            snapshot.worst("xmip:///playground"),
            Some(Health::Holding),
            "faults should surface — a Done leaf rolls up to Holding (ADR-0041)"
        );
        assert_eq!(
            snapshot.worst("xmip:///playground/receive/file"),
            Some(Health::Fine),
            "file is left alone"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn throughput_counts_per_stage() {
        let dir = scratch("throughput");
        let transports = crate::support::three(&dir);
        let pairs = (CONTRACTS.len() * transports.len()) as u64;
        let mut schedule = Schedule::new("xmip:///playground", &dir).over(transports);

        let snapshot = schedule.tick();

        // Every pair is judged at every stage, and the stages are judged
        // apart: a pair that failed at Receive can still be delivered at
        // Process, so each figure is counted against its own stage and not
        // against one number for all three.
        //
        // A pair that did not deliver moved nothing: the transport either
        // declared it could not carry the probe, which is Stressed, or tried
        // and failed, which is Done. Only the Stressed ones were subtracted
        // until 2026-09-21, and only at Receive, which was the same number
        // only while nothing could fail. It could: once the transport's
        // unbounded waits became bounded failures, a machine out of ephemeral
        // ports produced the first real ones the estate had seen here and the
        // assertion was thirty out at Receive and twenty-nine at Process.
        //
        // Counting the Fine records instead is not the fix — they are
        // published at every scope under a stage, not one per pair, and that
        // reads four times too high.
        let delivered = |stage: &str| {
            let undelivered = snapshot
                .health(&format!("xmip:///playground/{stage}"))
                .iter()
                .filter(|record| matches!(record.health, Health::Stressed | Health::Done))
                .count() as u64;
            pairs - undelivered
        };

        assert_eq!(
            snapshot
                .measure("xmip:///playground", Counted::Streams)
                .map(|c| c.value),
            Some(delivered("receive")),
            "one Stream in per pair delivered at Receive"
        );
        assert_eq!(
            snapshot
                .measure("xmip:///playground", Counted::Journeys)
                .map(|c| c.value),
            Some(delivered("process")),
            "one Journey per pair delivered at Process"
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
