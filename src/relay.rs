//! The relay: the one stage of the `RoundTrip` test a role node runs, and the
//! handoff to the node that runs the next.
//!
//! The owner, 2026-09-19: *nodes run the test that was named*, and the
//! topology shows *cluster, nodes, receive, process, send*. So `RoundTrip`
//! over role nodes is the message path between real processes:
//!
//!   - an **`R`** node takes its share of the (transport x contract) matrix —
//!     the pairs whose index modulo the `R` count is its own — and, a bounded
//!     rotating slice per round, lets the Stream arrive through the transport.
//!     What arrived whole is handed to a `P` node.
//!   - a **`P`** node claims what is in its inbox, holds the content contract
//!     over it, and hands it to an `S` node.
//!   - an **`S`** node claims what is in its inbox, sends it out through the
//!     same transport, and closes the verdict: bytes are counted here.
//!
//! Each stage publishes at `<cluster>/node/<name>/<stage>/<transport>/
//! <contract>` through the same [`Ledger`] the schedule uses, with the same
//! injected faults, decided by the round the `R` node received the pair in. A
//! stage that fails hands nothing on: there is nothing to process or send.
//! An offline node takes handoffs like any other — `online` gates only what
//! is outside the cluster (ADR-0045).

use std::path::Path;

use observe::{Health, HealthRecord, Snapshot};

use crate::fault::FaultPlan;
use crate::handoff::{Handoff, Hops, Inbox};
use crate::identity::IdentityFaults;
use crate::identity::verdicts::{receive_verdicts, send_verdict};
use crate::role::{Role, Roster};
use crate::round_trip::{carried, held};
use crate::roundtrip::{RoundTrip, all_transports};
use crate::schedule::{Ledger, all_pairs, drive_each, slice};
use crate::stress::Stress;
use crate::support::now_unix_nanos;
use crate::verdict::{Contract, Outcome, Stage, Verdict};

/// What one pair came to at this node's stage: the stage's verdict first,
/// then any identity steps, and what to hand on when the stage delivered.
struct Judged {
    verdicts: Vec<Verdict>,
    forward: Option<Handoff>,
}

/// One role node's part in `RoundTrip`.
pub struct Relay {
    name: String,
    role: Role,
    stage: Stage,
    roster: Roster,
    shared: std::path::PathBuf,
    inbox: Inbox,
    transports: Vec<Box<dyn RoundTrip>>,
    faults: FaultPlan,
    identity_faults: IdentityFaults,
    ledger: Ledger,
    hops: Hops,
    round: u64,
    cursor: usize,
    per_round: usize,
    workers: usize,
    sequence: u64,
    unreadable: u64,
}

impl Relay {
    /// The relay of the node called `name`, publishing under `node`, handing
    /// over through `shared`; `file_dir` is where its own file transport
    /// round-trips. `None` when the name gives the node no role, or the
    /// roster cannot run the path (a role is missing).
    #[must_use]
    pub fn new(
        name: &str,
        node: impl Into<String>,
        roster: &Roster,
        shared: &Path,
        file_dir: &Path,
    ) -> Option<Self> {
        let role = Role::of(name);
        let stage = role.stage()?;
        if roster.refusal().is_some() || !roster.with(role).contains(&name) {
            return None;
        }
        // A P node touches no transport: it holds the contract and hands on.
        let transports = if role == Role::Process {
            Vec::new()
        } else {
            all_transports(file_dir)
        };
        Some(Self {
            name: name.to_string(),
            role,
            stage,
            roster: roster.clone(),
            shared: shared.to_path_buf(),
            inbox: Inbox::of(shared, name),
            transports,
            faults: FaultPlan::none(),
            identity_faults: IdentityFaults::none(),
            ledger: Ledger::new(node),
            hops: Hops::default(),
            round: 0,
            cursor: 0,
            per_round: 16,
            workers: 1,
            sequence: 0,
            unreadable: 0,
        })
    }

    /// The same relay injecting `faults`, identity faults with them, as
    /// [`Schedule::with_faults`](crate::Schedule::with_faults) does.
    #[must_use]
    pub fn with_faults(mut self, faults: FaultPlan) -> Self {
        self.identity_faults = if faults.is_empty() {
            IdentityFaults::none()
        } else {
            IdentityFaults::realistic()
        };
        self.faults = faults;
        self
    }

    /// The same relay at a [`Stress`] level, as
    /// [`Schedule::at`](crate::Schedule::at) sets one: the realistic faults
    /// scaled to it, none at `Calm`, and a round bounded to four pairs per
    /// worker — an `R` node's from its share; a `P` or `S` node's times the
    /// `R` count, so an inbox drains as fast as it can fill.
    #[must_use]
    pub fn at(self, stress: Stress) -> Self {
        let faults = if stress == Stress::Calm {
            FaultPlan::none()
        } else {
            FaultPlan::realistic().at(stress)
        };
        let senders = match self.role {
            Role::Receive => 1,
            _ => self.roster.with(Role::Receive).len().max(1),
        };
        let workers = stress.workers();
        self.with_faults(faults)
            .bounded(workers * 4 * senders, workers)
    }

    /// Drive at most `pairs` pairs a round — an `R` node from its share of
    /// the matrix, rotating; a `P` or `S` node from its inbox — from
    /// `workers` threads at once, so a round lands in seconds.
    #[must_use]
    pub fn bounded(mut self, pairs: usize, workers: usize) -> Self {
        self.per_round = pairs.max(1);
        self.workers = workers.max(1);
        self
    }

    /// Drive these transports rather than every one, as a test wants.
    #[must_use]
    pub fn over(mut self, transports: Vec<Box<dyn RoundTrip>>) -> Self {
        if self.role != Role::Process {
            self.transports = transports;
        }
        self
    }

    /// The stage this relay runs.
    #[must_use]
    pub const fn stage(&self) -> Stage {
        self.stage
    }

    /// The links this node has handed over, with their hops.
    #[must_use]
    pub const fn hops(&self) -> &Hops {
        &self.hops
    }

    /// One round of this node's stage: judge, hand on what delivered, fold
    /// every verdict, and return the snapshot to publish.
    pub fn tick(&mut self) -> Snapshot {
        self.round += 1;
        let now = now_unix_nanos();
        let judged = match self.role {
            Role::Receive => self.receive(now),
            Role::Process | Role::Send => self.drain_inbox(now),
            Role::Whole => Vec::new(),
        };

        let mut snapshot = Snapshot::new();
        for mut one in judged {
            if let (Some(handoff), Some(verdict)) = (one.forward.take(), one.verdicts.first_mut())
                && let Err(why) = self.hand_on(&handoff, now)
            {
                verdict.outcome = Outcome::Failed(why);
                verdict.bytes = 0;
            }
            for verdict in &one.verdicts {
                self.ledger.fold(verdict, &mut snapshot, now);
            }
        }
        self.ledger.close(&mut snapshot, now);
        for record in self.handoff_records(now) {
            snapshot.record_health(record);
        }
        snapshot
    }

    /// This round's slice of this `R` node's share of the matrix, each pair
    /// arriving through its transport.
    fn receive(&mut self, now: i64) -> Vec<Judged> {
        let share: Vec<(usize, Contract)> = all_pairs(&self.transports)
            .into_iter()
            .enumerate()
            .filter(|(index, _)| self.roster.receives(&self.name, *index))
            .map(|(_, pair)| pair)
            .collect();
        let pairs = slice(&share, self.cursor, Some(self.per_round));
        self.cursor = (self.cursor + pairs.len()) % share.len().max(1);

        drive_each(&pairs, self.workers, |&(at, contract)| {
            let transport = self.transports[at].as_ref();
            let name = transport.transport();
            let arrival = match self.fault(name, contract, self.round) {
                Some(outcome) => Err(outcome),
                None => carried(transport, &contract.payload()),
            };
            let (outcome, forward) = match arrival {
                Ok(bytes) => (
                    Outcome::Delivered,
                    Some(self.handoff(name, contract, self.round, bytes)),
                ),
                Err(outcome) => (outcome, None),
            };
            let mut verdicts = vec![self.verdict(name, contract, outcome, 0, now)];
            let identity = &self.identity_faults;
            verdicts.extend(receive_verdicts(identity, name, contract, self.round, now));
            Judged { verdicts, forward }
        })
    }

    /// What is in the inbox, up to the round's bound: a `P` node holds the
    /// contract over each and hands it on, an `S` node sends each out.
    fn drain_inbox(&mut self, now: i64) -> Vec<Judged> {
        let (claimed, unreadable) = self.inbox.claim(self.per_round);
        self.unreadable += unreadable as u64;
        drive_each(&claimed, self.workers, |handoff| {
            let name = handoff.transport.as_str();
            let contract = handoff.contract;
            if let Some(outcome) = self.fault(name, contract, handoff.round) {
                let verdicts = vec![self.verdict(name, contract, outcome, 0, now)];
                return Judged {
                    verdicts,
                    forward: None,
                };
            }
            if self.role == Role::Process {
                let outcome = held(contract, handoff.bytes.clone());
                let forward = matches!(outcome, Outcome::Delivered)
                    .then(|| self.handoff(name, contract, handoff.round, handoff.bytes.clone()));
                let verdicts = vec![self.verdict(name, contract, outcome, 0, now)];
                return Judged { verdicts, forward };
            }
            let (outcome, bytes) = match self.sent(name, &handoff.bytes) {
                Ok(bytes) => (Outcome::Delivered, bytes),
                Err(outcome) => (outcome, 0),
            };
            let identity = &self.identity_faults;
            let verdicts = vec![
                self.verdict(name, contract, outcome, bytes, now),
                send_verdict(identity, name, contract, handoff.round, now),
            ];
            Judged {
                verdicts,
                forward: None,
            }
        })
    }

    /// `bytes` out through the transport called `name`: how many went, or the
    /// outcome when they did not.
    fn sent(&self, name: &str, bytes: &[u8]) -> Result<u64, Outcome> {
        let transport = self
            .transports
            .iter()
            .find(|transport| transport.transport() == name)
            .ok_or_else(|| Outcome::Failed(format!("no transport named {name} on this node")))?;
        carried(transport.as_ref(), bytes).map(|back| back.len() as u64)
    }

    /// The injected fault for this stage of the pair in `round`, as the
    /// outcome to publish.
    fn fault(&self, transport: &str, contract: Contract, round: u64) -> Option<Outcome> {
        self.faults
            .fault_for(self.stage, transport, contract, round)
            .map(|fault| Outcome::Failed(fault.evidence()))
    }

    fn verdict(
        &self,
        transport: &str,
        contract: Contract,
        outcome: Outcome,
        bytes: u64,
        now: i64,
    ) -> Verdict {
        Verdict {
            stage: self.stage,
            transport: transport.to_string(),
            contract,
            outcome,
            bytes,
            point: None,
            observed_unix_nanos: now,
        }
    }

    fn handoff(&self, transport: &str, contract: Contract, round: u64, bytes: Vec<u8>) -> Handoff {
        Handoff {
            transport: transport.to_string(),
            contract,
            round,
            from: self.name.clone(),
            bytes,
        }
    }

    /// Deliver `handoff` to the node of the next role the pair hashes to, and
    /// record the hop.
    fn hand_on(&mut self, handoff: &Handoff, now: i64) -> Result<(), String> {
        let to = self
            .role
            .next()
            .and_then(|next| {
                self.roster
                    .target(next, &handoff.transport, handoff.contract)
            })
            .ok_or("no node to hand on to")?
            .to_string();
        self.sequence += 1;
        Inbox::of(&self.shared, &to)
            .deliver(handoff, self.sequence)
            .map_err(|error| format!("handoff to {to} failed: {error}"))?;
        self.hops.record(&self.name, &to, now);
        Ok(())
    }

    /// One record per link handed over, and one for the inbox: how many
    /// handoffs wait, and whether any arrived unreadable.
    fn handoff_records(&self, now: i64) -> Vec<HealthRecord> {
        let node = self.ledger.node();
        let record = |scope: String, health, severity, evidence| HealthRecord {
            scope,
            health,
            severity,
            evidence,
            observed_unix_nanos: now,
        };
        let mut records: Vec<HealthRecord> = self
            .hops
            .links()
            .map(|hop| {
                record(
                    format!("{node}/handoff/{}", hop.to),
                    Health::Fine,
                    0,
                    format!("{} handoffs to {}", hop.count, hop.to),
                )
            })
            .collect();
        if self.role != Role::Receive {
            let waiting = self.inbox.waiting();
            let (health, severity) = if self.unreadable > 0 {
                (Health::Stressed, 50)
            } else {
                (Health::Fine, 0)
            };
            records.push(record(
                format!("{node}/handoff/inbox"),
                health,
                severity,
                format!("{waiting} waiting, {} unreadable", self.unreadable),
            ));
        }
        records
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::roundtrip::{FileRoundTrip, TcpRoundTrip, UdpRoundTrip};
    use crate::schedule::CONTRACTS;
    use crate::support::scratch;

    const ROOT: &str = "xmip:///playground";

    fn relay(name: &str, names: &[&str], dir: &Path) -> Relay {
        let names: Vec<String> = names.iter().map(ToString::to_string).collect();
        let work = dir.join(name);
        Relay::new(
            name,
            format!("{ROOT}/node/{name}"),
            &Roster::of(&names),
            &dir.join("shared"),
            &work,
        )
        .expect("a role node of a whole path")
        .over(vec![
            Box::new(FileRoundTrip::new(&work)),
            Box::new(TcpRoundTrip),
        ])
        .bounded(2 * CONTRACTS.len(), 2)
    }

    #[test]
    fn a_pair_goes_r_to_p_to_s_and_each_stage_publishes_under_its_own_node() {
        let dir = scratch("relay");
        let names = ["R1", "P1", "S1"];
        let mut r1 = relay("R1", &names, &dir);
        let mut p1 = relay("P1", &names, &dir);
        let mut s1 = relay("S1", &names, &dir);
        let pairs = 2 * CONTRACTS.len();

        let received = r1.tick();
        let processed = p1.tick();
        let sent = s1.tick();

        for (snapshot, node, stage) in [
            (&received, "R1", "receive"),
            (&processed, "P1", "process"),
            (&sent, "S1", "send"),
        ] {
            let scope = format!("{ROOT}/node/{node}/{stage}");
            let leaves: Vec<_> = snapshot
                .health(&scope)
                .into_iter()
                .filter(|record| record.scope.split('/').count() == 9)
                .collect();
            assert_eq!(leaves.len(), pairs, "{scope}: one verdict per pair");
            for record in leaves {
                assert_eq!(
                    record.health,
                    Health::Fine,
                    "{}: {}",
                    record.scope,
                    record.evidence
                );
            }
        }
        let hops: Vec<_> = r1.hops().links().chain(p1.hops().links()).collect();
        assert_eq!(hops.len(), 2);
        assert_eq!((hops[0].from.as_str(), hops[0].to.as_str()), ("R1", "P1"));
        assert_eq!((hops[1].from.as_str(), hops[1].to.as_str()), ("P1", "S1"));
        assert!(hops.iter().all(|hop| hop.count == pairs as u64));
        assert!(s1.hops().links().next().is_none(), "S closes the verdict");
        assert_eq!(
            sent.measure(&format!("{ROOT}/node/S1"), observe::Counted::Messages)
                .map(|count| count.value),
            Some(pairs as u64)
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn two_receivers_share_the_matrix_and_a_bounded_round_rotates_through_it() {
        let dir = scratch("relay-share");
        let names = ["R1", "R2", "P1", "S1"];
        let mut scopes = std::collections::BTreeSet::new();
        for name in ["R1", "R2"] {
            let mut receive = relay(name, &names, &dir).bounded(7, 1);
            let mut last = receive.tick();
            for _ in 0..2 {
                last = receive.tick();
            }
            let mine: Vec<String> = last
                .health(&format!("{ROOT}/node/{name}/receive"))
                .into_iter()
                .filter(|record| record.scope.split('/').count() == 9)
                .map(|record| record.scope.replace(&format!("/node/{name}/"), "/"))
                .collect();
            assert_eq!(
                mine.len(),
                CONTRACTS.len(),
                "{name}: half of two transports"
            );
            scopes.extend(mine);
        }
        assert_eq!(
            scopes.len(),
            2 * CONTRACTS.len(),
            "no pair has two receivers"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_faulted_stage_hands_nothing_on_and_a_roster_without_a_role_has_no_relay() {
        let dir = scratch("relay-fault");
        let names = ["R1", "P1", "S1"];
        let mut receive = relay("R1", &names, &dir)
            .over(vec![Box::new(UdpRoundTrip)])
            .with_faults(FaultPlan::realistic());
        let mut failed = 0;
        for _ in 0..12 {
            let snapshot = receive.tick();
            failed += snapshot
                .health(&format!("{ROOT}/node/R1/receive/udp"))
                .iter()
                .filter(|record| record.health == Health::Done)
                .count();
        }
        assert!(failed > 0, "realistic faults reach an R node's receive");
        let rounds = 12 * CONTRACTS.len() as u64;
        let handed: u64 = receive.hops().links().map(|hop| hop.count).sum();
        assert!(
            handed < rounds,
            "{handed} of {rounds}: a failed arrival is not handed on"
        );

        let roster = Roster::of(&["R1".to_string(), "S1".to_string()]);
        assert!(Relay::new("R1", ROOT, &roster, &dir, &dir).is_none());
        let whole = Roster::of(&["node-01".to_string()]);
        assert!(Relay::new("node-01", ROOT, &whole, &dir, &dir).is_none());
        std::fs::remove_dir_all(&dir).ok();
    }
}
