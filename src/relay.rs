//! The relay: one stage of the `RoundTrip` test a node declared it can serve,
//! and the handoff to a node that declared the next.
//!
//! The owner, 2026-09-19: *nodes run the test that was named*, and the
//! topology shows *cluster, nodes, receive, process, send*. So `RoundTrip`
//! across nodes is the message path between real processes, and which node
//! serves which stage is what each **declared** (`capability.rs`, ADR-0056),
//! never what it is called:
//!
//!   - a node that declared **receive** takes its share of the (transport x
//!     contract) matrix — the pairs whose index modulo the count of receiving
//!     nodes is its own — and, a bounded rotating slice per round, lets the
//!     Stream arrive through the transport. What arrived whole is handed to a
//!     node that declared process.
//!   - a node that declared **process** claims what is in its inbox, holds the
//!     content contract over it, and hands it to one that declared send.
//!   - a node that declared **send** claims what is in its inbox, sends it out
//!     through the same transport, and closes the verdict: bytes are counted
//!     here.
//!
//! A node that declared two stages runs one relay per stage, each with its own
//! inbox. Each stage publishes at `<cluster>/node/<name>/<stage>/<transport>/
//! <contract>` through the same [`Ledger`] the schedule uses, with the same
//! injected faults, decided by the round the receiving node took the pair in.
//! A stage that fails hands nothing on: there is nothing to process or send.
//! An offline node takes handoffs like any other — online capability gates
//! only what is outside the cluster (ADR-0045).

use std::path::Path;
use std::sync::Arc;

use node::Stage;
use observe::Snapshot;

use crate::exchange::{RoundTrip, all_transports};
use crate::fault::FaultPlan;
use crate::handoff::{Handoff, Hops, Inbox};
use crate::identity::IdentityFaults;
use crate::identity::verdicts::{receive_verdicts, send_verdict};
use crate::roster::Roster;
use crate::round_trip::{carried, held};
use crate::schedule::{Ledger, all_pairs, drive_each, slice};
use crate::stress::Stress;
use crate::subscribing::Subscribing;
use crate::verdict::{Contract, Outcome, Verdict};
use observe::now_unix_nanos;

/// What one pair came to at this node's stage: the stage's verdict first,
/// then any identity steps, and what to hand on when the stage delivered.
struct Judged {
    verdicts: Vec<Verdict>,
    forward: Option<Handoff>,
}

/// One node's part in `RoundTrip` at one stage it declared.
pub struct Relay {
    name: String,
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
    /// The node's Subscriptions, which route what its process stage hands
    /// on (ADR-0013, amendment 2026-09-30).
    subscribing: Option<Arc<Subscribing>>,
}

impl Relay {
    /// The relay of the node called `name` at `stage`, publishing under
    /// `node`, handing over through `shared`; `file_dir` is where its own file
    /// transport round-trips. `None` when the roster says this node did not
    /// declare `stage`, or the roster cannot run the path (a capability is
    /// missing).
    #[must_use]
    pub fn new(
        name: &str,
        node: impl Into<String>,
        stage: Stage,
        roster: &Roster,
        shared: &Path,
        file_dir: &Path,
    ) -> Option<Self> {
        if roster.refusal().is_some() || !roster.with(stage).contains(&name) {
            return None;
        }
        // A process stage touches no transport: it holds the contract on.
        let transports = if stage == Stage::Process {
            Vec::new()
        } else {
            all_transports(file_dir)
        };
        Some(Self {
            name: name.to_string(),
            stage,
            roster: roster.clone(),
            shared: shared.to_path_buf(),
            inbox: Inbox::of(shared, name, stage),
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
            subscribing: None,
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
    /// worker — a receive stage's from its share; a process or send stage's
    /// times the count of receiving nodes, so an inbox drains as fast as it
    /// can fill.
    #[must_use]
    pub fn at(self, stress: Stress) -> Self {
        let faults = if stress == Stress::Calm {
            FaultPlan::none()
        } else {
            FaultPlan::realistic().at(stress)
        };
        let senders = if self.stage == Stage::Receive {
            1
        } else {
            self.roster.with(Stage::Receive).len().max(1)
        };
        let workers = stress.workers();
        self.with_faults(faults)
            .bounded(workers * 4 * senders, workers)
    }

    /// Drive at most `pairs` pairs a round — a receive stage from its share of
    /// the matrix, rotating; a process or send stage from its inbox — from
    /// `workers` threads at once, so a round lands in seconds.
    #[must_use]
    pub fn bounded(mut self, pairs: usize, workers: usize) -> Self {
        self.per_round = pairs.max(1);
        self.workers = workers.max(1);
        self
    }

    /// The same relay routing what its process stage hands on through the
    /// node's Subscriptions: a pair one picks up goes on to the send stage,
    /// a pair a paused one holds waits until it is resumed.
    #[must_use]
    pub fn subscribing(mut self, subscribing: Option<Arc<Subscribing>>) -> Self {
        if self.stage == Stage::Process {
            self.subscribing = subscribing;
        }
        self
    }

    /// Drive these transports rather than every one, as a test wants.
    #[must_use]
    pub fn over(mut self, transports: Vec<Box<dyn RoundTrip>>) -> Self {
        if self.stage != Stage::Process {
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
        let judged = match self.stage {
            Stage::Receive => self.receive(now),
            Stage::Process | Stage::Send => self.drain_inbox(now),
        };

        let mut snapshot = Snapshot::new();
        for mut one in judged {
            if let (Some(handoff), Some(verdict)) = (one.forward.take(), one.verdicts.first_mut()) {
                // A pair whose Publication was not taken is neither held nor
                // handed on: it fails here, in words, and is not lost silently.
                let picked = self
                    .subscribing
                    .as_ref()
                    .map_or(Ok(true), |subscribing| subscribing.route(&handoff));
                let handed = match picked {
                    Ok(true) => self.hand_on(&handoff, now),
                    Ok(false) => Ok(()),
                    Err(why) => Err(why),
                };
                if let Err(why) = handed {
                    verdict.outcome = Outcome::Failed(why);
                    verdict.bytes = 0;
                }
            }
            for verdict in &one.verdicts {
                self.ledger.fold(verdict, &mut snapshot, now);
            }
        }
        self.hand_on_released(now);
        self.ledger.close(&mut snapshot, now);
        let inbox = (self.stage != Stage::Receive).then(|| (self.inbox.waiting(), self.unreadable));
        for record in self.hops.records(self.ledger.node(), inbox, now) {
            snapshot.record_health(record);
        }
        snapshot
    }

    /// This round's slice of this node's share of the matrix, each pair
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

    /// What is in the inbox, up to the round's bound: a process stage holds
    /// the contract over each and hands it on, a send stage sends each out.
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
            if self.stage == Stage::Process {
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

    /// Hand on what a resume let go of, oldest first; what could not be
    /// handed on stays held, Failed, until a resume tries it again.
    fn hand_on_released(&mut self, now: i64) {
        let Some(subscribing) = self.subscribing.clone() else {
            return;
        };
        for picked in subscribing.released() {
            let handed = self.hand_on(&picked.handoff, now);
            subscribing.done(picked, &handed);
        }
    }

    /// Deliver `handoff` to a node that declared the next stage — the one the
    /// pair hashes to — and record the hop.
    fn hand_on(&mut self, handoff: &Handoff, now: i64) -> Result<(), String> {
        let next = self.stage.next().ok_or("no stage to hand on to")?;
        let to = self
            .roster
            .target(&self.name, next, &handoff.transport, handoff.contract)
            .ok_or("no node declares the role the next stage needs")?
            .to_string();
        self.sequence += 1;
        Inbox::of(&self.shared, &to, next)
            .deliver(handoff, self.sequence)
            .map_err(|error| format!("handoff to {to} failed: {error}"))?;
        self.hops.record((&self.name, self.stage), (&to, next), now);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::{FileRoundTrip, TcpRoundTrip, UdpRoundTrip};
    use crate::schedule::CONTRACTS;
    use crate::support::{
        cluster_root, declaring, path, path_roster, roster_text, scratch, test_cluster,
    };
    use observe::Health;

    /// The roster `text` declares, and the relay of `name` at the one stage
    /// it declared there.
    fn relay(name: &str, text: &str, dir: &Path) -> Relay {
        let roster = Roster::parse(text).expect("a well-formed roster");
        let stage = roster.capability(name).stages()[0];
        let work = dir.join(name);
        Relay::new(
            name,
            format!("{}/node/{name}", cluster_root()),
            stage,
            &roster,
            &dir.join("shared"),
            &work,
        )
        .expect("a node declaring a stage of a whole path")
        .over(vec![
            Box::new(FileRoundTrip::new(&work)),
            Box::new(TcpRoundTrip),
        ])
        .bounded(2 * CONTRACTS.len(), 2)
    }

    #[test]
    fn a_pair_goes_receive_to_process_to_send_and_each_publishes_under_its_node() {
        let dir = scratch("relay");
        // Names that say nothing: the stages come from the declarations.
        let cluster = test_cluster();
        let [receiving, processing, sending] = path(&cluster);
        let roster = path_roster(&cluster);
        let root = cluster_root();
        let mut receive_relay = relay(receiving, &roster, &dir);
        let mut process_relay = relay(processing, &roster, &dir);
        let mut sender = relay(sending, &roster, &dir);
        let pairs = 2 * CONTRACTS.len();

        let received = receive_relay.tick();
        let processed = process_relay.tick();
        let sent = sender.tick();

        for (snapshot, node, stage) in [
            (&received, receiving, "receive"),
            (&processed, processing, "process"),
            (&sent, sending, "send"),
        ] {
            let scope = format!("{root}/node/{node}/{stage}");
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
        let hops: Vec<_> = receive_relay
            .hops()
            .links()
            .chain(process_relay.hops().links())
            .collect();
        assert_eq!(hops.len(), 2);
        assert_eq!(
            (hops[0].from.as_str(), hops[0].to.as_str()),
            (receiving, processing)
        );
        assert_eq!(
            (hops[0].from_stage.as_str(), hops[0].to_stage.as_str()),
            ("receive", "process")
        );
        assert_eq!(
            (hops[1].from.as_str(), hops[1].to.as_str()),
            (processing, sending)
        );
        assert!(hops.iter().all(|hop| hop.count == pairs as u64));
        assert!(
            sender.hops().links().next().is_none(),
            "send closes the verdict"
        );
        assert_eq!(
            sent.measure(
                &format!("{root}/node/{sending}"),
                observe::Counted::Messages
            )
            .map(|count| count.value),
            Some(pairs as u64)
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn two_receivers_share_the_matrix_and_a_bounded_round_rotates_through_it() {
        let dir = scratch("relay-share");
        // The test cluster declares one receiving node and two sending ones:
        // the second sending node's name is taken here for a second receiver.
        let cluster = test_cluster();
        let [receiving, processing, _] = path(&cluster);
        let [sending, second] = declaring(&cluster, "sending")[..] else {
            panic!("the test cluster has two sending nodes");
        };
        let roster = roster_text(&[
            (receiving, "receiving"),
            (second, "receiving"),
            (processing, "processing"),
            (sending, "sending"),
        ]);
        let root = cluster_root();
        let mut scopes = std::collections::BTreeSet::new();
        for name in [receiving, second] {
            let mut receive = relay(name, &roster, &dir).bounded(7, 1);
            let mut last = receive.tick();
            for _ in 0..2 {
                last = receive.tick();
            }
            let mine: Vec<String> = last
                .health(&format!("{root}/node/{name}/receive"))
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
    fn a_faulted_stage_hands_nothing_on_and_an_uncovered_path_has_no_relay() {
        let dir = scratch("relay-fault");
        let cluster = test_cluster();
        let [receiving, _, sending] = path(&cluster);
        let names = path_roster(&cluster);
        let root = cluster_root();
        let mut receive = relay(receiving, &names, &dir)
            .over(vec![Box::new(UdpRoundTrip)])
            .with_faults(FaultPlan::realistic());
        let mut failed = 0;
        for _ in 0..12 {
            let snapshot = receive.tick();
            failed += snapshot
                .health(&format!("{root}/node/{receiving}/receive/udp"))
                .iter()
                .filter(|record| record.health == Health::Done)
                .count();
        }
        assert!(
            failed > 0,
            "realistic faults reach a receiving node's receive"
        );
        let rounds = 12 * CONTRACTS.len() as u64;
        let handed: u64 = receive.hops().links().map(|hop| hop.count).sum();
        assert!(
            handed < rounds,
            "{handed} of {rounds}: a failed arrival is not handed on"
        );

        let short = Roster::parse(&roster_text(&[
            (receiving, "receiving"),
            (sending, "sending"),
        ]))
        .expect("a roster with no process");
        assert!(Relay::new(receiving, &root, Stage::Receive, &short, &dir, &dir).is_none());
        let whole = Roster::of(&[receiving.to_string()]);
        assert!(Relay::new(receiving, &root, Stage::Receive, &whole, &dir, &dir).is_none());
        let full = Roster::parse(&names).expect("a whole path");
        assert!(
            Relay::new(receiving, &root, Stage::Send, &full, &dir, &dir).is_none(),
            "a node runs only the stages it declared"
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
