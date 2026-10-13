//! What one stage of a [`Relay`] makes of a pair: a receive stage's arrival
//! through its transport, a process stage's contract held, a send stage's
//! sending out — each with its faults and its Party's identity steps — and
//! the verdict and the handoff they come to. Split from `relay.rs` on
//! 2026-10-13, when the Parties and the rotation made it long.

use node::Stage;

use super::{Judged, Relay};
use crate::handoff::Handoff;
use crate::identity::Facing;
use crate::identity::verdicts::{receive_verdicts, send_verdict};
use crate::round_trip::{carried, held};
use crate::schedule::{all_pairs, drive_each, seconds, slice};
use crate::verdict::{Contract, Outcome, Verdict};

impl Relay {
    /// This round's slice of this node's share of the matrix, each pair
    /// arriving through its transport.
    pub(super) fn receive(&mut self, now: i64) -> Vec<Judged> {
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
            let arrival = match self.fault(name, contract, self.round, now) {
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
            let facing = self.facing(self.spread.sender(name));
            let round = self.round;
            verdicts.extend(receive_verdicts(
                identity, &facing, name, contract, round, now,
            ));
            Judged { verdicts, forward }
        })
    }

    /// What is in the inbox, up to the round's bound: a process stage holds
    /// the contract over each and hands it on, a send stage sends each out.
    pub(super) fn drain_inbox(&mut self, now: i64) -> Vec<Judged> {
        let (claimed, unreadable) = self.inbox.claim(self.per_round);
        self.unreadable += unreadable as u64;
        drive_each(&claimed, self.workers, |handoff| {
            let name = handoff.transport.as_str();
            let contract = handoff.contract;
            if let Some(outcome) = self.fault(name, contract, handoff.round, now) {
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
            let facing = self.facing(self.spread.receiver(name));
            let verdicts = vec![
                self.verdict(name, contract, outcome, bytes, now),
                send_verdict(identity, &facing, name, contract, handoff.round, now),
            ];
            Judged {
                verdicts,
                forward: None,
            }
        })
    }

    /// `bytes` out through the transport called `name`: how many went, or the
    /// outcome when they did not.
    pub(super) fn sent(&self, name: &str, bytes: &[u8]) -> Result<u64, Outcome> {
        let transport = self
            .transports
            .iter()
            .find(|transport| transport.transport() == name)
            .ok_or_else(|| Outcome::Failed(format!("no transport named {name} on this node")))?;
        carried(transport.as_ref(), bytes).map(|back| back.len() as u64)
    }

    /// The injected fault for this stage of the pair in `round` at `now`,
    /// as the outcome to publish: what the rotation strikes first.
    pub(super) fn fault(
        &self,
        transport: &str,
        contract: Contract,
        round: u64,
        now: i64,
    ) -> Option<Outcome> {
        self.rotation
            .as_ref()
            .and_then(|rotation| rotation.fault_for(self.stage, transport, contract, seconds(now)))
            .or_else(|| {
                self.faults
                    .fault_for(self.stage, transport, contract, round)
            })
            .map(|fault| Outcome::Failed(fault.evidence()))
    }

    /// The Party at `place`, among the configuration's.
    pub(super) fn facing(&self, place: usize) -> Facing<'_> {
        Facing {
            parties: self.spread.parties(),
            party: place,
        }
    }

    pub(super) fn verdict(
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

    pub(super) fn handoff(
        &self,
        transport: &str,
        contract: Contract,
        round: u64,
        bytes: Vec<u8>,
    ) -> Handoff {
        Handoff {
            transport: transport.to_string(),
            contract,
            round,
            from: self.name.clone(),
            bytes,
        }
    }
}
