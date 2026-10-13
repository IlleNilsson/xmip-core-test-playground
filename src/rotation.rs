//! The fault every fifty seconds, moving across Parties, transports and
//! streams (ADR-0028, amendments 2026-10-06 and 2026-10-13).
//!
//! The owner, 2026-10-06: *All parties has mostly good communication there
//! are good and bad days over parties, transports and streams*, and *inject
//! faults every 50 seconds*, so a one-minute span always shows trouble
//! somewhere and, paused and widened, the history of it. Each turn of
//! [`PERIOD`] seconds strikes one target for its first [`STRIKE`] seconds and
//! leaves everything else alone: one Party's transport on one turn —
//! every stream over it — and one stream of another Party's on the next,
//! on the side it sends into or the side delivered to. No Party is the
//! failing one: the turns go round the Parties in order.
//!
//! A turn is the wall clock's, so every node of a run agrees on it, and
//! the target is a function of the turn alone, so a test asserts it and a
//! run reproduces. The steady transport carries no fault here either
//! ([`STEADY`]).

use node::Stage;

use crate::fault::{Fault, FaultKind, FaultPlan, STEADY};
use crate::parties::Spread;
use crate::schedule::CONTRACTS;
use crate::stress::Stress;
use crate::verdict::Contract;

/// Seconds from one fault to the next.
pub const PERIOD: u64 = 50;

/// Seconds a fault lasts from the start of its turn.
pub const STRIKE: u64 = 10;

/// What one turn strikes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Strike {
    /// The place of the Party struck.
    pub party: usize,
    /// Receive where it sends into the cluster, Send where it is delivered
    /// to.
    pub stage: Stage,
    pub transport: String,
    /// The one stream struck, or every stream over the transport.
    pub contract: Option<Contract>,
    pub fault: Fault,
}

/// The rotation over a run's Parties and their transports.
#[derive(Clone, Debug)]
pub struct Rotation {
    spread: Spread,
}

impl Rotation {
    #[must_use]
    pub const fn new(spread: Spread) -> Self {
        Self { spread }
    }

    /// What turn `turn` strikes: the Parties in order; each second round of
    /// them on the side delivered to; every other turn one stream rather
    /// than a whole transport. `None` where the Party has no transport on
    /// that side but the steady one.
    #[must_use]
    pub fn turn(&self, turn: u64) -> Option<Strike> {
        let count = self.spread.parties().all().len() as u64;
        let party = usize::try_from(turn % count).ok()?;
        let stage = if (turn / count).is_multiple_of(2) {
            Stage::Receive
        } else {
            Stage::Send
        };
        let over = match stage {
            Stage::Send => self.spread.receives(party),
            _ => self.spread.sends(party),
        };
        let candidates: Vec<String> = over.into_iter().filter(|one| one != STEADY).collect();
        let mixed = mix(turn);
        let at = usize::try_from(mixed % candidates.len().max(1) as u64).ok()?;
        let transport = candidates.get(at)?.clone();
        let stream = turn % 2 == 1;
        let contract = stream.then(|| {
            let index = usize::try_from((mixed >> 16) % CONTRACTS.len() as u64).unwrap_or(0);
            CONTRACTS[index]
        });
        Some(Strike {
            party,
            stage,
            transport,
            contract,
            fault: fault(stage, stream, turn),
        })
    }

    /// What is struck at `unix_seconds`: its turn's target during the first
    /// [`STRIKE`] seconds, nothing for the rest.
    #[must_use]
    pub fn striking(&self, unix_seconds: u64) -> Option<Strike> {
        (unix_seconds % PERIOD < STRIKE)
            .then(|| self.turn(unix_seconds / PERIOD))
            .flatten()
    }

    /// The fault the pair (`transport`, `contract`) suffers at `stage` at
    /// `unix_seconds`, if it is what is struck.
    #[must_use]
    pub fn fault_for(
        &self,
        stage: Stage,
        transport: &str,
        contract: Contract,
        unix_seconds: u64,
    ) -> Option<Fault> {
        self.striking(unix_seconds)
            .filter(|strike| strike.stage == stage && strike.transport == transport)
            .filter(|strike| strike.contract.is_none_or(|only| only == contract))
            .map(|strike| strike.fault)
    }
}

/// The faults a level injects, and whether the rotation strikes: nothing at
/// `Calm`; at `Realistic` the rotation alone, so between its faults
/// everything is green (the owner, 2026-10-06); above it the realistic
/// table scaled to the level as well.
#[must_use]
pub fn leveled(stress: Stress) -> (FaultPlan, bool) {
    match stress {
        Stress::Calm => (FaultPlan::none(), false),
        Stress::Realistic => (FaultPlan::none(), true),
        _ => (FaultPlan::realistic().at(stress), true),
    }
}

/// The fault a turn strikes with, of clause 4a's kinds: a transport's
/// connection or address, or a stream's content.
const fn fault(stage: Stage, stream: bool, turn: u64) -> Fault {
    let (kind, reason) = match (stage, stream, (turn / 2) % 2) {
        (Stage::Send, false, 0) => (FaultKind::Addressing, "the target host did not resolve"),
        (Stage::Send, false, _) => (
            FaultKind::Transport,
            "the send target refused the connection",
        ),
        (Stage::Send, true, _) => (
            FaultKind::Contract,
            "the Party refused the content: its contract was not held",
        ),
        (_, false, _) => (FaultKind::Transport, "connection reset by peer"),
        (_, true, _) => (
            FaultKind::Contract,
            "schema validation failed: unexpected element",
        ),
    };
    Fault { kind, reason }
}

/// A turn mixed into bits that look varied from one turn to the next:
/// `SplitMix64`'s finalizer.
const fn mix(turn: u64) -> u64 {
    let mut bits = turn.wrapping_add(0x9E37_79B9_7F4A_7C15);
    bits = (bits ^ (bits >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    bits = (bits ^ (bits >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    bits ^ (bits >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parties::Parties;

    fn rotation() -> Rotation {
        Rotation::new(Parties::configured().over_every_transport())
    }

    #[test]
    fn one_target_a_turn_moving_across_parties_sides_transports_and_streams() {
        let rotation = rotation();
        let count = Parties::configured().all().len() as u64;
        let turns: Vec<Strike> = (0..4 * count)
            .map(|turn| rotation.turn(turn).expect("a target every turn"))
            .collect();
        let parties: std::collections::BTreeSet<usize> = turns.iter().map(|s| s.party).collect();
        assert_eq!(parties.len() as u64, count, "every Party has its bad day");
        assert!(turns.iter().any(|s| s.stage == Stage::Receive));
        assert!(turns.iter().any(|s| s.stage == Stage::Send));
        assert!(turns.iter().any(|s| s.contract.is_some()), "a stream");
        assert!(turns.iter().any(|s| s.contract.is_none()), "a transport");
        let transports: std::collections::BTreeSet<&str> =
            turns.iter().map(|s| s.transport.as_str()).collect();
        assert!(transports.len() > 2, "{transports:?}");
        for (turn, strike) in turns.iter().enumerate() {
            let spread = Parties::configured().over_every_transport();
            let theirs = match strike.stage {
                Stage::Send => spread.receives(strike.party),
                _ => spread.sends(strike.party),
            };
            assert!(
                theirs.contains(&strike.transport),
                "turn {turn}: its Party's"
            );
            assert_ne!(strike.transport, STEADY);
        }
        assert_eq!(rotation.turn(7), rotation.turn(7), "deterministic");
    }

    #[test]
    fn a_fault_lasts_the_first_seconds_of_its_turn_and_strikes_only_its_target() {
        let rotation = rotation();
        let turn = 1000;
        let start = turn * PERIOD;
        let strike = rotation.turn(turn).expect("a target");
        let contract = strike.contract.unwrap_or(CONTRACTS[0]);
        let at = |seconds| rotation.fault_for(strike.stage, &strike.transport, contract, seconds);
        assert_eq!(at(start), Some(strike.fault));
        assert_eq!(at(start + STRIKE - 1), Some(strike.fault));
        assert_eq!(at(start + STRIKE), None, "green between faults");
        assert_eq!(at(start + PERIOD - 1), None);
        let other = if strike.stage == Stage::Send {
            Stage::Receive
        } else {
            Stage::Send
        };
        assert_eq!(
            rotation.fault_for(other, &strike.transport, contract, start),
            None
        );
        if let Some(only) = strike.contract {
            let another = CONTRACTS.into_iter().find(|one| *one != only).expect("two");
            let spared = rotation.fault_for(strike.stage, &strike.transport, another, start);
            assert_eq!(
                spared, None,
                "a stream strike spares the transport's others"
            );
        }
    }

    #[test]
    fn most_of_the_time_every_party_is_good() {
        let rotation = rotation();
        let struck = (0..PERIOD * 60)
            .filter(|second| rotation.striking(*second).is_some())
            .count() as u64;
        assert_eq!(struck, STRIKE * 60, "one fifth of the time, one target");
    }
}
