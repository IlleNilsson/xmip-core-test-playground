//! The `RoundTrip` test: one round over one transport, judged.
//!
//! A scenario, not a protocol — it runs over any [`RoundTrip`] adapter, so the
//! transport is the variable and the scenario is the constant. Send the
//! contract's Stream, take what returned, check it matches and the contract
//! holds over it. ADR-0028.
//!
//! It returns the *base* outcome of the real exchange and how many bytes moved.
//! The [`Schedule`](crate::Schedule) expands that into a verdict per message-path
//! stage — Receive, Process, Send — and injects the faults a real integration
//! suffers, since loopback itself never fails.

use stream::Stream;
use xcore::StreamId;

use crate::roundtrip::{Exchange, RoundTrip};
use crate::verdict::{Contract, Outcome};

/// Run one `RoundTrip` round for one transport and one contract, and judge it.
///
/// The probe sends an actual Stream; on a clean round trip the arrived bytes are
/// rebuilt into a Stream and the real contract is run over it. Returns the base
/// outcome and the bytes that moved (zero unless delivered).
#[must_use]
pub fn round_trip(transport: &dyn RoundTrip, contract: Contract) -> (Outcome, u64) {
    round_trip_with(transport, contract, contract.stream().bytes())
}

/// The same round with a chosen probe — a stress level's payload at the size
/// protocols break on, still holding `contract` — judged the same way: what
/// came back must equal what went out, and the contract must hold over it.
#[must_use]
pub fn round_trip_with(
    transport: &dyn RoundTrip,
    contract: Contract,
    payload: &[u8],
) -> (Outcome, u64) {
    // It round-tripped; now the contract must hold over what arrived.
    let outcome = match carried(transport, payload) {
        Ok(back) => held(contract, back),
        Err(outcome) => outcome,
    };

    let bytes = if matches!(outcome, Outcome::Delivered) {
        payload.len() as u64
    } else {
        0
    };

    (outcome, bytes)
}

/// The transport's half of a round: `payload` through `transport`, and what
/// arrived when it arrived whole. A node runs this half alone — a receiving
/// node as the arrival, an `S` node as the send — and leaves the contract to
/// the `P` node between them.
///
/// # Errors
///
/// The outcome to publish when nothing arrived whole: one-sided when the
/// transport declares it cannot carry the payload or supports one direction
/// only, failed when the exchange failed or what came back did not match.
pub fn carried(transport: &dyn RoundTrip, payload: &[u8]) -> Result<Vec<u8>, Outcome> {
    if let Some(why) = transport.refuses(payload) {
        return Err(Outcome::OneSided(why));
    }
    match transport.exchange(payload) {
        Exchange::Returned(back) if back == payload => Ok(back),
        Exchange::Returned(_) => Err(Outcome::Failed(
            "what came back did not match what was sent".to_string(),
        )),
        Exchange::OneSided(why) => Err(Outcome::OneSided(why)),
        Exchange::Failed(why) => Err(Outcome::Failed(why)),
    }
}

/// The contract's half of a round: the bytes that arrived rebuilt into a
/// Stream, and the real contract run over it.
#[must_use]
pub fn held(contract: Contract, arrived: Vec<u8>) -> Outcome {
    let arrived = Stream::new(
        StreamId::new(1),
        arrived,
        Some(contract.representation().to_string()),
    );
    match contract.validate(&arrived) {
        Ok(()) => Outcome::Delivered,
        Err(why) => Outcome::Failed(format!("contract not held: {why}")),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;
    use crate::roundtrip::{FileRoundTrip, TIMEOUT, TcpRoundTrip, UdpRoundTrip, all_transports};
    use crate::schedule::drive_pairs;
    use crate::stress::Stress;
    use crate::support::scratch;

    #[test]
    fn a_payload_that_makes_the_round_trip_over_file_is_delivered() {
        let dir = scratch("delivered");
        let (outcome, bytes) = round_trip(&FileRoundTrip::new(&dir), Contract::Text);

        assert_eq!(outcome, Outcome::Delivered);
        assert_eq!(bytes, Contract::Text.payload().len() as u64);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn the_same_scenario_runs_over_tcp() {
        // The point of the RoundTrip adapter: one scenario, a different
        // transport underneath, no change here.
        let (outcome, _) = round_trip(&TcpRoundTrip, Contract::Bytes);

        assert_eq!(outcome, Outcome::Delivered);
    }

    /// Every round at `stress` over `transports`: the level's payload for the
    /// round, every contract, judged rather than waited on, and a failure
    /// always carrying its reason. Returns how many rounds a transport refused
    /// above its declared ceiling, and the lines for every other failure — a
    /// transport that did not carry what it declares no ceiling against.
    fn drive(transports: &[Box<dyn RoundTrip>], stress: Stress) -> (u64, Vec<String>) {
        let mut refused = 0;
        let mut unexplained = Vec::new();
        for round in 1..=stress.rounds() {
            let size = stress.size_for(round);
            // The level's workers, as the schedule drives them, so a datagram
            // refused after its timeout holds up one worker, not the test.
            let judged = drive_pairs(transports, stress.workers(), |transport, contract| {
                let payload = crate::stress::payload(contract, size);
                let started = Instant::now();
                let (outcome, bytes) = round_trip_with(transport, contract, &payload);
                let ceiling = transport
                    .ceiling()
                    .or((transport.transport() == "udp").then_some(65_507));
                (
                    format!("{}/{}", transport.transport(), contract.name()),
                    payload.len(),
                    outcome,
                    bytes,
                    started.elapsed(),
                    ceiling,
                )
            });
            for (pair, sent, outcome, bytes, took, ceiling) in judged {
                assert!(
                    took < TIMEOUT * 3,
                    "{pair} took {took:?} at {sent} bytes: judged, never waited on"
                );
                match outcome {
                    Outcome::Delivered => assert_eq!(bytes, sent as u64),
                    Outcome::Failed(why) | Outcome::OneSided(why) => {
                        assert!(!why.is_empty(), "a failure carries its reason");
                        // udp declares no ceiling yet and cannot carry past a
                        // datagram; that is the refusal expected.
                        if ceiling.is_some_and(|limit| sent > limit) {
                            refused += 1;
                        } else {
                            unexplained.push(format!("{pair} at {sent} bytes: {why}"));
                        }
                    }
                }
            }
        }
        (refused, unexplained)
    }

    #[test]
    fn harsh_sizes_are_carried_or_refused_with_a_reason() {
        let dir = scratch("round-trip-harsh");
        let transports: Vec<Box<dyn RoundTrip>> = vec![
            Box::new(FileRoundTrip::new(&dir)),
            Box::new(TcpRoundTrip),
            Box::new(UdpRoundTrip),
        ];
        let (refused, unexplained) = drive(&transports, Stress::Harsh);
        // Harsh reaches sixteen bits plus one, which no datagram carries.
        assert!(refused > 0, "udp refuses what is above its ceiling");
        assert!(
            unexplained.is_empty(),
            "not carried:\n{}",
            unexplained.join("\n")
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    #[ignore = "brutal: every transport at every edge size, for the runner"]
    fn brutal_sizes_over_every_transport() {
        let dir = scratch("round-trip-brutal");
        let (_, unexplained) = drive(&all_transports(&dir), Stress::Brutal);
        std::fs::remove_dir_all(&dir).ok();
        assert!(
            unexplained.is_empty(),
            "not carried:\n{}",
            unexplained.join("\n")
        );
    }
}
