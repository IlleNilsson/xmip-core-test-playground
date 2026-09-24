//! The identity pipeline's outcomes as verdicts beneath a pair's stage.
//!
//! The three Receive steps — Identification, Authentication, Authorization —
//! and the Send presentation each publish as a child scope under their stage,
//! so an operator drills to the step that failed (ADR-0019, ADR-0033). The
//! schedule publishes both ends; a node publishes its own — a receiving node
//! the Receive steps, an `S` node the presentation.

use node::Stage;

use super::{IdentityFaults, receive, send};
use crate::verdict::{Contract, Verdict};

/// The Receive steps for one (transport, contract) in `round`, each a verdict
/// at its step's point under the Receive stage.
#[must_use]
pub fn receive_verdicts(
    faults: &IdentityFaults,
    transport: &str,
    contract: Contract,
    round: u64,
    now: i64,
) -> Vec<Verdict> {
    receive(faults, transport, contract, round)
        .into_iter()
        .map(|(step, outcome)| Verdict {
            stage: Stage::Receive,
            transport: transport.to_string(),
            contract,
            outcome,
            bytes: 0,
            point: Some(step.name()),
            observed_unix_nanos: now,
        })
        .collect()
}

/// The Send presentation for one (transport, contract) in `round`, a verdict
/// at the `identity` point under the Send stage.
#[must_use]
pub fn send_verdict(
    faults: &IdentityFaults,
    transport: &str,
    contract: Contract,
    round: u64,
    now: i64,
) -> Verdict {
    Verdict {
        stage: Stage::Send,
        transport: transport.to_string(),
        contract,
        outcome: send(faults, transport, contract, round),
        bytes: 0,
        point: Some("identity"),
        observed_unix_nanos: now,
    }
}
