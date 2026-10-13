//! The identity pipeline a Receive Location runs, and what a Send Location
//! presents.
//!
//! ADR-0019: **Identification → Authentication → Authorization**, invariant
//! across security technologies. ADR-0033: certificates on Receive and Send. A
//! Receive Location must identify who is claimed, authenticate the claim, and
//! authorize what it may do; a Send Location must present an identity to the far
//! end.
//!
//! The playground drives the estate's **real** gates — `identify_transport`,
//! `authenticate`, `authorize` — with stand-in implementors, so a fault is a
//! genuine `Refusal` or `Decision::Denied`, not a fabricated string. The
//! mechanism is mutual-TLS throughout (ADR-0033's first mechanism). A fault
//! feeds the gate the input that makes it truly refuse: an arrival with no
//! certificate, an authenticator that will not prove, a policy that denies.

mod gates;
pub mod verdicts;

use authorize::Action;
use identify::Presented;
use xcore::mechanism;

use crate::fault::{STEADY, fires_keyed};
use crate::parties::Parties;
use crate::verdict::{Contract, Outcome};

/// The Party a Location faces: the one that sends into a Receive Location,
/// or the one a Send Location delivers to, by its place among the
/// configuration's Parties (`parties.rs`). A well-formed arrival claims its
/// first identity as its certificate's subject, the Receive Location
/// accepts that Party alone and resolves the claim through every Party's
/// identities; a Send Location presents the same.
#[derive(Clone, Copy, Debug)]
pub struct Facing<'a> {
    pub parties: &'a Parties,
    pub party: usize,
}

impl Facing<'_> {
    /// The Party's name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.parties.at(self.party).name
    }

    /// The subject it presents: its first identity.
    fn subject(&self) -> &str {
        self.parties
            .at(self.party)
            .identities
            .first()
            .map_or("", String::as_str)
    }
}

/// The steps a Receive Location runs, in order. Each is its own scope so an
/// operator drills to the one that failed. ADR-0019.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Step {
    Identification,
    Authentication,
    Authorization,
}

impl Step {
    /// The three, in message-path order.
    pub const RECEIVE: [Step; 3] = [
        Step::Identification,
        Step::Authentication,
        Step::Authorization,
    ];

    /// The token as it appears in the scope: `receive/<t>/<c>/<step>`.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Step::Identification => "identification",
            Step::Authentication => "authentication",
            Step::Authorization => "authorization",
        }
    }
}

/// The identity faults the playground injects, or none. Mirrors `FaultPlan` for
/// the identity pipeline: certificate-lifecycle and policy faults on the three
/// Receive steps and on Send presentation. `file` is exempt, so one transport's
/// identity stays green, exactly as its transport does.
pub struct IdentityFaults {
    enabled: bool,
}

impl IdentityFaults {
    /// No injected identity faults — every step proves and is allowed.
    #[must_use]
    pub const fn none() -> Self {
        Self { enabled: false }
    }

    /// A realistic spread of identity faults across the pipeline.
    #[must_use]
    pub const fn realistic() -> Self {
        Self { enabled: true }
    }

    /// The Receive step this (transport, contract) fails this round, if any, and
    /// the operator-facing reason. The first rule that fires wins.
    fn receive_fault(
        &self,
        transport: &str,
        contract: Contract,
        round: u64,
    ) -> Option<(Step, &'static str)> {
        const RULES: &[(Step, &str, u8, &str)] = &[
            (
                Step::Identification,
                "http",
                3,
                "no client certificate presented",
            ),
            (
                Step::Authentication,
                "http",
                4,
                "client certificate rejected",
            ),
            (
                Step::Authentication,
                "http",
                3,
                "Let's Encrypt certificate expired; renewal pending",
            ),
            (
                Step::Authentication,
                "smtp",
                3,
                "STARTTLS certificate not trusted",
            ),
            (
                Step::Authorization,
                "tcp",
                3,
                "the party is not permitted on this Receive Location",
            ),
        ];

        if !self.enabled || transport == STEADY {
            return None;
        }

        for &(step, only, rate, reason) in RULES {
            if only == transport && fires_keyed(rate, &key(step.name(), transport, contract), round)
            {
                return Some((step, reason));
            }
        }
        None
    }

    /// The reason Send cannot present an identity this round, if any.
    fn send_fault(&self, transport: &str, contract: Contract, round: u64) -> Option<&'static str> {
        const RULES: &[(&str, u8, &str)] = &[
            ("http", 4, "no certificate to present"),
            ("smtp", 3, "the presented certificate has expired"),
            (
                "websocket",
                3,
                "ACME renewal pending; cannot present a certificate",
            ),
        ];

        if !self.enabled || transport == STEADY {
            return None;
        }

        for &(only, rate, reason) in RULES {
            if only == transport
                && fires_keyed(rate, &key("send-identity", transport, contract), round)
            {
                return Some(reason);
            }
        }
        None
    }
}

fn key(step: &str, transport: &str, contract: Contract) -> String {
    format!("{step}/{transport}/{}", contract.name())
}

/// Run the Receive Location's identity pipeline for one (transport, contract)
/// this round, the Party `facing` sending. Returns an outcome per step in
/// order. The pipeline stops at the first failure: earlier steps ran for
/// real and passed, the failing step carries the reason, and the rest report
/// "not reached".
#[must_use]
pub fn receive(
    faults: &IdentityFaults,
    facing: &Facing<'_>,
    transport: &str,
    contract: Contract,
    round: u64,
) -> Vec<(Step, Outcome)> {
    let faulted = faults.receive_fault(transport, contract, round);
    let mut out = Vec::with_capacity(3);

    // Identification.
    let subject_present = faulted.is_none_or(|(step, _)| step != Step::Identification);
    let claims = gates::identification(subject_present.then(|| facing.subject()));
    let Some(presented) = claims.first() else {
        let reason = faulted.map_or("no identity was presented", |(_, reason)| reason);
        out.push((Step::Identification, Outcome::Failed(reason.to_string())));
        out.push((Step::Authentication, not_reached(Step::Identification)));
        out.push((Step::Authorization, not_reached(Step::Identification)));
        return out;
    };
    out.push((Step::Identification, Outcome::Delivered));

    // Authentication.
    let honest = faulted.is_none_or(|(step, _)| step != Step::Authentication);
    let identity = match gates::authentication(facing, presented, honest) {
        Ok(identity) => {
            out.push((Step::Authentication, Outcome::Delivered));
            identity
        }
        Err(refusal) => {
            out.push((
                Step::Authentication,
                Outcome::Failed(reason_or(faulted, Step::Authentication, &refusal)),
            ));
            out.push((Step::Authorization, not_reached(Step::Authentication)));
            return out;
        }
    };

    // Authorization.
    let permits = faulted.is_none_or(|(step, _)| step != Step::Authorization);
    let decision = gates::authorization(facing, identity, Action::Receive, permits);
    if decision.allowed() {
        out.push((Step::Authorization, Outcome::Delivered));
    } else {
        out.push((
            Step::Authorization,
            Outcome::Failed(reason_or(faulted, Step::Authorization, &decision)),
        ));
    }
    out
}

/// The Send Location presents an identity to the Party `facing` (ADR-0033:
/// a Send Location presents a client certificate). Delivered when Xmip holds
/// a credential and is authorized to present it here; Failed when it has
/// none, the one it has is unusable, or presenting it is not permitted.
#[must_use]
pub fn send(
    faults: &IdentityFaults,
    facing: &Facing<'_>,
    transport: &str,
    contract: Contract,
    round: u64,
) -> Outcome {
    if let Some(reason) = faults.send_fault(transport, contract, round) {
        return Outcome::Failed(reason.to_string());
    }

    let presented = Presented::passed(mechanism::mutual_tls(), facing.subject());
    match gates::authentication(facing, &presented, true) {
        Ok(identity) => {
            let decision = gates::authorization(facing, identity, Action::Send, true);
            if decision.allowed() {
                Outcome::Delivered
            } else {
                Outcome::Failed(decision.to_string())
            }
        }
        Err(refusal) => Outcome::Failed(refusal.to_string()),
    }
}

/// The scenario reason when this step is the faulted one, else what the real
/// gate said. A fault names the operator-facing cause; the gate still genuinely
/// refused, which is what proves the pipeline is wired.
fn reason_or(
    faulted: Option<(Step, &'static str)>,
    step: Step,
    gate: &dyn std::fmt::Display,
) -> String {
    faulted
        .filter(|(faulted_step, _)| *faulted_step == step)
        .map_or_else(|| gate.to_string(), |(_, reason)| reason.to_string())
}

fn not_reached(after: Step) -> Outcome {
    Outcome::OneSided(format!("not reached — {} did not pass", after.name()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The configuration's Party at `place`.
    fn facing(place: usize) -> Facing<'static> {
        Facing {
            parties: Parties::configured(),
            party: place,
        }
    }

    #[test]
    fn each_party_is_proven_by_its_own_identity_and_no_other() {
        let parties = Parties::configured();
        let none = IdentityFaults::none();
        for place in 0..parties.all().len() {
            let steps = receive(&none, &facing(place), "http", Contract::Json, 1);
            assert!(
                steps.iter().all(|(_, o)| *o == Outcome::Delivered),
                "{place}"
            );
            assert_eq!(
                send(&none, &facing(place), "http", Contract::Json, 1),
                Outcome::Delivered
            );
        }
        // Another Party's certificate at this Party's Location is refused at
        // authentication: the Location accepts its own Party alone.
        let other = facing(1);
        let claims = gates::identification(Some(facing(0).subject()));
        let refused = gates::authentication(&other, &claims[0], true);
        assert!(refused.is_err(), "{refused:?}");
    }

    #[test]
    fn a_clean_receive_passes_all_three_steps() {
        let steps = receive(
            &IdentityFaults::none(),
            &facing(0),
            "http",
            Contract::Json,
            1,
        );
        assert_eq!(steps.len(), 3);
        assert!(
            steps
                .iter()
                .all(|(_, outcome)| *outcome == Outcome::Delivered),
            "every step delivered on the happy path"
        );
    }

    #[test]
    fn a_clean_send_presents_identity() {
        assert_eq!(
            send(
                &IdentityFaults::none(),
                &facing(1),
                "http",
                Contract::Json,
                1
            ),
            Outcome::Delivered
        );
    }

    #[test]
    fn a_faulted_step_fails_and_the_rest_are_not_reached() {
        // Find a round where http suffers an identity fault, then assert the
        // pipeline stops at it.
        let faults = IdentityFaults::realistic();
        let mut seen_stop = false;
        for round in 0..500 {
            let steps = receive(&faults, &facing(2), "http", Contract::Json, round);
            if let Some(index) = steps
                .iter()
                .position(|(_, o)| matches!(o, Outcome::Failed(_)))
            {
                seen_stop = true;
                for (_, outcome) in &steps[index + 1..] {
                    assert!(
                        matches!(outcome, Outcome::OneSided(_)),
                        "steps after a failure are not reached"
                    );
                }
            }
        }
        assert!(
            seen_stop,
            "http should suffer an identity fault within 500 rounds"
        );
    }

    #[test]
    fn file_never_faults_its_identity() {
        let faults = IdentityFaults::realistic();
        for round in 0..2000 {
            for contract in [Contract::Bytes, Contract::Json, Contract::Xml] {
                let steps = receive(&faults, &facing(2), "file", contract, round);
                assert!(steps.iter().all(|(_, o)| *o == Outcome::Delivered));
                assert_eq!(
                    send(&faults, &facing(3), "file", contract, round),
                    Outcome::Delivered
                );
            }
        }
    }

    #[test]
    fn identity_faults_are_deterministic() {
        let faults = IdentityFaults::realistic();
        for round in 0..500 {
            assert_eq!(
                receive(&faults, &facing(2), "smtp", Contract::Json, round),
                receive(&faults, &facing(2), "smtp", Contract::Json, round)
            );
        }
    }
}
