//! The stand-in implementors the estate's real gates are driven with, and
//! one run of each gate (ADR-0019): a mutual-TLS subject read off the
//! arrival, a certificate proven or refused, the subject resolved through
//! the configuration's Parties, and a Location's policy.

use authenticate::{
    Acceptance, AuthenticateError, Authenticator, PartyRegistry, Refusal, authenticate,
};
use authorize::{Action, Attempt, Authorizer, Decision, authorize};
use context::{Alignment, AuthenticatedIdentity, IdentityFacts, OnMisalignment, Verified};
use identify::{IdentifyError, Presented, StreamArrival, TransportIdentifier, identify_transport};
use xcore::{Arriving, Layer, Mechanism, PartyId, Purpose, mechanism};

use super::Facing;
use crate::parties::Parties;

/// Reads a mutual-TLS subject out of the arrival. A well-formed arrival carries
/// a `peer.subject`; an identification fault carries none, and `None` is the
/// honest answer — nothing was presented to identify.
struct SubjectIdentifier;

impl TransportIdentifier for SubjectIdentifier {
    fn mechanism(&self) -> Mechanism {
        mechanism::mutual_tls()
    }

    fn identify(&self, arrival: &StreamArrival<'_>) -> Result<Option<Presented>, IdentifyError> {
        Ok(arrival
            .property("peer.subject")
            .map(|subject| Presented::passed(mechanism::mutual_tls(), subject)))
    }
}

/// Proves a mutual-TLS subject. `honest` proves it; otherwise it refuses, which
/// is what a rejected or expired certificate does at the handshake.
struct TlsAuthenticator {
    honest: bool,
}

impl Authenticator for TlsAuthenticator {
    fn mechanism(&self) -> Mechanism {
        mechanism::mutual_tls()
    }

    fn verify(&self, _presented: &Presented) -> Result<Verified, AuthenticateError> {
        Ok(if self.honest {
            Verified::Proven
        } else {
            Verified::Refused
        })
    }
}

/// Resolves a subject to the Party that holds it among the configuration's
/// Parties, under the mechanism that verified it and for the purpose
/// asked, by the identifier written with it; a subject none holds resolves
/// to nobody.
struct Configured<'a>(&'a Parties);

impl PartyRegistry for Configured<'_> {
    fn resolve(&self, mechanism: &str, purpose: Purpose, value: &str) -> Option<PartyId> {
        self.0
            .holding(mechanism, purpose, value)
            .map(|place| self.0.id(place))
    }
}

/// Allows the action on this location. `permits` allows; otherwise it denies —
/// the Party is proven but not permitted here.
struct LocationPolicy {
    permits: bool,
}

impl Authorizer for LocationPolicy {
    // The trait fixes the signature to `-> &str`; the name is a literal, and an
    // impl cannot widen the lifetime the trait elided.
    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "playground-location"
    }

    fn layer(&self) -> Layer {
        Layer::Transport
    }

    fn decide(&self, _identity: &IdentityFacts, attempt: &Attempt) -> Option<Decision> {
        Some(if self.permits {
            Decision::Allowed
        } else {
            Decision::denied(
                "playground-location",
                format!("the party is not permitted to {} here", attempt.action),
            )
        })
    }
}

/// What the arrival claims: `subject` as its certificate's, or nothing.
pub(super) fn identification(subject: Option<&str>) -> Vec<Presented> {
    let properties: Vec<(String, String)> = subject
        .map(|subject| ("peer.subject".to_string(), subject.to_string()))
        .into_iter()
        .collect();
    let location = crate::support::cluster_root();
    let arrival = StreamArrival::new(Arriving::Pushed, &location, &properties);
    let identifier = SubjectIdentifier;
    let identifiers: [&dyn TransportIdentifier; 1] = [&identifier];
    identify_transport(&identifiers, &arrival).unwrap_or_default()
}

/// `presented` proven, or refused where not `honest`, at a Location that
/// accepts mutual TLS from the Party `facing` alone.
pub(super) fn authentication(
    facing: &Facing<'_>,
    presented: &Presented,
    honest: bool,
) -> Result<AuthenticatedIdentity, Refusal> {
    let acceptance = Acceptance::closed()
        .accepting(&mechanism::mutual_tls())
        .from_party(facing.parties.id(facing.party));
    let authenticator = TlsAuthenticator { honest };
    let authenticators: [&dyn Authenticator; 1] = [&authenticator];
    let registry = Configured(facing.parties);
    authenticate(&acceptance, &authenticators, &registry, presented)
}

/// Whether the Party `facing` may `action` here: denied where not
/// `permits`.
pub(super) fn authorization(
    facing: &Facing<'_>,
    identity: AuthenticatedIdentity,
    action: Action,
    permits: bool,
) -> Decision {
    let facts = IdentityFacts::evaluate(Alignment::None, identity, None);
    let policy = LocationPolicy { permits };
    let policies: [&dyn Authorizer; 1] = [&policy];
    let attempt = Attempt::new(action, facing.name());
    authorize(&policies, &facts, &attempt, OnMisalignment::Accept)
}
