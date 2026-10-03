//! What a node raises and who hears it: the Events of its stages, and the
//! Event subscriptions its hub holds (ADR-0065, amendment 2026-09-29).
//!
//! Every node subscribes two Parties in its own process — declared here, by
//! name, as [`operations`] and [`on_call`] — through the event
//! crate's hub and nothing of its own: **operations** hears every Event on
//! the node and is called back ([`xevent::listener::Listener`]), and
//! **on-call** hears the failures and is drained each round. Each round the
//! node raises one Event per stage it serves whose standing changed — a
//! failure when more of its pairs fail than the round before, a success
//! when fewer — so a steady node is quiet and a stirring one is heard. An
//! operator's order on one of them reaches it through [`Eventing::act`]
//! (`operator_orders.rs` takes the orders), and each round it records what
//! its hub holds in its snapshot, which the cluster and the roll publish as
//! the node's Event subscriptions. They are real ones: an operator who
//! pauses one sees its queue fill, and one who removes one sees it gone.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use authorize_party::PartyPolicy;

use node::{Capability, Stage};
use observe::{Act, Health, Scope, Snapshot};
use party::{Party, PartyKind};
use xaudit::program_audit::ProgramAudit;
use xcore::PartyId;
use xevent::Event;
use xevent::filter::Filter;
use xevent::hub::{EventSubscription, Hub};
use xevent::listener::Listener;
use xevent::outcome::Outcome;
use xevent::subscriber::Subscriber;

use crate::process_audit;

/// The Party that hears every Event on a node, as the Playground declares
/// it: its identifier and the name an operator reads it by, here and
/// nowhere else.
#[must_use]
pub fn operations() -> Party {
    Party::new(
        PartyId::new(0x0199_8a23_3ee9_7000_8000_0000_0000_0001),
        PartyKind::Service,
        "operations",
    )
}

/// The Party that hears a node's failures, declared the same way.
#[must_use]
pub fn on_call() -> Party {
    Party::new(
        PartyId::new(0x0199_8a23_3ee9_7000_8000_0000_0000_0002),
        PartyKind::Service,
        "on-call",
    )
}

/// How many Events each queue holds: small, so a paused one is seen to fill.
const CAPACITY: usize = 64;

/// A node's eventing.
pub struct Eventing {
    node: String,
    stages: Vec<Stage>,
    _listening: Option<Listener>,
    drained: Option<EventSubscription>,
    failing: BTreeMap<Stage, usize>,
}

impl Eventing {
    /// The node at `node` subscribes its two Parties, which the Playground,
    /// the program hosting the hub, allows by handing the hub its policy:
    /// being in its process admits nobody (ADR-0065, amendment 2026-09-26).
    /// A subscription the hub refused is audited as the failure to
    /// `subscribe`, and the node runs on without it.
    #[must_use]
    pub fn start(node: &str, capability: &Capability, audit: &ProgramAudit) -> Self {
        let hub = Hub::process();
        let its = PartyPolicy::new()
            .allow(operations().party_id)
            .allow(on_call().party_id);
        hub.authorize_by(vec![Arc::new(its)]);
        let everything = Filter::everything().beneath(node);
        let failures = Filter::everything().ending(Outcome::Failure).beneath(node);
        let listening = hub
            .subscribe(
                Subscriber::declared(&operations(), audit.clone()),
                everything,
                CAPACITY,
            )
            .map_err(|refused| refused.to_string())
            .and_then(|subscription| {
                subscription
                    .listen(|_| {})
                    .map_err(|error| error.to_string())
            });
        let drained = hub.subscribe(
            Subscriber::declared(&on_call(), audit.clone()),
            failures,
            CAPACITY,
        );
        let listening = listening
            .map_err(|problem| process_audit::fail(audit, "subscribe", &problem))
            .ok();
        let drained = drained
            .map_err(|refused| process_audit::fail(audit, "subscribe", &refused.to_string()))
            .ok();

        Self {
            node: node.to_string(),
            stages: capability.stages(),
            _listening: listening,
            drained,
            failing: BTreeMap::new(),
        }
    }

    /// Apply `act` to the Event subscription numbered `target` in this
    /// node's hub, by `who`.
    ///
    /// # Errors
    /// The hub's refusal, or a target that is no number, in words.
    pub fn act(&self, target: &str, act: Act, who: &str) -> Result<String, String> {
        let id = target
            .parse()
            .map_err(|_| format!("REFUSED: '{target}' numbers no Event subscription"))?;
        Hub::process()
            .act(id, act, who)
            .map_err(|refused| refused.to_string())
    }

    /// One round: raise what changed, drain on-call, and record what the hub
    /// holds in `snapshot`.
    pub fn round(&mut self, snapshot: &mut Snapshot) {
        let hub = Hub::process();
        for stage in self.stages.clone() {
            self.raise(hub, snapshot, stage);
        }

        if let Some(drained) = &self.drained {
            let _ = drained.next(Duration::ZERO, CAPACITY);
        }
        if self
            .drained
            .as_ref()
            .is_some_and(EventSubscription::is_closed)
        {
            self.drained = None;
        }

        for subscription in hub.standing(&self.node) {
            snapshot.record_event_subscription(subscription);
        }
        snapshot.clear_unheard(&self.node);
        for unheard in hub
            .unheard()
            .into_iter()
            .filter(|gone| gone.by == self.node)
        {
            snapshot.record_unheard(unheard);
        }
    }

    /// An Event for `stage` when its failing pairs changed since the round
    /// before.
    fn raise(&mut self, hub: &Hub, snapshot: &Snapshot, stage: Stage) {
        let at = format!("{}/{}", self.node, stage.name());
        let records = snapshot.health(&at);
        let failing = records
            .iter()
            .filter(|record| record.health == Health::Done)
            .filter(|record| Scope::new(&record.scope).stage() == Some(stage))
            .count();
        let before = self.failing.insert(stage, failing).unwrap_or(0);
        if records.is_empty() || failing == before {
            return;
        }
        let outcome = if failing > before {
            Outcome::Failure
        } else {
            Outcome::Success
        };
        hub.publish(
            Event::completed(stage, outcome, at)
                .saying("pairs", records.len().to_string())
                .saying("failing", failing.to_string()),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::support::scratch;
    use observe::{HealthRecord, PauseState};

    fn record(scope: &str, health: Health) -> HealthRecord {
        HealthRecord {
            scope: scope.to_string(),
            health,
            severity: 0,
            evidence: String::new(),
            observed_unix_nanos: 1,
        }
    }

    #[test]
    fn a_node_publishes_its_event_subscriptions_and_applies_an_act_on_one() {
        let shared = scratch("eventing");
        let cluster = crate::support::test_cluster();
        let receiving = &cluster.with_role("receiving").name;
        let node = format!("{}/node/{receiving}", cluster.scope());
        let node = node.as_str();
        let audit = ProgramAudit::new("xmip-playground-eventing-test", Some(&shared));
        let mut eventing =
            Eventing::start(node, &Capability::of(&[node::NodeRole::Receiving]), &audit);

        let mut first = Snapshot::new();
        first.record_health(record(&format!("{node}/receive/tcp/json"), Health::Done));
        eventing.round(&mut first);
        let mine: Vec<_> = first.event_subscriptions().cloned().collect();
        assert_eq!(mine.len(), 2, "operations and on-call");
        assert!(mine.iter().all(|held| held.node == node));
        let on_call = mine
            .iter()
            .find(|held| held.subscriber == "on-call")
            .expect("on-call");
        assert_eq!(on_call.party, super::on_call().party_id.to_string());
        assert!(mine.iter().any(|held| held.subscriber == "operations"));
        assert_eq!(on_call.action, "every Event ending failure");
        assert_eq!(on_call.delivered, 1, "the failure was raised and drained");

        eventing
            .act(&on_call.id.to_string(), Act::Pause, "ilian")
            .expect("paused");
        let mut second = Snapshot::new();
        second.record_health(record(&format!("{node}/receive/tcp/json"), Health::Done));
        second.record_health(record(&format!("{node}/receive/tcp/xml"), Health::Done));
        eventing.round(&mut second);
        let held = second
            .event_subscriptions()
            .find(|held| held.id == on_call.id)
            .expect("still there");
        assert_eq!(held.state, PauseState::Paused);
        assert_eq!(
            (held.queued, held.delivered),
            (1, 1),
            "queued, not handed over"
        );

        eventing
            .act(&on_call.id.to_string(), Act::Remove, "ilian")
            .expect("removed");
        assert!(eventing.act("seven", Act::Pause, "ilian").is_err());
        let mut third = Snapshot::new();
        eventing.round(&mut third);
        assert!(
            third
                .event_subscriptions()
                .all(|held| held.id != on_call.id)
        );
        assert!(eventing.drained.is_none(), "the node let go of it");
    }
}
