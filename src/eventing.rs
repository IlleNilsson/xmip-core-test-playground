//! What a node raises and who hears it: the Events of its stages, the
//! subscriptions its hub holds, and the acts an operator leaves for them
//! (ADR-0065, amendment 2026-09-29).
//!
//! Every node subscribes two Parties in its own process — declared here, by
//! name, as [`operations`] and [`on_call`] — through the event
//! crate's hub and nothing of its own: **operations** hears every Event on
//! the node and is called back ([`xevent::listener::Listener`]), and
//! **on-call** hears the failures and is drained each round. Each round the
//! node raises one Event per stage it serves whose standing changed — a
//! failure when more of its pairs fail than the round before, a success
//! when fewer — so a steady node is quiet and a stirring one is heard. Each
//! round it takes the orders an operator left for it under
//! `<shared>/orders` ([`xevent::order::Order`]) and applies them to its hub,
//! and records what its hub holds in its snapshot, which the cluster and
//! the roll publish as the node's subscriptions. The subscriptions are real
//! ones: an operator who pauses one sees its queue fill, and one who removes
//! one sees it gone.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use node::{Capability, Stage};
use observe::{Health, Scope, Snapshot};
use party::{Party, PartyKind};
use xaudit::program_audit::ProgramAudit;
use xcore::PartyId;
use xevent::Event;
use xevent::filter::Filter;
use xevent::hub::{Hub, Subscription};
use xevent::listener::Listener;
use xevent::order::Order;
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

/// Where a cluster's nodes take an operator's orders, beneath the directory
/// they share: the roll says so in its snapshot and every node looks there.
#[must_use]
pub fn orders(shared: &Path) -> PathBuf {
    shared.join("orders")
}

/// How many Events each queue holds: small, so a paused one is seen to fill.
const CAPACITY: usize = 64;

/// A node's eventing.
pub struct Eventing {
    node: String,
    orders: PathBuf,
    stages: Vec<Stage>,
    audit: ProgramAudit,
    _listening: Option<Listener>,
    drained: Option<Subscription>,
    failing: BTreeMap<Stage, usize>,
}

impl Eventing {
    /// The node at `node` subscribes its two Parties and takes its orders
    /// from `<shared>/orders`. A subscription the hub refused is audited as
    /// the failure to `subscribe`, and the node runs on without it.
    #[must_use]
    pub fn start(node: &str, shared: &Path, capability: &Capability, audit: &ProgramAudit) -> Self {
        let hub = Hub::process();
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
            orders: orders(shared),
            stages: capability.features().to_vec(),
            audit: audit.clone(),
            _listening: listening,
            drained,
            failing: BTreeMap::new(),
        }
    }

    /// One round: take the orders left, raise what changed, drain on-call,
    /// and record what the hub holds in `snapshot`.
    pub fn round(&mut self, snapshot: &mut Snapshot) {
        let hub = Hub::process();
        for taken in Order::take(&self.orders, &self.node) {
            let applied = taken
                .map_err(|problem| format!("an order no node can take: {problem}"))
                .and_then(|order| {
                    hub.act(order.id, order.act, &order.who)
                        .map_err(|refused| refused.to_string())
                });
            if let Err(problem) = applied {
                process_audit::fail(&self.audit, "order", &problem);
            }
        }

        for stage in self.stages.clone() {
            self.raise(hub, snapshot, stage);
        }

        if let Some(drained) = &self.drained {
            let _ = drained.next(Duration::ZERO, CAPACITY);
        }
        if self.drained.as_ref().is_some_and(Subscription::is_closed) {
            self.drained = None;
        }

        for subscription in hub.standing(&self.node) {
            snapshot.record_subscription(subscription);
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
    use observe::{HealthRecord, SubscriptionState};
    use xevent::act::Act;

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
    fn a_node_publishes_its_subscriptions_and_applies_the_orders_left_for_it() {
        let shared = scratch("eventing");
        let node = "xmip:///CT/node/eventing-test";
        let audit = ProgramAudit::new("xmip-playground-eventing-test", Some(&shared));
        let mut eventing =
            Eventing::start(node, &shared, &Capability::of(&[Stage::Receive]), &audit);

        let mut first = Snapshot::new();
        first.record_health(record(&format!("{node}/receive/tcp/json"), Health::Done));
        eventing.round(&mut first);
        let mine: Vec<_> = first.subscriptions().cloned().collect();
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

        Order {
            node: node.to_string(),
            id: on_call.id,
            act: Act::Pause,
            who: "ilian".to_string(),
        }
        .leave(&orders(&shared))
        .expect("left");
        let mut second = Snapshot::new();
        second.record_health(record(&format!("{node}/receive/tcp/json"), Health::Done));
        second.record_health(record(&format!("{node}/receive/tcp/xml"), Health::Done));
        eventing.round(&mut second);
        let held = second
            .subscriptions()
            .find(|held| held.id == on_call.id)
            .expect("still there");
        assert_eq!(held.state, SubscriptionState::Paused);
        assert_eq!(
            (held.queued, held.delivered),
            (1, 1),
            "queued, not handed over"
        );

        Order {
            node: node.to_string(),
            id: on_call.id,
            act: Act::Remove,
            who: "ilian".to_string(),
        }
        .leave(&orders(&shared))
        .expect("left");
        let mut third = Snapshot::new();
        eventing.round(&mut third);
        assert!(third.subscriptions().all(|held| held.id != on_call.id));
        assert!(eventing.drained.is_none(), "the node let go of it");
    }
}
