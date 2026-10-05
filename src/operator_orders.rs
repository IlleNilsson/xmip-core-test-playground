//! The orders an operator leaves for a node the surfaces reach through its
//! publication only: an act on one of its Event subscriptions (ADR-0065,
//! amendment 2026-09-29), on one of its Subscriptions (ADR-0013, amendment
//! 2026-09-30), or a Replay from its Dead Message Queue (ADR-0052,
//! amendment 2026-10-01).
//!
//! The roll says in its publication that the cluster's nodes take orders
//! under `<shared>/orders`; a surface leaves each there as `observe::Order`
//! writes it, and every node takes its own at each round and applies it to
//! what it names — its hub's Event subscription ([`Eventing::act`]), its
//! Subscription ([`Subscribing::act`]) or a Message its Dead Message Queue
//! keeps ([`Subscribing::replay`]). An order a node cannot apply is
//! audited as the failure to `order`, in the words that refused it.

use std::path::{Path, PathBuf};

use observe::{Noun, Order};
use xaudit::program_audit::ProgramAudit;

use crate::eventing::Eventing;
use crate::process_audit;
use crate::subscribing::Subscribing;

/// Where a cluster's nodes take an operator's orders, beneath the directory
/// they share: the roll says so in its snapshot and every node looks there.
#[must_use]
pub fn orders(shared: &Path) -> PathBuf {
    shared.join("orders")
}

/// Take every order left for the node at `node` under `shared`, oldest
/// first, and apply each to what it names.
pub fn take(
    shared: &Path,
    node: &str,
    eventing: &Eventing,
    subscribing: Option<&Subscribing>,
    audit: &ProgramAudit,
) {
    for taken in Order::take(&orders(shared), node) {
        let applied = taken
            .map_err(|problem| format!("an order no node can take: {problem}"))
            .and_then(|order| match (order.noun, subscribing) {
                (Noun::EventSubscription, _) => eventing.act(&order.target, order.act, &order.who),
                (Noun::Subscription, Some(subscribing)) => {
                    subscribing.act(&order.target, order.act, &order.who)
                }
                (Noun::Subscription, None) => Err(format!(
                    "REFUSED: {node} routes by no Subscription; the Subscription '{}' is \
                     configured on another node",
                    order.target
                )),
                (Noun::DeadMessage, Some(subscribing)) => {
                    subscribing.replay(&order.target, &order.who)
                }
                (Noun::Journey, _) => Err(format!(
                    "REFUSED: {node} sends no Journey from the Ledger, so the Journey {} \
                     is retried or dismissed on the node that sends it",
                    order.target
                )),
                (Noun::DeadMessage, None) => Err(format!(
                    "REFUSED: {node} routes nothing, so its Dead Message Queue keeps no \
                     Message {}",
                    order.target
                )),
            });
        if let Err(problem) = applied {
            process_audit::fail(audit, "order", &problem);
        }
    }
}
