//! A node's Subscriptions (ADR-0013, amendments 2026-09-30 and 2026-10-03),
//! taken up from the TOML configuration it binds, as a real node takes them.
//!
//! The Playground's `RoundTrip` test runs the Xmip Applications held as
//! sections of the Playground's cluster configuration,
//! `configuration/xmip.toml` — business flows an ERP runs with its Parties —
//! each Subscription picking up a family of content contracts and routing it
//! to a Send Port the send stage serves (ADR-0028, amendment 2026-10-13).
//! A node that declared process writes the cluster's file with the run's
//! names and the bindings that place every Application on the run's nodes,
//! read from the sections, never named here; it slices its own configuration from it as
//! deployment does (`configure::slice`), writes that into
//! `<shared>/configuration`, reads it back through the runtime's own reading
//! (`runtime::start::read`, the execution tree), and
//! routes every pair its process stage handed on through them: each pair is
//! published into the node's own Ledger as a real receive publishes — its
//! bytes a Stream in chunks, a Message, a Journey per matched Subscription
//! (`runtime::ledger::publish`) — and the runtime's pickup decides, as it
//! does on a running node, whether a pair is picked up — handed to the send
//! stage — or held, its Journey kept in its Subscription's queue in the
//! Ledger, because the Subscription is paused. The node is its own embedded
//! Storage node, `<shared>/storage/<node>`: `RocksDB` for the Ledger,
//! `SQLite` for the administration database its pause is kept in, sealed
//! under a key-encryption key of the machine's key store in
//! `<shared>/keys/<node>` — so a pause, and what it holds, survives the
//! node's restart. A resume lets go of what was held, oldest first; a pair
//! handed on is released, and one that could not be is kept, Failed, until
//! a resume tries it again. Nothing here is a Subscription of its own: the
//! rows an operator sees are the runtime's, of the configuration's.

mod configured;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use codec::cursor::Cursor;
use codec::field::{read_text, text};
use context::MessageContext;
use journey::{Journey, JourneyState};
use message::{Message, MessageSection, MessageTreatment};
use observe::{Act, Snapshot};
use persist::storage::{Embedded, XmipStorage};
use route::{Promoted, publish};
use runtime::configured_subscription::ConfiguredSubscription;
use runtime::dead_message::Unmatched;
use runtime::ledger::{CHUNK, Publisher, write_stream};
use runtime::pickup::{Pickup, Released};
use runtime::send_step::SendStep;
use runtime::sending::Sends;
use secret::{Held, KekName};
use xaudit::origin::Origin;
use xaudit::program_audit::ProgramAudit;
use xcore::{IdGenerator, MessageId, SectionId, StreamId, SystemClock, UuidV7Generator};

use crate::handoff::Handoff;
use crate::ledger_stream::read_whole;
use crate::roster::Roster;
use crate::schedule::CONTRACTS;

/// The Playground's cluster configuration, as this repository ships it: the
/// `RoundTrip` Application's section.
pub const CLUSTER: &str = include_str!("../configuration/xmip.toml");

/// A node's Subscriptions, the runtime's pickup that holds them, and the
/// node's own Storage node.
pub struct Subscribing {
    /// Each Subscription's id and the Receive Location a pair it picks up
    /// is audited as published at: the first of its Application's.
    published_at: Vec<(String, String)>,
    pickup: Arc<Pickup>,
    storage: Arc<dyn XmipStorage>,
    origin: Origin,
    subscriptions: Vec<route::Subscription>,
    /// It sends nothing itself: the send stage is a node of its own.
    send: SendStep,
    sends: Sends,
}

/// A held pair let go of to hand on, and the Journey it is.
pub struct Picked {
    pub handoff: Handoff,
    released: Released,
    journey: Journey,
}

impl Subscribing {
    /// Take up the Subscriptions of the node called `name` at `node`, which
    /// binds the `RoundTrip` Application with its Receive Location on the
    /// node the roster says receives and its Send Port on the one that
    /// sends; its Storage node kept under `shared`, its acts audited in
    /// `audit`.
    ///
    /// # Errors
    /// The configuration did not read or bind, or the Storage node did not
    /// open or could not be read: the reason, for the node to audit.
    pub fn start(
        node: &str,
        name: &str,
        roster: &Roster,
        shared: &Path,
        audit: &ProgramAudit,
    ) -> Result<Self, String> {
        let designed = configure::applications(CLUSTER)?;
        let path = configured::configured(node, name, roster, shared, &designed)?;
        let text = path.display().to_string();
        let (document, applications, files) =
            runtime::start::read(&text).map_err(|unread| unread.reason)?;
        let declared = configure::Declarations::new();
        let (tree, _) =
            runtime::execution_tree::build_execution_tree(document, &applications, &declared)
                .map_err(|report| report.errors.join("; "))?;
        let configured = ConfiguredSubscription::of(&applications, &files);
        let storage = storage(shared, name)?;
        let pickup = Pickup::open(node, configured, Arc::clone(&storage), Some(audit.clone()))?;
        let cluster = runtime::running::publication::cluster_location(&tree.service.cluster_name);
        let send = SendStep::new((&cluster, node), Arc::clone(&storage), &tree.tuning, None);
        let published_at = designed
            .iter()
            .filter_map(|application| {
                let first = application.receive_locations.first()?;
                Some(
                    application
                        .subscriptions
                        .iter()
                        .map(|subscription| (subscription.id.clone(), first.name.clone())),
                )
            })
            .flatten()
            .collect();
        Ok(Self {
            published_at,
            pickup,
            storage,
            origin: runtime::running::origin(Some(audit), node),
            subscriptions: tree.subscriptions,
            send,
            sends: Sends::default(),
        })
    }

    /// Route `handoff` as the node publishes it: true when a Subscription
    /// picked it up and it goes on to the send stage now, false when a
    /// paused one holds it — or none wanted it.
    ///
    /// # Errors
    /// Its Publication was not taken, in words: nothing of it is held, and
    /// it is not handed on.
    pub fn route(&self, handoff: &Handoff) -> Result<bool, String> {
        let promoted = Promoted::new()
            .set("file.name", handoff.contract.file_name(handoff.round))
            .set("Transport", handoff.transport.as_str());
        let routing = publish(&promoted, &self.subscriptions);
        let picked = routing.evaluations.iter().find(|one| one.matched());
        let location = picked
            .and_then(|one| {
                self.published_at
                    .iter()
                    .find(|(id, _)| *id == one.subscription_id)
            })
            .or_else(|| self.published_at.first())
            .map_or("", |(_, location)| location.as_str());
        let ids = UuidV7Generator;
        let stream = StreamId::new(ids.next_u128());
        let mut bytes = handoff.bytes.as_slice();
        let kept = write_stream(self.storage.as_ref(), stream, &mut bytes, CHUNK)?;
        let section = MessageSection {
            section_id: SectionId::new(ids.next_u128()),
            name: None,
            stream: kept.stream(&self.storage, None),
            contract: None,
        };
        let message = Message::received(
            MessageId::new(ids.next_u128()),
            vec![section],
            MessageContext::new(),
            MessageTreatment::default(),
        );
        let through = Publisher {
            storage: self.storage.as_ref(),
            ids: &ids,
            clock: &SystemClock,
            origin: &self.origin,
            send: &self.send,
            sends: &self.sends,
        };
        let published = runtime::ledger::publish(
            &through,
            &self.pickup,
            location,
            &message,
            &routing,
            &Unmatched {
                promoted: Some(&promoted),
                facts: None,
            },
            || said(handoff),
        )?;
        // Some Journey no paused Subscription holds goes on to be sent.
        let departing = published.journeys.len() > published.holding.holds().len();
        Ok(departing)
    }

    /// What a resume let go of, oldest first, as the pairs to hand on. One
    /// that cannot be read back now is read again later, and so is every
    /// one after it its Subscription let go of.
    #[must_use]
    pub fn released(&self) -> Vec<Picked> {
        let mut picked = Vec::new();
        let mut stalled: Vec<String> = Vec::new();
        for released in self.pickup.released(Duration::ZERO, usize::MAX) {
            if stalled.contains(&released.subscription) {
                self.pickup.again(&released);
                continue;
            }
            match self.read_back(&released) {
                Ok(Some((handoff, journey))) => picked.push(Picked {
                    handoff,
                    released,
                    journey,
                }),
                Ok(None) => {}
                Err(_) => {
                    self.pickup.again(&released);
                    stalled.push(released.subscription);
                }
            }
        }
        picked
    }

    /// The node handed `picked` on, or could not, `handed` says: released
    /// from its queue where it was, kept Failed where not. A write the
    /// Storage node did not take leaves it to be read again.
    pub fn done(&self, picked: Picked, handed: &Result<(), String>) {
        let Picked {
            released,
            mut journey,
            ..
        } = picked;
        let written = if handed.is_ok() {
            journey.state = JourneyState::Completed;
            self.pickup.delivered(&released, &journey)
        } else {
            journey.state = JourneyState::Failed;
            self.pickup.kept(&released, &journey)
        };
        if written.is_err() {
            self.pickup.again(&released);
        }
    }

    /// Pause or resume the Subscription called `name`, by `who`.
    ///
    /// # Errors
    /// The runtime's refusal, in its words.
    pub fn act(&self, name: &str, act: Act, who: &str) -> Result<String, String> {
        self.pickup.act(name, act, who)
    }

    /// Replay the Message `message` from the node's Dead Message Queue, by
    /// `who`: what it opens is held and picked up as a resume's is.
    ///
    /// # Errors
    /// The runtime's refusal, in its words.
    pub fn replay(&self, message: &str, who: &str) -> Result<String, String> {
        self.pickup.replay(message, who)
    }

    /// Record every Subscription, and the oldest of the Dead Message Queue,
    /// as the node publishes them.
    pub fn round(&self, snapshot: &mut Snapshot) {
        for subscription in self.pickup.standing() {
            snapshot.record_subscription(subscription);
        }
        if let Ok((_, dead)) = self.pickup.dead_messages(runtime::pickup::PUBLISHED) {
            for entry in dead {
                snapshot.record_dead_message(entry);
            }
        }
    }

    /// The pair `released` is and its Journey, read back from the Ledger;
    /// none where it is not a pair, which is passed over. A Failed one is
    /// let go of again only by a resume: the operator's retry.
    fn read_back(&self, released: &Released) -> Result<Option<(Handoff, Journey)>, String> {
        let id = released.held.hold.journey;
        let record = self.storage.read_journey(id).map_err(|e| e.to_string())?;
        let journey = record.and_then(|record| Journey::from_record(&record.body).ok());
        let Some(journey) = journey else {
            self.pickup
                .unreadable(released, "its Journey does not read");
            return Ok(None);
        };
        let Some(held) = journey.messages().last().copied() else {
            self.pickup
                .unreadable(released, "its Journey holds no Message");
            return Ok(None);
        };
        let bytes = read_whole(&self.storage, held.stream_id)?;
        let Some(handoff) = pair(&released.held.hold.body, bytes) else {
            self.pickup
                .unreadable(released, "it is not a pair this node held");
            return Ok(None);
        };
        Ok(Some((handoff, journey)))
    }
}

/// The node's own embedded Storage node: `RocksDB` for the Ledger and
/// `SQLite` for the administration and the audit databases, sealed under
/// the machine's key store — DPAPI on Windows, a private file elsewhere.
fn storage(shared: &Path, name: &str) -> Result<Arc<dyn XmipStorage>, String> {
    let keys = shared.join("keys").join(name);
    #[cfg(windows)]
    let keys = Held::new(secret_dpapi::Dpapi::new(keys));
    #[cfg(not(windows))]
    let keys = Held::new(secret_file::KeyFile::new(keys));
    let kek = KekName::new(runtime::storage::KEK).map_err(|error| error.to_string())?;
    let directory = shared.join("storage").join(name);
    std::fs::create_dir_all(&directory).map_err(|error| error.to_string())?;
    let opened = || -> Result<_, persist::PersistError> {
        let ledger = rocksdb::RocksDb::open(&directory.join("runtime"))?;
        let [_, administration, audit] = runtime::storage::FILES;
        let administration = sqlite::Sqlite::open(&directory.join(administration))?;
        let audit = sqlite::Sqlite::open(&directory.join(audit))?;
        Embedded::open(ledger, administration, audit, &keys, &kek)
    };
    Ok(Arc::new(opened().map_err(|error| error.to_string())?))
}

/// What the node keeps beside a held pair to hand it on once it is
/// resumed: what the pair is, its bytes being its Stream's.
fn said(handoff: &Handoff) -> Vec<u8> {
    let mut out = Vec::new();
    for field in [
        handoff.transport.as_str(),
        handoff.contract.name(),
        &handoff.round.to_string(),
        &handoff.from,
    ] {
        text(&mut out, field);
    }
    out
}

/// The pair a held one was, its Stream's `bytes` with what was said of it.
fn pair(said: &[u8], bytes: Vec<u8>) -> Option<Handoff> {
    let mut cursor = Cursor::new(said);
    let mut next = || read_text(&mut cursor).ok();
    let (transport, contract, round, from) = (next()?, next()?, next()?, next()?);
    Some(Handoff {
        transport,
        contract: CONTRACTS
            .into_iter()
            .find(|known| known.name() == contract)?,
        round: round.parse().ok()?,
        from,
        bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::support::{path, path_roster, scratch, test_cluster};
    use crate::verdict::Contract;

    /// The test cluster's processing node, which takes up the Subscriptions.
    fn started(shared: &Path, audit: &ProgramAudit) -> Subscribing {
        let cluster = test_cluster();
        let [_, processing, _] = path(&cluster);
        let node = format!("{}/node/{processing}", cluster.scope());
        let roster = Roster::parse(&path_roster(&cluster)).expect("a roster");
        Subscribing::start(&node, processing, &roster, shared, audit).expect("started")
    }

    /// The Applications the shipped configuration holds.
    fn designed() -> Vec<configure::XmipApplication> {
        configure::applications(CLUSTER).expect("the shipped configuration reads")
    }

    /// The id of the Subscription the shipped configuration picks up
    /// `contract` by, and the Send Port it routes to.
    fn routing(contract: Contract) -> (String, String) {
        let promoted = Promoted::new().set("file.name", contract.file_name(0));
        designed()
            .into_iter()
            .flat_map(|application| application.subscriptions)
            .find(|subscription| subscription.filter.evaluate(&promoted).holds())
            .map(|subscription| (subscription.id, subscription.destination.name().to_string()))
            .expect("every contract is picked up")
    }

    #[test]
    fn every_contract_is_picked_up_by_one_subscription_of_the_configuration() {
        let subscriptions: Vec<_> = designed()
            .into_iter()
            .flat_map(|application| application.subscriptions)
            .collect();
        for contract in CONTRACTS {
            let promoted = Promoted::new().set("file.name", contract.file_name(0));
            let picking = subscriptions
                .iter()
                .filter(|subscription| subscription.filter.evaluate(&promoted).holds())
                .count();
            assert_eq!(picking, 1, "{}", contract.name());
        }
        for application in designed() {
            assert!(
                application.problems().is_empty(),
                "{:?}",
                application.problems()
            );
        }
    }

    fn pair(contract: Contract, round: u64) -> Handoff {
        Handoff {
            transport: "tcp".to_string(),
            contract,
            round,
            from: test_cluster().with_role("receiving").name.clone(),
            bytes: contract.payload(),
        }
    }

    #[test]
    fn the_configured_subscriptions_route_hold_while_paused_and_survive_a_restart() {
        let shared = scratch("subscribing");
        let audit = ProgramAudit::new("xmip-playground-subscribing-test", Some(&shared));
        let subscribing = started(&shared, &audit);

        let mut snapshot = Snapshot::new();
        subscribing.round(&mut snapshot);
        let names: Vec<String> = snapshot.subscriptions().map(|s| s.name.clone()).collect();
        let mut configured: Vec<String> = designed()
            .iter()
            .flat_map(|application| &application.subscriptions)
            .map(|subscription| subscription.id.clone())
            .collect();
        configured.sort();
        assert_eq!(names, configured, "every Application's, by id");
        // The Subscription that picks up JSON, which picks up XML too, and
        // the Send Port it routes to: read from the configuration.
        let (id, port) = routing(Contract::Json);
        assert_eq!(routing(Contract::Xml).0, id);
        assert_ne!(routing(Contract::Csv).0, id);
        let structured = snapshot
            .subscriptions()
            .find(|s| s.name == id)
            .expect("configured");
        assert!(
            structured.file.ends_with(".node.toml"),
            "{}",
            structured.file
        );
        assert!(
            structured
                .configuration
                .starts_with("[[xmip_applications.subscriptions]]\n")
                && structured.configuration.contains(&format!("id = \"{id}\"")),
            "{}",
            structured.configuration
        );
        assert!(
            structured.configuration.contains(&port)
                && structured.configuration.contains("'json-%'"),
            "{}",
            structured.configuration
        );

        let routed = |subscribing: &Subscribing, contract, round| {
            subscribing
                .route(&pair(contract, round))
                .expect("published")
        };
        assert!(routed(&subscribing, Contract::Json, 1), "picked up");
        subscribing.act(&id, Act::Pause, "ilian").expect("paused");
        assert!(!routed(&subscribing, Contract::Json, 2), "held");
        assert!(!routed(&subscribing, Contract::Xml, 3), "held");
        assert!(
            routed(&subscribing, Contract::Csv, 4),
            "another Subscription's"
        );
        drop(subscribing);

        let again = started(&shared, &audit);
        let mut snapshot = Snapshot::new();
        again.round(&mut snapshot);
        let held = snapshot
            .subscriptions()
            .find(|s| s.name == id)
            .expect("configured");
        assert_eq!((held.state, held.held), (observe::PauseState::Paused, 2));

        again.act(&id, Act::Resume, "ilian").expect("resumed");
        let released = again.released();
        let rounds: Vec<u64> = released.iter().map(|one| one.handoff.round).collect();
        assert_eq!(rounds, [2, 3], "oldest first, nothing lost");
        assert_eq!(released[1].handoff.bytes, Contract::Xml.payload());
        let mut released = released.into_iter();
        let first = released.next().expect("round 2");
        again.done(first, &Err("the send stage is gone".to_string()));
        for one in released {
            again.done(one, &Ok(()));
        }
        let mut snapshot = Snapshot::new();
        again.round(&mut snapshot);
        let held = snapshot
            .subscriptions()
            .find(|s| s.name == id)
            .expect("configured");
        assert_eq!(held.held, 1, "what was not handed on stays held");
        assert!(again.released().is_empty(), "Failed: not tried by itself");
        assert!(again.act(&id, Act::Remove, "ilian").is_err());
        drop(again);
        let _ = std::fs::remove_dir_all(&shared);
    }
}
