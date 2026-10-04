//! A node's Subscriptions (ADR-0013, amendments 2026-09-30 and 2026-10-03),
//! taken up from the TOML configuration it binds, as a real node takes them.
//!
//! The Playground's `RoundTrip` test is an Xmip Application, a section of
//! the Playground's cluster configuration, `configuration/xmip.toml`: four
//! Subscriptions, one per family of content contracts, each routing to the
//! Send Port the send stage serves. A node that declared process writes the
//! cluster's file with the run's names and the bindings that place the
//! Application on the run's nodes, slices its own configuration from it as
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

use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use codec::cursor::Cursor;
use codec::field::{read_text, text};
use context::MessageContext;
use journey::{Journey, JourneyState};
use message::{Message, MessageSection, MessageTreatment};
use node::Stage;
use observe::{Act, Snapshot};
use persist::storage::{Embedded, XmipStorage};
use route::{Promoted, publish};
use runtime::configured_subscription::ConfiguredSubscription;
use runtime::dead_message::Unmatched;
use runtime::ledger::{CHUNK, Chunks, Publisher, write_stream};
use runtime::pickup::{Pickup, Released};
use runtime::send_step::SendStep;
use runtime::sending::Sends;
use secret::{Held, KekName};
use stream::Content;
use xaudit::origin::Origin;
use xaudit::program_audit::ProgramAudit;
use xcore::{IdGenerator, MessageId, SectionId, StreamId, SystemClock, UuidV7Generator};

use crate::handoff::Handoff;
use crate::roster::Roster;
use crate::schedule::CONTRACTS;

/// The Playground's cluster configuration, as this repository ships it: the
/// `RoundTrip` Application's section.
pub const CLUSTER: &str = include_str!("../configuration/xmip.toml");

/// The Receive Location the Application binds, which a pair's Publication
/// is audited as published at.
const LOCATION: &str = "RoundTripIn";

/// A node's Subscriptions, the runtime's pickup that holds them, and the
/// node's own Storage node.
pub struct Subscribing {
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
        let path = configured(node, name, roster, shared)?;
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
        Ok(Self {
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
            .set("MessageType", handoff.contract.name())
            .set("Transport", handoff.transport.as_str());
        let routing = publish(&promoted, &self.subscriptions);
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
            LOCATION,
            &message,
            &routing,
            &Unmatched {
                promoted: Some(&promoted),
                facts: None,
            },
            || said(handoff),
        )?;
        let departing = published.holding.departing(&routing, &published.journeys);
        Ok(!departing.is_empty())
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
        let mut bytes = Vec::new();
        Chunks::of(Arc::clone(&self.storage), held.stream_id)
            .reader()
            .and_then(|mut reader| reader.read_to_end(&mut bytes))
            .map_err(|error| error.to_string())?;
        let Some(handoff) = pair(&released.held.hold.body, bytes) else {
            self.pickup
                .unreadable(released, "it is not a pair this node held");
            return Ok(None);
        };
        Ok(Some((handoff, journey)))
    }
}

/// Write this node's configuration, sliced from the Playground's cluster
/// configuration with the run's names and the bindings that place the
/// Application on the run's nodes, into `<shared>/configuration`, and say
/// where it is.
fn configured(node: &str, name: &str, roster: &Roster, shared: &Path) -> Result<PathBuf, String> {
    let cluster = observe::Scope::new(node)
        .segments()
        .next()
        .unwrap_or_default();
    let at = |stage: Stage| roster.with(stage).first().map(ToString::to_string);
    let receives = at(Stage::Receive).ok_or("no node declares receive")?;
    let sends = at(Stage::Send).ok_or("no node declares send")?;
    let inbox = |to: &str, stage: Stage| {
        shared
            .join("handoff")
            .join(to)
            .join(stage.name())
            .display()
            .to_string()
    };
    let directory = shared.join("configuration");
    std::fs::create_dir_all(&directory).map_err(|error| error.to_string())?;
    let written = |file: &Path, text: &str| {
        std::fs::write(file, text).map_err(|error| format!("{}: {error}", file.display()))
    };
    let file = format!(
        r#"[service]
name = "xmip-playground"
cluster_name = "{cluster}"

[[applications]]
name = "RoundTrip"

[[applications.receive_locations]]
name = "RoundTripIn"
node = "{receives}"
start = true
transport = "xmip-core-transport-file"
address = '{inbox}'

[[applications.send_ports]]
name = "RoundTripOut"
node = "{sends}"
start = true
transport = "xmip-core-transport-file"
address = '{outbox}'

{CLUSTER}
[nodes.{name}.service]
name = "xmip-playground-{name}"
"#,
        inbox = inbox(&receives, Stage::Process),
        outbox = inbox(&sends, Stage::Send),
    );
    let text = configure::slice(&file, name)?;
    let path = directory.join(format!("{name}.node.toml"));
    written(&path, &text)?;
    Ok(path)
}

/// The node's own embedded Storage node: `RocksDB` for the Ledger and
/// `SQLite` for the administration database, sealed under the machine's key
/// store — DPAPI on Windows, a private file elsewhere.
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
        let administration = sqlite::Sqlite::open(&directory.join("administration.sqlite"))?;
        Embedded::open(ledger, administration, &keys, &kek)
    };
    let node = opened().map_err(|error| error.to_string())?;
    Ok(Arc::new(node))
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
        assert_eq!(names, ["edi", "flat", "schema", "structured"]);
        let structured = snapshot
            .subscriptions()
            .find(|s| s.name == "structured")
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
                && structured.configuration.contains("id = \"structured\""),
            "{}",
            structured.configuration
        );
        assert!(
            structured.configuration.contains("RoundTripOut")
                && structured.configuration.contains("'schematron'"),
            "{}",
            structured.configuration
        );

        let routed = |subscribing: &Subscribing, contract, round| {
            subscribing
                .route(&pair(contract, round))
                .expect("published")
        };
        assert!(routed(&subscribing, Contract::Json, 1), "picked up");
        subscribing
            .act("structured", Act::Pause, "ilian")
            .expect("paused");
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
            .find(|s| s.name == "structured")
            .expect("configured");
        assert_eq!((held.state, held.held), (observe::PauseState::Paused, 2));

        again
            .act("structured", Act::Resume, "ilian")
            .expect("resumed");
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
            .find(|s| s.name == "structured")
            .expect("configured");
        assert_eq!(held.held, 1, "what was not handed on stays held");
        assert!(again.released().is_empty(), "Failed: not tried by itself");
        assert!(again.act("structured", Act::Remove, "ilian").is_err());
        drop(again);
        let _ = std::fs::remove_dir_all(&shared);
    }
}
