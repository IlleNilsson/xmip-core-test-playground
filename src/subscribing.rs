//! A node's Subscriptions (ADR-0013, amendment 2026-09-30), taken up from
//! the TOML configuration it binds, as a real node takes them.
//!
//! The Playground's `RoundTrip` test is an Xmip Application,
//! `configuration/round-trip.application.toml`: four Subscriptions, one per
//! family of content contracts, each routing to the Send Port the send stage
//! serves. A node that declared process writes it, and a node configuration
//! binding it, into `<shared>/configuration`, reads both back through the
//! runtime's own reading (`runtime::start::read`, the execution tree), and
//! routes every pair its process stage handed on through them: the
//! runtime's pickup decides, as it does on a running node, whether a pair is
//! picked up — handed to the send stage — or held, because its Subscription
//! is paused. What is held is kept in the node's runtime store,
//! `<shared>/store/<node>`: persist's, over `RocksDB`, sealed under a
//! key-encryption key of the machine's key store in `<shared>/keys/<node>` —
//! so a pause, and what it holds, survives the node's restart. A resume
//! lets go of what was held, oldest first, and the node hands each on.
//! Nothing here is a Subscription of its own: the rows an operator sees are
//! the runtime's, of the configuration's.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use node::Stage;
use observe::{Act, Snapshot};
use persist::{EncryptedStore, HeldMessage};
use route::{Promoted, publish};
use runtime::configured_subscription::ConfiguredSubscription;
use runtime::pickup::{Pickup, Released, Store};
use secret::{Held, KekName};
use xaudit::program_audit::ProgramAudit;

use crate::handoff::Handoff;
use crate::roster::Roster;
use crate::schedule::CONTRACTS;

/// The Application, as this repository ships it.
pub const APPLICATION: &str = include_str!("../configuration/round-trip.application.toml");

/// The file it is written to beside each node's configuration.
const APPLICATION_FILE: &str = "round-trip.application.toml";

/// A node's Subscriptions, and the runtime's pickup that holds them.
pub struct Subscribing {
    pickup: Arc<Pickup>,
    subscriptions: Vec<route::Subscription>,
}

impl Subscribing {
    /// Take up the Subscriptions of the node called `name` at `node`, which
    /// binds the `RoundTrip` Application with its Receive Location on the
    /// node the roster says receives and its Send Port on the one that
    /// sends; its standing kept under `shared`, its acts audited in `audit`.
    ///
    /// # Errors
    /// The configuration did not read or bind, or the store did not open:
    /// the reason, for the node to audit.
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
        let store = store(shared, name)?;
        let pickup = Pickup::open(node, configured, Some(store), Some(audit.clone()))?;
        Ok(Self {
            pickup,
            subscriptions: tree.subscriptions,
        })
    }

    /// Route `handoff` as the node publishes it: true when a Subscription
    /// picked it up and it goes on to the send stage now, false when a
    /// paused one holds it — or none wanted it.
    #[must_use]
    pub fn route(&self, handoff: &Handoff) -> bool {
        let promoted = Promoted::new()
            .set("MessageType", handoff.contract.name())
            .set("Transport", handoff.transport.as_str());
        let routing = publish(&promoted, &self.subscriptions);
        let picked = self.pickup.route(&routing, &|| held(handoff));
        !picked.destinations().is_empty()
    }

    /// What a resume let go of, oldest first, as the pairs to hand on.
    #[must_use]
    pub fn released(&self) -> Vec<(Handoff, Released)> {
        self.pickup
            .released(Duration::ZERO, usize::MAX)
            .into_iter()
            .filter_map(|released| Some((pair(&released.held)?, released)))
            .collect()
    }

    /// The node handed `released` on.
    pub fn picked_up(&self, released: &Released) {
        self.pickup.picked_up(released);
    }

    /// Pause or resume the Subscription called `name`, by `who`.
    ///
    /// # Errors
    /// The runtime's refusal, in its words.
    pub fn act(&self, name: &str, act: Act, who: &str) -> Result<String, String> {
        self.pickup.act(name, act, who)
    }

    /// Record every Subscription as the node publishes it.
    pub fn round(&self, snapshot: &mut Snapshot) {
        for subscription in self.pickup.standing() {
            snapshot.record_subscription(subscription);
        }
    }
}

/// Write the Application and this node's configuration binding it into
/// `<shared>/configuration`, and say where the configuration is.
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
    written(&directory.join(APPLICATION_FILE), APPLICATION)?;
    let text = format!(
        "# Node {name} of the Playground's cluster {cluster}, binding its RoundTrip\n\
         # Application as a real node binds one (ADR-0064).\n\
         [service]\nname = \"xmip-playground-{name}\"\ncluster_name = \"{cluster}\"\n\
         node_name = \"{name}\"\n\n\
         [[applications]]\nname = \"RoundTrip\"\ndocument = \"{APPLICATION_FILE}\"\n\n\
         [[applications.receive_locations]]\nname = \"RoundTripIn\"\nnode = \"{receives}\"\n\
         start = true\ntransport = \"xmip-core-transport-file\"\naddress = '{}'\n\n\
         [[applications.send_ports]]\nname = \"RoundTripOut\"\nnode = \"{sends}\"\n\
         start = true\ntransport = \"xmip-core-transport-file\"\naddress = '{}'\n",
        inbox(&receives, Stage::Process),
        inbox(&sends, Stage::Send),
    );
    let path = directory.join(format!("{name}.node.toml"));
    written(&path, &text)?;
    Ok(path)
}

/// The node's runtime store: persist's over `RocksDB`, sealed under the
/// machine's key store — DPAPI on Windows, a private file elsewhere.
fn store(shared: &Path, name: &str) -> Result<Store, String> {
    let keys = shared.join("keys").join(name);
    #[cfg(windows)]
    let keys = Held::new(secret_dpapi::Dpapi::new(keys));
    #[cfg(not(windows))]
    let keys = Held::new(secret_file::KeyFile::new(keys));
    let kek = KekName::new("runtime").map_err(|error| error.to_string())?;
    let directory = shared.join("store").join(name);
    std::fs::create_dir_all(&directory).map_err(|error| error.to_string())?;
    let engine = rocksdb::RocksDb::open(&directory).map_err(|error| error.to_string())?;
    let store = EncryptedStore::open(engine, &keys, &kek).map_err(|error| error.to_string())?;
    Ok(Arc::new(store))
}

/// What a paused Subscription keeps of a pair: its bytes, and what the pair
/// is, to hand it on once it is resumed.
fn held(handoff: &Handoff) -> HeldMessage {
    HeldMessage {
        content: handoff.bytes.clone(),
        said: vec![
            ("transport".to_string(), handoff.transport.clone()),
            ("contract".to_string(), handoff.contract.name().to_string()),
            ("round".to_string(), handoff.round.to_string()),
            ("from".to_string(), handoff.from.clone()),
        ],
        ..HeldMessage::default()
    }
}

/// The pair a held Message was.
fn pair(held: &HeldMessage) -> Option<Handoff> {
    let contract = held.said("contract")?;
    Some(Handoff {
        transport: held.said("transport")?.to_string(),
        contract: CONTRACTS
            .into_iter()
            .find(|known| known.name() == contract)?,
        round: held.said("round")?.parse().ok()?,
        from: held.said("from")?.to_string(),
        bytes: held.content.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::support::scratch;
    use crate::verdict::Contract;

    const NODE: &str = "xmip:///CT/node/beta";

    fn started(shared: &Path, audit: &ProgramAudit) -> Subscribing {
        let roster =
            Roster::parse("alpha=receiving,beta=processing,gamma=sending").expect("a roster");
        Subscribing::start(NODE, "beta", &roster, shared, audit).expect("started")
    }

    fn pair(contract: Contract, round: u64) -> Handoff {
        Handoff {
            transport: "tcp".to_string(),
            contract,
            round,
            from: "alpha".to_string(),
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
            structured.file.ends_with(APPLICATION_FILE),
            "{}",
            structured.file
        );
        assert!(
            structured
                .configuration
                .starts_with("[[subscriptions]]\nid = \"structured\"")
        );

        assert!(subscribing.route(&pair(Contract::Json, 1)), "picked up");
        subscribing
            .act("structured", Act::Pause, "ilian")
            .expect("paused");
        assert!(!subscribing.route(&pair(Contract::Json, 2)), "held");
        assert!(!subscribing.route(&pair(Contract::Xml, 3)), "held");
        assert!(
            subscribing.route(&pair(Contract::Csv, 4)),
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
        let rounds: Vec<u64> = released.iter().map(|(pair, _)| pair.round).collect();
        assert_eq!(rounds, [2, 3], "oldest first, nothing lost");
        assert_eq!(released[1].0.bytes, Contract::Xml.payload());
        for (_, one) in &released {
            again.picked_up(one);
        }
        assert!(again.act("structured", Act::Remove, "ilian").is_err());
        drop(again);
        let _ = std::fs::remove_dir_all(&shared);
    }
}
