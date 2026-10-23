//! The Parties' far end, for the nodes on machines of their own (ADR-0028,
//! amendment 2026-10-22). The far end is Xmip (clause 5): no server of the
//! machines' is asked of them. The rig serves SFTP on this computer, on
//! the far end's port (`configure::placed::FAR_END_PORT`) on every address,
//! with Xmip's own server (`transport_sftp::serving_each`), and the real
//! nodes reach it as their environment bound them (`xmip-service --slice
//! --bind`): each Receive Location fetches from, and each Send Port puts
//! on, a user of its own, whose directory it is — placed over the real
//! nodes exactly as the environment placed them (`configure::placed`), so
//! the rig drops where a node fetches and reads where one puts.
//!
//! The host's firewall must let the machines in on that port; the
//! environment's README says how, and that it needs elevation.

use std::collections::BTreeMap;
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use configure::placed::{self, FAR_END_PORT, Placed};
use ed25519_dalek::SigningKey;
use transport::concurrent::Concurrent;
use transport_sftp::directory::{Directory, Shared};

use crate::carried::{Arrived, Binding, Spot, told};
use crate::subscribing::CLUSTER;

/// How long a node that stops mid-exchange is waited on.
const PATIENCE: Duration = Duration::from_secs(10);

/// The far end being served, and the Locations placed on it.
pub struct FarEnd {
    _serving: Concurrent,
    /// Every Receive Location and Send Port placed over the real nodes,
    /// each a user's directory here.
    pub bindings: Vec<Binding>,
    /// Where it is served.
    pub address: String,
}

impl FarEnd {
    /// Serve the far end for the real nodes `real`, each Send Port's
    /// directory telling `arrived` of what a node put there; none, where
    /// the real nodes take no Location.
    ///
    /// # Errors
    ///
    /// REFUSED, in words: a node the cluster's file does not declare, or a
    /// port this computer will not give.
    pub fn serve(real: &[String], arrived: &Arrived) -> Result<Option<Self>, String> {
        let nodes = placed::present(CLUSTER, real)?;
        let placements = placed::placements(CLUSTER, &nodes)?;
        if placements.is_empty() {
            return Ok(None);
        }
        let mut directories: BTreeMap<String, Shared> = BTreeMap::new();
        let mut bindings = Vec::new();
        for placement in placements {
            let directory = match placement.placed {
                Placed::Receive => Directory::default(),
                Placed::Send => Directory::default().telling(told(arrived)),
            };
            let shared: Shared = Arc::new(std::sync::Mutex::new(directory));
            directories.insert(placement.folded(), Arc::clone(&shared));
            bindings.push(Binding {
                placement,
                spot: Spot::FarEnd(shared),
            });
        }
        let address = format!("0.0.0.0:{FAR_END_PORT}");
        let listener = TcpListener::bind(&address).map_err(|error| {
            format!("REFUSED: the Parties' far end cannot be served at {address}: {error}")
        })?;
        let host = SigningKey::from_bytes(&codec::random::array::<32>());
        let chosen = move |user: &str| {
            directories
                .get(user)
                .map_or_else(Shared::default, Arc::clone)
        };
        let serving = transport_sftp::serving_each(listener, host, chosen, Some(PATIENCE))
            .map_err(|error| format!("REFUSED: the Parties' far end at {address}: {error}"))?;
        Ok(Some(Self {
            _serving: serving,
            bindings,
            address,
        }))
    }

    /// What the run says of it among its first lines.
    #[must_use]
    pub fn said(&self) -> String {
        format!(
            "the Parties' far end at {}, each Location placed on a real node a user of its own",
            self.address
        )
    }
}
