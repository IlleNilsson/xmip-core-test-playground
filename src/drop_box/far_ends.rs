//! The drop box's far ends, one remote directory per Party per protocol,
//! and the transport each side speaks to it with (ADR-0024 and ADR-0028,
//! amendments 2026-10-13).
//!
//! The owner, 2026-10-13: *induce multiple files for FILE and (S)FTP(S)*.
//! The roll stands up an FTP, an FTPS and an SFTP server for every Party —
//! Xmip's own server sides (`transport_ftp::serving`,
//! `transport_sftp::serving`), FTPS over Xmip's TLS with a certificate from
//! the estate's own self-signed source — each serving every receiving node
//! at once over one directory, and writes where each listens beside the
//! drop box, so every node finds them. The file protocol's directory is the
//! Party's own under the drop box, as it was.

use std::net::TcpListener;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ed25519_dalek::SigningKey;
use provision::{Order, Purpose, Source};
use provision_self_signed::{SETTINGS, SelfSigned, TECHNOLOGY};
use transport::concurrent::Concurrent;
use transport::{Configured, NodeLocation, Transport};
use transport_file::FileTransport;
use transport_ftp::{FtpTransport, Secured, Store};
use transport_sftp::SftpTransport;
use transport_sftp::directory::Directory;
use xcore::settings::Given;

use super::DropBox;

/// Every protocol a Party drops over: the file family.
pub const PROTOCOLS: [&str; 4] = ["file", "ftp", "ftps", "sftp"];

/// The name the FTPS servers' certificate is for.
const SERVER_NAME: &str = "localhost";

/// How long a server waits on a client that stops mid-exchange, and a
/// client on a server.
const PATIENCE: Duration = Duration::from_secs(30);

/// The SFTP servers' host key, fixed: nothing checks it (no known hosts).
const HOST_SEED: [u8; 32] = [0x5e; 32];

/// A transport one side speaks to a Party's directory with.
pub type Speaking = Box<dyn Transport + Send + Sync>;

/// Whether `protocol` reaches its far end over a socket, each transfer a
/// connection of its own for FTP: its bursts and its listings are paced, so
/// the machine's ports are never spent.
#[must_use]
pub fn remote(protocol: &str) -> bool {
    protocol != "file"
}

/// Every how many rounds a Party drops a burst over `protocol`: every
/// round on a directory, every fifth over a server.
#[must_use]
pub fn bursts_every(protocol: &str) -> u64 {
    if remote(protocol) { 5 } else { 1 }
}

/// Every how many rounds a receiving node lists `protocol`'s directories:
/// every second over FTP and FTPS, whose every listing is a data
/// connection of its own, and so a local port held a while after it.
#[must_use]
pub fn listed_every(protocol: &str) -> u64 {
    if protocol.starts_with("ftp") { 2 } else { 1 }
}

/// The far ends the roll stands up; dropped, they stop accepting.
pub struct FarEnds {
    _serving: Vec<Concurrent>,
}

impl FarEnds {
    /// An FTP, an FTPS and an SFTP server for each of `parties`, each where
    /// `drop_box` says it listens, and the FTPS anchor beside them.
    ///
    /// # Errors
    /// A certificate not made, a port not bound, an address not written,
    /// in words.
    pub fn start(drop_box: &DropBox, parties: &[&str]) -> Result<Self, String> {
        let (chain, key, anchor) = certificate()?;
        std::fs::create_dir_all(drop_box.servers()).map_err(|error| error.to_string())?;
        std::fs::write(drop_box.anchor(), anchor).map_err(|error| error.to_string())?;
        let guard = Arc::new(
            tls::server_config(chain.as_bytes(), key.as_bytes())
                .map_err(|error| error.to_string())?,
        );
        let mut serving = Vec::new();
        for party in parties {
            for protocol in PROTOCOLS.into_iter().filter(|one| remote(one)) {
                let listener = TcpListener::bind("127.0.0.1:0").map_err(|e| e.to_string())?;
                let far = match protocol {
                    "sftp" => transport_sftp::serving(
                        listener,
                        SigningKey::from_bytes(&HOST_SEED),
                        Arc::new(Mutex::new(Directory::default())),
                        Some(PATIENCE),
                    ),
                    _ => transport_ftp::serving(
                        listener,
                        Store::default(),
                        Some(PATIENCE),
                        (protocol == "ftps").then(|| Arc::clone(&guard)),
                    ),
                }
                .map_err(|error| format!("{protocol} for {party}: {error}"))?;
                std::fs::write(drop_box.address(protocol, party), far.address())
                    .map_err(|error| error.to_string())?;
                serving.push(far);
            }
        }
        Ok(Self { _serving: serving })
    }
}

/// The transport of `protocol` over the Party `party`'s directory, built on
/// `node` where a receiving node builds it; `None` while its far end has
/// not said where it listens.
///
/// # Errors
/// A transport that could not be built, in words.
pub fn transport(
    drop_box: &DropBox,
    protocol: &str,
    party: &str,
    node: Option<&NodeLocation>,
) -> Option<Result<Speaking, String>> {
    if protocol == "file" {
        let file = FileTransport::new(drop_box.location(party));
        return Some(on(file, node));
    }
    let address = std::fs::read_to_string(drop_box.address(protocol, party)).ok()?;
    let address = address.trim();
    Some(match protocol {
        "sftp" => on(SftpTransport::new(address).timing_out_after(PATIENCE), node),
        "ftps" => trusted(drop_box.anchor().as_path()).and_then(|secured| {
            let ftps = FtpTransport::new(address).timing_out_after(PATIENCE);
            on(ftps.secured(secured), node)
        }),
        _ => on(FtpTransport::new(address).timing_out_after(PATIENCE), node),
    })
}

/// `built` on `node` where one is given, as the runtime builds a Receive
/// Location's transport.
fn on<T: Configured + Transport + Send + Sync + 'static>(
    built: T,
    node: Option<&NodeLocation>,
) -> Result<Speaking, String> {
    match node {
        Some(node) => built
            .on_node(node)
            .map(|built| Box::new(built) as Speaking)
            .map_err(|error| error.to_string()),
        None => Ok(Box::new(built)),
    }
}

/// FTPS checked against the anchor at `anchor`, for the servers' name.
fn trusted(anchor: &Path) -> Result<Secured, String> {
    let pem = std::fs::read(anchor).map_err(|error| error.to_string())?;
    let config = tls::trusting(&pem).map_err(|error| error.to_string())?;
    Ok(Secured::trusting(Arc::new(config)).named(SERVER_NAME))
}

/// A certificate for the servers' name from the estate's own self-signed
/// source: its chain, its key and the anchor a client trusts, as PEM.
fn certificate() -> Result<(String, String, String), String> {
    let given = [
        ("source".to_string(), Given::Text(TECHNOLOGY.to_string())),
        ("names".to_string(), Given::Text(SERVER_NAME.to_string())),
        ("lifetime".to_string(), Given::Text("24h".to_string())),
    ];
    let read = provision::read("node", &given, &[SETTINGS]).map_err(|error| error.to_string())?;
    let names = vec![SERVER_NAME.to_string()];
    let order = Order {
        purpose: Purpose::Node,
        names: &names,
        keep: Path::new("."),
    };
    let held = SelfSigned::configured(&read.settings)
        .obtain(&order)
        .map_err(|error| error.to_string())?;
    let anchor = held.anchors.clone().unwrap_or_default();
    Ok((held.chain.clone(), held.key.to_string(), anchor))
}
