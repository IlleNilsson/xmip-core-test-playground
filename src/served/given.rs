//! What the run gives each node it starts, as an environment gives a
//! machine's node (`super`): the bindings of the file's Applications, its
//! slice of the cluster's file with its addresses on loopback and the
//! Storage nodes' names resolved, and its certificate from the run's
//! authority.

use std::path::{Path, PathBuf};

use toml::{Table, Value};

use super::Binding;
use crate::bindings::{self, Bound, Placed};
use crate::subscribing::CLUSTER;

/// A port no one holds on loopback now.
fn free() -> Result<u16, String> {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .map(|address| address.port())
        .map_err(|error| format!("no port on loopback: {error}"))
}

/// A folder's name for a Location's or a Port's.
fn folded(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect()
}

/// Each Receive Location of `designed` bound to the next of `receivers`,
/// round robin, a drop folder of its own; each Send Port to the next of
/// `senders`, a folder the rig reads.
pub(super) fn placed(
    designed: &[configure::XmipApplication],
    (receivers, senders): (&[String], &[String]),
    area: &Path,
) -> Vec<Binding> {
    let mut bound = Vec::new();
    let (mut receiving, mut sending) = (0, 0);
    for application in designed {
        for location in &application.receive_locations {
            if let Some(node) = receivers.get(receiving % receivers.len().max(1)) {
                bound.push(Binding {
                    placed: Placed::Receive,
                    name: location.name.clone(),
                    node: node.clone(),
                    folder: area.join("drop").join(folded(&location.name)),
                });
            }
            receiving += 1;
        }
        for port in &application.send_ports {
            if let Some(node) = senders.get(sending % senders.len().max(1)) {
                bound.push(Binding {
                    placed: Placed::Send,
                    name: port.name.clone(),
                    node: node.clone(),
                    folder: area.join("out").join(folded(&port.name)),
                });
            }
            sending += 1;
        }
    }
    bound
}

/// The cluster's file as the run gives it to the nodes it starts: the
/// bindings `placed` says before the Playground's own, each node's roles as
/// the run declares them, each Storage node's name resolved to the address
/// on loopback it serves at, each node its own data folder, its local
/// endpoint named for the run's cluster and its operate listener on
/// loopback.
pub(super) fn cluster_text(
    cluster: &str,
    declared: &[(String, Vec<String>)],
    placed: &[Binding],
) -> Result<String, String> {
    let designed = configure::applications(CLUSTER)?;
    let bound = bindings::written(&designed, |kind, name| {
        let binding = placed
            .iter()
            .find(|one| one.placed == kind && one.name == name);
        binding.map_or_else(
            || Bound {
                node: String::new(),
                address: String::new(),
                accepting: false,
            },
            |one| Bound {
                node: one.node.clone(),
                address: one.folder.display().to_string().replace('\\', "/"),
                accepting: kind == Placed::Receive,
            },
        )
    });
    let mut file: Table = format!("{bound}{CLUSTER}")
        .parse()
        .map_err(|error| format!("the cluster's file: {error}"))?;
    let mut serving: Vec<(String, String)> = Vec::new();
    for (name, roles) in declared {
        if roles.iter().any(|role| role == "storage") {
            serving.push((name.clone(), format!("127.0.0.1:{}", free()?)));
        }
    }
    let resolved = |listed: &str| {
        let host = listed.rsplit_once(':').map_or(listed, |(host, _)| host);
        serving
            .iter()
            .find(|(name, _)| name == host)
            .map_or_else(|| listed.to_string(), |(_, at)| at.clone())
    };
    let storage = table(&mut file, &["storage"]);
    if let Some(Value::Array(nodes)) = storage.get_mut("nodes") {
        for listed in nodes.iter_mut() {
            if let Value::String(text) = listed {
                *text = resolved(text);
            }
        }
    }
    for (name, roles) in declared {
        let node = table(&mut file, &["nodes", name]);
        node.insert("roles".to_string(), Value::String(roles.join(", ")));
        table(&mut file, &["nodes", name, "service"])
            .insert("data".to_string(), Value::String("data".to_string()));
        let operate = table(&mut file, &["nodes", name, "operate"]);
        operate.insert("local".to_string(), Value::String(endpoint(cluster, name)));
        operate.insert(
            "listen".to_string(),
            Value::String(format!("127.0.0.1:{}", free()?)),
        );
        if let Some((_, at)) = serving.iter().find(|(serving, _)| serving == name) {
            table(&mut file, &["nodes", name, "storage"])
                .insert("listen".to_string(), Value::String(at.clone()));
        }
    }
    toml::to_string(&file).map_err(|error| format!("the cluster's file: {error}"))
}

/// The table at `path` in `file`, made where it is not.
fn table<'a>(file: &'a mut Table, path: &[&str]) -> &'a mut Table {
    let mut at = file;
    for key in path {
        let entry = at
            .entry((*key).to_string())
            .or_insert_with(|| Value::Table(Table::new()));
        if !entry.is_table() {
            *entry = Value::Table(Table::new());
        }
        at = entry
            .as_table_mut()
            .unwrap_or_else(|| unreachable!("made a table"));
    }
    at
}

/// The local endpoint the run names the node `node` of `cluster`'s, so two
/// clusters' runs on one computer never meet on `xmip-<node>`.
#[must_use]
pub fn endpoint(cluster: &str, node: &str) -> String {
    format!("xmip-{cluster}-{node}")
}

/// `node`'s configuration, sliced from `text` under `cluster` into a folder
/// of its own in `area`, with its certificate from the run's authority
/// beside it: the configuration's path.
pub(super) fn configured(
    text: &str,
    (cluster, node): (&str, &str),
    area: &Path,
) -> Result<PathBuf, String> {
    let folder = area.join(node);
    std::fs::create_dir_all(&folder).map_err(|error| format!("{}: {error}", folder.display()))?;
    let sliced = configure::slice_named(text, node, cluster)?;
    let path = folder.join(format!("{node}.toml"));
    std::fs::write(&path, sliced).map_err(|error| format!("{}: {error}", path.display()))?;
    let now = observe::now_unix_nanos() / 1_000_000_000;
    provision::node::issue_into(
        &area.join("authority"),
        &format!("Xmip cluster {cluster} authority"),
        node,
        &["127.0.0.1".to_string(), "localhost".to_string()],
        &folder.join("tls"),
        now,
    )
    .map_err(|error| format!("{node}'s certificate: {error}"))?;
    Ok(path)
}
