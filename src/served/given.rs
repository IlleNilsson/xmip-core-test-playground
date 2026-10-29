//! What the run gives each node it starts, as an environment gives a
//! machine's node (`super`): the bindings of the file's Applications, its
//! slice of the cluster's file with its addresses on loopback and the
//! Storage nodes' names resolved, and its certificate from the run's
//! authority.

use std::path::{Path, PathBuf};

use toml::{Table, Value};

use configure::placed::{self, Placed, Placement};

use crate::carried::{Binding, Spot};
use crate::subscribing::CLUSTER;

/// A port no one holds on loopback now.
fn free() -> Result<u16, String> {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .map(|address| address.port())
        .map_err(|error| format!("no port on loopback: {error}"))
}

/// Each Receive Location and Send Port the file designs placed over the
/// run's nodes `declared` (`configure::placed`), a Receive Location a drop
/// folder of its own, a Send Port a folder the rig reads.
pub(super) fn placed(
    declared: &[(String, Vec<String>)],
    area: &Path,
) -> Result<Vec<Binding>, String> {
    Ok(placed::placements(CLUSTER, declared)?
        .into_iter()
        .map(|placement| {
            let under = match placement.placed {
                Placed::Receive => "drop",
                Placed::Send => "out",
            };
            let folder = area.join(under).join(placement.folded());
            Binding {
                placement,
                spot: Spot::Folder(folder),
            }
        })
        .collect())
}

/// The cluster's file as the run gives it to the nodes it starts: the
/// Playground's own with the `bindings` the run placed, each node's roles as
/// the run declares them, each Storage node's name resolved to the address
/// on loopback it serves at, each node its own data folder, its local
/// endpoint named for the run's cluster and its operate listener on
/// loopback.
pub(super) fn cluster_text(
    cluster: &str,
    declared: &[(String, Vec<String>)],
    bindings: &[Binding],
) -> Result<String, String> {
    let placements: Vec<Placement> = bindings.iter().map(|one| one.placement.clone()).collect();
    let bound = placed::written(&placements, |placement| {
        let folder = bindings
            .iter()
            .find(|one| one.placement == *placement)
            .and_then(|one| match &one.spot {
                Spot::Folder(folder) => Some(folder.display().to_string().replace('\\', "/")),
                Spot::FarEnd(_) => None,
            });
        placed::Binding {
            address: folder.unwrap_or_default(),
            settings: Vec::new(),
            accepting: placement.placed == Placed::Receive,
        }
    });
    let mut file: Table = format!("{CLUSTER}\n{bound}")
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
/// of its own in `area`, with its certificate from the Playground's
/// authority (`environment::authority`) beside it: the configuration's
/// path.
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
    let authority = crate::environment::authority();
    provision::node::issue_into(
        &authority,
        &authority_name(&authority)?,
        node,
        &["127.0.0.1".to_string(), "localhost".to_string()],
        &folder.join("tls"),
        now,
    )
    .map_err(|error| format!("{node}'s certificate: {error}"))?;
    Ok(path)
}

/// The name the Playground's authority kept in `place` was made under, read
/// from its certificate: `Initialize-XmipPlaygroundAuthority` alone names
/// it, and makes it before a run starts any node.
///
/// # Errors
///
/// Where no authority is kept there, or its certificate names nothing.
fn authority_name(place: &Path) -> Result<String, String> {
    let path = place.join(provision::issuer::CERTIFICATE);
    let text = std::fs::read_to_string(&path).map_err(|error| {
        format!(
            "no Playground authority at {}: {error}; Start-XmipTest makes it",
            path.display()
        )
    })?;
    provision::Parsed::first(&text)
        .and_then(|parsed| parsed.common_name())
        .map_err(|error| format!("{}: {error}", path.display()))?
        .ok_or_else(|| {
            format!(
                "the Playground's authority at {} names nothing",
                path.display()
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn two_runs_issue_their_nodes_under_the_one_playground_authority() {
        // A node that keeps Xmip Storage and one that does not, as the
        // Playground's own file declares them, and the test cluster's name.
        let roles = configure::cluster::roles(CLUSTER).expect("the file's roles");
        let storage = |node: &&(String, Vec<String>)| node.1.iter().any(|role| role == "storage");
        let declared: Vec<(String, Vec<String>)> = [
            roles.iter().find(storage),
            roles.iter().find(|node| !storage(node)),
        ]
        .into_iter()
        .map(|node| node.expect("a node of each kind").clone())
        .collect();
        let cluster = crate::cluster_name().expect("the test cluster is named");
        let base = std::env::temp_dir().join(format!("xmip-given-{}", std::process::id()));
        let now = observe::now_unix_nanos() / 1_000_000_000;
        provision::Issuer::kept_or_made(
            &crate::environment::authority(),
            "Xmip Playground test authority",
            now,
            3600,
        )
        .expect("the authority Start-XmipTest makes");
        let mut anchors = Vec::new();
        for run in ["first", "second"] {
            let area = base.join(run);
            let text = cluster_text(
                &cluster,
                &declared,
                &placed(&declared, &area).expect("placed"),
            )
            .expect("the cluster's text");
            for (node, _) in &declared {
                configured(&text, (&cluster, node), &area).expect("configured");
                let tls = area.join(node).join("tls");
                anchors
                    .push(std::fs::read(tls.join(provision::node::ANCHORS_FILE)).expect("anchors"));
            }
        }
        let authority = crate::environment::authority().join(provision::issuer::CERTIFICATE);
        let kept = std::fs::read(authority).expect("the Playground's authority");
        assert!(
            anchors.iter().all(|anchor| *anchor == kept),
            "one authority for every run"
        );
        let _ = std::fs::remove_dir_all(&base);
    }
}
