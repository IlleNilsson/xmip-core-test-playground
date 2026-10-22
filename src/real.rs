//! A real node is used, a simulated one is the fallback (ADR-0028,
//! amendments 2026-09-19 and 2026-10-21; the owner: *if there are actual
//! DNS names for clusters and nodes, the test shall use them instead of
//! spawning test processes*).
//!
//! Only what the operator typed is resolved — the cluster, `-Cluster`, and
//! the nodes `-Nodes` named (`environment::named`) — through this machine's
//! own resolver, at the port the cluster's own xmip.toml says the node is
//! operated on. A name that does not resolve is a node the rig spawns, as
//! before. A name that resolves is not yet trusted: it must answer over its
//! operate listener, inside Xmip's mutual TLS with the operator certificate
//! the run names (`environment::certificate`), declare itself the node of
//! that name in that cluster (ADR-0053) and say what it can do (ADR-0056);
//! anything else is REFUSED by name, never quietly replaced with a spawned
//! process — the operator meant that machine. A resolving cluster name must
//! answer as a node of that cluster.
//!
//! The rig never spawns, configures or stops a real node: an orchestrator
//! owns a node's life (ADR-0052, ruling 8). It reads what the node
//! publishes, each round, and the run says per node which it is.

use std::net::ToSocketAddrs;
use std::path::Path;

use node::Capability;
use observe::{Health, HealthRecord, Publication, Snapshot};
use runtime::plane::Remote;

use crate::environment;
use crate::roster::Roster;
use crate::subscribing::CLUSTER;

/// A run's nodes, sorted: the real ones as they answered, their names, the
/// roster with each real node's roles as it declared them, and the
/// simulated nodes alone, which are all the run spawns.
pub struct Sorted {
    pub real: Vec<RealNode>,
    pub names: Vec<String>,
    pub roster: Roster,
    pub simulated: Roster,
}

/// The nodes of `roster`, sorted into real and simulated by the names the
/// operator typed (`environment::named`), asked with the run's operator
/// certificate (`environment::certificate`), as [`found`] does.
///
/// # Errors
///
/// As [`found`].
pub fn sorted(cluster: &str, roster: Roster) -> Result<Sorted, String> {
    let certificate = environment::certificate();
    let real = found(cluster, &environment::named(), certificate.as_deref())?;
    let names: Vec<String> = real.iter().map(|node| node.name.clone()).collect();
    let roster = real.iter().fold(roster, |roster, node| {
        roster.declared(&node.name, node.capability.clone())
    });
    let simulated = roster.without(&names);
    Ok(Sorted {
        real,
        names,
        roster,
        simulated,
    })
}

/// What every real node publishes now, read over its operate listener into
/// `snapshot` beneath `root`'s scope of it, as [`RealNode::published_into`].
pub fn published_into(real: &[RealNode], root: &str, snapshot: &mut Snapshot, now: i64) {
    for node in real {
        let scope = format!("{root}/node/{}", node.name);
        node.published_into(&scope, snapshot, now);
    }
}

/// A node on a machine of its own, as it answered.
pub struct RealNode {
    /// Its name, as typed and as it declared itself.
    pub name: String,
    /// Where it was reached: its name and its operate port.
    pub address: String,
    /// What it declared it can do.
    pub capability: Capability,
    remote: Remote,
}

impl RealNode {
    /// The node `name` the run started on this computer (`served`), read at
    /// its local endpoint `endpoint`, which the operating system proves the
    /// rig to, declaring `capability`.
    ///
    /// # Errors
    ///
    /// An endpoint that is no name in the endpoints' folder.
    pub fn local(name: &str, endpoint: &str, capability: Capability) -> Result<Self, String> {
        Ok(Self {
            name: name.to_string(),
            address: format!("the local endpoint {endpoint}"),
            capability,
            remote: Remote::local(endpoint, std::time::Duration::ZERO)?,
        })
    }

    /// What the node publishes now, read over its operate listener, into
    /// `snapshot`, beneath its own scope; where it does not answer, that is
    /// recorded at `<node>/reached`, with why, and nothing else of it.
    pub fn published_into(&self, scope: &str, snapshot: &mut Snapshot, now: i64) {
        let read = self
            .remote
            .publication()
            .and_then(|(_, text)| Publication::read(&text));
        let reached = format!("{scope}/reached");
        let (health, evidence) = match read {
            Ok(publication) => {
                crate::cluster::merge(snapshot, &publication.snapshot());
                (Health::Fine, format!("real, at {}", self.address))
            }
            Err(problem) => (
                Health::Exhausted,
                format!("real, at {}, did not answer: {problem}", self.address),
            ),
        };
        snapshot.record_health(HealthRecord {
            scope: reached,
            health,
            severity: 0,
            evidence,
            observed_unix_nanos: now,
        });
    }
}

/// The nodes among `typed` that are real, each having answered and declared
/// itself a node of `cluster`; every other typed name is simulated. The
/// cluster's own name is asked too where it resolves.
///
/// # Errors
///
/// REFUSED, naming the name: a resolving name the rig holds no certificate
/// to ask, one that does not answer, or one that answers as something else.
pub fn found(
    cluster: &str,
    typed: &[String],
    certificate: Option<&Path>,
) -> Result<Vec<RealNode>, String> {
    if let Some(address) = resolving(cluster, &operate_port(cluster, None)?) {
        let remote = answering(cluster, &address, certificate)?;
        let declared = publication(cluster, &address, &remote)?.node;
        if !declared.starts_with(&format!("xmip:///{cluster}/node/")) {
            return Err(format!(
                "REFUSED: the cluster name {cluster} resolves, at {address}, and answers as \
                 {declared}, no node of cluster {cluster}"
            ));
        }
    }
    let mut real = Vec::new();
    for name in typed {
        let Some(address) = resolving(name, &operate_port(cluster, Some(name))?) else {
            continue;
        };
        let remote = answering(name, &address, certificate)?;
        let capability = declared(cluster, name, &address, &remote)?;
        real.push(RealNode {
            name: name.clone(),
            address,
            capability,
            remote,
        });
    }
    Ok(real)
}

/// The port the cluster's own xmip.toml says `node` is operated on — the
/// first node it declares where `node` is none of them, or none is named —
/// as `[operate] listen` says it, read through the configuration's own
/// slice and reader.
fn operate_port(cluster: &str, node: Option<&str>) -> Result<String, String> {
    let declared = configure::cluster::nodes(CLUSTER)?;
    let asked = node
        .filter(|name| declared.iter().any(|one| one == name))
        .or_else(|| declared.first().map(String::as_str))
        .ok_or("REFUSED: the Playground's xmip.toml declares no node")?;
    let document = configure::parse_toml(&configure::slice_named(CLUSTER, asked, cluster)?)?;
    let listen = document
        .operate
        .and_then(|operate| operate.listen)
        .ok_or_else(|| format!("REFUSED: the Playground's xmip.toml operates {asked} nowhere"))?;
    listen
        .rsplit_once(':')
        .map(|(_, port)| port.to_string())
        .ok_or_else(|| format!("REFUSED: [operate] listen {listen} names no port"))
}

/// `name` at `port`, where this machine's resolver knows the name.
fn resolving(name: &str, port: &str) -> Option<String> {
    let address = format!("{name}:{port}");
    address
        .to_socket_addrs()
        .ok()
        .and_then(|mut found| found.next())
        .map(|_| address)
}

/// The operate listener at `address`, opened with the operator certificate
/// in `folder`, which a resolving name needs to be asked at all.
fn answering(name: &str, address: &str, folder: Option<&Path>) -> Result<Remote, String> {
    let folder = folder.ok_or_else(|| {
        format!(
            "REFUSED: {name} resolves, at {address}, and the run holds no operator certificate \
             to ask it whether it is an Xmip node; name the folder of one with -Certificate"
        )
    })?;
    let file = |leaf: &str| {
        let path = folder.join(leaf);
        std::fs::read(&path).map_err(|error| format!("REFUSED: {}: {error}", path.display()))
    };
    let identity = tls::Identity::from_pem(
        &file("operator.pem")?,
        &file("operator.key")?,
        &file("anchors.pem")?,
    )
    .map_err(|error| format!("REFUSED: the certificate in {}: {error}", folder.display()))?;
    Remote::open(address, &identity)
        .map_err(|problem| format!("REFUSED: {name} resolves, at {address}: {problem}"))
}

/// What the node at `address` publishes, or REFUSED where it does not
/// answer with a publication.
fn publication(name: &str, address: &str, remote: &Remote) -> Result<Publication, String> {
    let refused = |what: String| format!("REFUSED: {name} resolves, at {address}, and {what}");
    let (_, text) = remote
        .publication()
        .map_err(|problem| refused(format!("does not answer: {problem}")))?;
    Publication::read(&text)
        .map_err(|problem| refused(format!("answers with no Xmip publication: {problem}")))
}

/// What the node at `address` declares: itself the node `name` of
/// `cluster`, and its capability; REFUSED by name where it does not.
fn declared(
    cluster: &str,
    name: &str,
    address: &str,
    remote: &Remote,
) -> Result<Capability, String> {
    let refused = |what: String| format!("REFUSED: {name} resolves, at {address}, and {what}");
    let publication = publication(name, address, remote)?;
    let scope = format!("xmip:///{cluster}/node/{name}");
    if publication.node != scope {
        return Err(refused(format!(
            "answers as {}, not as {scope}",
            publication.node
        )));
    }
    let record = observe::capability::scope(&scope);
    publication
        .records
        .iter()
        .find(|one| one.scope == record)
        .and_then(|one| observe::capability::declared(&one.scope, &one.evidence))
        .map(|(_, capability)| capability)
        .ok_or_else(|| refused("says nothing of what it can do".to_string()))?
        .map_err(refused)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::support::test_cluster;

    #[test]
    fn a_name_that_does_not_resolve_is_simulated_and_the_port_is_the_file_s() {
        let cluster = test_cluster();
        let port = operate_port(&cluster.name, None).expect("the file operates its nodes");
        assert!(port.parse::<u16>().is_ok(), "{port}");
        // A name no resolver knows: `.invalid` is reserved for it (RFC 6761).
        let unknown = vec!["no-such-node.invalid".to_string()];
        let found = found(&cluster.name, &unknown, None).expect("nothing resolves");
        assert!(
            found.is_empty(),
            "a name that does not resolve is simulated"
        );
    }

    #[test]
    fn a_resolving_name_with_no_certificate_to_ask_it_is_refused_by_name() {
        let refused = answering("R", "localhost:7401", None)
            .err()
            .expect("refused");
        assert!(refused.starts_with("REFUSED: R resolves"), "{refused}");
        assert!(refused.contains("-Certificate"), "{refused}");
    }
}
