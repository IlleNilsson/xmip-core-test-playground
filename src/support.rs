//! Small helpers shared across the scenarios: the cluster a roll is, and —
//! for tests — a scratch directory and the one judgement every cabinet is
//! held to. The wall clock every scenario stamps its records with is
//! observe's, `observe::now_unix_nanos`.

/// The cluster a roll is (ADR-0028), by the name the owner gave it in
/// `XMIP_PLAYGROUND_CLUSTER`; `None` when nobody named one. A test may spawn
/// nodes, never a cluster (the owner, 2026-09-14; ADR-0052), so the binaries
/// refuse to start without a name rather than inventing one. A roll's nodes
/// inherit the variable, so the roll and every node agree on the root without
/// being told twice. This crate's own tests run in the test cluster, by the
/// name its `xmip.toml` gives it.
#[must_use]
pub fn cluster_name() -> Option<String> {
    std::env::var("XMIP_PLAYGROUND_CLUSTER")
        .ok()
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
        .or_else(test_cluster_name)
}

/// The test cluster's name (`configure::fixture`), read once.
#[cfg(test)]
#[allow(
    clippy::unnecessary_wraps,
    reason = "the same signature as the variant outside tests, which has none"
)]
fn test_cluster_name() -> Option<String> {
    static NAME: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    Some(
        NAME.get_or_init(|| configure::fixture::test_cluster().name)
            .clone(),
    )
}

/// Outside this crate's tests nobody names a cluster but the owner.
#[cfg(not(test))]
fn test_cluster_name() -> Option<String> {
    None
}

/// The scope root every record in this cluster hangs under,
/// `xmip:///<cluster>`. Unnamed, it is the bare `xmip:///`, which no binary
/// reaches: each has refused by then.
#[must_use]
pub fn cluster_root() -> String {
    format!("xmip:///{}", cluster_name().unwrap_or_default())
}

/// Three transports, one of each kind a schedule meets — a directory, a
/// connection and a datagram — for a test about what a schedule reports rather
/// than which transports carry it.
///
/// The whole matrix is 1,094 connections for one tick. Measured on 2026-09-21,
/// the suite made 16,550 against a dynamic port range of 16,384, and eleven
/// tests that ticked every transport made about 13,000 of them while testing
/// curves, reports and counts; under cargo's parallel run they spent the range
/// inside the two minutes a closed port is held, and every transport test
/// after them failed for want of a port. The matrix is the subject of
/// `a_tick_reports_every_pair_across_the_three_stages` and of the fault-free
/// schedule's test, and it is exercised there.
#[cfg(test)]
pub(crate) fn three(dir: &std::path::Path) -> Vec<Box<dyn crate::exchange::RoundTrip>> {
    vec![
        Box::new(crate::exchange::FileRoundTrip::new(dir)),
        Box::new(crate::exchange::TcpRoundTrip),
        Box::new(crate::exchange::UdpRoundTrip),
    ]
}

/// A fresh, empty scratch directory for a test, unique per name and run so
/// parallel tests never collide. The test removes it when done.
#[cfg(test)]
pub(crate) fn scratch(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("xmip-play-{name}-{}", observe::now_unix_nanos()));
    std::fs::remove_dir_all(&dir).ok();
    dir
}

/// The test cluster, what every test takes its cluster's and its nodes'
/// names from.
#[cfg(test)]
pub(crate) use configure::fixture::{TestCluster, test_cluster};

/// A scope beneath the test cluster's, `xmip:///<cluster>/<path>`.
#[cfg(test)]
pub(crate) fn scope(path: &str) -> String {
    format!("{}/{path}", cluster_root())
}

/// A roster's text, `<node>=<role>,...`, for the nodes and roles `declared`.
#[cfg(test)]
pub(crate) fn roster_text(declared: &[(&str, &str)]) -> String {
    declared
        .iter()
        .map(|(name, role)| format!("{name}={role}"))
        .collect::<Vec<_>>()
        .join(",")
}

/// The test cluster's receiving, processing and sending node, a
/// `RoundTrip`'s whole path, in that order.
#[cfg(test)]
pub(crate) fn path(cluster: &TestCluster) -> [&str; 3] {
    ["receiving", "processing", "sending"].map(|role| cluster.with_role(role).name.as_str())
}

/// The test cluster's nodes declaring `role`, in the order of their names.
#[cfg(test)]
pub(crate) fn declaring<'a>(cluster: &'a TestCluster, role: &str) -> Vec<&'a str> {
    cluster
        .nodes
        .iter()
        .filter(|node| node.roles.iter().any(|declared| declared == role))
        .map(|node| node.name.as_str())
        .collect()
}

/// The roster of the test cluster's whole path: its receiving, processing
/// and sending node, each declaring that.
#[cfg(test)]
pub(crate) fn path_roster(cluster: &TestCluster) -> String {
    let [receiving, processing, sending] = path(cluster);
    roster_text(&[
        (receiving, "receiving"),
        (processing, "processing"),
        (sending, "sending"),
    ])
}

/// A short, a long and an empty payload, each filed through `cabinet` and
/// returned whole. Shared by `cabinet.rs` and `remote.rs`, so every archive
/// technology is judged the same way.
#[cfg(test)]
pub(crate) fn files_whole(cabinet: &dyn crate::cabinet::Cabinet) {
    use crate::cabinet::Filed;
    let payloads = [b"filed".to_vec(), vec![0x2a; 3_000], Vec::new()];
    for (n, bytes) in payloads.into_iter().enumerate() {
        let item = archive::ArchiveItem {
            data_type: "bytes".to_string(),
            identifier: format!("{n}-bytes"),
            bytes,
            metadata: vec![("source".to_string(), "playground".to_string())],
        };
        assert_eq!(
            cabinet.file(item.clone()),
            Filed::Returned(item),
            "{} files payload {n} whole",
            cabinet.technology()
        );
    }
}

/// Every edge payload under a transport's ceiling comes back whole, and every
/// one above it is refused with a reason rather than hung on or panicked at.
/// Shared by every adapter file, so every transport is judged the same way at
/// the sizes protocols break on.
#[cfg(test)]
pub(crate) fn carries_the_edges(rt: &dyn crate::exchange::RoundTrip) {
    use crate::exchange::Exchange;
    for (name, bytes) in crate::stress::edge_payloads(None) {
        let refused = rt.refuses(&bytes);
        let started = std::time::Instant::now();
        let exchange = rt.exchange(&bytes);
        let took = started.elapsed();
        assert!(
            took < crate::exchange::TIMEOUT * 3,
            "{} took {took:?} on {name}: a round is judged, never waited on",
            rt.transport()
        );
        // A declared refusal must be true: the bytes really do not survive.
        // The scenarios never send a refused payload (RoundTrip judges it
        // one-sided first); here it is sent so an over-broad refusal shows.
        if let Some(why) = refused {
            assert!(
                !matches!(&exchange, Exchange::Returned(back) if *back == bytes),
                "{} declares it cannot carry {name} ({why}) yet returned it whole",
                rt.transport()
            );
            continue;
        }
        match (rt.ceiling(), exchange) {
            (Some(limit), Exchange::Returned(back)) if bytes.len() > limit => {
                panic!(
                    "{} returned {name} above its ceiling of {limit}: {}",
                    rt.transport(),
                    back.len()
                )
            }
            (Some(limit), Exchange::Failed(_) | Exchange::OneSided(_)) if bytes.len() > limit => {}
            (_, Exchange::Returned(back)) => {
                assert!(
                    back == bytes,
                    "{} changed {name} ({} bytes)",
                    rt.transport(),
                    bytes.len()
                );
            }
            (_, Exchange::OneSided(why) | Exchange::Failed(why)) => {
                panic!(
                    "{} did not carry {name} ({} bytes): {why}",
                    rt.transport(),
                    bytes.len()
                )
            }
        }
    }
}
