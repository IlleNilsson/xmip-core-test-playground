//! Where the executables a run spawns are: the cluster the roll starts, and
//! the node the cluster starts.
//!
//! 2026-09-19, the owner: *even clusters have to be spawned as processes
//! during tests*, so there are two to find rather than one. Both are looked
//! for the same way and both may be named outright.

use std::io;
use std::path::{Path, PathBuf};

/// The `xmip-playground-node` binary: `XMIP_PLAYGROUND_NODE` if set, else
/// beside the current executable, else one directory up from it (a test runs
/// from `deps/`).
///
/// # Errors
///
/// When none of those is a file.
pub fn node_binary() -> io::Result<PathBuf> {
    found("XMIP_PLAYGROUND_NODE", "xmip-playground-node")
}

/// The `xmip-playground-cluster` binary, found the same way.
/// `XMIP_PLAYGROUND_CLUSTER` is the cluster's own name, so the override here
/// is `XMIP_PLAYGROUND_CLUSTER_BINARY`.
///
/// # Errors
///
/// When none of those is a file.
pub fn cluster_binary() -> io::Result<PathBuf> {
    found("XMIP_PLAYGROUND_CLUSTER_BINARY", "xmip-playground-cluster")
}

/// The executable called `stem`: what `variable` names, else beside the
/// current executable, else one directory up from it.
fn found(variable: &str, stem: &str) -> io::Result<PathBuf> {
    if let Some(named) = std::env::var_os(variable) {
        return Ok(PathBuf::from(named));
    }
    let current = std::env::current_exe()?;
    let file = format!("{stem}{}", std::env::consts::EXE_SUFFIX);
    let beside = current.parent().map(|dir| dir.join(&file));
    let above = current
        .parent()
        .and_then(Path::parent)
        .map(|dir| dir.join(&file));
    [beside, above]
        .into_iter()
        .flatten()
        .find(|candidate| candidate.is_file())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("no {stem} beside the playground; build it, or set {variable}"),
            )
        })
}

/// Build this crate's two binaries now, once for the whole test process, so
/// a test never runs a stale one. Cargo sets `CARGO_BIN_EXE_<name>` for
/// integration tests only, never for a library's unit tests, so a unit test
/// builds them itself: one cargo call for both, made by the first test that
/// needs them while every other waits for it, and none after — where every
/// test built its own, four builds ran at once on every core and spoiled the
/// timing tests beside them (2026-10-23). The jobs are `CARGO_BUILD_JOBS`
/// where the run sets it, else two, as everywhere in the estate. Under a
/// landing it builds through the landing's patch, which `XMIP_CARGO_CONFIG`
/// names, as the test itself was built.
#[cfg(test)]
fn built() {
    static BUILT: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    let built = BUILT.get_or_init(|| {
        let jobs = std::env::var("CARGO_BUILD_JOBS")
            .ok()
            .filter(|jobs| !jobs.is_empty())
            .unwrap_or_else(|| "2".to_string());
        let mut cargo = std::process::Command::new(env!("CARGO"));
        cargo.args(["build", "-q", "-j", &jobs]);
        cargo.args([
            "--bin",
            "xmip-playground-node",
            "--bin",
            "xmip-playground-cluster",
        ]);
        if let Some(config) = std::env::var_os("XMIP_CARGO_CONFIG").filter(|it| !it.is_empty()) {
            cargo.arg("--config").arg(config);
        }
        cargo
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .status()
            .is_ok_and(|status| status.success())
    });
    assert!(
        *built,
        "xmip-playground-node and xmip-playground-cluster build"
    );
}

/// The node binary, built now.
#[cfg(test)]
pub(crate) fn built_node_binary() -> PathBuf {
    built();
    node_binary().expect("the node binary is beside the test executable")
}

/// The cluster binary, built now, and the node binary it will spawn with it.
#[cfg(test)]
pub(crate) fn built_cluster_binary() -> PathBuf {
    built();
    cluster_binary().expect("the cluster binary is beside the test executable")
}
