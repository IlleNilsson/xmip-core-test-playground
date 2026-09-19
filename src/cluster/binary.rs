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

/// Build one of this crate's binaries now, so a test never runs a stale one.
/// Cargo sets `CARGO_BIN_EXE_<name>` for integration tests only, never for a
/// library's unit tests, so a unit test builds the binary itself: a no-op when
/// it is fresh, and cargo has released the build lock by the time tests run.
#[cfg(test)]
fn build(stem: &str) {
    let status = std::process::Command::new(env!("CARGO"))
        .args(["build", "-q", "--bin", stem])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .status()
        .expect("cargo runs");
    assert!(status.success(), "{stem} builds");
}

/// The node binary, built now.
#[cfg(test)]
pub(crate) fn built_node_binary() -> PathBuf {
    build("xmip-playground-node");
    node_binary().expect("the node binary is beside the test executable")
}

/// The cluster binary, built now, and the node binary it will spawn with it.
#[cfg(test)]
pub(crate) fn built_cluster_binary() -> PathBuf {
    build("xmip-playground-cluster");
    build("xmip-playground-node");
    cluster_binary().expect("the cluster binary is beside the test executable")
}
