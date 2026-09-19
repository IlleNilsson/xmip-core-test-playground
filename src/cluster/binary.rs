//! Where the node binary is: the one executable a cluster spawns per node.

use std::io;
use std::path::{Path, PathBuf};

/// The `node` binary: `XMIP_PLAYGROUND_NODE` if set, else beside the current
/// executable, else one directory up from it (a test runs from `deps/`).
///
/// # Errors
///
/// When none of those is a file.
pub fn node_binary() -> io::Result<PathBuf> {
    if let Some(named) = std::env::var_os("XMIP_PLAYGROUND_NODE") {
        return Ok(PathBuf::from(named));
    }
    let current = std::env::current_exe()?;
    let file = format!("xmip-playground-node{}", std::env::consts::EXE_SUFFIX);
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
                "no node binary beside the playground; build it, or set XMIP_PLAYGROUND_NODE",
            )
        })
}

/// The `node` binary, built now so a test never runs a stale one. Cargo sets
/// `CARGO_BIN_EXE_<name>` for integration tests only, never for a library's
/// unit tests, so a unit test builds the binary itself: a no-op when it is
/// fresh, and cargo has released the build lock by the time tests run.
#[cfg(test)]
pub(crate) fn built_node_binary() -> PathBuf {
    let status = std::process::Command::new(env!("CARGO"))
        .args(["build", "-q", "--bin", "xmip-playground-node"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .status()
        .expect("cargo runs");
    assert!(status.success(), "the node binary builds");
    node_binary().expect("the node binary is beside the test executable")
}
