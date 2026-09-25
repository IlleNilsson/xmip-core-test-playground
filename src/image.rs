//! The image file a spawned process runs, named for the cluster and the
//! instance it is.
//!
//! The owner, 2026-09-20, reading `Get-Process Xmip-*` with two clusters up:
//! *these process names does not tell an operator or developer much. …
//! Cluster, Node and test suite shall be incorporated in the process name.*
//! Eleven rows said `xmip-playground-node` six times over and nothing said
//! which cluster or which node.
//!
//! The shape is `xmip-playground-<cluster>-roll`, `-cluster` and
//! `-node-<name>`: the kind is a marker the shape carries, so a node may be
//! called anything a file may be called — including `roll` (the owner,
//! 2026-09-20: *a node is a node and can have one or more roles, roll is
//! something different*).
//!
//! On Windows a process name is the **image file's** name, and a running
//! process cannot be renamed, so a name per instance is a file per instance.
//! The bytes are not copied where they need not be: a hard link costs a
//! directory entry, and the node binary is twenty-one megabytes, which forty
//! nodes would turn into eight hundred. A copy is the fallback where a link
//! cannot be made — another volume, a file system without them.
//!
//! The images are device-local and live where `XMIP_PLAYGROUND_IMAGES` says,
//! which `Start-XmipTest` sets under `.local-work/` (`CONTRIBUTING.md`:
//! `.local-work` is where). Told nothing, nothing is linked and the base
//! binary is run under its own name, which is what a roll started by hand and
//! this crate's own tests do.

use std::io;
use std::path::{Path, PathBuf};

use crate::environment::image_directory;

/// What the roll, the cluster and a node are called where no cluster names
/// them — the names ADR-0053 gave them, which stay the fallback.
const SUITE: &str = "xmip-playground";

/// The word that marks a node, so that what a process is is carried by the
/// name's own shape rather than by words a node may not be called. The owner,
/// 2026-09-20: *a node is a node and can have one or more roles, roll is
/// something different.*
const NODE: &str = "node";

/// The process name one instance of the Playground goes by:
/// `xmip-playground-<cluster>-<what>`, where `what` is `roll`, `cluster` or
/// `node-<name>`.
#[must_use]
pub fn named(cluster: &str, what: &str) -> String {
    format!("{SUITE}-{cluster}-{what}")
}

/// What the node called `name` of `cluster` goes by:
/// `xmip-playground-<cluster>-node-<name>`. A node called `roll` reads as
/// `xmip-playground-W1-node-roll` and is nobody's roll, which is why no name
/// has to be forbidden.
#[must_use]
pub fn node_named(cluster: &str, name: &str) -> String {
    named(cluster, &marked(name))
}

/// A node's word in a process name.
fn marked(name: &str) -> String {
    format!("{NODE}-{name}")
}

/// Why a node cannot be called `name`, if it cannot: a node's name becomes a
/// file name, so it takes the shape a cluster's name takes and nothing more.
/// No word is reserved — the kind marker of `node_named` carries the
/// distinction. REFUSED at the door, never mangled quietly (ADR-0055).
#[must_use]
pub fn refusal(name: &str) -> Option<String> {
    let shaped = name.starts_with(|first: char| first.is_ascii_alphabetic())
        && name
            .chars()
            .all(|letter| letter.is_ascii_alphanumeric() || letter == '-');
    if shaped {
        return None;
    }
    Some(format!(
        "REFUSED: a node's name is letters, digits and hyphens starting with a \
         letter, because it is the last word of {SUITE}-<cluster>-{NODE}-<node> \
         and a file name: not {name}"
    ))
}

/// The image the instance called `what` of `cluster` runs: a hard link to
/// `base` under the directory `XMIP_PLAYGROUND_IMAGES` names, a copy where a
/// link cannot be made, and `base` itself where no directory was named.
///
/// An image already there is used as it is, so a node the cluster restarts
/// runs the image it was started from rather than failing to replace a file
/// it still holds open.
///
/// # Errors
///
/// When the directory cannot be made, or the bytes can be neither linked nor
/// copied.
pub fn of(base: &Path, cluster: &str, what: &str) -> io::Result<PathBuf> {
    let Some(directory) = image_directory() else {
        return Ok(base.to_path_buf());
    };
    std::fs::create_dir_all(&directory)?;
    let image = directory.join(format!(
        "{}{}",
        named(cluster, what),
        std::env::consts::EXE_SUFFIX
    ));
    if image.is_file() {
        return Ok(image);
    }
    if std::fs::hard_link(base, &image).is_err() {
        std::fs::copy(base, &image)?;
    }
    Ok(image)
}

/// The image the node called `name` of `cluster` runs, which is [`of`] with
/// the node marker in front of the name.
///
/// # Errors
///
/// As [`of`].
pub fn of_node(base: &Path, cluster: &str, name: &str) -> io::Result<PathBuf> {
    of(base, cluster, &marked(name))
}

/// What this process is called: the stem of the image it is running, which is
/// the name the operating system lists it under, else `fallback` where there
/// is no reading it. A process declares the name it actually goes by
/// (ADR-0053 clause 3), so the declaration cannot drift from the process list.
#[must_use]
pub fn this_process(fallback: &str) -> String {
    std::env::current_exe()
        .ok()
        .as_deref()
        .and_then(Path::file_stem)
        .and_then(|stem| stem.to_str())
        .map_or_else(|| fallback.to_string(), ToString::to_string)
}

/// Take away every image this run made that is not still being run. A process
/// holds its own image open, so the one that clears the directory cannot
/// remove its own; `Stop-XmipTest` removes what is left, and the next roll on
/// the same cluster clears it before it starts.
pub fn clear() {
    let Some(directory) = image_directory() else {
        return;
    };
    if let Ok(entries) = std::fs::read_dir(&directory) {
        for entry in entries.flatten() {
            std::fs::remove_file(entry.path()).ok();
        }
    }
    std::fs::remove_dir(&directory).ok();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_name_says_the_suite_the_cluster_and_which_of_the_tree_it_is() {
        assert_eq!(named("W1", "roll"), "xmip-playground-W1-roll");
        assert_eq!(named("W1", "cluster"), "xmip-playground-W1-cluster");
        assert_eq!(node_named("W1", "alpha"), "xmip-playground-W1-node-alpha");
        // Every one still answers the owner's one line, Get-Process xmip-*.
        for name in [named("W1", "roll"), node_named("W1", "alpha")] {
            assert!(name.starts_with("xmip-"), "{name}");
        }
    }

    /// The owner, 2026-09-20: *a node is a node and can have one or more
    /// roles, roll is something different.* The marker keeps the two apart,
    /// so a node may be called anything a file may be called.
    #[test]
    fn a_node_called_roll_or_cluster_is_a_node_and_collides_with_nothing() {
        assert_eq!(refusal("roll"), None);
        assert_eq!(refusal("cluster"), None);
        assert_eq!(node_named("U1", "roll"), "xmip-playground-U1-node-roll");
        assert_ne!(node_named("U1", "roll"), named("U1", "roll"));
        assert_ne!(node_named("U1", "cluster"), named("U1", "cluster"));
    }

    #[test]
    fn a_node_named_what_no_file_can_be_called_is_refused_before_anything_spawns() {
        assert_eq!(refusal("alpha"), None);
        assert_eq!(refusal("node-01"), None);
        for wrong in ["", "9lives", "a/b", "a b", "a.b", "a:b"] {
            let refusal = refusal(wrong).unwrap_or_else(|| panic!("{wrong}"));
            assert!(refusal.starts_with("REFUSED"), "{refusal}");
            assert!(refusal.contains("file name"), "{refusal}");
        }
    }

    /// Told no directory, nothing is linked: the base binary runs under its
    /// own name, which is what a hand-started roll and these tests do.
    #[test]
    fn no_image_directory_is_the_binary_itself() {
        let base = Path::new("target/debug/xmip-playground-node");
        assert_eq!(
            of_node(base, "W1", "alpha").expect("nothing to do"),
            base.to_path_buf()
        );
    }

    /// A process declares the name it goes by, and the name it goes by is its
    /// image. Under cargo that image is the test binary, never the fallback.
    #[test]
    fn a_process_is_called_what_its_image_is_called() {
        let said = this_process("xmip-playground-node");
        let running = std::env::current_exe().expect("a test has an image");
        assert_eq!(
            said,
            running
                .file_stem()
                .and_then(|stem| stem.to_str())
                .expect("a readable stem")
        );
    }
}
