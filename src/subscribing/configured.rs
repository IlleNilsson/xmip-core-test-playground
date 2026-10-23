//! The node's configuration a process stage reads its Subscriptions from:
//! the Playground's cluster configuration with the run's names and the
//! bindings that place every Application on the run's nodes, sliced as
//! deployment slices it. Split from `subscribing.rs` on 2026-10-13, when the
//! bindings came to be written from the sections. The file names no
//! cluster: the run's is given to the slice as an environment gives its
//! own (`configure::slice_named`; ADR-0028, amendment 2026-10-21), and the
//! bindings' addresses are the run's directories, never the file's.

use std::path::{Path, PathBuf};

use node::Stage;

use configure::placed::{self, Placed};

use super::CLUSTER;
use crate::roster::Roster;

/// Write this node's configuration, sliced from the Playground's cluster
/// configuration with the run's names and the bindings that place every
/// Application the file designs on the run's nodes (`configure::placed`) — each Receive Location
/// on the first node that declared receive, each Send Port on the first
/// that declared send — into `<shared>/configuration`, and say where it is.
pub(super) fn configured(
    node: &str,
    name: &str,
    roster: &Roster,
    shared: &Path,
) -> Result<PathBuf, String> {
    let cluster = observe::Scope::new(node)
        .segments()
        .next()
        .unwrap_or_default();
    let at = |stage: Stage| roster.with(stage).first().map(ToString::to_string);
    let receives = at(Stage::Receive).ok_or("no node declares receive")?;
    let sends = at(Stage::Send).ok_or("no node declares send")?;
    let inbox = |to: &str, stage: Stage| {
        shared
            .join("handoff")
            .join(to)
            .join(stage.name())
            .display()
            .to_string()
    };
    let directory = shared.join("configuration");
    std::fs::create_dir_all(&directory).map_err(|error| error.to_string())?;
    let (inbox, outbox) = (inbox(&receives, Stage::Process), inbox(&sends, Stage::Send));
    let nodes = [
        (receives, vec!["receiving".to_string()]),
        (sends, vec!["sending".to_string()]),
    ];
    let placements = placed::placements(CLUSTER, &nodes)?;
    let bound = placed::written(&placements, |placement| placed::Binding {
        transport: "xmip-core-transport-file".to_string(),
        address: match placement.placed {
            Placed::Receive => inbox.clone(),
            Placed::Send => outbox.clone(),
        },
        settings: Vec::new(),
        accepting: false,
    });
    let file =
        format!("{CLUSTER}\n{bound}\n[nodes.{name}.service]\nname = \"xmip-playground-{name}\"\n");
    let text = configure::slice_named(&file, name, cluster)?;
    let path = directory.join(format!("{name}.node.toml"));
    std::fs::write(&path, &text).map_err(|error| format!("{}: {error}", path.display()))?;
    Ok(path)
}
