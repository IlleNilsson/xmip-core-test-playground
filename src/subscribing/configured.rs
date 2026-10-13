//! The node's configuration a process stage reads its Subscriptions from:
//! the Playground's cluster configuration with the run's names and the
//! bindings that place every Application on the run's nodes, sliced as
//! deployment slices it. Split from `subscribing.rs` on 2026-10-13, when the
//! bindings came to be written from the sections.

use std::path::{Path, PathBuf};

use node::Stage;

use super::CLUSTER;
use crate::roster::Roster;

/// Write this node's configuration, sliced from the Playground's cluster
/// configuration with the run's names and the bindings that place every
/// Application `designed` holds on the run's nodes — each Receive Location
/// on the first node that declared receive, each Send Port on the first
/// that declared send — into `<shared>/configuration`, and say where it is.
pub(super) fn configured(
    node: &str,
    name: &str,
    roster: &Roster,
    shared: &Path,
    designed: &[configure::XmipApplication],
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
    let written = |file: &Path, text: &str| {
        std::fs::write(file, text).map_err(|error| format!("{}: {error}", file.display()))
    };
    let (inbox, outbox) = (inbox(&receives, Stage::Process), inbox(&sends, Stage::Send));
    let mut bindings: Vec<String> = Vec::new();
    for application in designed {
        bindings.push(format!(
            "[[applications]]\nname = {}\n\n",
            quoted(&application.name)
        ));
        let receive = application.receive_locations.iter().map(|one| &one.name);
        let send = application.send_ports.iter().map(|one| &one.name);
        for (list, names, node, address) in [
            (
                "receive_locations",
                receive.collect::<Vec<_>>(),
                &receives,
                &inbox,
            ),
            ("send_ports", send.collect(), &sends, &outbox),
        ] {
            for bound in names {
                bindings.push(format!(
                    "[[applications.{list}]]\nname = {}\nnode = {}\nstart = true\n\
                     transport = \"xmip-core-transport-file\"\naddress = {}\n\n",
                    quoted(bound),
                    quoted(node),
                    quoted(address),
                ));
            }
        }
    }
    let file = format!(
        "[service]\nname = \"xmip-playground\"\ncluster_name = {}\n\n{}{CLUSTER}\n\
         [nodes.{name}.service]\nname = \"xmip-playground-{name}\"\n",
        quoted(cluster),
        bindings.concat(),
    );
    let text = configure::slice(&file, name)?;
    let path = directory.join(format!("{name}.node.toml"));
    written(&path, &text)?;
    Ok(path)
}

/// `text` as a TOML string, quoted and escaped as TOML writes it.
fn quoted(text: &str) -> String {
    toml::Value::String(text.to_string()).to_string()
}
