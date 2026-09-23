//! The roll's environment: every switch a roll reads, as the variable it
//! reads it from. The variables are external names and keep the prefix
//! (ADR-0030); `Xmip/New-XmipPlaygroundEnvironment.ps1` sets them, and
//! `test/XmipTest.Test.ps1` holds that file against this one, so a switch the
//! roll reads is a switch an operator can reach.

use std::path::PathBuf;
use std::time::Duration;

use crate::complement;
use crate::roster::Roster;
use crate::scenario;
use crate::stress::Stress;
use crate::switch::{names, node_is_online};

/// The scenarios named in `XMIP_PLAYGROUND_SCENARIOS`: none named is every
/// one.
///
/// # Errors
///
/// When a name is no scenario, as [`scenario::chosen`] refuses it.
pub fn scenarios() -> Result<Vec<String>, String> {
    let raw = std::env::var("XMIP_PLAYGROUND_SCENARIOS").ok();
    scenario::chosen(raw.as_deref())
}

/// What a roll was **told** to spawn, if it was told at all:
/// `XMIP_PLAYGROUND_NODE_NAMES` names the nodes outright, and
/// `XMIP_PLAYGROUND_NODES` says how many instead, of which `0` is none at any
/// level. `None` when neither is set, and when the count is set but empty —
/// nothing was said, which is the level's full complement (ADR-0059,
/// amendment 2026-09-19).
#[must_use]
pub fn told() -> Option<Told> {
    if let Ok(raw) = std::env::var("XMIP_PLAYGROUND_NODE_NAMES") {
        return Some(Told::Named(names(&raw)));
    }
    let count = std::env::var("XMIP_PLAYGROUND_NODES").ok()?;

    Some(Told::Count(count.trim().parse::<usize>().ok()?))
}

/// How many nodes, or which: an operator says one or the other.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Told {
    /// The nodes by name, each declaring what it was given and no more.
    Named(Vec<String>),
    /// How many, numbered `node-01` up and dealt over the message path, as
    /// the level's own complement is: the owner, 2026-09-23, wanting to say
    /// how many nodes a cluster has without naming each one. A count that
    /// cannot cover the path leaves every node declaring nothing, which is
    /// what `complement::of_count` already does for a small level.
    Count(usize),
}

/// The roster a roll spawns: the nodes [`node_names`] was told, each declaring
/// what `XMIP_PLAYGROUND_NODE_CAPABILITIES` gives it — `R1=receive,
/// P1=process+send`, comma separated, a node it does not name declaring
/// nothing — or, told nothing, the level's full complement dealt over the
/// message path (`complement.rs`). Every node carries the online capability
/// the environment says (ADR-0045, ADR-0056). Nothing is read out of a node's
/// name.
///
/// # Errors
///
/// When a word is no capability, or a capability was given to a node that is
/// no node of this roll: REFUSED, naming both sides (ADR-0055).
pub fn roster(stress: Stress) -> Result<Roster, String> {
    let declared = std::env::var("XMIP_PLAYGROUND_NODE_CAPABILITIES").unwrap_or_default();
    let mut roster = match told() {
        Some(Told::Named(named)) => Roster::declaring(&named, &declared)?,
        Some(Told::Count(count)) if declared.trim().is_empty() => complement::of_count(count),
        Some(Told::Count(count)) => {
            let dealt = complement::of_count(count);
            let names: Vec<String> = dealt.names().into_iter().map(str::to_string).collect();
            Roster::declaring(&names, &declared)?
        }
        None => complement::full(stress),
    };
    let nodes: Vec<String> = roster.names().into_iter().map(str::to_string).collect();
    for node in &nodes {
        let capability = roster.capability(node).with_online(node_is_online(node));
        roster = roster.declared(node, capability);
    }
    Ok(roster)
}

/// Where the roll publishes its snapshot, history and activity: the three
/// variables the cmdlet sets — `XMIP_PLAYGROUND_SNAPSHOT`, `_HISTORY`,
/// `_ACTIVITY` — else `<cluster>-snapshot.toml` and kin in the temp
/// directory, which the GUI defaults to as well.
#[must_use]
pub fn publish_paths(cluster: &str) -> (PathBuf, PathBuf, PathBuf) {
    (
        env_path(
            "XMIP_PLAYGROUND_SNAPSHOT",
            &format!("{cluster}-snapshot.toml"),
        ),
        env_path(
            "XMIP_PLAYGROUND_HISTORY",
            &format!("{cluster}-history.toml"),
        ),
        env_path(
            "XMIP_PLAYGROUND_ACTIVITY",
            &format!("{cluster}-activity.toml"),
        ),
    )
}

/// Where the per-instance images of this run go, from
/// `XMIP_PLAYGROUND_IMAGES`: the device-local directory a cluster's roll, its
/// cluster process and its nodes are linked into, so each runs under a name
/// that says which cluster and which node it is (ADR-0053, amendment
/// 2026-09-20). `None` where nothing named one, and then nothing is linked and
/// every process keeps the binary's own name.
#[must_use]
pub fn image_directory() -> Option<PathBuf> {
    std::env::var_os("XMIP_PLAYGROUND_IMAGES")
        .filter(|named| !named.is_empty())
        .map(PathBuf::from)
}

fn env_path(variable: &str, default: &str) -> PathBuf {
    std::env::var_os(variable).map_or_else(|| std::env::temp_dir().join(default), PathBuf::from)
}

/// The `HeavyLoad` payload size: `XMIP_PLAYGROUND_LOAD_BYTES` if set — a
/// plain number or a human size like `512mb` or `2gb` — else a megabyte. Note
/// the memory: peak is roughly twice this per pair, so a gigabyte wants a few
/// free.
#[must_use]
pub fn load_bytes() -> usize {
    let Some(raw) = std::env::var("XMIP_PLAYGROUND_LOAD_BYTES").ok() else {
        return 1024 * 1024;
    };
    let text = raw.trim().to_lowercase();
    let (number, unit) = text
        .find(|c: char| c.is_alphabetic())
        .map_or((text.as_str(), ""), |at| text.split_at(at));
    let scale: usize = match unit {
        "gb" | "g" => 1024 * 1024 * 1024,
        "mb" | "m" => 1024 * 1024,
        "kb" | "k" => 1024,
        _ => 1,
    };
    number
        .trim()
        .parse::<usize>()
        .map_or(1024 * 1024, |value| value.saturating_mul(scale))
}

/// The maximum wall-clock time to roll: `XMIP_PLAYGROUND_MAX_SECONDS` if set,
/// else no ceiling.
#[must_use]
pub fn max_seconds() -> Option<Duration> {
    std::env::var("XMIP_PLAYGROUND_MAX_SECONDS")
        .ok()
        .and_then(|raw| raw.trim().parse::<f64>().ok())
        .filter(|seconds| *seconds > 0.0)
        .map(Duration::from_secs_f64)
}

/// The factor on time: `XMIP_PLAYGROUND_TIME_FACTOR` if set, else `1.0` (real
/// time). Below one runs faster than real time, above one slower.
#[must_use]
pub fn time_factor() -> f64 {
    std::env::var("XMIP_PLAYGROUND_TIME_FACTOR")
        .ok()
        .and_then(|raw| raw.trim().parse::<f64>().ok())
        .unwrap_or(1.0)
}
