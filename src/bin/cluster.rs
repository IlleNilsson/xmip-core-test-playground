//! One cluster: a System Process a roll spawns, which spawns and supervises
//! its own nodes (the owner, 2026-09-19: *even clusters have to be spawned as
//! processes during tests*).
//!
//! ```text
//! cluster --name <cluster> --shared <dir> --nodes <a[=capability],b,...>
//!         --stress <level> --rounds <n> --snapshot <path>
//!         [--online <a,b>] [--scenarios <a,b>] [--interval-ms <ms>]
//! ```
//!
//! `--nodes` carries what each node is **declared** with — `R1=receive`,
//! `P1=process+send`, or a bare name for a node that declares no stage of the
//! message path — and the cluster passes each node's own to it as `--can`
//! (ADR-0056). It infers nothing: a name is not a capability.
//!
//! The tree the owner asked for is three deep. `xmip-playground-roll` is the
//! test: it chooses the scenarios, sets the stress, judges and draws the
//! board. `xmip-playground-cluster` is this — the cluster itself, which owns
//! the store its nodes share, starts one `xmip-playground-node` per name,
//! watches each, restarts one that hangs, merges what each published, adds the
//! rollup at `xmip:///<cluster>/node` that no node can say about itself
//! (ADR-0027 decision 8), and publishes all of it to `--snapshot` atomically
//! each round. The roll merges that one file into the snapshot the prompt, the
//! CLI and the GUI read.
//!
//! Each of the three declares itself where it starts (ADR-0053): this one as
//! `xmip-playground-cluster` at `xmip:///<cluster>`, purpose Test.
//!
//! **Bad input is refused at the door** (ADR-0055). Every argument is checked
//! before a node is spawned: an unknown or malformed value is REFUSED with
//! exit code 2, naming what was wrong and what would be right. The scope root
//! is `XMIP_PLAYGROUND_CLUSTER`, as it is for a roll and for a node, so
//! `--name` must be that same word — records that hang under another cluster
//! would be found out only on a board.
//!
//! It rolls until `--rounds` are done — `0` means until stopped — or as soon
//! as `<shared>/stop` appears, checked between rounds. However it ends, it
//! stops its own nodes before it goes: nothing is orphaned.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use xmip_test_playground::cluster::{Cluster, Orders, node_binary};
use xmip_test_playground::image;
use xmip_test_playground::roster::Roster;
use xmip_test_playground::scenario;
use xmip_test_playground::stress::Stress;
use xmip_test_playground::switch::names;
use xmip_test_playground::{cluster_name, cluster_root, node_toml, write_atomic};

/// What the command line said.
struct Arguments {
    /// The cluster's name: the last segment of its scope root.
    name: String,
    /// The directory the cluster and its nodes share — handoffs, the
    /// contended tests' stores, the nodes' own snapshots, and `stop`.
    shared: PathBuf,
    /// Every node to spawn, one process each, with the capability each is
    /// declared with; the cluster passes it on and infers nothing.
    nodes: Roster,
    /// The nodes that may assume the internet (ADR-0045); `None` when the
    /// flag was not given, which leaves it to the environment.
    online: Option<Vec<String>>,
    stress: Stress,
    /// The scenarios every node is told to run; empty means every one.
    scenarios: Vec<String>,
    /// Rounds the cluster publishes before it stops; `0` runs until stopped.
    rounds: u64,
    /// Where the cluster publishes its own snapshot.
    snapshot: PathBuf,
    interval: Duration,
}

const USAGE: &str = "usage: cluster --name <cluster> --shared <dir> \
     --nodes <a[=capability],b,...> --stress <level> --rounds <n> \
     --snapshot <path> [--online <a,b>] [--scenarios <a,b>] [--interval-ms <ms>]";

fn main() -> ExitCode {
    let arguments = match parse(std::env::args().skip(1)) {
        Ok(arguments) => arguments,
        Err(problem) => return refuse(&problem),
    };
    let root = cluster_root();
    if cluster_name().as_deref() != Some(arguments.name.as_str()) {
        return refuse(&format!(
            "REFUSED: the scope root is XMIP_PLAYGROUND_CLUSTER and it says {}, not {}; \
             a cluster is started by a roll, which sets it.",
            cluster_name().unwrap_or_else(|| "nothing".to_string()),
            arguments.name
        ));
    }

    // What this process says of itself while it runs (ADR-0053): the cluster
    // is the scope it is, and everything the Playground runs is test. The name
    // is the image's own, so the declaration says what Get-Process says
    // (amendment 2026-09-20).
    let called = image::this_process("xmip-playground-cluster");
    let _declared = ::node::Declaration::new(called, &root, ::node::Purpose::Test)
        .declare()
        .map_err(|error| eprintln!("cluster {root}: could not declare itself: {error}"));

    let orders = Orders::of(arguments.stress, arguments.nodes.clone(), 0)
        .driving(&arguments.scenarios)
        .with_online(arguments.online.clone())
        .in_cluster(&arguments.name);
    if let Some(refusal) = orders.refusal() {
        return refuse(&refusal);
    }

    let binary = match node_binary() {
        Ok(binary) => binary,
        Err(error) => return refuse(&format!("REFUSED: {error}")),
    };
    let snapshots = arguments.shared.join("snapshots");
    let spawning = Cluster::spawn(&binary, &orders, &arguments.shared, &snapshots);
    let mut cluster = match spawning {
        Ok(cluster) => cluster,
        Err(error) => {
            eprintln!("cluster {root}: no nodes: {error}");
            return ExitCode::FAILURE;
        }
    };

    let stop = arguments.shared.join("stop");
    let mut round = 0;
    while arguments.rounds == 0 || round < arguments.rounds {
        if stop.exists() {
            break;
        }
        round += 1;

        let snapshot = cluster.tick();
        let text = node_toml(&root, &snapshot, cluster.hops());
        if let Err(error) = write_atomic(&arguments.snapshot, &text) {
            eprintln!(
                "cluster {root}: could not publish to {}: {error}",
                arguments.snapshot.display()
            );
        }

        if !arguments.interval.is_zero() && (arguments.rounds == 0 || round < arguments.rounds) {
            std::thread::sleep(arguments.interval);
        }
    }

    // However this ends, its nodes end with it: no orphans.
    cluster.stop();
    ExitCode::SUCCESS
}

/// Say what was wrong and what would be right, and start nothing (ADR-0055).
fn refuse(problem: &str) -> ExitCode {
    eprintln!("cluster: {problem}");
    eprintln!("{USAGE}");
    ExitCode::from(2)
}

/// `--flag value` pairs, every required one present and well-formed.
fn parse(args: impl Iterator<Item = String>) -> Result<Arguments, String> {
    let mut name = None;
    let mut shared = None;
    let mut nodes = None;
    let mut online = None;
    let mut stress = None;
    let mut scenarios = Vec::new();
    let mut rounds = None;
    let mut snapshot = None;
    let mut interval = Duration::from_millis(250);

    let mut args = args.peekable();
    while let Some(flag) = args.next() {
        let value = args.next().ok_or_else(|| format!("{flag} needs a value"))?;
        match flag.as_str() {
            "--name" => name = Some(cluster_named(&value)?),
            "--shared" => shared = Some(PathBuf::from(value)),
            "--nodes" => nodes = Some(Roster::parse(&value)?),
            "--online" => online = Some(names(&value)),
            "--stress" => stress = Some(level(&value)?),
            "--scenarios" => scenarios = scenario::chosen(Some(&value))?,
            "--rounds" => rounds = Some(number(&flag, &value)?),
            "--snapshot" => snapshot = Some(PathBuf::from(value)),
            "--interval-ms" => interval = Duration::from_millis(number(&flag, &value)?),
            other => {
                return Err(format!(
                    "REFUSED: no flag is called {other}; the flags are --name, --shared, \
                     --nodes, --online, --stress, --scenarios, --rounds, --snapshot and \
                     --interval-ms"
                ));
            }
        }
    }

    Ok(Arguments {
        name: name.ok_or("REFUSED: --name is required; a cluster is named, never invented")?,
        shared: shared.ok_or("REFUSED: --shared is required; it is the store the nodes share")?,
        nodes: nodes.ok_or(
            "REFUSED: --nodes is required; --nodes R1=receive,P1=process,S1=send \
             names them and what each declares",
        )?,
        online,
        stress: stress.ok_or(format!(
            "REFUSED: --stress is required; it is one of {}",
            Stress::NAMES.join(", ")
        ))?,
        scenarios,
        rounds: rounds.ok_or("REFUSED: --rounds is required; 0 rolls until stopped")?,
        snapshot: snapshot
            .ok_or("REFUSED: --snapshot is required; it is where the cluster publishes")?,
        interval,
    })
}

/// A cluster's name: letters, digits and hyphens, starting with a letter —
/// the same shape `Start-XmipTest -Cluster` takes.
fn cluster_named(value: &str) -> Result<String, String> {
    let named = value.trim();
    let shaped = named.starts_with(|first: char| first.is_ascii_alphabetic())
        && named
            .chars()
            .all(|letter| letter.is_ascii_alphanumeric() || letter == '-');
    if shaped {
        Ok(named.to_string())
    } else {
        Err(format!(
            "REFUSED: a cluster's name is letters, digits and hyphens starting with a \
             letter: not {named}"
        ))
    }
}

/// A stress level by name, or a refusal naming the four there are.
fn level(value: &str) -> Result<Stress, String> {
    Stress::parse(value).ok_or_else(|| {
        format!(
            "REFUSED: no stress level is called {value}; the levels are {}",
            Stress::NAMES.join(", ")
        )
    })
}

fn number(flag: &str, value: &str) -> Result<u64, String> {
    value
        .parse()
        .map_err(|_| format!("REFUSED: {flag} wants a whole number, not {value}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arguments(extra: &[&str]) -> Result<Arguments, String> {
        let required = [
            "--name",
            "Zt",
            "--shared",
            "s",
            "--nodes",
            "R1=receive,P1=process,S1=send",
            "--stress",
            "calm",
            "--rounds",
            "0",
            "--snapshot",
            "Zt-cluster.toml",
        ];
        parse(required.iter().chain(extra).map(ToString::to_string))
    }

    #[test]
    fn the_required_flags_are_read_and_the_optional_ones_have_the_nodes_defaults() {
        let bare = arguments(&[]).expect("the required flags suffice");
        assert_eq!(bare.name, "Zt");
        assert_eq!(bare.nodes.names(), ["R1", "P1", "S1"]);
        assert_eq!(bare.nodes.capability("P1").words(), "process");
        assert_eq!(bare.stress, Stress::Calm);
        assert_eq!(bare.interval, Duration::from_millis(250));
        assert!(bare.online.is_none() && bare.scenarios.is_empty());

        let told = arguments(&[
            "--online",
            "R1, S1",
            "--scenarios",
            "Round-Trip",
            "--interval-ms",
            "500",
        ])
        .expect("all three are well formed");
        assert_eq!(
            told.online.as_deref(),
            Some(["R1".to_string(), "S1".into()].as_slice())
        );
        assert_eq!(told.scenarios, ["round-trip"]);
        assert_eq!(told.interval, Duration::from_millis(500));
    }

    /// ADR-0055: a refusal says it refused, what was wrong and what would be
    /// right — and nothing is spawned, because parsing comes first.
    #[test]
    fn every_malformed_argument_is_refused_with_what_would_be_right() {
        for (extra, wrong, right) in [
            (["--stress", "gentle"], "gentle", "brutal"),
            (["--name", "9lives"], "9lives", "starting with a letter"),
            (["--rounds", "many"], "many", "whole number"),
            (["--scenarios", "pingpong"], "pingpong", "exclusive-claim"),
            (["--nodes", "R1=relay"], "relay", "receive, process, send"),
            (["--wobble", "yes"], "--wobble", "the flags are"),
        ] {
            let refusal = arguments(&extra)
                .err()
                .unwrap_or_else(|| panic!("{extra:?}"));
            assert!(refusal.starts_with("REFUSED"), "{refusal}");
            assert!(refusal.contains(wrong), "{refusal}");
            assert!(refusal.contains(right), "{refusal}");
        }

        for missing in [
            "--name",
            "--shared",
            "--nodes",
            "--stress",
            "--rounds",
            "--snapshot",
        ] {
            let given: Vec<String> = ["--name", "Zt", "--shared", "s", "--nodes", "R1=receive"]
                .into_iter()
                .chain(["--stress", "calm", "--rounds", "0", "--snapshot", "z.toml"])
                .map(ToString::to_string)
                .collect();
            let without: Vec<String> = given
                .chunks(2)
                .filter(|pair| pair[0] != missing)
                .flatten()
                .cloned()
                .collect();
            let refusal = parse(without.into_iter()).err().expect(missing);
            assert!(refusal.contains(missing), "{refusal}");
        }
    }
}
