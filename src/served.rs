//! Every node of a run that is neither real nor simulated: started by the
//! run as `xmip-service`, a process of the assembled service, on this
//! computer — test level 1 with nodes the same code runs as on machines of
//! their own (ADR-0028, amendment 2026-10-22). The run is their environment
//! and gives each what an environment gives a machine's node: its slice of
//! the Playground's cluster file under the run's cluster name, its
//! certificate from the Playground's authority, kept across runs
//! (`environment::authority`, `provision::node`), each of
//! its planes an address on loopback where a machine's has its own, the
//! name of each Storage node resolved to the address it serves on, and the
//! bindings that place the file's Applications on the run's nodes — each
//! Receive Location a drop folder of the run's, each Send Port a folder the
//! run reads (`bindings`). Every node that does not keep Xmip Storage
//! reaches the Storage nodes over Xmip's mutual TLS, as on machines of
//! their own.
//!
//! The rig reads what each publishes over its local endpoint, as it reads a
//! real node's (`real::RealNode`), and drives them through their Receive
//! Locations (`carried`). It stops them by ending their input, the stop a
//! node a test started takes, and waits for each to drain.

mod given;

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::time::{Duration, Instant};

pub use crate::carried::Binding;
use crate::real::RealNode;
use crate::roster::Roster;

pub use given::endpoint;
use given::{cluster_text, configured, placed};

/// How long a node may take to say it accepts work: a bound on the test,
/// never on Xmip.
const STARTING: Duration = Duration::from_secs(60);

/// How long a node may take to drain once its input ends.
const STOPPING: Duration = Duration::from_secs(30);

/// The nodes the run started, and where their Applications are bound.
pub struct Served {
    started: Vec<Started>,
    pub bindings: Vec<Binding>,
}

/// One node the run started: whether it keeps Xmip Storage, and its
/// process.
struct Started {
    stores: bool,
    child: Child,
}

/// The roles each node of the run declares, from the run's cluster file.
fn roles() -> Result<Vec<(String, Vec<String>)>, String> {
    let (_, text) = crate::environment::cluster_file()?;
    configure::cluster::roles(&text)
}

/// Whether any of `names` declares the Storage role: only then can the
/// nodes the run starts reach Xmip Storage, and only then does it start
/// them, every node simulated where none does.
///
/// # Errors
///
/// Where the run's cluster file cannot be read.
pub fn stores(names: &[String]) -> Result<bool, String> {
    Ok(roles()?
        .iter()
        .any(|node| names.contains(&node.0) && holding(node, &["storage"])))
}

impl Served {
    /// Start every node of `names`, a run of `cluster` whose roster is
    /// `roster`, as the `xmip-service` at `service`, beneath `base`: the
    /// nodes keeping Xmip Storage first, the rest once those accept work.
    /// The real nodes the rig reads them as, each at its local endpoint.
    ///
    /// # Errors
    ///
    /// REFUSED, in words naming the node, where its configuration cannot be
    /// written, its certificate issued, or it does not start.
    pub fn start(
        (cluster, roster): (&str, &Roster),
        names: &[String],
        (base, service): (&Path, &Path),
    ) -> Result<(Self, Vec<RealNode>), String> {
        let area = base.join("served");
        let declared: Vec<(String, Vec<String>)> = roles()?
            .into_iter()
            .filter(|(name, _)| names.contains(name))
            .collect();
        let placed = placed(&declared, &area)?;
        let text = cluster_text(cluster, &declared, &placed)?;
        let mut started = Vec::new();
        let stores: Vec<&(String, Vec<String>)> = declared
            .iter()
            .filter(|node| holding(node, &["storage"]))
            .collect();
        let rest: Vec<&(String, Vec<String>)> = declared
            .iter()
            .filter(|node| !holding(node, &["storage"]))
            .collect();
        for group in [stores, rest] {
            let mut waiting = Vec::new();
            for (name, roles) in group {
                let stores = roles.iter().any(|role| role == "storage");
                let spawning = configured(&text, (cluster, name), &area)
                    .and_then(|configuration| spawned(service, &configuration, &area));
                match spawning {
                    Ok((child, said)) => {
                        started.push(Started { stores, child });
                        waiting.push((name.clone(), said));
                    }
                    Err(problem) => {
                        started.iter_mut().for_each(Started::ended);
                        return Err(format!("REFUSED: {name}, as xmip-service: {problem}"));
                    }
                }
            }
            for (name, said) in waiting {
                let scope = format!("xmip:///{cluster}/node/{name}");
                if let Err(problem) = accepting(&said, &scope) {
                    started.iter_mut().for_each(Started::ended);
                    return Err(format!(
                        "REFUSED: {name}, started as xmip-service, {problem}"
                    ));
                }
            }
        }
        let real = declared
            .iter()
            .map(|(name, _)| {
                RealNode::local(name, &endpoint(cluster, name), roster.capability(name))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok((
            Self {
                started,
                bindings: placed,
            },
            real,
        ))
    }

    /// Stop every node it started: the nodes that keep no Storage first,
    /// each by the end of its input, then the Storage nodes, each waited
    /// for to drain, and one that does not killed.
    pub fn stop(self) {
        let (stores, rest): (Vec<Started>, Vec<Started>) =
            self.started.into_iter().partition(|one| one.stores);
        for group in [rest, stores] {
            let mut ending: Vec<Started> = group;
            for one in &mut ending {
                drop(one.child.stdin.take());
            }
            for mut one in ending {
                if !exited(&mut one.child, STOPPING) {
                    one.ended();
                }
            }
        }
    }
}

impl Started {
    /// Killed, where it did not drain.
    fn ended(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Whether `child` exited within `within`: asked each tenth of a second, a
/// stop's drain being what it waits out.
fn exited(child: &mut Child, within: Duration) -> bool {
    let deadline = Instant::now() + within;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return true,
            Ok(None) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(100));
            }
            _ => return false,
        }
    }
}

/// `service` started over `configuration` in a console of its own, its
/// input held open until the run ends it, what it says written to
/// `console.log` beside its configuration and handed on.
fn spawned(
    service: &Path,
    configuration: &Path,
    area: &Path,
) -> Result<(Child, Receiver<String>), String> {
    let folder = configuration.parent().unwrap_or(area);
    let mut command = Command::new(service);
    command
        .arg("--configuration")
        .arg(configuration)
        .args(["--console", "--purpose", "test"])
        .env(node::declaration::DIRECTORY_VARIABLE, area.join("process"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        command.creation_flags(CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW);
    }
    let mut child = command
        .spawn()
        .map_err(|error| format!("{} did not start: {error}", service.display()))?;
    let stdout = child.stdout.take().ok_or("its output")?;
    let log = folder.join("console.log");
    let (tell, told) = mpsc::channel();
    std::thread::spawn(move || {
        let mut kept = std::fs::File::create(&log).ok();
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if let Some(kept) = kept.as_mut() {
                let _ = writeln!(kept, "{line}");
            }
            let _ = tell.send(line);
        }
    });
    Ok((child, told))
}

/// Whether the node said, within the bound, that `scope` accepts work.
///
/// # Errors
///
/// What it said instead, where it never did.
fn accepting(said: &Receiver<String>, scope: &str) -> Result<(), String> {
    let deadline = Instant::now() + STARTING;
    let mut heard = Vec::new();
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        match said.recv_timeout(left) {
            Ok(line) if line.contains(&format!("{scope} accepts work")) => return Ok(()),
            Ok(line) => heard.push(line),
            Err(_) => {
                return Err(format!(
                    "never said it accepts work: {}",
                    heard.last().map_or("it said nothing", String::as_str)
                ));
            }
        }
    }
}

/// Whether `node` declares any of the roles `any`.
fn holding((_, roles): &(String, Vec<String>), any: &[&str]) -> bool {
    any.iter().any(|role| roles.iter().any(|word| word == role))
}
