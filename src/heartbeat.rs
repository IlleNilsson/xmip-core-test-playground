//! [`Heartbeat`]: a node saying it is alive, apart from saying how its rounds
//! went.
//!
//! The owner, 2026-09-26: a node that has not shown it is alive counts as
//! starting for ten seconds, after which its silence is a hang — and *if
//! something takes more than a millisecond, apart from load, something is
//! wrong.* Until then the cluster judged a node silent by the rounds it
//! published, and a brutal roll's first round outlasts any allowance: on
//! 2026-09-25 seventeen nodes on sixteen cores were each killed as hung and
//! restarted into the same wait. So liveness and round completion are two
//! things now. A node beats first, before it loads anything or starts a
//! round, and then every tenth of a second from a thread of its own, into a
//! beat file beside the publication it already writes — the same channel, a
//! file in the same directory, kept tiny so a beat costs no more than a
//! small write. Whether a round is done decides nothing about whether the
//! node is alive.
//!
//! The beat is the Playground's: the cluster reads it, no surface does.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::JoinHandle;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use observe::now_unix_nanos;

/// One beat: when the node said it is alive, and how many rounds it had
/// finished by then — `0` while its first is under way.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Beat {
    /// When the node beat, by its own clock.
    pub unix_nanos: i64,
    /// The rounds it had finished by then.
    pub rounds: u64,
}

impl Beat {
    /// A beat read back, or `None` for text that is not one — a beat caught
    /// half-written is still a sign of life, and the reader counts it so.
    #[must_use]
    pub fn read(text: &str) -> Option<Self> {
        toml::from_str(text).ok()
    }
}

/// Where the node publishing to `publication` beats: beside it, as
/// `<name>.beat`. The node and the cluster both ask here.
#[must_use]
pub fn beside(publication: &Path) -> PathBuf {
    publication.with_extension("beat")
}

/// A node's heartbeat: a beat when started and then every interval until
/// dropped. A beat is a plain overwrite of a few bytes, neither flushed nor
/// renamed: a reader that catches one half-written sees a change all the
/// same, and a beat lost to a crash is exactly a missed beat. A beat that
/// cannot be written is not retried or said — the missing beat is what the
/// cluster watches for.
pub struct Heartbeat {
    path: PathBuf,
    /// The rounds finished; held while a beat is written, so the beating
    /// thread and a finished round never write over each other.
    rounds: Arc<Mutex<u64>>,
    stopping: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl Heartbeat {
    /// Beat now beside `publication`, and every `interval` from here on.
    #[must_use]
    pub fn start(publication: &Path, interval: Duration) -> Self {
        let path = beside(publication);
        let rounds = Arc::new(Mutex::new(0));
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        beat(&path, &rounds);
        let stopping = Arc::new(AtomicBool::new(false));
        let thread = {
            let (rounds, stopping) = (Arc::clone(&rounds), Arc::clone(&stopping));
            let path = path.clone();
            std::thread::spawn(move || {
                loop {
                    std::thread::park_timeout(interval);
                    if stopping.load(Ordering::Relaxed) {
                        break;
                    }
                    beat(&path, &rounds);
                }
            })
        };
        Self {
            path,
            rounds,
            stopping,
            thread: Some(thread),
        }
    }

    /// A round finished and was published: beat now, saying so, so the
    /// cluster reads the round at once — and the last one before the node
    /// leaves is never lost.
    pub fn round(&self) {
        *self.rounds.lock().unwrap_or_else(PoisonError::into_inner) += 1;
        beat(&self.path, &self.rounds);
    }
}

/// One beat, written over the last while the count is held.
fn beat(path: &Path, rounds: &Mutex<u64>) {
    let rounds = rounds.lock().unwrap_or_else(PoisonError::into_inner);
    let beat = Beat {
        unix_nanos: now_unix_nanos(),
        rounds: *rounds,
    };
    if let Ok(text) = toml::to_string(&beat) {
        std::fs::write(path, text).ok();
    }
}

impl Drop for Heartbeat {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            thread.thread().unpark();
            thread.join().ok();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::support::scratch;
    use std::time::Instant;

    /// The beat beside `path`, read again where one was caught half-written.
    fn read(path: &Path) -> Beat {
        (0..100)
            .find_map(|_| {
                let text = std::fs::read_to_string(beside(path)).ok();
                text.as_deref().and_then(Beat::read).or_else(|| {
                    std::thread::sleep(Duration::from_millis(1));
                    None
                })
            })
            .expect("a beat")
    }

    /// The owner, 2026-09-26: more than a millisecond, apart from load, is
    /// wrong. The first beat is on disk when `start` returns, within
    /// milliseconds — the bound is generous for a loaded runner, and it is
    /// not seconds.
    #[test]
    fn the_first_beat_is_written_within_milliseconds_of_the_start() {
        let dir = scratch("heartbeat-first");
        std::fs::create_dir_all(&dir).expect("the scratch directory");
        let path = dir.join("alpha.toml");
        let started = Instant::now();
        let heart = Heartbeat::start(&path, Duration::from_secs(60));
        let took = started.elapsed();
        assert_eq!(read(&path).rounds, 0, "beating before its first round");
        assert!(
            took < Duration::from_millis(100),
            "the first beat took {took:?}"
        );
        drop(heart);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A node keeps beating while no round finishes, and says the rounds it
    /// has finished once it has.
    #[test]
    fn a_node_beats_between_rounds_and_counts_them() {
        let dir = scratch("heartbeat");
        let path = dir.join("alpha.toml");
        let heart = Heartbeat::start(&path, Duration::from_millis(20));
        let first = read(&path);
        std::thread::sleep(Duration::from_millis(200));
        let later = read(&path);
        assert!(later.unix_nanos > first.unix_nanos, "it beat again");
        assert_eq!(later.rounds, 0);

        heart.round();
        assert_eq!(read(&path).rounds, 1, "a finished round is a beat at once");
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(read(&path).rounds, 1);
        let stopping = Instant::now();
        drop(heart);
        assert!(
            stopping.elapsed() < Duration::from_secs(1),
            "it stops at once"
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
