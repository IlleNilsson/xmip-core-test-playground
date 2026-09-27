//! When the cluster counts a node alive, starting or hung.
//!
//! The owner, 2026-09-26: a node that has not shown it is alive counts as
//! starting for **ten seconds**, after which its silence is a hang and the
//! cluster restarts it. On 2026-09-25 an agent had allowed two minutes,
//! because the cluster judged silence by the rounds a node published and a
//! brutal roll's first round took longer than three of them on sixteen cores
//! with seventeen nodes: every node was killed as hung and restarted into the
//! same wait, and the status read *Nodes: none*. The cause was the measure,
//! not the allowance. A node now beats (`heartbeat.rs`) — once as it starts,
//! then every beat interval while it works — and the cluster judges by beats
//! alone: none within the starting allowance, or none for the missed-beats
//! count of intervals after the last one — ten seconds either way — is a
//! hang. How long a round takes decides nothing about whether a node is
//! alive.

use std::time::Duration;

/// How the cluster judges a node's liveness, and the beat interval it tells
/// every node it starts, so the node and its judge agree.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Liveness {
    /// How often a node beats.
    pub(crate) beat: Duration,
    /// How many beat intervals may pass after the last beat before the node
    /// is hung.
    pub(crate) missed: u32,
    /// How long a node that has not beaten yet counts as starting.
    pub(crate) starting: Duration,
}

impl Liveness {
    /// The owner's numbers, 2026-09-26: ten seconds to show it is alive, a
    /// beat every tenth of a second — *if something takes more than a
    /// millisecond, apart from load, something is wrong* — and ten seconds
    /// of silence after the last beat, a hundred beats missed, as the outer
    /// bound after which silence is a hang.
    pub(crate) const OWNERS: Self = Self {
        beat: Duration::from_millis(100),
        missed: 100,
        starting: Duration::from_secs(10),
    };

    /// Whether a node started `running` ago is hung, given how long ago it
    /// last beat — `None` when it has not beaten since it started.
    pub(crate) fn hung(&self, running: Duration, quiet: Option<Duration>) -> bool {
        quiet.map_or(running > self.starting, |quiet| quiet > self.silence())
    }

    /// How long a node that has beaten may stay silent.
    pub(crate) fn silence(&self) -> Duration {
        self.beat * self.missed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const fn secs(seconds: u64) -> Duration {
        Duration::from_secs(seconds)
    }

    /// The owner's ten seconds, 2026-09-26: silent from the start is starting
    /// until then and hung after.
    #[test]
    fn a_node_that_never_beats_is_starting_for_ten_seconds_and_hung_after() {
        let owners = Liveness::OWNERS;
        assert_eq!(owners.starting, secs(10));
        assert_eq!(owners.silence(), secs(10), "ten seconds is the outer bound");
        assert_eq!(owners.beat, Duration::from_millis(100));
        assert!(!owners.hung(Duration::from_millis(9_900), None));
        assert!(owners.hung(Duration::from_millis(10_100), None));
    }

    /// A node that beats is alive however long its first round runs.
    #[test]
    fn a_beating_node_is_alive_however_long_it_has_run() {
        let owners = Liveness::OWNERS;
        for running in [secs(11), secs(120), secs(3_600)] {
            assert!(!owners.hung(running, Some(Duration::from_millis(900))));
            assert!(!owners.hung(running, Some(Duration::from_millis(9_900))));
            assert!(!owners.hung(running, Some(owners.silence())));
        }
    }

    /// A node that stops beating is hung once the missed beats have passed.
    #[test]
    fn a_node_that_stops_beating_is_hung_after_the_missed_beats() {
        let owners = Liveness::OWNERS;
        let past = owners.silence() + Duration::from_millis(1);
        assert!(owners.hung(secs(12), Some(past)), "during its first round");
        assert!(owners.hung(secs(600), Some(past)), "or any later one");
    }
}
