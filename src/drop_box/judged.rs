//! What the roll has judged of every file dropped over one protocol: each
//! taken by exactly one receiving node, the split between them, or each
//! taken twice and by whom and each never taken (the owner, 2026-10-13:
//! *only one Rn should win*).

use std::collections::BTreeMap;
use std::time::SystemTime;

use observe::{Health, HealthRecord};

use super::{DropBox, GRACE};
use crate::parties::Parties;

/// How many files a judgment names before it counts the rest.
const NAMED: usize = 8;

/// What the roll has judged of every file dropped so far over one protocol.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Judged {
    pub files: u64,
    /// The files each receiving node took, by its name.
    pub by_node: BTreeMap<String, u64>,
    /// Each file taken more than once: `<party>/<file>` and who took it.
    pub twice: Vec<(String, Vec<String>)>,
    /// Each file nobody took within its grace.
    pub never: Vec<String>,
}

impl Judged {
    /// Judge every file dropped over `protocol` longer than [`GRACE`]
    /// before `now`, and clear its records.
    pub fn judge(
        &mut self,
        drop_box: &DropBox,
        protocol: &str,
        parties: &Parties,
        now: SystemTime,
    ) {
        for party in parties.names() {
            let Ok(dropped) = std::fs::read_dir(drop_box.dropped(protocol, party)) else {
                continue;
            };
            for record in dropped.flatten() {
                let due = record
                    .metadata()
                    .and_then(|meta| meta.modified())
                    .is_ok_and(|at| now.duration_since(at).is_ok_and(|age| age >= GRACE));
                if !due {
                    continue;
                }
                let file = record.file_name().to_string_lossy().to_string();
                let signed = drop_box.won(protocol, party, &file);
                let mut winners: Vec<String> = std::fs::read_dir(&signed)
                    .map(|read| {
                        read.flatten()
                            .map(|one| one.file_name().to_string_lossy().to_string())
                            .collect()
                    })
                    .unwrap_or_default();
                winners.sort();
                self.files += 1;
                let named = format!("{party}/{file}");
                match winners.as_slice() {
                    [] => self.never.push(named),
                    [one] => *self.by_node.entry(one.clone()).or_default() += 1,
                    _ => self.twice.push((named, winners)),
                }
                let _ = std::fs::remove_file(record.path());
                let _ = std::fs::remove_dir_all(&signed);
            }
        }
    }

    /// Whether every file judged was taken once.
    #[must_use]
    pub fn held(&self) -> bool {
        self.twice.is_empty() && self.never.is_empty()
    }

    /// The judgment in words.
    #[must_use]
    pub fn words(&self) -> String {
        let split: Vec<String> = self
            .by_node
            .iter()
            .map(|(node, files)| format!("{node} {files}"))
            .collect();
        let split = if split.is_empty() {
            "none yet".to_string()
        } else {
            split.join(", ")
        };
        if self.held() {
            return format!(
                "every file taken by exactly one receiving node: {} files, {split}",
                self.files
            );
        }
        let mut said = Vec::new();
        if !self.twice.is_empty() {
            let twice: Vec<String> = self
                .twice
                .iter()
                .map(|(file, by)| format!("{file} by {}", by.join(" and ")))
                .collect();
            said.push(format!(
                "{} taken twice: {}",
                self.twice.len(),
                listed(&twice)
            ));
        }
        if !self.never.is_empty() {
            said.push(format!(
                "{} never taken: {}",
                self.never.len(),
                listed(&self.never)
            ));
        }
        format!(
            "{} of {} files; once each: {split}",
            said.join("; "),
            self.files
        )
    }

    /// The judgment as the record at `<root>/drop-box/<protocol>`: Fine
    /// while every file was taken once, Done where one was not.
    #[must_use]
    pub fn record(&self, root: &str, protocol: &str, now: i64) -> HealthRecord {
        let held = self.held();
        HealthRecord {
            scope: format!("{root}/drop-box/{protocol}"),
            health: if held { Health::Fine } else { Health::Done },
            severity: if held { 0 } else { 80 },
            evidence: self.words(),
            observed_unix_nanos: now,
        }
    }
}

/// `files`, the first [`NAMED`] of them, and how many more.
fn listed(files: &[String]) -> String {
    let shown = files
        .iter()
        .take(NAMED)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ");
    match files.len().saturating_sub(NAMED) {
        0 => shown,
        more => format!("{shown} and {more} more"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::support::{declaring, test_cluster};

    #[test]
    fn a_file_taken_twice_or_never_is_named_in_words() {
        let cluster = test_cluster();
        let [first, second] = declaring(&cluster, "sending")[..] else {
            panic!("the test cluster has two nodes of a kind");
        };
        let judged = Judged {
            files: 3,
            by_node: BTreeMap::from([(first.to_string(), 1)]),
            twice: vec![("a/1".to_string(), vec![first.into(), second.into()])],
            never: vec!["a/2".to_string()],
        };
        assert_eq!(
            judged.words(),
            format!(
                "1 taken twice: a/1 by {first} and {second}; 1 never taken: a/2 of 3 files; \
                 once each: {first} 1"
            )
        );
        let record = judged.record("r", "ftp", 1);
        assert_eq!(record.health, Health::Done);
        assert_eq!(record.scope, "r/drop-box/ftp");
        let none = Judged::default();
        assert_eq!(none.record("r", "sftp", 1).health, Health::Fine);
    }
}
