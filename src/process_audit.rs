//! What a Playground process audits (ADR-0062 clause 1): the roll, the
//! cluster and every node record that they started and stopped, and every
//! failure that ended one or cost it a round, through `xmip-core-audit`'s
//! [`ProgramAudit`] — the one place a record is written. Nothing here writes a
//! record of its own or reaches the operating system's log: where no audit
//! directory is configured (`XMIP_AUDIT_DIRECTORY`), the audit capability
//! sends the record there itself.
//!
//! Each process holds its own [`ProgramAudit`], named for its image as
//! ADR-0053 names it, and watches its panics with it; these three calls are
//! how the three processes say the same things the same way.

use std::collections::BTreeMap;

use xaudit::program_audit::ProgramAudit;
use xcore::{ExecutionPhase, Severity};

/// Record that the process started, with what it was started as.
pub fn start(audit: &ProgramAudit, properties: &[(&str, &str)]) {
    kept(audit.record(
        "start",
        ExecutionPhase::Begin,
        Severity::Information,
        None,
        owned(properties),
    ));
}

/// Record that the process stopped as it meant to.
pub fn stop(audit: &ProgramAudit, properties: &[(&str, &str)]) {
    kept(audit.record(
        "stop",
        ExecutionPhase::Finished,
        Severity::Information,
        None,
        owned(properties),
    ));
}

/// Say `problem` on stderr, as the process always has, and record it as the
/// failure of `action`: a failure is never only on a screen (ADR-0062
/// clause 4).
pub fn fail(audit: &ProgramAudit, action: &str, problem: &str) {
    eprintln!("{problem}");
    kept(audit.failed(action, problem));
}

/// A record neither the sink nor the operating system's log kept is said on
/// stderr, the last place left.
fn kept<T>(outcome: Result<T, xaudit::AuditError>) {
    if let Err(error) = outcome {
        eprintln!("audit: {error}");
    }
}

fn owned(properties: &[(&str, &str)]) -> BTreeMap<String, String> {
    properties
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::support::scratch;

    #[test]
    fn a_process_records_its_start_failure_and_stop_where_it_was_told() {
        let directory = scratch("process-audit");
        let audit = ProgramAudit::new("xmip-playground-Zt-roll", Some(&directory));

        start(&audit, &[("cluster", "Zt"), ("stress", "calm")]);
        fail(&audit, "publish", "could not write the snapshot");
        stop(&audit, &[("rounds", "3")]);

        let text = std::fs::read_to_string(audit.file().expect("a file sink")).expect("read");
        for said in [
            "program = \"xmip-playground-Zt-roll\"",
            "action = \"start\"",
            "\"cluster\" = \"Zt\"",
            "action = \"publish\"",
            "could not write the snapshot",
            "action = \"stop\"",
            "\"rounds\" = \"3\"",
        ] {
            assert!(text.contains(said), "{said} in {text}");
        }
        std::fs::remove_dir_all(&directory).ok();
    }
}
