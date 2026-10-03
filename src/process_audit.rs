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

use node::Declaration;
use xaudit::program_audit::ProgramAudit;
use xcore::{ExecutionPhase, Severity};

use crate::environment;

/// What the process declares of itself (ADR-0053 clause 3): `declaration`,
/// saying `hidden = "true"` beside it where the run declared itself hidden
/// ([`environment::hidden`]) — and then every record `audit` makes from
/// here says so too (ADR-0028, amendment 2026-09-30). The roll, the cluster
/// and every node declare through this, so the three say it the same way.
///
/// # Errors
///
/// When the declaration cannot say it, as [`Declaration::with`] refuses.
pub fn declared(audit: &ProgramAudit, declaration: Declaration) -> Result<Declaration, String> {
    declared_as(audit, declaration, environment::hidden())
}

/// [`declared`], told whether the run is hidden rather than reading it.
fn declared_as(
    audit: &ProgramAudit,
    declaration: Declaration,
    hidden: bool,
) -> Result<Declaration, String> {
    if !hidden {
        return Ok(declaration);
    }
    audit.hide();
    declaration.with("hidden", "true")
}

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
    use crate::support::{cluster_name, cluster_root, scratch};
    use node::Purpose;

    /// The test cluster's name, and its roll's program name.
    fn named() -> (String, String) {
        let cluster = cluster_name().expect("the test cluster is named");
        let roll = crate::image::named(&cluster, "roll");
        (cluster, roll)
    }

    #[test]
    fn a_process_records_its_start_failure_and_stop_where_it_was_told() {
        let directory = scratch("process-audit");
        let (cluster, roll) = named();
        let audit = ProgramAudit::new(&roll, Some(&directory));

        start(&audit, &[("cluster", cluster.as_str()), ("stress", "calm")]);
        fail(&audit, "publish", "could not write the snapshot");
        stop(&audit, &[("rounds", "3")]);

        let text = std::fs::read_to_string(audit.file().expect("a file sink")).expect("read");
        let program = format!("program = \"{roll}\"");
        let property = format!("\"cluster\" = \"{cluster}\"");
        for said in [
            program.as_str(),
            "action = \"start\"",
            property.as_str(),
            "action = \"publish\"",
            "could not write the snapshot",
            "action = \"stop\"",
            "\"rounds\" = \"3\"",
        ] {
            assert!(text.contains(said), "{said} in {text}");
        }
        std::fs::remove_dir_all(&directory).ok();
    }

    #[test]
    fn a_hidden_run_says_so_in_the_declaration_and_on_every_record() {
        let directory = scratch("process-audit-hidden");
        let (cluster, roll) = named();
        let audit = ProgramAudit::new(&roll, Some(&directory));
        let bare = || Declaration::new(&roll, cluster_root(), Purpose::Test);

        let shown = declared_as(&audit, bare(), false).expect("declared");
        assert!(!format!("{shown:?}").contains("hidden"), "{shown:?}");
        assert!(!audit.hidden(), "a run that declared nothing is not hidden");

        let hidden = declared_as(&audit, bare(), true).expect("declared");
        assert!(format!("{hidden:?}").contains("hidden"), "{hidden:?}");
        start(&audit, &[("cluster", cluster.as_str())]);
        let text = std::fs::read_to_string(audit.file().expect("a file sink")).expect("read");
        assert!(text.contains("hidden = \"true\""), "{text}");
        std::fs::remove_dir_all(&directory).ok();
    }
}
