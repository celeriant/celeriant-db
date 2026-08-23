use std::collections::HashMap;

use crate::invariants::CheckResult;

/// Check a node's journal text for panic strings, core-dump signals, and
/// error storms. All checks are purely textual — no I/O.
pub fn journal_checks(node: &str, journal_text: &str) -> Vec<CheckResult> {
    vec![
        check_no_panics(node, journal_text),
        check_no_abort(node, journal_text),
        check_no_error_storm(node, journal_text),
    ]
}

/// DEFECT 2, red condition A: the leadership challenge panicked the shard.
///
/// `shard.rs` panics unconditionally when `set_node_role_via_s3` cannot
/// complete: `panic!("Election failed after retries: {e}")`. Separate from
/// `JournalNoPanics` — which catches it too, as any panic — because this
/// scenario exists to falsify THIS line and the report has to name it verbatim
/// rather than as "some panic happened". The barrier-timeout warning that
/// precedes it is carried along as context; it is not itself a failure.
pub fn check_no_election_panic(node: &str, text: &str) -> CheckResult {
    const NAME: &str = "NoElectionPanic";
    const PANIC: &str = "Election failed after retries";
    const CONTEXT: &str = "S3 catchup completion barrier timed out";

    let hit = text.lines().find(|l| l.contains(PANIC));
    let Some(hit) = hit else {
        let context: Vec<&str> = text.lines().filter(|l| l.contains(CONTEXT)).collect();
        return CheckResult::pass_with_detail(
            NAME,
            if context.is_empty() {
                format!("{node}: no \"{PANIC}\" in the journal")
            } else {
                // The proximate trigger fired and the node survived it: worth
                // saying out loud, because it means the run reached the path.
                format!(
                    "{node}: no \"{PANIC}\", but the S3 catchup barrier timed out {} time(s) — \
                     the promotion path was reached and survived",
                    context.len()
                )
            },
        );
    };
    let context = text
        .lines()
        .filter(|l| l.contains(CONTEXT))
        .map(|l| first_chars(l, 300))
        .collect::<Vec<_>>()
        .join(" | ");
    CheckResult::fail(
        NAME,
        if context.is_empty() {
            format!("{node}: {}", first_chars(hit, 300))
        } else {
            format!("{node}: {} [preceded by: {context}]", first_chars(hit, 300))
        },
    )
}

fn check_no_panics(node: &str, text: &str) -> CheckResult {
    const NAME: &str = "JournalNoPanics";
    for line in text.lines() {
        if line.contains("panicked at") || line.contains("PANIC:") || line.contains("BorrowMutError") {
            return CheckResult::fail(NAME, format!("{node}: {}", first_chars(line, 200)));
        }
    }
    CheckResult::pass(NAME)
}

fn check_no_abort(node: &str, text: &str) -> CheckResult {
    const NAME: &str = "JournalNoAbort";
    // systemd lines of interest:
    //   "celeriant.service: Main process exited, code=killed, status=6/ABRT"
    //   "celeriant.service: Main process exited, code=killed, status=11/SEGV"
    for line in text.lines() {
        if line.contains("celeriant.service: Main process exited, code=killed") {
            if line.contains("/ABRT") || line.contains("/SEGV") {
                return CheckResult::fail(NAME, format!("{node}: {}", first_chars(line, 200)));
            }
        }
    }
    CheckResult::pass(NAME)
}

/// Normalize a log line for dedup: strip timestamps and runs of digits,
/// replacing them with `#`. This collapses different-timestamp or
/// different-counter instances of the same logical message.
fn normalize_line(line: &str) -> String {
    // Use a simple state machine rather than pulling in a regex crate
    // (the crate has no regex dependency).
    let mut out = String::with_capacity(line.len());
    let mut in_digits = false;
    for ch in line.chars() {
        if ch.is_ascii_digit() {
            if !in_digits {
                out.push('#');
                in_digits = true;
            }
        } else {
            in_digits = false;
            out.push(ch);
        }
    }
    out
}

fn check_no_error_storm(node: &str, text: &str) -> CheckResult {
    const NAME: &str = "JournalNoErrorStorm";
    const STORM_THRESHOLD: usize = 2000;

    let mut counts: HashMap<String, usize> = HashMap::new();

    for line in text.lines() {
        // Only consider ERROR-level lines. journalctl output typically has
        // syslog priority markers or the word "ERROR" in structured logs.
        if !is_error_level(line) {
            continue;
        }
        let key = normalize_line(line);
        let count = counts.entry(key).or_insert(0);
        *count += 1;
    }

    for (msg, count) in &counts {
        if *count > STORM_THRESHOLD {
            return CheckResult::fail(
                NAME,
                format!("{node}: message repeated {count} times (threshold {STORM_THRESHOLD}): {}", first_chars(msg, 150)),
            );
        }
    }

    CheckResult::pass(NAME)
}

/// Heuristic: a line is ERROR-level if it contains ` ERROR ` or `<3>` (syslog
/// priority kernel-level critical) or the structured log field `level=error`.
fn is_error_level(line: &str) -> bool {
    line.contains(" ERROR ") || line.contains("level=error") || line.contains("<3>")
}

fn first_chars(s: &str, n: usize) -> &str {
    let end = s.char_indices().nth(n).map(|(i, _)| i).unwrap_or(s.len());
    &s[..end]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_issues_passes() {
        let journal = "Jun 05 12:00:00 node celeriant[123]: INFO starting up\n\
                       Jun 05 12:00:01 node celeriant[123]: INFO listening on :10000\n";
        let results = journal_checks("node1", journal);
        assert!(results.iter().all(|r| r.passed()), "{:?}", results);
    }

    #[test]
    fn panicked_at_detected() {
        let journal = "Jun 05 12:00:05 node celeriant[123]: thread 'main' panicked at 'index out of bounds'\n";
        let results = journal_checks("node1", journal);
        let fail = results.iter().find(|r| r.name == "JournalNoPanics" && !r.passed());
        assert!(fail.is_some(), "{:?}", results);
        assert!(fail.unwrap().detail.contains("node1"));
    }

    #[test]
    fn panic_keyword_detected() {
        let journal = "some log line PANIC: something went wrong\n";
        let results = journal_checks("node1", journal);
        let fail = results.iter().find(|r| r.name == "JournalNoPanics" && !r.passed());
        assert!(fail.is_some(), "{:?}", results);
    }

    #[test]
    fn borrow_mut_error_detected() {
        let journal = "thread panicked due to BorrowMutError in module\n";
        let results = journal_checks("node1", journal);
        let fail = results.iter().find(|r| r.name == "JournalNoPanics" && !r.passed());
        assert!(fail.is_some(), "{:?}", results);
    }

    #[test]
    fn abort_signal_detected() {
        let journal = "Jun 05 12:01:00 node systemd[1]: celeriant.service: Main process exited, code=killed, status=6/ABRT\n";
        let results = journal_checks("node1", journal);
        let fail = results.iter().find(|r| r.name == "JournalNoAbort" && !r.passed());
        assert!(fail.is_some(), "{:?}", results);
    }

    #[test]
    fn segv_signal_detected() {
        let journal = "Jun 05 12:01:00 node systemd[1]: celeriant.service: Main process exited, code=killed, status=11/SEGV\n";
        let results = journal_checks("node1", journal);
        let fail = results.iter().find(|r| r.name == "JournalNoAbort" && !r.passed());
        assert!(fail.is_some(), "{:?}", results);
    }

    #[test]
    fn non_abrt_exit_does_not_trigger_abort_check() {
        // code=exited is a normal graceful stop.
        let journal = "celeriant.service: Main process exited, code=exited, status=0/SUCCESS\n";
        let results = journal_checks("node1", journal);
        let fail = results.iter().find(|r| r.name == "JournalNoAbort" && !r.passed());
        assert!(fail.is_none(), "{:?}", results);
    }

    #[test]
    fn the_election_panic_is_reported_verbatim_with_its_trigger() {
        // The field sequence, both lines. The report must carry the panic text
        // itself — "a panic occurred" is not enough to accept a later fix.
        let journal = "\
Jun 05 10:25:32 cs2 celeriant[9]: WARN S3 catchup completion barrier timed out; bailing (role=Promoting)\n\
Jun 05 10:25:32 cs2 celeriant[9]: thread 'main' panicked at shard.rs:1296: Election failed after retries: unavailable: Could not catch up WAL via S3\n";
        let r = check_no_election_panic("cs2", journal);
        assert!(r.failed(), "{}", r.detail);
        assert!(r.detail.contains("Election failed after retries: unavailable: Could not catch up WAL via S3"), "{}", r.detail);
        assert!(r.detail.contains("barrier timed out"), "{}", r.detail);
    }

    #[test]
    fn a_barrier_timeout_the_node_survived_is_reported_but_not_a_failure() {
        // Reaching the S3 catch-up path without dying is the outcome a fix
        // should produce, and the run has to be able to say it got there.
        let journal = "Jun 05 10:25:32 cs2 celeriant[9]: WARN S3 catchup completion barrier timed out; bailing (role=Promoting)\n";
        let r = check_no_election_panic("cs2", journal);
        assert!(r.passed(), "{}", r.detail);
        assert!(r.detail.contains("promotion path was reached and survived"), "{}", r.detail);
    }

    #[test]
    fn a_clean_journal_passes_the_election_check() {
        let r = check_no_election_panic("cs1", "INFO Lease epoch 1 -> 2, shards -> Promoting\n");
        assert!(r.passed(), "{}", r.detail);
    }

    #[test]
    fn error_storm_detected() {
        let mut journal = String::new();
        for i in 0..2001 {
            journal.push_str(&format!("Jun 05 12:00:{:02} node celeriant: ERROR failed to connect to S3: timeout {i}\n", i % 60));
        }
        let results = journal_checks("node1", &journal);
        let fail = results.iter().find(|r| r.name == "JournalNoErrorStorm" && !r.passed());
        assert!(fail.is_some(), "expected storm failure");
        assert!(fail.unwrap().detail.contains("2001"));
    }

    #[test]
    fn error_storm_exactly_at_threshold_passes() {
        // Exactly 2000 identical errors — at the threshold, not exceeding it.
        let line = "Jun 05 12:00:00 node celeriant: ERROR repeated message\n";
        let journal = line.repeat(2000);
        let results = journal_checks("node1", &journal);
        let fail = results.iter().find(|r| r.name == "JournalNoErrorStorm" && !r.passed());
        assert!(fail.is_none(), "2000 is at threshold, should pass: {:?}", results);
    }

    #[test]
    fn normalize_collapses_digits() {
        let a = normalize_line("ERROR at 2026-06-05 12:34:56 count=42 seq=12345");
        let b = normalize_line("ERROR at 2026-06-05 12:34:57 count=43 seq=12346");
        assert_eq!(a, b, "different timestamps/counts should normalize to same key");
    }

    #[test]
    fn warn_level_not_counted_in_storm() {
        let mut journal = String::new();
        for _ in 0..3000 {
            journal.push_str("Jun 05 12:00:00 node celeriant: WARN replication lag 50ms\n");
        }
        let results = journal_checks("node1", &journal);
        let fail = results.iter().find(|r| r.name == "JournalNoErrorStorm" && !r.passed());
        assert!(fail.is_none(), "WARN should not trigger storm: {:?}", results);
    }
}
