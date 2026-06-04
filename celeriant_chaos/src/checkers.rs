//! History-based consistency checkers (correctness-testing.md Phase 2).
//!
//! Pure functions over the per-op history (`celeriant_bench::history`) plus
//! the final-read records. Each emits a `CheckResult` alongside the metric
//! predicates — history is the client's ground truth, metrics corroborate.
//!
//! Soundness under incomplete history: dropped records can only HIDE
//! evidence, never invent it, for every check except idempotency (which
//! infers a violation from the *absence* of a prior ack/info for a 2002).
//! `check_idempotency` therefore degrades to a skip when any record was
//! dropped; the others stay sound.
//!
//! `aggregate_version` semantics: the idempotent bench writes 1-event
//! batches with `client_seq` starting at 1, so a node's
//! `max_aggregate_version` (committed batch count) equals the highest
//! contiguous `client_seq` present. A version below the acked maximum is a
//! lost acked write (no-gaps rule); nodes disagreeing post-quiesce is a
//! convergence failure.

use celeriant_bench::history::{FinalReadRecord, HistoryLine, OpOutcome, OpRecord};
use crate::invariants::CheckResult;
use std::collections::{HashMap, HashSet};

const SAMPLE_CAP: usize = 10;

pub fn run_history_checks(lines: &[HistoryLine], records_dropped: u64) -> Vec<CheckResult> {
    let ops: Vec<&OpRecord> = lines
        .iter()
        .filter_map(|l| match l {
            HistoryLine::Op(op) => Some(op),
            _ => None,
        })
        .collect();
    let finals: Vec<&FinalReadRecord> = lines
        .iter()
        .filter_map(|l| match l {
            HistoryLine::FinalRead(fr) => Some(fr),
            _ => None,
        })
        .collect();

    vec![
        check_idempotency(&ops, records_dropped),
        check_occ(&ops),
        check_wal_monotonicity(&ops, &finals),
        check_final_read_parity(&finals),
    ]
}

type AggClient = (u128, u128, u128, u128); // org, type, agg, client

fn agg_client(op: &OpRecord) -> AggClient {
    (op.org_id, op.type_id, op.agg_id, op.client_id)
}

/// A `ClientIdempotencyViolation` is the server asserting "already applied".
/// It is legal only when the client has previously seen an ack for that seq,
/// or a prior attempt of the same seq ended `info` (the ambiguous attempt
/// committed server-side). A 2002 with neither is the server inventing an
/// ack — false-ack data loss in its most direct form.
///
/// Also flags a second `Ok` for the same `(agg, client, seq)`: the workloads
/// never resubmit an acked seq expecting success, so a duplicate ack means
/// the server committed the same write twice. This backstop is final-read
/// independent (the version upper bound in `check_wal_monotonicity` only
/// fires when the final-read phase ran).
fn check_idempotency(ops: &[&OpRecord], records_dropped: u64) -> CheckResult {
    const NAME: &str = "HistoryIdempotency";
    if records_dropped > 0 {
        return CheckResult::pass_with_detail(
            NAME,
            format!("skipped: history incomplete ({records_dropped} records dropped) — absence-based inference unsound"),
        );
    }

    // File order is per-(agg, client) attempt order: each workload task owns
    // its (agg, client) pair and has one op in flight at a time, so its own
    // records are sequential. Cross-task interleaving doesn't matter — all
    // state below is keyed per (agg, client).
    let mut acked_seqs: HashMap<AggClient, HashSet<u64>> = HashMap::new();
    let mut max_acked: HashMap<AggClient, u64> = HashMap::new();
    let mut info_seqs: HashMap<AggClient, HashSet<u64>> = HashMap::new();
    let mut violations: Vec<String> = Vec::new();
    let mut audited_2002 = 0u64;

    for op in ops {
        let key = agg_client(op);
        match op.outcome {
            OpOutcome::Ok => {
                if !acked_seqs.entry(key).or_default().insert(op.client_seq)
                    && violations.len() < SAMPLE_CAP
                {
                    violations.push(format!(
                        "agg={} client={} seq={}: second Ok for an already-acked seq — duplicate commit",
                        op.agg_id, op.client_id, op.client_seq
                    ));
                }
                let e = max_acked.entry(key).or_insert(0);
                *e = (*e).max(op.client_seq);
            }
            OpOutcome::Info => {
                info_seqs.entry(key).or_default().insert(op.client_seq);
            }
            OpOutcome::Fail => {
                if op.error.as_deref() == Some("ClientIdempotencyViolation") {
                    audited_2002 += 1;
                    let acked = max_acked.get(&key).copied().unwrap_or(0);
                    let had_info = info_seqs.get(&key).is_some_and(|s| s.contains(&op.client_seq));
                    if op.client_seq > acked && !had_info {
                        if violations.len() < SAMPLE_CAP {
                            violations.push(format!(
                                "agg={} client={} seq={}: 2002 with no prior ack (max_acked={}) and no ambiguous attempt",
                                op.agg_id, op.client_id, op.client_seq, acked
                            ));
                        }
                    } else {
                        // Treated as an ack by the client; track it so later
                        // 2002s for lower seqs stay legal.
                        let e = max_acked.entry(key).or_insert(0);
                        *e = (*e).max(op.client_seq);
                    }
                }
            }
        }
    }

    if violations.is_empty() {
        CheckResult::pass_with_detail(NAME, format!("{audited_2002} idempotency rejections audited"))
    } else {
        CheckResult::fail(NAME, format!("unjustified 2002s: {}", violations.join("; ")))
    }
}

/// OCC mutual exclusion, keyed on what the client SENT: among all attempts
/// against the same `(aggregate, expected_version)`, at most one may be
/// acked. (Write responses don't expose the committed version, so the sent
/// CAS token is the groupable evidence; vacuous for workloads that write
/// with `expected_version: None`.)
fn check_occ(ops: &[&OpRecord]) -> CheckResult {
    const NAME: &str = "HistoryOcc";
    let mut ok_counts: HashMap<(u128, u128, u128, u64), u64> = HashMap::new();
    let mut groups = 0u64;
    for op in ops {
        if let Some(ev) = op.expected_version {
            let key = (op.org_id, op.type_id, op.agg_id, ev);
            let e = ok_counts.entry(key).or_insert_with(|| {
                groups += 1;
                0
            });
            if op.outcome == OpOutcome::Ok {
                *e += 1;
            }
        }
    }
    let violations: Vec<String> = ok_counts
        .iter()
        .filter(|(_, oks)| **oks > 1)
        .take(SAMPLE_CAP)
        .map(|((_, _, agg, ev), oks)| format!("agg={agg} expected_version={ev}: {oks} acked writes"))
        .collect();
    if !violations.is_empty() {
        CheckResult::fail(NAME, format!("CAS exclusivity broken: {}", violations.join("; ")))
    } else if groups == 0 {
        CheckResult::pass_with_detail(NAME, "no CAS ops in history (workload writes without expected_version)")
    } else {
        CheckResult::pass_with_detail(NAME, format!("{groups} CAS groups audited"))
    }
}

/// Every acked `client_seq` (Ok, or a 2002 the client took as an ack) must
/// be covered by each node's final-read `max_aggregate_version`. A node
/// reporting fewer committed batches than the client's acked maximum lost
/// an acknowledged write.
///
/// Upper bound: the workloads are single-writer-per-aggregate with 1-event
/// batches and a sequential seq, so at most ONE write can ever be
/// outstanding-ambiguous per aggregate. `version > acked + 1` therefore
/// means the server committed a duplicate (same seq twice) or a write the
/// client never issued.
fn check_wal_monotonicity(ops: &[&OpRecord], finals: &[&FinalReadRecord]) -> CheckResult {
    const NAME: &str = "HistoryWalMonotonicity";
    if finals.is_empty() {
        return CheckResult::pass_with_detail(NAME, "skipped: no final-read records");
    }

    let mut max_acked: HashMap<(u128, u128, u128), u64> = HashMap::new();
    for op in ops {
        let acked = match op.outcome {
            OpOutcome::Ok => true,
            OpOutcome::Fail => op.error.as_deref() == Some("ClientIdempotencyViolation"),
            OpOutcome::Info => false,
        };
        if acked {
            let e = max_acked.entry((op.org_id, op.type_id, op.agg_id)).or_insert(0);
            *e = (*e).max(op.client_seq);
        }
    }

    let mut violations = Vec::new();
    let mut unreadable = 0u64;
    let mut audited = 0u64;
    for fr in finals {
        let Some(&acked) = max_acked.get(&(fr.org_id, fr.type_id, fr.agg_id)) else {
            continue;
        };
        match fr.max_aggregate_version {
            Some(version) => {
                audited += 1;
                if version < acked && violations.len() < SAMPLE_CAP {
                    violations.push(format!(
                        "agg={} node={}: final version {} < acked {} — acked write lost",
                        fr.agg_id, fr.node, version, acked
                    ));
                }
                if version > acked + 1 && violations.len() < SAMPLE_CAP {
                    violations.push(format!(
                        "agg={} node={}: final version {} > acked {} + 1 — duplicate acceptance",
                        fr.agg_id, fr.node, version, acked
                    ));
                }
            }
            None => unreadable += 1,
        }
    }

    if violations.is_empty() {
        CheckResult::pass_with_detail(
            NAME,
            format!("{audited} node-aggregate reads within [acked, acked+1] ({unreadable} unreadable)"),
        )
    } else {
        CheckResult::fail(NAME, violations.join("; "))
    }
}

/// Post-quiesce, both nodes must report the same committed batch count per
/// aggregate. Ambiguous (`info`) writes may have committed or not — but
/// never on exactly one node.
fn check_final_read_parity(finals: &[&FinalReadRecord]) -> CheckResult {
    const NAME: &str = "HistoryFinalReadParity";
    if finals.is_empty() {
        return CheckResult::pass_with_detail(NAME, "skipped: no final-read records");
    }

    let mut by_agg: HashMap<(u128, u128, u128), Vec<&&FinalReadRecord>> = HashMap::new();
    for fr in finals {
        by_agg.entry((fr.org_id, fr.type_id, fr.agg_id)).or_default().push(fr);
    }

    let mut mismatches = Vec::new();
    let mut incomplete = 0u64;
    let mut compared = 0u64;
    for ((_, _, agg), recs) in &by_agg {
        let versions: Vec<(&str, Option<u64>)> =
            recs.iter().map(|r| (r.node.as_str(), r.max_aggregate_version)).collect();
        if versions.len() < 2 || versions.iter().any(|(_, v)| v.is_none()) {
            incomplete += 1;
            continue;
        }
        compared += 1;
        let first = versions[0].1;
        if versions.iter().any(|(_, v)| *v != first) {
            if mismatches.len() < SAMPLE_CAP {
                let detail: Vec<String> =
                    versions.iter().map(|(n, v)| format!("{n}={}", v.unwrap())).collect();
                mismatches.push(format!("agg={agg}: {}", detail.join(" vs ")));
            }
        }
    }

    if mismatches.is_empty() {
        CheckResult::pass_with_detail(
            NAME,
            format!("{compared} aggregates byte-count-identical across nodes ({incomplete} incomplete)"),
        )
    } else {
        CheckResult::fail(NAME, format!("node disagreement after quiesce: {}", mismatches.join("; ")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use celeriant_bench::history::{FinalReadRecord, HistoryLine, OpOutcome, OpRecord};

    fn op(seq: u64, outcome: OpOutcome, error: Option<&str>) -> HistoryLine {
        op_for(1, seq, outcome, error, None)
    }

    fn op_for(agg: u128, seq: u64, outcome: OpOutcome, error: Option<&str>, expected_version: Option<u64>) -> HistoryLine {
        HistoryLine::Op(OpRecord {
            process: agg as u32,
            org_id: 1,
            type_id: 1,
            agg_id: agg,
            client_id: agg + 1,
            client_seq: seq,
            expected_version,
            outcome,
            error: error.map(str::to_string),
            t_start_ns: 0,
            t_end_ns: 0,
        })
    }

    fn final_read(node: &str, agg: u128, version: Option<u64>) -> HistoryLine {
        HistoryLine::FinalRead(FinalReadRecord {
            node: node.to_string(),
            org_id: 1,
            type_id: 1,
            agg_id: agg,
            client_id: agg + 1,
            max_aggregate_version: version,
            error: version.is_none().then(|| "timeout".to_string()),
        })
    }

    fn results(lines: &[HistoryLine], dropped: u64) -> HashMap<String, CheckResult> {
        run_history_checks(lines, dropped)
            .into_iter()
            .map(|c| (c.name.to_string(), c))
            .collect()
    }

    #[test]
    fn clean_history_passes_all() {
        let lines = vec![
            op(1, OpOutcome::Ok, None),
            op(2, OpOutcome::Ok, None),
            final_read("cs1", 1, Some(2)),
            final_read("cs2", 1, Some(2)),
        ];
        for (name, c) in results(&lines, 0) {
            assert!(c.passed, "{name}: {}", c.detail);
        }
    }

    #[test]
    fn idempotency_2002_after_info_is_legal() {
        let lines = vec![
            op(1, OpOutcome::Ok, None),
            op(2, OpOutcome::Info, Some("RequestTimeout")), // committed server-side, ack lost
            op(2, OpOutcome::Fail, Some("ClientIdempotencyViolation")), // retry sees 2002 — legal
        ];
        let r = results(&lines, 0);
        assert!(r["HistoryIdempotency"].passed, "{}", r["HistoryIdempotency"].detail);
    }

    #[test]
    fn idempotency_2002_from_nowhere_fails() {
        let lines = vec![
            op(1, OpOutcome::Ok, None),
            // seq 3 was never attempted before: the server invented an ack.
            op(3, OpOutcome::Fail, Some("ClientIdempotencyViolation")),
        ];
        let r = results(&lines, 0);
        assert!(!r["HistoryIdempotency"].passed);
    }

    #[test]
    fn idempotency_double_ok_same_seq_fails() {
        let lines = vec![
            op(1, OpOutcome::Ok, None),
            op(2, OpOutcome::Ok, None),
            op(2, OpOutcome::Ok, None), // server committed seq 2 twice
        ];
        let r = results(&lines, 0);
        assert!(!r["HistoryIdempotency"].passed);
        assert!(r["HistoryIdempotency"].detail.contains("duplicate commit"));
    }

    #[test]
    fn idempotency_skips_when_history_dropped() {
        let lines = vec![op(3, OpOutcome::Fail, Some("ClientIdempotencyViolation"))];
        let r = results(&lines, 5);
        assert!(r["HistoryIdempotency"].passed);
        assert!(r["HistoryIdempotency"].detail.contains("skipped"));
    }

    #[test]
    fn occ_two_acks_same_expected_version_fails() {
        let lines = vec![
            op_for(7, 1, OpOutcome::Ok, None, Some(4)),
            op_for(7, 1, OpOutcome::Fail, Some("OccConflict"), Some(4)),
            op_for(7, 2, OpOutcome::Ok, None, Some(4)), // second ack on same CAS token
        ];
        let r = results(&lines, 0);
        assert!(!r["HistoryOcc"].passed);
    }

    #[test]
    fn occ_exactly_one_ack_passes() {
        let lines = vec![
            op_for(7, 1, OpOutcome::Ok, None, Some(4)),
            op_for(7, 1, OpOutcome::Fail, Some("OccConflict"), Some(4)),
            op_for(7, 1, OpOutcome::Fail, Some("OccConflict"), Some(4)),
        ];
        let r = results(&lines, 0);
        assert!(r["HistoryOcc"].passed, "{}", r["HistoryOcc"].detail);
    }

    #[test]
    fn monotonicity_lost_acked_write_fails() {
        let lines = vec![
            op(1, OpOutcome::Ok, None),
            op(2, OpOutcome::Ok, None),
            final_read("cs1", 1, Some(1)), // acked 2, node has 1 — loss
            final_read("cs2", 1, Some(2)),
        ];
        let r = results(&lines, 0);
        assert!(!r["HistoryWalMonotonicity"].passed);
        assert!(!r["HistoryFinalReadParity"].passed); // and the nodes disagree
    }

    #[test]
    fn duplicate_acceptance_fails_monotonicity() {
        let lines = vec![
            op(1, OpOutcome::Ok, None),
            op(1, OpOutcome::Fail, Some("ClientIdempotencyViolation")), // deliberate replay, rejected
            final_read("cs1", 1, Some(3)), // server holds 3 batches for 1 acked seq: duplicates
            final_read("cs2", 1, Some(3)),
        ];
        let r = results(&lines, 0);
        assert!(!r["HistoryWalMonotonicity"].passed);
        assert!(r["HistoryWalMonotonicity"].detail.contains("duplicate acceptance"));
    }

    #[test]
    fn ambiguous_write_on_both_nodes_passes_parity() {
        let lines = vec![
            op(1, OpOutcome::Ok, None),
            op(2, OpOutcome::Info, Some("RequestTimeout")), // may or may not have landed
            final_read("cs1", 1, Some(2)),                  // landed on both — fine
            final_read("cs2", 1, Some(2)),
        ];
        let r = results(&lines, 0);
        assert!(r["HistoryFinalReadParity"].passed, "{}", r["HistoryFinalReadParity"].detail);
        assert!(r["HistoryWalMonotonicity"].passed);
    }

    #[test]
    fn ambiguous_write_on_one_node_fails_parity() {
        let lines = vec![
            op(1, OpOutcome::Ok, None),
            op(2, OpOutcome::Info, Some("RequestTimeout")),
            final_read("cs1", 1, Some(2)), // info write visible here...
            final_read("cs2", 1, Some(1)), // ...but not here: split outcome
        ];
        let r = results(&lines, 0);
        assert!(!r["HistoryFinalReadParity"].passed);
    }

    #[test]
    fn unreadable_node_skips_parity_but_reports() {
        let lines = vec![
            op(1, OpOutcome::Ok, None),
            final_read("cs1", 1, Some(1)),
            final_read("cs2", 1, None),
        ];
        let r = results(&lines, 0);
        assert!(r["HistoryFinalReadParity"].passed);
        assert!(r["HistoryFinalReadParity"].detail.contains("1 incomplete"));
    }
}
