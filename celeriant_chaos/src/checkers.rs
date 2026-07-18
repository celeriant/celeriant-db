//! History-based consistency checkers (correctness-testing.md Phase 2).
//!
//! Pure functions over the per-op history (`celeriant_bench::history`) plus
//! the final-read records. Each emits a `CheckResult` alongside the metric
//! predicates — history is the client's ground truth, metrics corroborate.
//!
//! Soundness under incomplete history: dropped records can only HIDE
//! evidence, never invent it, for every check except idempotency (which
//! infers a violation from the *absence* of a prior ack/info for a 2002).
//! `check_idempotency` therefore fails closed (indeterminate) when any
//! record was dropped, rather than reporting an unearned pass; the others
//! stay sound under drops.
//!
//! `aggregate_version` semantics: the idempotent bench writes 1-event
//! batches with `client_seq` starting at 1, so a node's
//! `max_aggregate_version` (committed batch count) equals the highest
//! contiguous `client_seq` present. A version below the acked maximum is a
//! lost acked write (no-gaps rule); nodes disagreeing post-quiesce is a
//! convergence failure.

use celeriant_bench::history::{FinalReadRecord, HistoryLine, OpOutcome, OpRecord, RywRecord, WatchDeliveryRecord};
use crate::invariants::CheckResult;
use std::collections::{HashMap, HashSet};

const SAMPLE_CAP: usize = 10;

pub fn run_history_checks(lines: &[HistoryLine], records_dropped: u64) -> Vec<CheckResult> {
    run_history_checks_with_windows(lines, records_dropped, &[])
}

/// Like `run_history_checks` but also accepts `exclusion_windows` (t_ns ranges)
/// for the `HistoryReadYourWrites` checker. Records whose `t_ns` falls inside any
/// window are skipped (role-transition windows where stale reads are by-design).
pub fn run_history_checks_with_windows(
    lines: &[HistoryLine],
    records_dropped: u64,
    exclusion_windows: &[(u64, u64)],
) -> Vec<CheckResult> {
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
    let ryws: Vec<&RywRecord> = lines
        .iter()
        .filter_map(|l| match l {
            HistoryLine::Ryw(r) => Some(r),
            _ => None,
        })
        .collect();
    let watch_deliveries: Vec<&WatchDeliveryRecord> = lines
        .iter()
        .filter_map(|l| match l {
            HistoryLine::WatchDelivery(w) => Some(w),
            _ => None,
        })
        .collect();

    vec![
        check_idempotency(&ops, records_dropped),
        check_occ(&ops),
        check_wal_monotonicity(&ops, &finals),
        check_final_read_parity(&finals),
        check_ryw(&ryws, exclusion_windows),
        check_watch_per_connection_ordered(&watch_deliveries),
        check_watch_delivered_durable(&watch_deliveries, &finals),
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
        // Absence-based inference (a 2002 with no prior ack in history) is
        // unsound once history is incomplete — dropped records are exactly
        // what a fault would drop. The only caller (finish_history_and_check)
        // always expects a verdict, so failing closed here is ungated.
        return CheckResult::fail(
            NAME,
            format!("indeterminate: history incomplete ({records_dropped} records dropped) — absence-based inference unsound"),
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

/// OCC exclusivity, judged from both sides of the exchange:
///
/// 1. Sent side: among all attempts against the same
///    `(aggregate, expected_version)` CAS token, at most one may be acked.
/// 2. Told side: no two acks may report the same committed
///    `(aggregate, max_aggregate_version)` — the server naming the same
///    version twice is a duplicate commit regardless of what was sent.
///    (Version reuse would be legal across a delete + non-continuation
///    recreate; no workload deletes, so any collision here is a bug.)
fn check_occ(ops: &[&OpRecord]) -> CheckResult {
    const NAME: &str = "HistoryOcc";
    let mut ok_counts: HashMap<(u128, u128, u128, u64), u64> = HashMap::new();
    let mut groups = 0u64;
    let mut told_counts: HashMap<(u128, u128, u128, u64), u64> = HashMap::new();
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
        if op.outcome == OpOutcome::Ok {
            if let Some(v) = op.acked_max_aggregate_version {
                *told_counts.entry((op.org_id, op.type_id, op.agg_id, v)).or_insert(0) += 1;
            }
        }
    }
    let mut violations: Vec<String> = ok_counts
        .iter()
        .filter(|(_, oks)| **oks > 1)
        .take(SAMPLE_CAP)
        .map(|((_, _, agg, ev), oks)| format!("agg={agg} expected_version={ev}: {oks} acked writes"))
        .collect();
    let told_versions = told_counts.len() as u64;
    violations.extend(
        told_counts
            .iter()
            .filter(|(_, acks)| **acks > 1)
            .take(SAMPLE_CAP)
            .map(|((_, _, agg, v), acks)| {
                format!("agg={agg} committed_version={v}: {acks} acks report the same version — duplicate commit")
            }),
    );
    if !violations.is_empty() {
        CheckResult::fail(NAME, format!("OCC exclusivity broken: {}", violations.join("; ")))
    } else if groups == 0 && told_versions == 0 {
        CheckResult::pass_with_detail(NAME, "no CAS ops and no server-told versions in history")
    } else {
        CheckResult::pass_with_detail(
            NAME,
            format!("{groups} CAS groups, {told_versions} server-told versions audited"),
        )
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

/// Read-your-writes: a probe group violates only when reads from BOTH nodes
/// succeeded and BOTH observed below the acked version — a cluster-wide
/// invisible ack. A group missing a successful read from either node cannot
/// witness "cluster-wide": during fault windows the only reachable node may
/// be a fenced one legally serving stale reads, so n=1 groups are skipped
/// (counted, so coverage loss stays visible). Probes inside any
/// `exclusion_windows` t_ns range are skipped (stale-read windows by design).
fn check_ryw(ryws: &[&RywRecord], exclusion_windows: &[(u64, u64)]) -> CheckResult {
    const NAME: &str = "HistoryReadYourWrites";
    if ryws.is_empty() {
        return CheckResult::pass_with_detail(NAME, "skipped: no RYW probe records");
    }

    let in_window = |t: u64| exclusion_windows.iter().any(|&(lo, hi)| t >= lo && t <= hi);

    let mut violations: Vec<String> = Vec::new();
    let mut violation_count: u64 = 0;
    let mut first_viol_ns: u64 = u64::MAX;
    let mut last_viol_ns: u64 = 0;
    let mut read_errors: u64 = 0;
    let mut skipped: u64 = 0;
    let mut skipped_incomplete: u64 = 0;
    let mut checked: u64 = 0;

    // Group multi-node pinned reads of one probe instant by (process, probe_id).
    let mut groups: HashMap<(u32, u64), Vec<&&RywRecord>> = HashMap::new();
    for r in ryws {
        if in_window(r.t_ns) {
            skipped += 1;
            continue;
        }
        groups.entry((r.process, r.probe_id)).or_default().push(r);
    }
    for ((_, _), recs) in groups {
        let successes: Vec<_> = recs.iter().filter_map(|r| r.observed_max_aggregate_version.map(|o| (o, *r))).collect();
        read_errors += (recs.len() - successes.len()) as u64;
        let nodes: HashSet<&str> = successes.iter().filter_map(|(_, r)| r.node.as_deref()).collect();
        if nodes.len() < 2 {
            skipped_incomplete += 1;
            continue;
        }
        checked += 1;
        if successes.iter().all(|(o, r)| *o < r.acked_max_aggregate_version) {
            let (observed, r) = successes[0];
            violation_count += 1;
            first_viol_ns = first_viol_ns.min(r.t_ns);
            last_viol_ns = last_viol_ns.max(r.t_ns);
            if violations.len() < SAMPLE_CAP {
                violations.push(format!(
                    "agg={} client={} acked={} observed={} on all {} node(s) — acked write invisible cluster-wide",
                    r.agg_id, r.client_id, r.acked_max_aggregate_version, observed, successes.len()
                ));
            }
        }
    }

    if violations.is_empty() {
        CheckResult::pass_with_detail(
            NAME,
            format!(
                "{checked} full probe groups passed ({skipped_incomplete} incomplete groups skipped, {skipped} in exclusion windows, {read_errors} read errors)"
            ),
        )
    } else {
        CheckResult::fail(
            NAME,
            format!(
                "{violation_count} RYW violations of {checked} full probe groups, clustered {:.1}s–{:.1}s ({skipped_incomplete} incomplete skipped, {read_errors} read errors, {skipped} in windows): {}",
                first_viol_ns as f64 / 1e9,
                last_viol_ns as f64 / 1e9,
                violations.join("; ")
            ),
        )
    }
}

/// Per (connection, epoch, aggregate), watch deliveries sorted by t_ns must have
/// strictly increasing, non-overlapping version ranges: each delivery's
/// `from_version` must be greater than the previous delivery's `to_version`.
/// Duplicates or out-of-order deliveries within one connection = FAIL.
fn check_watch_per_connection_ordered(deliveries: &[&WatchDeliveryRecord]) -> CheckResult {
    const NAME: &str = "WatchPerConnectionOrdered";
    if deliveries.is_empty() {
        return CheckResult::pass_with_detail(NAME, "skipped: no WatchDelivery records");
    }

    // Key: (connection, epoch, org, type, agg)
    type ConnAgg = (u32, u32, u128, u128, u128);
    let mut by_conn_agg: HashMap<ConnAgg, Vec<&WatchDeliveryRecord>> = HashMap::new();
    for d in deliveries {
        by_conn_agg
            .entry((d.connection, d.epoch, d.org_id, d.type_id, d.agg_id))
            .or_default()
            .push(d);
    }

    let mut violations: Vec<String> = Vec::new();
    let mut checked_streams: u64 = 0;

    for ((conn, _, _, _, agg), mut recs) in by_conn_agg {
        recs.sort_unstable_by_key(|r| r.t_ns);
        checked_streams += 1;
        let mut prev_to: Option<u64> = None;
        for r in recs {
            if let Some(prev) = prev_to {
                if r.from_version <= prev {
                    if violations.len() < SAMPLE_CAP {
                        violations.push(format!(
                            "conn={conn} agg={agg}: from_version={} <= prev_to={prev} — overlap or duplicate",
                            r.from_version
                        ));
                    }
                }
            }
            prev_to = Some(r.to_version);
        }
    }

    if violations.is_empty() {
        CheckResult::pass_with_detail(NAME, format!("{checked_streams} (connection, epoch, aggregate) streams ordered"))
    } else {
        CheckResult::fail(NAME, format!("out-of-order or overlapping deliveries: {}", violations.join("; ")))
    }
}

/// Every delivered `to_version` must be ≤ the final-read `max_aggregate_version`
/// for that aggregate on at least one node. A delivery whose version exceeds every
/// node's final-read count was delivered before it was durable (ghost delivery).
fn check_watch_delivered_durable(
    deliveries: &[&WatchDeliveryRecord],
    finals: &[&FinalReadRecord],
) -> CheckResult {
    const NAME: &str = "WatchDeliveredDurable";
    if deliveries.is_empty() {
        return CheckResult::pass_with_detail(NAME, "skipped: no WatchDelivery records");
    }
    if finals.is_empty() {
        return CheckResult::pass_with_detail(NAME, "skipped: no final-read records");
    }

    // Build: (org, type, agg) -> max version seen across all nodes
    let mut max_final: HashMap<(u128, u128, u128), u64> = HashMap::new();
    for fr in finals {
        if let Some(v) = fr.max_aggregate_version {
            let e = max_final.entry((fr.org_id, fr.type_id, fr.agg_id)).or_insert(0);
            *e = (*e).max(v);
        }
    }

    let mut violations: Vec<String> = Vec::new();
    let mut checked: u64 = 0;
    let mut aggs_missing_final: HashSet<(u128, u128, u128)> = HashSet::new();

    for d in deliveries {
        let key = (d.org_id, d.type_id, d.agg_id);
        match max_final.get(&key) {
            None => {
                aggs_missing_final.insert(key);
            }
            Some(&max_v) => {
                checked += 1;
                if d.to_version > max_v && violations.len() < SAMPLE_CAP {
                    violations.push(format!(
                        "conn={} agg={}: delivered to_version={} > final max_version={} — delivered before durable",
                        d.connection, d.agg_id, d.to_version, max_v
                    ));
                }
            }
        }
    }

    if violations.is_empty() {
        CheckResult::pass_with_detail(
            NAME,
            format!(
                "{checked} deliveries within final-read bounds ({} aggregates skipped: no final read)",
                aggs_missing_final.len()
            ),
        )
    } else {
        CheckResult::fail(
            NAME,
            format!("ghost deliveries (before durable): {}", violations.join("; ")),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use celeriant_bench::history::{FinalReadRecord, HistoryLine, OpOutcome, OpRecord, RywRecord, WatchDeliveryRecord};

    fn op(seq: u64, outcome: OpOutcome, error: Option<&str>) -> HistoryLine {
        op_for(1, seq, outcome, error, None)
    }

    fn op_for(agg: u128, seq: u64, outcome: OpOutcome, error: Option<&str>, expected_version: Option<u64>) -> HistoryLine {
        let acked = if outcome == OpOutcome::Ok { Some(seq) } else { None };
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
            acked_max_aggregate_version: acked,
            t_start_ns: 0,
            t_end_ns: 0,
        })
    }

    fn ryw(agg: u128, acked: u64, observed: Option<u64>, t_ns: u64) -> HistoryLine {
        ryw_on(agg, acked, observed, t_ns, None, t_ns)
    }

    /// Pinned probe: one record per (node, probe_id); two records sharing a
    /// probe_id form one group.
    fn ryw_on(agg: u128, acked: u64, observed: Option<u64>, t_ns: u64, node: Option<&str>, probe_id: u64) -> HistoryLine {
        HistoryLine::Ryw(RywRecord {
            process: agg as u32,
            node: node.map(str::to_string),
            probe_id,
            org_id: 1,
            type_id: 1,
            agg_id: agg,
            client_id: agg + 1,
            acked_max_aggregate_version: acked,
            observed_max_aggregate_version: observed,
            read_error: if observed.is_none() { Some("timeout".to_string()) } else { None },
            t_ns,
        })
    }

    fn watch_delivery(conn: u32, agg: u128, from: u64, to: u64, t_ns: u64) -> HistoryLine {
        HistoryLine::WatchDelivery(WatchDeliveryRecord {
            connection: conn,
            epoch: 0,
            org_id: 1,
            type_id: 1,
            agg_id: agg,
            from_version: from,
            to_version: to,
            t_ns,
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
    fn idempotency_fails_closed_when_history_dropped() {
        // Was: passed with "skipped: ..." (unsound-by-skip). Dropped history
        // means the absence-based idempotency inference can't be trusted, so
        // the checker must fail closed, not report a false verdict.
        let lines = vec![op(3, OpOutcome::Fail, Some("ClientIdempotencyViolation"))];
        let r = results(&lines, 5);
        assert!(!r["HistoryIdempotency"].passed);
        assert!(r["HistoryIdempotency"].detail.contains("indeterminate"));
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
    fn occ_two_acks_reporting_same_version_fails() {
        // Different seqs, different CAS tokens — but the server told both
        // writers they committed version 5. Duplicate commit.
        let mut a = op_for(7, 1, OpOutcome::Ok, None, Some(4));
        let mut b = op_for(7, 2, OpOutcome::Ok, None, Some(5));
        for line in [&mut a, &mut b] {
            if let HistoryLine::Op(op) = line {
                op.acked_max_aggregate_version = Some(5);
            }
        }
        let r = results(&[a, b], 0);
        assert!(!r["HistoryOcc"].passed);
        assert!(r["HistoryOcc"].detail.contains("duplicate commit"), "{}", r["HistoryOcc"].detail);
    }

    #[test]
    fn occ_distinct_told_versions_pass() {
        let lines = vec![
            op_for(7, 1, OpOutcome::Ok, None, Some(4)),  // told version 1 (op_for: = seq)
            op_for(7, 2, OpOutcome::Ok, None, Some(5)),  // told version 2
            op_for(8, 1, OpOutcome::Ok, None, None),     // same version, different aggregate
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

    // --- RYW tests ---

    #[test]
    fn ryw_no_records_skips() {
        let r = results(&[], 0);
        assert!(r["HistoryReadYourWrites"].passed);
        assert!(r["HistoryReadYourWrites"].detail.contains("skipped"));
    }

    #[test]
    fn ryw_observed_gte_acked_passes() {
        let lines = vec![
            ryw_on(1, 5, Some(5), 1000, Some("cs1"), 5),
            ryw_on(1, 5, Some(5), 1010, Some("cs2"), 5),
            ryw_on(1, 6, Some(7), 2000, Some("cs1"), 6), // observed ahead — fine
            ryw_on(1, 6, Some(6), 2010, Some("cs2"), 6),
        ];
        let r = results(&lines, 0);
        assert!(r["HistoryReadYourWrites"].passed, "{}", r["HistoryReadYourWrites"].detail);
        assert!(r["HistoryReadYourWrites"].detail.contains("2 full probe groups"), "{}", r["HistoryReadYourWrites"].detail);
    }

    #[test]
    fn ryw_stale_on_both_nodes_fails() {
        let lines = vec![
            ryw_on(1, 10, Some(9), 1000, Some("cs1"), 10), // both below acked —
            ryw_on(1, 10, Some(8), 1010, Some("cs2"), 10), // cluster-wide invisible ack
        ];
        let r = results(&lines, 0);
        assert!(!r["HistoryReadYourWrites"].passed);
        assert!(r["HistoryReadYourWrites"].detail.contains("acked write invisible cluster-wide"));
    }

    #[test]
    fn ryw_stale_on_one_node_passes() {
        let lines = vec![
            ryw_on(1, 10, Some(9), 1000, Some("cs1"), 10), // stale replica —
            ryw_on(1, 10, Some(10), 1010, Some("cs2"), 10), // acking node visible
        ];
        let r = results(&lines, 0);
        assert!(r["HistoryReadYourWrites"].passed, "{}", r["HistoryReadYourWrites"].detail);
    }

    #[test]
    fn ryw_incomplete_group_skipped_not_judged() {
        // Only one node answered (the other errored): a lone stale read on a
        // possibly-fenced node cannot witness "cluster-wide".
        let lines = vec![
            ryw_on(1, 10, Some(9), 1000, Some("cs1"), 10),
            ryw_on(1, 10, None, 1010, Some("cs2"), 10),
        ];
        let r = results(&lines, 0);
        assert!(r["HistoryReadYourWrites"].passed, "{}", r["HistoryReadYourWrites"].detail);
        assert!(r["HistoryReadYourWrites"].detail.contains("1 incomplete groups skipped"), "{}", r["HistoryReadYourWrites"].detail);
        assert!(r["HistoryReadYourWrites"].detail.contains("1 read errors"), "{}", r["HistoryReadYourWrites"].detail);
    }

    #[test]
    fn ryw_unpinned_groups_skipped_as_incomplete() {
        // Un-pinned probes carry no node identity — they can never witness
        // cluster-wide invisibility under the full-group rule.
        let lines = vec![
            ryw(1, 10, Some(9), 1000),
        ];
        let r = results(&lines, 0);
        assert!(r["HistoryReadYourWrites"].passed, "{}", r["HistoryReadYourWrites"].detail);
        assert!(r["HistoryReadYourWrites"].detail.contains("1 incomplete groups skipped"), "{}", r["HistoryReadYourWrites"].detail);
    }

    #[test]
    fn ryw_violation_in_exclusion_window_skipped() {
        // Stale full group at t=5000/5010 inside the exclusion window [4000, 6000].
        let lines = vec![
            ryw_on(1, 10, Some(9), 5000, Some("cs1"), 10),
            ryw_on(1, 10, Some(9), 5010, Some("cs2"), 10),
        ];
        let checks = run_history_checks_with_windows(&lines, 0, &[(4000, 6000)]);
        let r: HashMap<String, CheckResult> = checks.into_iter().map(|c| (c.name.to_string(), c)).collect();
        assert!(r["HistoryReadYourWrites"].passed, "{}", r["HistoryReadYourWrites"].detail);
        assert!(r["HistoryReadYourWrites"].detail.contains("2 in exclusion windows"));
    }

    #[test]
    fn ryw_violation_outside_exclusion_window_fails() {
        // Stale full group at t=9000, exclusion window [4000, 6000] — still fails.
        let lines = vec![
            ryw_on(1, 10, Some(9), 9000, Some("cs1"), 10),
            ryw_on(1, 10, Some(9), 9010, Some("cs2"), 10),
        ];
        let checks = run_history_checks_with_windows(&lines, 0, &[(4000, 6000)]);
        let r: HashMap<String, CheckResult> = checks.into_iter().map(|c| (c.name.to_string(), c)).collect();
        assert!(!r["HistoryReadYourWrites"].passed, "{}", r["HistoryReadYourWrites"].detail);
    }

    #[test]
    fn sampling_determinism() {
        // Verify the 1/16 formula is deterministic and fires ~1/16 of the time.
        let fires: Vec<u64> = (0u64..160)
            .filter(|&seq| (3u64).wrapping_mul(31).wrapping_add(seq) % 16 == 0)
            .collect();
        // Exactly 10 out of 160 = 1/16.
        assert_eq!(fires.len(), 10, "expected 10/160, got {}: {:?}", fires.len(), fires);
    }

    // --- Watch ordering tests ---

    #[test]
    fn watch_ordered_no_records_skips() {
        let r = results(&[], 0);
        assert!(r["WatchPerConnectionOrdered"].passed);
        assert!(r["WatchPerConnectionOrdered"].detail.contains("skipped"));
    }

    #[test]
    fn watch_ordered_clean_sequence_passes() {
        let lines = vec![
            watch_delivery(0, 1, 1, 5, 100),
            watch_delivery(0, 1, 6, 10, 200),
            watch_delivery(0, 1, 11, 15, 300),
        ];
        let r = results(&lines, 0);
        assert!(r["WatchPerConnectionOrdered"].passed, "{}", r["WatchPerConnectionOrdered"].detail);
    }

    #[test]
    fn watch_ordered_overlap_fails() {
        // from_version=5 but prev to_version=5 — overlap (5 <= 5)
        let lines = vec![
            watch_delivery(0, 1, 1, 5, 100),
            watch_delivery(0, 1, 5, 10, 200), // from_version 5 <= prev_to 5
        ];
        let r = results(&lines, 0);
        assert!(!r["WatchPerConnectionOrdered"].passed);
        assert!(r["WatchPerConnectionOrdered"].detail.contains("overlap or duplicate"));
    }

    #[test]
    fn watch_ordered_different_connections_independent() {
        // conn 0 gets 1-5, 6-10; conn 1 gets 1-5, 6-10 — both independent, both fine
        let lines = vec![
            watch_delivery(0, 1, 1, 5, 100),
            watch_delivery(0, 1, 6, 10, 200),
            watch_delivery(1, 1, 1, 5, 100),
            watch_delivery(1, 1, 6, 10, 200),
        ];
        let r = results(&lines, 0);
        assert!(r["WatchPerConnectionOrdered"].passed, "{}", r["WatchPerConnectionOrdered"].detail);
    }

    #[test]
    fn watch_ordered_different_aggregates_independent() {
        // conn 0 agg 1: 1-5; conn 0 agg 2: 1-5 — different aggregates, both fine
        let lines = vec![
            watch_delivery(0, 1, 1, 5, 100),
            watch_delivery(0, 2, 1, 5, 100),
        ];
        let r = results(&lines, 0);
        assert!(r["WatchPerConnectionOrdered"].passed, "{}", r["WatchPerConnectionOrdered"].detail);
    }

    // --- Watch durable delivery tests ---

    #[test]
    fn watch_durable_no_deliveries_skips() {
        let r = results(&[], 0);
        assert!(r["WatchDeliveredDurable"].passed);
        assert!(r["WatchDeliveredDurable"].detail.contains("skipped"));
    }

    #[test]
    fn watch_durable_no_finals_skips() {
        let lines = vec![watch_delivery(0, 1, 1, 5, 100)];
        let r = results(&lines, 0);
        assert!(r["WatchDeliveredDurable"].passed);
        assert!(r["WatchDeliveredDurable"].detail.contains("skipped"));
    }

    #[test]
    fn watch_durable_within_bounds_passes() {
        let lines = vec![
            watch_delivery(0, 1, 1, 5, 100),
            final_read("cs1", 1, Some(10)),
        ];
        let r = results(&lines, 0);
        assert!(r["WatchDeliveredDurable"].passed, "{}", r["WatchDeliveredDurable"].detail);
    }

    #[test]
    fn watch_durable_exceeds_final_fails() {
        // Delivered to_version=20 but final read only shows 10 — ghost delivery
        let lines = vec![
            watch_delivery(0, 1, 15, 20, 100),
            final_read("cs1", 1, Some(10)),
        ];
        let r = results(&lines, 0);
        assert!(!r["WatchDeliveredDurable"].passed);
        assert!(r["WatchDeliveredDurable"].detail.contains("delivered before durable"));
    }

    #[test]
    fn watch_durable_uses_max_across_nodes() {
        // cs1 has 18, cs2 has 10 — delivery to_version=15 is within cs1's 18
        let lines = vec![
            watch_delivery(0, 1, 12, 15, 100),
            final_read("cs1", 1, Some(18)),
            final_read("cs2", 1, Some(10)),
        ];
        let r = results(&lines, 0);
        assert!(r["WatchDeliveredDurable"].passed, "{}", r["WatchDeliveredDurable"].detail);
    }
}
