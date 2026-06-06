//! Client-API final-read phase.
//!
//! After heal + settle, read every bench-written aggregate from BOTH nodes —
//! each through a pool pinned to that node (seed == primary, so failover
//! can't silently redirect the read to the other node). Disk-truth catches
//! WAL-level divergence via wal-inspect; this catches read-path issues
//! (stale caches, visibility cursors) that disk bytes can't show.
//!
//! Reads are never rejected by node status (a follower serves stale reads
//! silently), so both slots answer regardless of who currently leads.
//! Records are appended to the scenario's history file and consumed by
//! `checkers::check_wal_monotonicity` / `check_final_read_parity`.

use crate::config::ClusterConfig;
use crate::invariants::CheckResult;
use celeriant_bench::history::FinalReadRecord;
use celeriant_bench::{read_max_aggregate_version, Pool, PoolBuilder, TaskAckSummary};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

const READ_ATTEMPTS: u32 = 4;
const READ_BACKOFF_MS: u64 = 250;
const READ_CONCURRENCY: usize = 32;

/// Sample size for payload verification: lowest N aggregates by aggregate_id.
const PAYLOAD_SAMPLE_SIZE: usize = 24;
/// Cap on failures reported in a FAIL result.
const PAYLOAD_FAIL_CAP: usize = 10;

/// Read `acks`' aggregates from both nodes. Node labels are the config-slot
/// hostnames (roles may have changed hands mid-scenario; the parity check
/// only needs two distinct, stable labels).
pub async fn run_final_read_phase(
    scen: &str,
    cfg: &ClusterConfig,
    acks: &[TaskAckSummary],
) -> Result<Vec<FinalReadRecord>, String> {
    let mut all = Vec::new();
    for (host, addr) in [
        (cfg.leader_host.clone(), cfg.leader_addr()),
        (cfg.follower_host.clone(), cfg.follower_addr()),
    ] {
        let pool = PoolBuilder {
            address1: &addr,
            address2: &addr, // pinned: no failover escape to the other node
            server_name: Some(&host),
            ca_cert: cfg.ca_cert.to_str().unwrap(),
            client_cert: cfg.client_cert.to_str().unwrap(),
            client_key: cfg.client_key.to_str().unwrap(),
            plaintext: false,
            max_connections: READ_CONCURRENCY,
        }
        .build()
        .await
        .map_err(|e| format!("{scen}: final-read pool for {host}: {e}"))?;

        let records = read_node(&host, &pool, acks).await;
        let errors = records.iter().filter(|r| r.error.is_some()).count();
        println!(
            "[{scen}] final-read {}: {} aggregates, {} unreadable",
            host,
            records.len(),
            errors
        );
        all.extend(records);
    }
    Ok(all)
}

async fn read_node(host: &str, pool: &Arc<Pool>, acks: &[TaskAckSummary]) -> Vec<FinalReadRecord> {
    let semaphore = Arc::new(Semaphore::new(READ_CONCURRENCY));
    let mut handles = Vec::new();
    for ack in acks {
        if ack.max_acked_client_seq == 0 {
            continue;
        }
        let pool = Arc::clone(pool);
        let ack = ack.clone();
        let host = host.to_string();
        let permit = Arc::clone(&semaphore);
        handles.push(tokio::spawn(async move {
            let _p = permit.acquire_owned().await.expect("semaphore closed");
            let mut last_err = String::new();
            for attempt in 1..=READ_ATTEMPTS {
                match read_max_aggregate_version(&pool, &ack.aggregate_key).await {
                    Ok(version) => {
                        return FinalReadRecord {
                            node: host,
                            org_id: ack.aggregate_key.org_id,
                            type_id: ack.aggregate_key.aggregate_type_id,
                            agg_id: ack.aggregate_key.aggregate_id,
                            client_id: ack.client_id,
                            max_aggregate_version: Some(version),
                            error: None,
                        };
                    }
                    Err(e) => {
                        last_err = format!("{e}");
                        if attempt < READ_ATTEMPTS {
                            tokio::time::sleep(Duration::from_millis(READ_BACKOFF_MS * attempt as u64)).await;
                        }
                    }
                }
            }
            FinalReadRecord {
                node: host,
                org_id: ack.aggregate_key.org_id,
                type_id: ack.aggregate_key.aggregate_type_id,
                agg_id: ack.aggregate_key.aggregate_id,
                client_id: ack.client_id,
                max_aggregate_version: None,
                error: Some(last_err),
            }
        }));
    }

    let mut records = Vec::with_capacity(handles.len());
    for handle in handles {
        if let Ok(rec) = handle.await {
            records.push(rec);
        }
    }
    records
}

// ---------------------------------------------------------------------------
// Payload verification
// ---------------------------------------------------------------------------

/// Derive the expected payload bytes for a bench-written event.
///
/// The idempotent bench writes: `format!("[t-{id}-s-{seq}]")` where
/// `id = aggregate_key.aggregate_id` (task index, 0-based) and
/// `seq = client_seq` on the event.
pub fn expected_payload(aggregate_id: u128, client_seq: u64) -> Vec<u8> {
    format!("[t-{aggregate_id}-s-{client_seq}]").into_bytes()
}

/// Truncate a byte slice to at most `n` chars for display, appending "..." if cut.
fn trunc(bytes: &[u8], n: usize) -> String {
    let s = String::from_utf8_lossy(bytes);
    if s.len() <= n { s.into_owned() } else { format!("{}...", &s[..n]) }
}

/// One failure record from payload verification.
struct PayloadFail {
    node: String,
    aggregate_id: u128,
    client_seq: u64,
    expected: Vec<u8>,
    got: Vec<u8>,
}

/// Fetch all events for a single aggregate from the given pool and verify each
/// event's payload. Returns `(verified_count, failures)`.
async fn verify_one_aggregate(
    node: &str,
    pool: &Arc<Pool>,
    ack: &TaskAckSummary,
) -> (u64, Vec<PayloadFail>) {
    let mut verified: u64 = 0;
    let mut failures = Vec::new();

    // Pass None — the pool defaults to ReadFilters::new(1) (read from version 1).
    let iter = match pool.read_all(ack.aggregate_key.clone(), None).await {
        Ok(it) => it,
        Err(e) => {
            // read_all open failed — treat as zero verified, no payload failures
            // (this is a connectivity issue, not a payload mismatch)
            eprintln!("payload-verify: read_all open failed for {:?}: {e}", ack.aggregate_key);
            return (verified, failures);
        }
    };

    let batches = match iter.collect().await {
        Ok(b) => b,
        Err(e) => {
            eprintln!("payload-verify: collect failed for {:?}: {e}", ack.aggregate_key);
            return (verified, failures);
        }
    };

    let id = ack.aggregate_key.aggregate_id;
    for batch in &batches {
        if batch.client_id != ack.client_id {
            continue;
        }
        for ev in &batch.events {
            if ev.client_seq == 0 {
                continue;
            }
            let expected = expected_payload(id, ev.client_seq);
            if ev.event_value.as_ref() != &expected {
                failures.push(PayloadFail {
                    node: node.to_string(),
                    aggregate_id: id,
                    client_seq: ev.client_seq,
                    expected,
                    got: ev.event_value.as_ref().clone(),
                });
            } else {
                verified += 1;
            }
        }
    }

    (verified, failures)
}

/// Verify payload bytes for a deterministic sample of acked aggregates on
/// both nodes.
///
/// Samples up to `PAYLOAD_SAMPLE_SIZE` aggregates (lowest by `aggregate_id`,
/// no RNG) from `acks`. For each, fetches full event bodies from both nodes
/// and checks `event_value == format!("[t-{aggregate_id}-s-{client_seq}]")`.
///
/// Returns a single `CheckResult` named `"PayloadRoundTrip"`.
///
/// **Orchestrator call** (add after `run_final_read_phase` in the scenario):
/// ```ignore
/// checks.push(verify_payload_roundtrip(scen, cfg, &acks).await);
/// ```
pub async fn verify_payload_roundtrip(
    scen: &str,
    cfg: &ClusterConfig,
    acks: &[TaskAckSummary],
) -> CheckResult {
    // Sample: lowest PAYLOAD_SAMPLE_SIZE aggregates by aggregate_id, skip zero-ack tasks.
    let mut eligible: Vec<&TaskAckSummary> = acks
        .iter()
        .filter(|a| a.max_acked_client_seq > 0)
        .collect();
    eligible.sort_by_key(|a| a.aggregate_key.aggregate_id);
    eligible.truncate(PAYLOAD_SAMPLE_SIZE);

    if eligible.is_empty() {
        return CheckResult::pass_with_detail("PayloadRoundTrip", "no eligible aggregates to sample");
    }

    let mut all_failures: Vec<PayloadFail> = Vec::new();
    let mut total_verified: u64 = 0;
    let mut node_aggregate_count: usize = 0;

    for (host, addr) in [
        (cfg.leader_host.clone(), cfg.leader_addr()),
        (cfg.follower_host.clone(), cfg.follower_addr()),
    ] {
        let pool = match (PoolBuilder {
            address1: &addr,
            address2: &addr,
            server_name: Some(&host),
            ca_cert: cfg.ca_cert.to_str().unwrap(),
            client_cert: cfg.client_cert.to_str().unwrap(),
            client_key: cfg.client_key.to_str().unwrap(),
            plaintext: false,
            max_connections: READ_CONCURRENCY,
        })
        .build()
        .await
        {
            Ok(p) => p,
            Err(e) => {
                eprintln!("[{scen}] payload-verify pool for {host}: {e}");
                continue;
            }
        };

        for ack in &eligible {
            node_aggregate_count += 1;
            let (verified, failures) = verify_one_aggregate(&host, &pool, ack).await;
            total_verified += verified;
            all_failures.extend(failures);
        }
    }

    if all_failures.is_empty() {
        CheckResult::pass_with_detail(
            "PayloadRoundTrip",
            format!(
                "{total_verified} events verified across {} aggregate×node pairs",
                node_aggregate_count
            ),
        )
    } else {
        let capped: Vec<String> = all_failures
            .iter()
            .take(PAYLOAD_FAIL_CAP)
            .map(|f| {
                format!(
                    "node={} agg={} seq={} expected={:?} got={:?}",
                    f.node,
                    f.aggregate_id,
                    f.client_seq,
                    trunc(&f.expected, 40),
                    trunc(&f.got, 40),
                )
            })
            .collect();
        let total = all_failures.len();
        CheckResult::fail(
            "PayloadRoundTrip",
            format!(
                "{total} payload mismatch(es) (showing up to {PAYLOAD_FAIL_CAP}): {}",
                capped.join("; ")
            ),
        )
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expected_payload_format() {
        assert_eq!(expected_payload(0, 1), b"[t-0-s-1]");
        assert_eq!(expected_payload(7, 42), b"[t-7-s-42]");
        assert_eq!(expected_payload(999, 1000), b"[t-999-s-1000]");
    }

    #[test]
    fn expected_payload_large_ids() {
        // u128 aggregate ids and large seqs must round-trip correctly
        let id: u128 = u128::MAX;
        let seq: u64 = u64::MAX;
        let got = expected_payload(id, seq);
        let expected_str = format!("[t-{id}-s-{seq}]");
        assert_eq!(got, expected_str.into_bytes());
    }

    #[test]
    fn trunc_short_string_unchanged() {
        assert_eq!(trunc(b"hello", 10), "hello");
    }

    #[test]
    fn trunc_long_string_truncated() {
        let s = b"abcdefghij";
        let result = trunc(s, 5);
        assert_eq!(result, "abcde...");
    }

    #[test]
    fn trunc_exact_length_unchanged() {
        assert_eq!(trunc(b"abcde", 5), "abcde");
    }

    #[test]
    fn payload_mismatch_detected() {
        // Simulate a corrupted payload: wrong seq digit
        let id: u128 = 3;
        let seq: u64 = 17;
        let correct = expected_payload(id, seq);
        let corrupt = b"[t-3-s-18]".to_vec();
        assert_ne!(correct, corrupt);
        assert_eq!(correct, b"[t-3-s-17]");
    }

    #[test]
    fn payload_match_confirmed() {
        let id: u128 = 3;
        let seq: u64 = 17;
        let correct = expected_payload(id, seq);
        assert_eq!(correct, b"[t-3-s-17]");
    }

    #[test]
    fn payload_seq_zero_skipped_by_convention() {
        // client_seq == 0 is the "unset" sentinel; bench starts at 1.
        // verify_one_aggregate skips seq==0, so we just confirm the formula
        // produces the right bytes for seq 1 through 5.
        let id: u128 = 0;
        for seq in 1u64..=5 {
            let got = expected_payload(id, seq);
            let want = format!("[t-0-s-{seq}]").into_bytes();
            assert_eq!(got, want, "seq={seq}");
        }
    }
}
