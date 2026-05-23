//! Post-audit disk-truth verifier.
//!
//! The bench's `deep_audit_failing_aggregates` reads via the server's
//! `read()` API which has known agg_version-reuse / cache-split / pagination
//! bugs that over-report missing seqs by ~5×. This module bypasses the
//! server entirely: for each entry the audit flagged, SSH into BOTH data
//! nodes, run `celeriant-wal-inspect` on every shard's WAL files, parse the
//! per-batch lines to extract the actual on-disk client_seq set, and
//! reclassify the entry's "missing" against ground truth.
//!
//! See `docs/missing-data-progress.md` — the audit-overcounts finding from
//! the 1779152363 run.

use celeriant_bench::DeepAuditEntry;
use std::process::{Command, Stdio};

/// Per-entry verification result.
#[derive(Debug, Clone, serde::Serialize)]
pub struct DiskTruthEntry {
    pub aggregate_key_str: String,
    pub client_id: u128,
    pub max_acked: u64,
    /// What the deep audit thought was missing.
    pub audit_missing: Vec<u64>,
    /// What's *actually* missing — absent on both nodes' disks.
    pub actually_missing: Vec<u64>,
    /// Seqs the audit reported missing but found on at least one node.
    /// Non-empty = audit over-reported.
    pub audit_overreported: Vec<u64>,
    /// Per-node summary line (shard with non-zero match per node).
    pub leader_summary: String,
    pub follower_summary: String,
}

const DISK_TRUTH_WORKERS: usize = 16;

/// Run wal-inspect on both nodes for each entry, reclassify against disk.
/// SSH failures for an entry produce an empty DiskTruthEntry with a "no-batches"
/// summary; the entry stays in the report.
pub fn verify_against_disk_truth(
    leader_host: &str,
    follower_host: &str,
    entries: &[DeepAuditEntry],
) -> Vec<DiskTruthEntry> {
    use std::sync::Mutex;

    let work: Vec<&DeepAuditEntry> = entries
        .iter()
        .filter(|e| parse_agg_key(&e.aggregate_key_str).is_some())
        .collect();

    if work.is_empty() {
        return Vec::new();
    }

    let next = std::sync::atomic::AtomicUsize::new(0);
    let results: Mutex<Vec<Option<DiskTruthEntry>>> = Mutex::new((0..work.len()).map(|_| None).collect());

    std::thread::scope(|scope| {
        for _ in 0..DISK_TRUTH_WORKERS.min(work.len()) {
            scope.spawn(|| loop {
                let idx = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if idx >= work.len() {
                    return;
                }
                let e = work[idx];
                let entry = verify_one_entry(leader_host, follower_host, e);
                results.lock().unwrap()[idx] = Some(entry);
            });
        }
    });

    results.into_inner().unwrap().into_iter().flatten().collect()
}

fn verify_one_entry(leader_host: &str, follower_host: &str, e: &DeepAuditEntry) -> DiskTruthEntry {
    let (org, type_id, agg_id) = parse_agg_key(&e.aggregate_key_str).expect("filtered above");
    let leader = scan_node(leader_host, org, type_id, agg_id, e.client_id);
    let follower = scan_node(follower_host, org, type_id, agg_id, e.client_id);

    let union: std::collections::BTreeSet<u64> = leader.seqs.union(&follower.seqs).copied().collect();
    let actually_missing: Vec<u64> = (1..=e.max_acked).filter(|s| !union.contains(s)).collect();
    let audit_overreported: Vec<u64> = e
        .missing_seqs
        .iter()
        .filter(|s| union.contains(s))
        .copied()
        .collect();

    DiskTruthEntry {
        aggregate_key_str: e.aggregate_key_str.clone(),
        client_id: e.client_id,
        max_acked: e.max_acked,
        audit_missing: e.missing_seqs.clone(),
        actually_missing,
        audit_overreported,
        leader_summary: leader.summary,
        follower_summary: follower.summary,
    }
}

#[derive(Default)]
struct NodeScan {
    seqs: std::collections::BTreeSet<u64>,
    summary: String,
}

/// Run wal-inspect on shards 1..=3 (skip coordinator shard 0) for one
/// (org, type, agg, client) tuple on one node. Returns union of client_seqs
/// found across all shards' log files.
fn scan_node(host: &str, org: u128, type_id: u128, agg_id: u128, client_id: u128) -> NodeScan {
    let mut result = NodeScan::default();
    let mut summary_parts: Vec<String> = Vec::new();

    // Try shards 1..=3 (covers default num_shards=4 with reserve-coordinator).
    for shard in 1u32..=3 {
        // Glob log_*.wal — usually one file but bench can rotate.
        // The shell loop handles missing files (sudo prints to stderr only).
        let cmd = format!(
            "for f in /var/lib/celeriant/shard_{shard}/log_*.wal; do \
                 [ -f \"$f\" ] && sudo /usr/local/bin/celeriant-wal-inspect \"$f\" client {org} {type_id} {agg_id} {client_id} 2>/dev/null; \
             done"
        );
        let output = Command::new("ssh")
            .arg(host)
            .arg(&cmd)
            .stdin(Stdio::null())
            .output();

        let Ok(out) = output else { continue };
        if !out.status.success() {
            continue;
        }
        let stdout = String::from_utf8_lossy(&out.stdout);

        let (seqs, total) = parse_wal_inspect(&stdout);
        if total > 0 {
            summary_parts.push(format!("shard_{shard}:{total}"));
        }
        result.seqs.extend(seqs);
    }

    result.summary = if summary_parts.is_empty() {
        "no-batches".to_string()
    } else {
        summary_parts.join(",")
    };
    result
}

/// Parse wal-inspect `client` output. Each batch line is:
///   wal_seq=N agg_version=N client_seq=[A..B] offset=N within_read=true
/// We extract the [A..B] range. Returns (set of seqs, total batch count).
fn parse_wal_inspect(text: &str) -> (std::collections::BTreeSet<u64>, usize) {
    let mut seqs = std::collections::BTreeSet::new();
    let mut total = 0usize;
    for line in text.lines() {
        let Some(client_seq_pos) = line.find("client_seq=[") else { continue };
        let after = &line[client_seq_pos + "client_seq=[".len()..];
        let Some(end) = after.find(']') else { continue };
        let range_str = &after[..end];
        let mut parts = range_str.split("..");
        let lo: Option<u64> = parts.next().and_then(|s| s.parse().ok());
        let hi: Option<u64> = parts.next().and_then(|s| s.parse().ok());
        if let (Some(lo), Some(hi)) = (lo, hi) {
            for s in lo..=hi {
                if s > 0 {
                    seqs.insert(s);
                }
            }
            total += 1;
        }
    }
    (seqs, total)
}

/// Parse "ORG-HEX/TYPE-HEX/AGG-HEX" UUID-style strings into u128 components.
/// The chaos audit emits aggregate_key_str via celeriant_wal::AggregateKey's
/// Display impl which formats as `00000000-0000-0000-0000-NNNNNNNNNNNN/.../...`.
fn parse_agg_key(s: &str) -> Option<(u128, u128, u128)> {
    let mut parts = s.split('/');
    let org = parse_uuid_to_u128(parts.next()?)?;
    let type_id = parse_uuid_to_u128(parts.next()?)?;
    let agg_id = parse_uuid_to_u128(parts.next()?)?;
    Some((org, type_id, agg_id))
}

fn parse_uuid_to_u128(s: &str) -> Option<u128> {
    let cleaned: String = s.chars().filter(|c| *c != '-').collect();
    u128::from_str_radix(&cleaned, 16).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_agg_key_uuid_form() {
        let s = "00000000-0000-0000-0000-000000000001/00000000-0000-0000-0000-000000000002/00000000-0000-0000-0000-000000000003";
        assert_eq!(parse_agg_key(s), Some((1, 2, 3)));
    }

    #[test]
    fn parse_wal_inspect_extracts_seqs() {
        let text = "wal_seq=10 agg_version=1 client_seq=[1..1] offset=100 within_read=true\n\
                    wal_seq=20 agg_version=2 client_seq=[2..5] offset=200 within_read=true\n\
                    summary: ignored line\n";
        let (seqs, total) = parse_wal_inspect(text);
        assert_eq!(total, 2);
        assert_eq!(seqs.iter().copied().collect::<Vec<_>>(), vec![1, 2, 3, 4, 5]);
    }
}
