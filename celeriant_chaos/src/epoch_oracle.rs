//! Post-quiesce epoch oracles. Both run via SSH against BOTH nodes after
//! services are stopped (same operational context as `disk_truth` and
//! `tip_fork`).
//!
//! Oracle 1 — `run_epoch_oracle`:
//!   For each shard, fetches the tail ~5000 wal_seq entries from each node
//!   and asserts:
//!   (a) per-node: lease epoch is non-decreasing along ascending wal_seq;
//!   (b) across nodes: no wal_seq maps to two different lease epochs.
//!   (c) tip-hash cross-check: delegated to tip_fork::check_no_divergent_shard_tips
//!       which already asserts same-seq => same-tip-hash at the current WAL tips.
//!       This oracle therefore restricts itself to epoch-only cross-node checks.
//!
//! Oracle 2 — `run_acked_durability_oracle`:
//!   From the acked-write history, samples up to 24 aggregates and verifies
//!   every acked client_seq is present on BOTH nodes' disks post-quiesce via
//!   the same wal-inspect `client` subcommand used by `disk_truth`.

use std::collections::BTreeMap;
use std::process::{Command, Stdio};

use crate::config::ClusterConfig;
use crate::invariants::CheckResult;

/// An aggregate for which at least one write was acked, with the full set of
/// acked client_seqs. The orchestrator builds this from history.jsonl parsing.
pub struct AckedAggregate {
    pub org_id: u128,
    pub type_id: u128,
    pub agg_id: u128,
    pub client_id: u128,
    /// All client_seqs confirmed acked (outcome == ok, or idempotency rejection
    /// treated as ack). Must be non-empty.
    pub acked_seqs: Vec<u64>,
    /// Committed versions the server reported in those acks. Joins against
    /// the WAL's `agg_version` — the exact key for workloads whose writes
    /// carry no client_seq on disk (OCC). Empty for pre-field histories.
    pub acked_versions: Vec<u64>,
}

// --- Oracle 1: lease-epoch sanity ---

/// Oracle 1 public entry point.
///
/// For each data shard (1..=shard_count-1, skipping coordinator shard 0),
/// fetches a tail window of metablocks from both nodes via SSH + wal-inspect
/// `range` and asserts epoch monotonicity and cross-node consistency.
///
/// Returns two CheckResults per-call:
/// - "EpochMonotonicPerChain"
/// - "EpochUniquePerWalSeq"
pub async fn run_epoch_oracle(cfg: &ClusterConfig, shard_count: u32) -> Vec<CheckResult> {
    let leader = &cfg.leader_host;
    let follower = &cfg.follower_host;

    // Use tokio::task::spawn_blocking so the blocking SSH calls don't stall
    // the async runtime.
    let leader = leader.clone();
    let follower = follower.clone();
    tokio::task::spawn_blocking(move || {
        run_epoch_oracle_sync(&leader, &follower, shard_count)
    })
    .await
    .unwrap_or_else(|_| vec![
        // A panic inside the SSH/parse task is a harness bug, not evidence
        // of correctness — must not read as PASS.
        CheckResult::fail("EpochMonotonicPerChain", "oracle task panicked — unattestable"),
        CheckResult::fail("EpochUniquePerWalSeq", "oracle task panicked — unattestable"),
    ])
}

fn run_epoch_oracle_sync(leader: &str, follower: &str, shard_count: u32) -> Vec<CheckResult> {
    let mut monotonic_issues: Vec<String> = Vec::new();
    let mut unique_issues: Vec<String> = Vec::new();
    let mut skipped: Vec<String> = Vec::new();
    let mut shards_checked = 0u32;

    // Shards 1..=shard_count-1: skip coordinator shard 0.
    let data_shards: Vec<u32> = (1..shard_count).collect();

    for shard in data_shards {
        // Step 1: find the tip wal_seq on each node via `bounds`.
        let leader_tip = ssh_shard_tip(leader, shard);
        let follower_tip = ssh_shard_tip(follower, shard);

        let (l_tip, f_tip) = match (leader_tip, follower_tip) {
            (Some(l), Some(f)) => (l, f),
            (None, _) => {
                skipped.push(format!("shard_{shard}: could not get tip from {leader}"));
                continue;
            }
            (_, None) => {
                skipped.push(format!("shard_{shard}: could not get tip from {follower}"));
                continue;
            }
        };

        // Step 2: fetch the tail range of metablocks from both nodes.
        // We bound by the last ~5000 wal_seqs from each node's tip to keep SSH time sane.
        const TAIL_WINDOW: u64 = 5000;
        let l_start = l_tip.saturating_sub(TAIL_WINDOW);
        let f_start = f_tip.saturating_sub(TAIL_WINDOW);

        let leader_epochs = ssh_range_epochs(leader, shard, l_start, l_tip);
        let follower_epochs = ssh_range_epochs(follower, shard, f_start, f_tip);

        if leader_epochs.is_empty() && follower_epochs.is_empty() {
            skipped.push(format!("shard_{shard}: empty range output from both nodes"));
            continue;
        }

        shards_checked += 1;

        // (a) monotonicity: per node, epoch must be non-decreasing along wal_seq.
        check_epoch_monotonicity(leader, shard, &leader_epochs, &mut monotonic_issues);
        check_epoch_monotonicity(follower, shard, &follower_epochs, &mut monotonic_issues);

        // (b) cross-node uniqueness: for any wal_seq present on both nodes,
        //     both must report the same lease epoch.
        //
        // (c) tip-hash cross-check is delegated to tip_fork::check_no_divergent_shard_tips
        //     which already asserts same-seq => same-tip-hash at the WAL tips. This oracle
        //     restricts itself to epoch-only cross-node checks accordingly.
        check_epoch_cross_node(shard, &leader_epochs, &follower_epochs, leader, follower, &mut unique_issues);
    }

    let skip_suffix = if skipped.is_empty() {
        format!("{shards_checked} shard(s) checked")
    } else {
        format!("{shards_checked} shard(s) checked, skipped: {}", skipped.join("; "))
    };

    let mono_result = epoch_verdict("EpochMonotonicPerChain", shards_checked, &monotonic_issues, skip_suffix.clone());
    let unique_result = epoch_verdict("EpochUniquePerWalSeq", shards_checked, &unique_issues, skip_suffix);

    vec![mono_result, unique_result]
}

/// Fail-closed verdict shared by the epoch oracles: 0 shards checked (SSH/tool
/// unavailable on every shard) means the invariant was never verified and
/// must not report PASS. Mirrors `tip_fork::verdict`.
fn epoch_verdict(name: &'static str, shards_checked: u32, issues: &[String], detail_suffix: String) -> CheckResult {
    if shards_checked == 0 {
        return CheckResult::fail(name, format!("no shards checked — oracle unattestable ({detail_suffix})"));
    }
    if issues.is_empty() {
        CheckResult::pass_with_detail(name, detail_suffix)
    } else {
        CheckResult::fail(name, format!("{} — {}", issues.join("; "), detail_suffix))
    }
}

/// Get the last wal_seq on a shard via `bounds`, returning None on SSH error
/// or empty output.
fn ssh_shard_tip(host: &str, shard: u32) -> Option<u64> {
    let cmd = format!(
        "for f in /var/lib/nvme/celeriant-data/shard_{shard}/log_*.wal; do \
             [ -f \"$f\" ] && sudo /usr/local/bin/celeriant-wal-inspect \"$f\" bounds 2>/dev/null; \
         done"
    );
    let out = Command::new("ssh")
        .arg(host)
        .arg(&cmd)
        .stdin(Stdio::null())
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    parse_last_wal_seq(&String::from_utf8_lossy(&out.stdout))
}

/// Parse the highest `last metablock: wal_seq = N` from concatenated `bounds`
/// output. We take the maximum across all segment files.
fn parse_last_wal_seq(text: &str) -> Option<u64> {
    let mut best: Option<u64> = None;
    for line in text.lines() {
        // Line format: "last  metablock: wal_seq = N, offset = M"
        let trimmed = line.trim();
        if !trimmed.starts_with("last") {
            continue;
        }
        if let Some(pos) = trimmed.find("wal_seq = ") {
            let after = &trimmed[pos + "wal_seq = ".len()..];
            // Value ends at comma or end of string.
            let end = after.find(',').unwrap_or(after.len());
            if let Ok(v) = after[..end].trim().parse::<u64>() {
                best = Some(best.map_or(v, |b: u64| b.max(v)));
            }
        }
    }
    best
}

/// Fetch (wal_seq → lease_epoch) pairs from a node for the given shard and
/// wal_seq range. Returns an empty map on SSH error or parse failure.
///
/// Uses `wal-inspect range start end` which prints one line per metablock:
///   wal_seq = N | lease = EPOCH | offset = ... | ...
fn ssh_range_epochs(host: &str, shard: u32, start: u64, end: u64) -> BTreeMap<u64, u64> {
    let cmd = format!(
        "for f in /var/lib/nvme/celeriant-data/shard_{shard}/log_*.wal; do \
             [ -f \"$f\" ] && sudo /usr/local/bin/celeriant-wal-inspect \"$f\" range {start} {end} 2>/dev/null; \
         done"
    );
    let out = match Command::new("ssh")
        .arg(host)
        .arg(&cmd)
        .stdin(Stdio::null())
        .output()
    {
        Ok(o) if o.status.success() => o,
        _ => return BTreeMap::new(),
    };
    parse_range_epochs(&String::from_utf8_lossy(&out.stdout))
}

/// Parse metablock summary lines from `wal-inspect range` output.
///
/// Each entry line has the form:
///   wal_seq = N | lease = EPOCH | offset = ... | ...
///
/// Returns a BTreeMap from wal_seq to lease_epoch. When the same wal_seq
/// appears in multiple segment files (shouldn't happen but handle gracefully),
/// we keep the first seen.
pub(crate) fn parse_range_epochs(text: &str) -> BTreeMap<u64, u64> {
    let mut out = BTreeMap::new();
    for line in text.lines() {
        // The summary/printed-count line: "printed N metablocks in [S..=E]" — skip.
        if line.trim().starts_with("printed ") {
            continue;
        }
        // Indented detail lines (previous_tip_hash etc) — skip.
        if line.starts_with(' ') || line.starts_with('\t') {
            continue;
        }
        // Main entry: "wal_seq = N | lease = E | ..."
        let Some(ws_pos) = line.find("wal_seq = ") else { continue };
        let after_ws = &line[ws_pos + "wal_seq = ".len()..];
        let ws_end = after_ws.find(" |").unwrap_or(after_ws.len());
        let Ok(wal_seq) = after_ws[..ws_end].trim().parse::<u64>() else { continue };

        let Some(le_pos) = line.find("lease = ") else { continue };
        let after_le = &line[le_pos + "lease = ".len()..];
        let le_end = after_le.find(" |").unwrap_or(after_le.len());
        let Ok(epoch) = after_le[..le_end].trim().parse::<u64>() else { continue };

        out.entry(wal_seq).or_insert(epoch);
    }
    out
}

/// Assert non-decreasing epoch along ascending wal_seq. Pushes at most one
/// violation per shard per node into `issues`.
fn check_epoch_monotonicity(
    host: &str,
    shard: u32,
    epochs: &BTreeMap<u64, u64>,
    issues: &mut Vec<String>,
) {
    if issues.len() >= 10 {
        return;
    }
    let mut prev_epoch: Option<u64> = None;
    let mut prev_seq: u64 = 0;
    for (&seq, &epoch) in epochs {
        if let Some(pe) = prev_epoch {
            if epoch < pe {
                issues.push(format!(
                    "shard_{shard} {host}: epoch decreased at wal_seq={seq} ({pe} → {epoch}, prev_seq={prev_seq})"
                ));
                if issues.len() >= 10 {
                    return;
                }
            }
        }
        prev_epoch = Some(epoch);
        prev_seq = seq;
    }
}

/// Assert that for any wal_seq present on both nodes, both report the same
/// lease epoch. Pushes at most 10 total violations into `issues`.
fn check_epoch_cross_node(
    shard: u32,
    leader_epochs: &BTreeMap<u64, u64>,
    follower_epochs: &BTreeMap<u64, u64>,
    leader: &str,
    follower: &str,
    issues: &mut Vec<String>,
) {
    for (&seq, &l_epoch) in leader_epochs {
        if issues.len() >= 10 {
            return;
        }
        if let Some(&f_epoch) = follower_epochs.get(&seq) {
            if l_epoch != f_epoch {
                issues.push(format!(
                    "shard_{shard} wal_seq={seq}: {leader} epoch={l_epoch} vs {follower} epoch={f_epoch}"
                ));
            }
        }
    }
}

// --- Oracle 2: acked ⊆ durable-on-both ---

/// Oracle 2 public entry point.
///
/// Samples up to 24 aggregates uniformly from `acked` (seeded by `seed` for
/// reproducibility). For each sample, runs `wal-inspect client` on BOTH nodes
/// across all data shards and asserts every acked client_seq is present on
/// both nodes' disks.
///
/// Returns one CheckResult: "AckedSubsetDurableBothNodes".
pub async fn run_acked_durability_oracle(
    cfg: &ClusterConfig,
    acked: &[AckedAggregate],
    seed: u64,
) -> Vec<CheckResult> {
    if acked.is_empty() {
        return vec![CheckResult::pass_with_detail(
            "AckedSubsetDurableBothNodes",
            "(skipped: no acked aggregates provided)",
        )];
    }

    let leader = cfg.leader_host.clone();
    let follower = cfg.follower_host.clone();

    // Clone the data we need for the blocking closure.
    let samples = sample_aggregates(acked, seed, 24);

    tokio::task::spawn_blocking(move || {
        run_acked_durability_sync(&leader, &follower, &samples)
    })
    .await
    .unwrap_or_else(|_| vec![
        // Same policy as run_epoch_oracle: a panicked oracle task proves
        // nothing and must not read as PASS.
        CheckResult::fail("AckedSubsetDurableBothNodes", "oracle task panicked — unattestable"),
    ])
}

/// A clone-able representation of AckedAggregate used for the blocking closure.
struct SampledAggregate {
    org_id: u128,
    type_id: u128,
    agg_id: u128,
    client_id: u128,
    acked_seqs: Vec<u64>,
    acked_versions: Vec<u64>,
}

/// Sample up to `cap` aggregates uniformly from `aggs` using a seeded LCG.
/// Deterministic: same seed + same aggs => same sample.
fn sample_aggregates(aggs: &[AckedAggregate], seed: u64, cap: usize) -> Vec<SampledAggregate> {
    let n = aggs.len();
    if n <= cap {
        return aggs.iter().map(|a| SampledAggregate {
            org_id: a.org_id,
            type_id: a.type_id,
            agg_id: a.agg_id,
            client_id: a.client_id,
            acked_seqs: a.acked_seqs.clone(),
            acked_versions: a.acked_versions.clone(),
        }).collect();
    }

    // Reservoir sampling with seeded LCG for the random stream.
    // Knuth's multiplicative LCG: next = state * 6364136223846793005 + seed.
    let mut state = seed ^ 0xa5a5a5a5a5a5a5a5;
    let lcg_next = |s: &mut u64| -> u64 {
        *s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        *s
    };

    let mut reservoir: Vec<usize> = (0..cap).collect();
    for i in cap..n {
        let j = (lcg_next(&mut state) as usize) % (i + 1);
        if j < cap {
            reservoir[j] = i;
        }
    }
    reservoir.sort_unstable();
    reservoir.iter().map(|&i| SampledAggregate {
        org_id: aggs[i].org_id,
        type_id: aggs[i].type_id,
        agg_id: aggs[i].agg_id,
        client_id: aggs[i].client_id,
        acked_seqs: aggs[i].acked_seqs.clone(),
        acked_versions: aggs[i].acked_versions.clone(),
    }).collect()
}

fn run_acked_durability_sync(
    leader: &str,
    follower: &str,
    samples: &[SampledAggregate],
) -> Vec<CheckResult> {
    let mut issues: Vec<String> = Vec::new();
    let mut checked = 0u32;

    for agg in samples {
        // Scan shards 1..=3 on both nodes (disk_truth pattern).
        let leader_scan = scan_node_client_seqs(leader, agg.org_id, agg.type_id, agg.agg_id, agg.client_id);
        let follower_scan = scan_node_client_seqs(follower, agg.org_id, agg.type_id, agg.agg_id, agg.client_id);

        checked += 1;

        // Workloads that don't ride the idempotency mechanism (e.g. cas_storm's
        // OCC writes) persist client_seq=[0..0] in the metablock — the bench's
        // bookkeeping seq has no on-disk counterpart to join on. For those,
        // join on the committed version the server named in each ack against
        // the WAL's agg_version: exact (names WHICH version is missing).
        // Count comparison remains the last resort for histories that predate
        // server-told versions.
        let seq_join_valid = !leader_scan.seqs.is_empty() || !follower_scan.seqs.is_empty();
        if !seq_join_valid {
            if !agg.acked_versions.is_empty() {
                for &v in &agg.acked_versions {
                    if issues.len() >= 10 {
                        break;
                    }
                    for (node, scan) in [(leader, &leader_scan), (follower, &follower_scan)] {
                        if !scan.versions.contains(&v) {
                            issues.push(format!(
                                "node={node} agg={}/{}/{} client={} missing acked version={v}",
                                agg.org_id, agg.type_id, agg.agg_id, agg.client_id
                            ));
                        }
                    }
                }
                continue;
            }
            for (node, batches) in [(leader, leader_scan.batches), (follower, follower_scan.batches)] {
                if issues.len() >= 10 {
                    break;
                }
                if batches < agg.acked_seqs.len() {
                    issues.push(format!(
                        "node={node} agg={}/{}/{} client={} has {} durable batches for {} acked writes (count join — WAL carries no client_seq for this workload)",
                        agg.org_id, agg.type_id, agg.agg_id, agg.client_id, batches, agg.acked_seqs.len()
                    ));
                }
            }
            continue;
        }

        for &acked_seq in &agg.acked_seqs {
            if issues.len() >= 10 {
                break;
            }
            if !leader_scan.seqs.contains(&acked_seq) {
                issues.push(format!(
                    "node={leader} agg={}/{}/{} client={} missing acked seq={}",
                    agg.org_id, agg.type_id, agg.agg_id, agg.client_id, acked_seq
                ));
            }
            if !follower_scan.seqs.contains(&acked_seq) {
                issues.push(format!(
                    "node={follower} agg={}/{}/{} client={} missing acked seq={}",
                    agg.org_id, agg.type_id, agg.agg_id, agg.client_id, acked_seq
                ));
            }
        }
    }

    let detail_prefix = format!("{checked} aggregate(s) checked");
    let result = if issues.is_empty() {
        CheckResult::pass_with_detail("AckedSubsetDurableBothNodes", detail_prefix)
    } else {
        let capped = if issues.len() == 10 { " (capped at 10 entries)" } else { "" };
        CheckResult::fail(
            "AckedSubsetDurableBothNodes",
            format!("{} — {}{}", detail_prefix, issues.join("; "), capped),
        )
    };
    vec![result]
}

/// One node's durable batches for a (org, type, agg, client) tuple.
struct NodeScan {
    /// Union of client_seqs across shards (empty for OCC workloads, which
    /// persist client_seq=[0..0]).
    seqs: std::collections::BTreeSet<u64>,
    /// agg_version of every batch found — the join key for OCC workloads.
    versions: std::collections::BTreeSet<u64>,
    /// Total batch lines, for the count-join fallback.
    batches: usize,
}

/// Scan shards 1..=3 for a (org, type, agg, client) tuple on one node.
/// Returns the union of all client_seqs found across shards. Mirrors
/// `disk_truth::scan_node` exactly: same path pattern, same binary, same
/// parsing via `parse_wal_inspect`.
fn scan_node_client_seqs(
    host: &str,
    org: u128,
    type_id: u128,
    agg_id: u128,
    client_id: u128,
) -> NodeScan {
    let mut scan = NodeScan {
        seqs: std::collections::BTreeSet::new(),
        versions: std::collections::BTreeSet::new(),
        batches: 0,
    };
    for shard in 1u32..=3 {
        let cmd = format!(
            "for f in /var/lib/nvme/celeriant-data/shard_{shard}/log_*.wal; do \
                 [ -f \"$f\" ] && sudo /usr/local/bin/celeriant-wal-inspect \"$f\" client {org} {type_id} {agg_id} {client_id} 2>/dev/null; \
             done"
        );
        let out = match Command::new("ssh")
            .arg(host)
            .arg(&cmd)
            .stdin(Stdio::null())
            .output()
        {
            Ok(o) if o.status.success() => o,
            _ => continue,
        };
        let shard_scan = parse_wal_inspect_client(&String::from_utf8_lossy(&out.stdout));
        scan.seqs.extend(shard_scan.seqs);
        scan.versions.extend(shard_scan.versions);
        scan.batches += shard_scan.batches;
    }
    scan
}

/// Parse wal-inspect `client` output. Identical logic to `disk_truth::parse_wal_inspect`.
/// Each batch line:
///   wal_seq=N agg_version=N client_seq=[A..B] offset=N within_read=true
fn parse_wal_inspect_client(text: &str) -> NodeScan {
    let mut scan = NodeScan {
        seqs: std::collections::BTreeSet::new(),
        versions: std::collections::BTreeSet::new(),
        batches: 0,
    };
    for line in text.lines() {
        let Some(pos) = line.find("client_seq=[") else { continue };
        let after = &line[pos + "client_seq=[".len()..];
        let Some(end) = after.find(']') else { continue };
        let range_str = &after[..end];
        let mut parts = range_str.split("..");
        let lo: Option<u64> = parts.next().and_then(|s| s.parse().ok());
        let hi: Option<u64> = parts.next().and_then(|s| s.parse().ok());
        if let (Some(lo), Some(hi)) = (lo, hi) {
            for s in lo..=hi {
                if s > 0 {
                    scan.seqs.insert(s);
                }
            }
            scan.batches += 1;
            if let Some(vp) = line.find("agg_version=") {
                let after_v = &line[vp + "agg_version=".len()..];
                let v_end = after_v.find(' ').unwrap_or(after_v.len());
                if let Ok(v) = after_v[..v_end].trim().parse::<u64>() {
                    scan.versions.insert(v);
                }
            }
        }
    }
    scan
}

// --- Unit tests ---

#[cfg(test)]
mod tests {
    use super::*;

    // --- parse_range_epochs tests ---

    #[test]
    fn parse_range_epochs_extracts_seq_and_epoch() {
        let text = "wal_seq = 100 | lease = 3 | offset = 1024 | server_ts = 12345 | node = 00000001\n\
                    wal_seq = 101 | lease = 3 | offset = 2048 | server_ts = 12346 | node = 00000001\n\
                    wal_seq = 102 | lease = 4 | offset = 3072 | server_ts = 12347 | node = 00000001\n\
                    printed 3 metablocks in [100..=102]\n";
        let epochs = parse_range_epochs(text);
        assert_eq!(epochs.len(), 3);
        assert_eq!(epochs[&100], 3);
        assert_eq!(epochs[&101], 3);
        assert_eq!(epochs[&102], 4);
    }

    #[test]
    fn parse_range_epochs_skips_indented_and_summary_lines() {
        let text = "wal_seq = 10 | lease = 1 | offset = 512 | server_ts = 0 | node = 00000001\n\
                      previous_tip_hash             = aabbccdd\n\
                      uncompressed_size             = 256\n\
                    printed 1 metablocks in [10..=10]\n";
        let epochs = parse_range_epochs(text);
        assert_eq!(epochs.len(), 1);
        assert_eq!(epochs[&10], 1);
    }

    #[test]
    fn parse_range_epochs_empty_input() {
        assert!(parse_range_epochs("").is_empty());
        assert!(parse_range_epochs("printed 0 metablocks in [100..=100]\n").is_empty());
    }

    #[test]
    fn parse_range_epochs_deduplicates_same_seq_keeps_first() {
        // Two segments both contain seq 50 with the same epoch.
        let text = "wal_seq = 50 | lease = 7 | offset = 100 | server_ts = 0 | node = 1\n\
                    wal_seq = 50 | lease = 8 | offset = 200 | server_ts = 0 | node = 1\n";
        let epochs = parse_range_epochs(text);
        assert_eq!(epochs.len(), 1);
        assert_eq!(epochs[&50], 7); // first wins
    }

    // --- epoch monotonicity logic tests ---

    #[test]
    fn epoch_monotonicity_passes_non_decreasing() {
        let mut epochs = BTreeMap::new();
        epochs.insert(1u64, 3u64);
        epochs.insert(2, 3);
        epochs.insert(3, 4);
        epochs.insert(4, 4);
        let mut issues = Vec::new();
        check_epoch_monotonicity("host1", 1, &epochs, &mut issues);
        assert!(issues.is_empty(), "non-decreasing sequence should pass: {:?}", issues);
    }

    #[test]
    fn epoch_monotonicity_fails_decrease() {
        let mut epochs = BTreeMap::new();
        epochs.insert(10u64, 5u64);
        epochs.insert(11, 4); // decreases: violation
        epochs.insert(12, 6);
        let mut issues = Vec::new();
        check_epoch_monotonicity("cs1", 2, &epochs, &mut issues);
        assert_eq!(issues.len(), 1);
        assert!(issues[0].contains("wal_seq=11"), "issue should name the violating seq: {}", issues[0]);
        assert!(issues[0].contains("5 → 4"), "issue should show the epoch transition: {}", issues[0]);
    }

    #[test]
    fn epoch_monotonicity_single_entry_passes() {
        let mut epochs = BTreeMap::new();
        epochs.insert(99u64, 2u64);
        let mut issues = Vec::new();
        check_epoch_monotonicity("cs2", 1, &epochs, &mut issues);
        assert!(issues.is_empty());
    }

    // --- cross-node epoch uniqueness tests ---

    #[test]
    fn cross_node_same_epoch_passes() {
        let mut leader = BTreeMap::new();
        leader.insert(100u64, 5u64);
        leader.insert(101, 5);
        leader.insert(102, 6);

        let mut follower = BTreeMap::new();
        follower.insert(100u64, 5u64);
        follower.insert(102, 6);

        let mut issues = Vec::new();
        check_epoch_cross_node(1, &leader, &follower, "cs1", "cs2", &mut issues);
        assert!(issues.is_empty(), "matching epochs should pass: {:?}", issues);
    }

    #[test]
    fn cross_node_epoch_mismatch_fails() {
        let mut leader = BTreeMap::new();
        leader.insert(50u64, 3u64);

        let mut follower = BTreeMap::new();
        follower.insert(50u64, 4u64); // same seq, different epoch

        let mut issues = Vec::new();
        check_epoch_cross_node(2, &leader, &follower, "cs1", "cs2", &mut issues);
        assert_eq!(issues.len(), 1);
        assert!(issues[0].contains("wal_seq=50"), "issue should name the seq: {}", issues[0]);
        assert!(issues[0].contains("epoch=3"), "issue should show leader epoch: {}", issues[0]);
        assert!(issues[0].contains("epoch=4"), "issue should show follower epoch: {}", issues[0]);
    }

    #[test]
    fn cross_node_disjoint_seq_ranges_pass() {
        // Leader has seq 1-100, follower has seq 200-300: no overlap, no check.
        let leader: BTreeMap<u64, u64> = (1u64..=100).map(|s| (s, 1u64)).collect();
        let follower: BTreeMap<u64, u64> = (200u64..=300).map(|s| (s, 2u64)).collect();
        let mut issues = Vec::new();
        check_epoch_cross_node(1, &leader, &follower, "cs1", "cs2", &mut issues);
        assert!(issues.is_empty());
    }

    // --- epoch_verdict fail-closed tests ---

    #[test]
    fn epoch_verdict_fails_closed_on_zero_shards_checked() {
        let r = epoch_verdict("EpochMonotonicPerChain", 0, &[], "0 shard(s) checked".into());
        assert!(!r.passed);
        assert!(r.detail.contains("oracle unattestable"), "{}", r.detail);
    }

    #[test]
    fn epoch_verdict_passes_when_shards_checked_and_clean() {
        let r = epoch_verdict("EpochMonotonicPerChain", 2, &[], "2 shard(s) checked".into());
        assert!(r.passed, "{}", r.detail);
    }

    #[test]
    fn epoch_verdict_fails_on_real_issue() {
        let r = epoch_verdict("EpochUniquePerWalSeq", 1, &["shard_1: mismatch".to_string()], "1 shard(s) checked".into());
        assert!(!r.passed);
        assert!(r.detail.contains("mismatch"));
    }

    // --- sampling determinism tests ---

    #[test]
    fn sampling_determinism_same_seed_same_result() {
        let aggs: Vec<AckedAggregate> = (0u128..50).map(|i| AckedAggregate {
            org_id: 1,
            type_id: 1,
            agg_id: i,
            client_id: i + 1,
            acked_seqs: vec![1, 2, 3],
            acked_versions: vec![],
        }).collect();
        let s1 = sample_aggregates(&aggs, 42, 24);
        let s2 = sample_aggregates(&aggs, 42, 24);
        let ids1: Vec<u128> = s1.iter().map(|a| a.agg_id).collect();
        let ids2: Vec<u128> = s2.iter().map(|a| a.agg_id).collect();
        assert_eq!(ids1, ids2, "same seed must produce identical samples");
    }

    #[test]
    fn sampling_determinism_different_seeds_differ() {
        let aggs: Vec<AckedAggregate> = (0u128..50).map(|i| AckedAggregate {
            org_id: 1,
            type_id: 1,
            agg_id: i,
            client_id: i + 1,
            acked_seqs: vec![1],
            acked_versions: vec![],
        }).collect();
        let s1 = sample_aggregates(&aggs, 0, 24);
        let s2 = sample_aggregates(&aggs, 9999, 24);
        let ids1: Vec<u128> = s1.iter().map(|a| a.agg_id).collect();
        let ids2: Vec<u128> = s2.iter().map(|a| a.agg_id).collect();
        assert_ne!(ids1, ids2, "different seeds should (statistically) produce different samples");
    }

    #[test]
    fn sampling_fewer_than_cap_returns_all() {
        let aggs: Vec<AckedAggregate> = (0u128..10).map(|i| AckedAggregate {
            org_id: 1,
            type_id: 1,
            agg_id: i,
            client_id: i + 1,
            acked_seqs: vec![1],
            acked_versions: vec![],
        }).collect();
        let samples = sample_aggregates(&aggs, 7, 24);
        assert_eq!(samples.len(), 10);
    }

    #[test]
    fn sampling_exactly_cap_returns_all() {
        let aggs: Vec<AckedAggregate> = (0u128..24).map(|i| AckedAggregate {
            org_id: 1,
            type_id: 1,
            agg_id: i,
            client_id: i + 1,
            acked_seqs: vec![1],
            acked_versions: vec![],
        }).collect();
        let samples = sample_aggregates(&aggs, 1, 24);
        assert_eq!(samples.len(), 24);
    }

    #[test]
    fn sampling_more_than_cap_returns_cap() {
        let aggs: Vec<AckedAggregate> = (0u128..100).map(|i| AckedAggregate {
            org_id: 1,
            type_id: 1,
            agg_id: i,
            client_id: i + 1,
            acked_seqs: vec![1],
            acked_versions: vec![],
        }).collect();
        let samples = sample_aggregates(&aggs, 42, 24);
        assert_eq!(samples.len(), 24);
    }

    // --- parse_last_wal_seq tests ---

    #[test]
    fn parse_last_wal_seq_extracts_value() {
        let text = "first metablock: wal_seq = 100, offset = 512\n\
                    last  metablock: wal_seq = 9999, offset = 102400\n\
                    count          = 9900\n";
        assert_eq!(parse_last_wal_seq(text), Some(9999));
    }

    #[test]
    fn parse_last_wal_seq_picks_max_across_segments() {
        // Two segment files concatenated: second has higher wal_seq.
        let text = "first metablock: wal_seq = 1, offset = 512\n\
                    last  metablock: wal_seq = 500, offset = 51200\n\
                    count          = 500\n\
                    first metablock: wal_seq = 501, offset = 512\n\
                    last  metablock: wal_seq = 1200, offset = 102400\n\
                    count          = 700\n";
        assert_eq!(parse_last_wal_seq(text), Some(1200));
    }

    #[test]
    fn parse_last_wal_seq_returns_none_for_empty() {
        assert!(parse_last_wal_seq("").is_none());
        assert!(parse_last_wal_seq("no metablocks found\n").is_none());
    }

    // --- parse_wal_inspect_client tests ---

    #[test]
    fn parse_wal_inspect_client_extracts_ranges() {
        let text = "wal_seq=10 agg_version=1 client_seq=[1..1] offset=100 within_read=true\n\
                    wal_seq=20 agg_version=2 client_seq=[2..5] offset=200 within_read=true\n\
                    summary: ignored\n";
        let scan = parse_wal_inspect_client(text);
        assert_eq!(scan.batches, 2);
        assert_eq!(scan.seqs.iter().copied().collect::<Vec<_>>(), vec![1, 2, 3, 4, 5]);
        assert_eq!(scan.versions.iter().copied().collect::<Vec<_>>(), vec![1, 2]);
    }

    #[test]
    fn parse_wal_inspect_client_occ_batches_carry_versions_only() {
        // OCC workloads persist client_seq=[0..0]: no seq join key, but every
        // batch still names its committed agg_version.
        let text = "wal_seq=10 agg_version=7 client_seq=[0..0] offset=100 within_read=true\n\
                    wal_seq=11 agg_version=8 client_seq=[0..0] offset=200 within_read=true\n";
        let scan = parse_wal_inspect_client(text);
        assert_eq!(scan.batches, 2);
        assert!(scan.seqs.is_empty());
        assert_eq!(scan.versions.iter().copied().collect::<Vec<_>>(), vec![7, 8]);
    }

    #[test]
    fn parse_wal_inspect_client_empty_input() {
        let scan = parse_wal_inspect_client("");
        assert_eq!(scan.batches, 0);
        assert!(scan.seqs.is_empty());
        assert!(scan.versions.is_empty());
    }
}
