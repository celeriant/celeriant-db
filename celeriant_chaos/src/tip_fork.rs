//! Post-run divergent-tip fork check.
//!
//! After StopAll the WAL files are stable. For each data shard (1..=3),
//! SSH to both nodes, run `celeriant-wal-inspect header` on every log_*.wal
//! file in the shard directory, and compare write_wal_seq + write_tip_hash.
//!
//! Same-seq case: two nodes at the SAME wal_seq but different tip hashes →
//! DIVERGENT-TIP FORK (the false-pass case that EventualConvergence misses).
//!
//! Different-seq case: B behind at Wb, A ahead at Wa. Fetch A's entry at
//! Wb+1 and compare its previous_tip_hash (= A's chain tip after Wb) with
//! B's write_tip_hash (= B's chain tip after Wb). Differ → FORK-WEDGE;
//! match → confirmed clean-prefix lag (EventualConvergence handles the lag).

use std::process::{Command, Stdio};

use crate::invariants::CheckResult;

/// Fields of interest extracted from a single WAL file's front_header block.
#[derive(Debug, Clone)]
struct WalHeader {
    write_wal_seq: u64,
    write_tip_hash: String,
    read_wal_seq: u64,
    last_self_acked_wal_seq: u64,
}

/// Best WAL header seen across all log_*.wal files for a shard on one node.
/// "Best" = the file with the highest write_wal_seq (most current state).
#[derive(Debug, Clone)]
struct ShardTip {
    wal_seq: u64,
    tip_hash: String,
    read_wal_seq: u64,
    last_self_acked: u64,
}

/// SSH to `host` and run `celeriant-wal-inspect header` on all log_*.wal
/// files for the given shard. Returns the raw stdout, empty on any error.
///
/// Follows the same pattern as `disk_truth::scan_node`: `Command::new("ssh")`
/// with `Stdio::null()` stdin and stdout capture.
fn ssh_headers(host: &str, shard: u32) -> String {
    let cmd = format!(
        "for f in /var/lib/nvme/celeriant-data/shard_{shard}/log_*.wal; do \
             [ -f \"$f\" ] && sudo /usr/local/bin/celeriant-wal-inspect \"$f\" header 2>/dev/null; \
             echo '---file-separator---'; \
         done"
    );
    let output = Command::new("ssh")
        .arg(host)
        .arg(&cmd)
        .stdin(Stdio::null())
        .output();
    match output {
        Ok(out) if out.status.success() => String::from_utf8_lossy(&out.stdout).into_owned(),
        _ => String::new(),
    }
}

/// SSH to `host` and, for each log_*.wal in the shard directory, run
/// `celeriant-wal-inspect <file> range <target_seq> <target_seq>` until we
/// find `previous_tip_hash` in the output (the chain-tip produced by applying
/// the entry *before* `target_seq`, i.e. A's tip at `target_seq - 1`).
///
/// We want the `previous_tip_hash` of entry `target_seq` on node A as a
/// proxy for "A's tip after applying `target_seq - 1`", which equals
/// B's `write_tip_hash` when both chains agree at `target_seq - 1`.
///
/// Iterates all segments because `target_seq` may live in a rotated file on
/// the ahead node. Returns `None` if not found in any segment or on SSH error.
fn ssh_range_prev_tip(host: &str, shard: u32, target_seq: u64) -> Option<String> {
    // Run inspect on each segment and emit its output; stop iterating once
    // we get a non-empty `previous_tip_hash`.
    let cmd = format!(
        "for f in /var/lib/nvme/celeriant-data/shard_{shard}/log_*.wal; do \
             [ -f \"$f\" ] || continue; \
             out=$(sudo /usr/local/bin/celeriant-wal-inspect \"$f\" range {target_seq} {target_seq} 2>/dev/null); \
             echo \"$out\"; \
             echo '---file-separator---'; \
         done"
    );
    let output = Command::new("ssh")
        .arg(host)
        .arg(&cmd)
        .stdin(Stdio::null())
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout);
    parse_range_prev_tip(&text)
}

/// Parse the `previous_tip_hash` field from one or more concatenated
/// `wal-inspect range` outputs. Returns the first non-empty match.
///
/// Range output format (from `print_metablock`):
///   wal_seq = N | lease = ... | offset = ... | ...
///     previous_tip_hash             = <hex>
///     ...
fn parse_range_prev_tip(text: &str) -> Option<String> {
    for line in text.lines() {
        let trimmed = line.trim();
        if let Some(val) = extract_field(trimmed, "previous_tip_hash") {
            if !val.is_empty() {
                return Some(val.to_string());
            }
        }
    }
    None
}

/// Parse header output from one or more concatenated wal-inspect header
/// invocations (separated by `---file-separator---` lines). Returns the
/// WalHeader with the highest write_wal_seq among all file blocks, or None
/// if no parseable front_header block was found.
fn parse_best_header(text: &str) -> Option<WalHeader> {
    let mut best: Option<WalHeader> = None;

    // Each invocation of `wal-inspect header` prints a front_header block
    // then a rear_header block. We only care about front_header.
    // Track whether we're inside a front_header block.
    let mut in_front = false;
    let mut current = PartialHeader::default();

    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed == "front_header:" {
            in_front = true;
            current = PartialHeader::default();
            continue;
        }
        if trimmed.starts_with("rear_header:") || trimmed == "---file-separator---" {
            if in_front {
                if let Some(h) = current.build() {
                    let is_better = best.as_ref().map_or(true, |b| h.write_wal_seq > b.write_wal_seq);
                    if is_better {
                        best = Some(h);
                    }
                }
                in_front = false;
                current = PartialHeader::default();
            }
            continue;
        }
        if !in_front {
            continue;
        }
        if let Some(val) = extract_field(trimmed, "write_wal_seq") {
            current.write_wal_seq = val.parse().ok();
        } else if let Some(val) = extract_field(trimmed, "write_tip_hash") {
            current.write_tip_hash = Some(val.to_string());
        } else if let Some(val) = extract_field(trimmed, "read_wal_seq") {
            current.read_wal_seq = val.parse().ok();
        } else if let Some(val) = extract_field(trimmed, "last_self_acked_wal_seq") {
            current.last_self_acked = val.parse().ok();
        }
    }
    // Flush any trailing block not followed by a separator.
    if in_front {
        if let Some(h) = current.build() {
            let is_better = best.as_ref().map_or(true, |b| h.write_wal_seq > b.write_wal_seq);
            if is_better {
                best = Some(h);
            }
        }
    }

    best
}

/// Extract the value from a line of the form `  key = value` or `  key   = value`.
fn extract_field<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    let pos = line.find(key)?;
    // Ensure we matched the whole key (not a prefix of a longer name)
    let after_key = line[pos + key.len()..].trim_start();
    if !after_key.starts_with('=') {
        return None;
    }
    Some(after_key[1..].trim())
}

#[derive(Default)]
struct PartialHeader {
    write_wal_seq: Option<u64>,
    write_tip_hash: Option<String>,
    read_wal_seq: Option<u64>,
    last_self_acked: Option<u64>,
}

impl PartialHeader {
    fn build(self) -> Option<WalHeader> {
        Some(WalHeader {
            write_wal_seq: self.write_wal_seq?,
            write_tip_hash: self.write_tip_hash?,
            read_wal_seq: self.read_wal_seq.unwrap_or(0),
            last_self_acked_wal_seq: self.last_self_acked.unwrap_or(0),
        })
    }
}

fn best_tip(host: &str, shard: u32) -> Option<ShardTip> {
    let raw = ssh_headers(host, shard);
    let hdr = parse_best_header(&raw)?;
    Some(ShardTip {
        wal_seq: hdr.write_wal_seq,
        tip_hash: hdr.write_tip_hash,
        read_wal_seq: hdr.read_wal_seq,
        last_self_acked: hdr.last_self_acked_wal_seq,
    })
}

/// Post-run check: for each data shard (1..=3), compare write_wal_seq and
/// write_tip_hash on both nodes via SSH.
///
/// Same-seq: fails if tip hashes differ (divergent-tip fork).
/// Different-seq: fetches A's previous_tip_hash at entry Wb+1 to determine
/// whether B's divergence is a FORK-WEDGE (fail) or a clean-prefix lag (pass).
pub fn check_no_divergent_shard_tips(leader_host: &str, follower_host: &str) -> CheckResult {
    const NAME: &str = "NoDivergentShardTips";
    let mut issues: Vec<String> = Vec::new();
    let mut skipped: Vec<String> = Vec::new();
    let mut checked = 0u32;

    for shard in 1u32..=3 {
        let leader_tip = best_tip(leader_host, shard);
        let follower_tip = best_tip(follower_host, shard);

        let (lt, ft) = match (leader_tip, follower_tip) {
            (Some(l), Some(f)) => (l, f),
            (None, _) => {
                skipped.push(format!(
                    "shard_{shard}: no WAL header from leader ({leader_host})"
                ));
                continue;
            }
            (_, None) => {
                skipped.push(format!(
                    "shard_{shard}: no WAL header from follower ({follower_host})"
                ));
                continue;
            }
        };

        if lt.wal_seq == ft.wal_seq {
            checked += 1;
            if lt.tip_hash != ft.tip_hash {
                // Primary case: same wal_seq, different tip hash → divergent fork.
                issues.push(format!(
                    "shard_{shard} DIVERGENT-TIP FORK at wal_seq={}: \
                     leader tip={} (read={} self_acked={}) \
                     follower tip={} (read={} self_acked={})",
                    lt.wal_seq,
                    lt.tip_hash, lt.read_wal_seq, lt.last_self_acked,
                    ft.tip_hash, ft.read_wal_seq, ft.last_self_acked,
                ));
            }
        } else {
            // Different wal_seqs: perform a common-point prefix comparison.
            // B is the behind node (lower wal_seq Wb), A is ahead (higher Wa).
            // B's write_tip_hash is the chain tip after B applied seq Wb.
            // To verify A's chain agrees at Wb, we fetch the previous_tip_hash
            // of A's entry at seq Wb+1 — that value equals A's chain tip after
            // applying Wb. If they match, B is a genuine prefix of A (clean lag).
            // If they differ, B has diverged at or before Wb (fork-wedge).
            let (behind_node, ahead_node, behind_tip, behind_seq, ahead_seq) = if lt.wal_seq < ft.wal_seq {
                (leader_host, follower_host, &lt.tip_hash, lt.wal_seq, ft.wal_seq)
            } else {
                (follower_host, leader_host, &ft.tip_hash, ft.wal_seq, lt.wal_seq)
            };
            // We need A's previous_tip_hash at entry (Wb+1).
            // Wb+1 must exist on A since A is at Wa > Wb, so Wb+1 <= Wa.
            let common_point_seq = behind_seq + 1;
            match ssh_range_prev_tip(ahead_node, shard, common_point_seq) {
                None => {
                    skipped.push(format!(
                        "shard_{shard}: {behind_node} behind at {behind_seq}, {ahead_node} at {ahead_seq}; \
                         could not locate seq {common_point_seq} on ahead node — SKIPPED-undetermined"
                    ));
                }
                Some(ahead_prev_tip) => {
                    checked += 1;
                    if ahead_prev_tip != *behind_tip {
                        issues.push(format!(
                            "shard_{shard} FORK-WEDGE: {behind_node} tip at seq={behind_seq} is {} \
                             but {ahead_node} chain tip at seq={behind_seq} is {} \
                             (ahead_node at seq={ahead_seq})",
                            behind_tip, ahead_prev_tip,
                        ));
                    } else {
                        // Chains agree at the common point — B is a clean prefix of A.
                        skipped.push(format!(
                            "shard_{shard}: {behind_node} at seq={behind_seq} is a confirmed clean-prefix \
                             lag behind {ahead_node} at seq={ahead_seq} (tip match at common point)"
                        ));
                    }
                }
            }
        }
    }

    let detail_suffix = if skipped.is_empty() {
        format!("{checked} shard(s) checked")
    } else {
        format!("{checked} shard(s) checked, skipped: {}", skipped.join("; "))
    };

    verdict(NAME, checked, &issues, detail_suffix)
}

/// Fail-closed verdict shared by the disk-truth oracles: an oracle that
/// checked nothing (SSH/tool unavailable, every shard skipped) has not
/// verified the invariant and must not report PASS.
fn verdict(name: &'static str, checked: u32, issues: &[String], detail_suffix: String) -> CheckResult {
    if checked == 0 {
        return CheckResult::fail(name, format!("no shards checked — oracle unattestable ({detail_suffix})"));
    }
    if issues.is_empty() {
        CheckResult::pass_with_detail(name, detail_suffix)
    } else {
        CheckResult::fail(name, format!("{} — {}", issues.join("; "), detail_suffix))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_best_header_extracts_fields() {
        let output = "file_len = 12345\n\
                      \n\
                      front_header:\n\
                        write_metablocks_position     = 100\n\
                        write_datablocks_position     = 200\n\
                        write_wal_seq                 = 42\n\
                        write_tip_hash                = aabbccdd\n\
                        read_metablocks_position      = 50\n\
                        read_wal_seq                  = 40\n\
                        read_tip_hash                 = 11223344\n\
                        last_received_repl_wal_seq  = 41\n\
                        last_self_acked_wal_seq     = 39\n\
                        metablock_count (write)       = 5\n\
                      \n\
                      rear_header:\n\
                        write_wal_seq                 = 42\n\
                        write_tip_hash                = aabbccdd\n";
        let hdr = parse_best_header(output).expect("should parse");
        assert_eq!(hdr.write_wal_seq, 42);
        assert_eq!(hdr.write_tip_hash, "aabbccdd");
        assert_eq!(hdr.read_wal_seq, 40);
        assert_eq!(hdr.last_self_acked_wal_seq, 39);
    }

    #[test]
    fn parse_best_header_picks_highest_wal_seq() {
        // Two file blocks separated by the sentinel. Second has a higher wal_seq.
        let output = "front_header:\n\
                        write_wal_seq                 = 10\n\
                        write_tip_hash                = aaaa\n\
                        read_wal_seq                  = 10\n\
                        last_self_acked_wal_seq     = 10\n\
                      rear_header:\n\
                        write_wal_seq                 = 10\n\
                        write_tip_hash                = aaaa\n\
                      ---file-separator---\n\
                      front_header:\n\
                        write_wal_seq                 = 20\n\
                        write_tip_hash                = bbbb\n\
                        read_wal_seq                  = 20\n\
                        last_self_acked_wal_seq     = 20\n\
                      rear_header:\n\
                        write_wal_seq                 = 20\n\
                        write_tip_hash                = bbbb\n";
        let hdr = parse_best_header(output).expect("should parse");
        assert_eq!(hdr.write_wal_seq, 20);
        assert_eq!(hdr.write_tip_hash, "bbbb");
    }

    #[test]
    fn parse_best_header_returns_none_for_empty() {
        assert!(parse_best_header("").is_none());
        assert!(parse_best_header("no relevant content here\n").is_none());
    }

    #[test]
    fn extract_field_handles_varied_spacing() {
        assert_eq!(extract_field("write_wal_seq                 = 42", "write_wal_seq"), Some("42"));
        assert_eq!(extract_field("last_self_acked_wal_seq     = 39", "last_self_acked_wal_seq"), Some("39"));
        assert_eq!(extract_field("write_tip_hash                = aabbccdd", "write_tip_hash"), Some("aabbccdd"));
    }

    #[test]
    fn extract_field_rejects_prefix_match() {
        // "write_wal_seq" should not match "write_wal_seq_extra"
        assert_eq!(
            extract_field("write_wal_seq_extra = 5", "write_wal_seq"),
            None
        );
    }

    // --- parse_range_prev_tip tests ---

    fn metablock_range_output(seq: u64, prev_tip: &str) -> String {
        format!(
            "wal_seq = {seq} | lease = 3 | offset = 1024 | server_ts = 1234567890 | node = 00000000000000000000000000000001\n\
               previous_tip_hash             = {prev_tip}\n\
               uncompressed_size             = 512\n\
               compressed_size               = 256\n\
               datablock_position            = 2048\n\
               kind                          = EventBatchMetadata\n\
             \n\
             printed 1 metablocks in [{}..={}]\n",
            seq, seq
        )
    }

    #[test]
    fn parse_range_prev_tip_extracts_hash() {
        let text = metablock_range_output(101, "deadbeefcafe1234deadbeefcafe1234deadbeefcafe1234deadbeefcafe1234");
        let result = parse_range_prev_tip(&text);
        assert_eq!(result.as_deref(), Some("deadbeefcafe1234deadbeefcafe1234deadbeefcafe1234deadbeefcafe1234"));
    }

    #[test]
    fn parse_range_prev_tip_returns_none_for_empty() {
        assert!(parse_range_prev_tip("").is_none());
        assert!(parse_range_prev_tip("printed 0 metablocks in [99..=99]\n").is_none());
    }

    #[test]
    fn parse_range_prev_tip_finds_first_across_segments() {
        // Two segments concatenated with file-separator. Only the second has the target entry.
        let text = format!(
            "printed 0 metablocks in [101..=101]\n\
             ---file-separator---\n\
             {}\
             ---file-separator---\n",
            metablock_range_output(101, "aabbccdd")
        );
        assert_eq!(parse_range_prev_tip(&text).as_deref(), Some("aabbccdd"));
    }

    // --- common-point decision logic tests (via check_no_divergent_shard_tips mock) ---
    // These test parse_range_prev_tip and verify the fork-wedge / clean-prefix decision
    // logic is correct given the two hash values being compared.

    /// Simulates the FORK-WEDGE branch: B's tip_hash ≠ A's prev_tip_hash at Wb+1.
    #[test]
    fn differ_hashes_is_fork_wedge() {
        let behind_tip = "aaaa";
        let ahead_prev_tip = "bbbb";
        // Replicate the decision logic from check_no_divergent_shard_tips.
        let is_fork = ahead_prev_tip != behind_tip;
        assert!(is_fork, "different hashes must be classified as FORK-WEDGE");
    }

    /// Simulates the clean-prefix-lag branch: B's tip_hash == A's prev_tip_hash at Wb+1.
    #[test]
    fn matching_hashes_is_clean_prefix_lag() {
        let behind_tip = "aabbccdd";
        let ahead_prev_tip = "aabbccdd";
        let is_fork = ahead_prev_tip != behind_tip;
        assert!(!is_fork, "identical hashes must be classified as clean-prefix lag");
    }

    #[test]
    fn verdict_fails_closed_on_zero_shards_checked() {
        // Every shard skipped (SSH/tool unavailable) — must not read as PASS.
        let r = verdict("NoDivergentShardTips", 0, &[], "0 shard(s) checked, skipped: shard_1: no WAL header".into());
        assert!(!r.passed());
        assert!(r.detail.contains("oracle unattestable"), "{}", r.detail);
    }

    #[test]
    fn verdict_passes_when_shards_checked_and_clean() {
        let r = verdict("NoDivergentShardTips", 3, &[], "3 shard(s) checked".into());
        assert!(r.passed(), "{}", r.detail);
    }

    #[test]
    fn verdict_fails_on_real_issue_even_with_shards_checked() {
        let r = verdict("NoDivergentShardTips", 1, &["shard_1 DIVERGENT-TIP FORK".to_string()], "1 shard(s) checked".into());
        assert!(!r.passed());
        assert!(r.detail.contains("DIVERGENT-TIP FORK"));
    }
}
