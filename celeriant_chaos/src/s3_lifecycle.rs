use serde::Serialize;
use tokio::process::Command;

use crate::invariants::CheckResult;

/// Per-shard summary from the MinIO fallback-object audit.
#[derive(Debug, Clone, Serialize)]
pub struct ShardLifecycle {
    pub shard: u32,
    pub file_count: usize,
    /// (start, end) pairs sorted by start, one entry per file.
    pub ranges: Vec<(u64, u64)>,
}

#[derive(Debug, Clone, Serialize)]
pub struct S3LifecycleReport {
    pub per_shard: Vec<ShardLifecycle>,
    pub total_objects: usize,
}

/// Audit MinIO fallback objects via `docker exec`. Returns `None` when docker
/// or the container is unavailable (remote-infra runs or CI without docker).
pub async fn audit_s3_fallback() -> Option<S3LifecycleReport> {
    let out = Command::new("docker")
        .args([
            "exec",
            "rpi-cluster-minio-1",
            "sh",
            "-c",
            "find /data/celeriant-cluster/cluster/fallback -type f 2>/dev/null",
        ])
        .output()
        .await
        .ok()?;

    // `find` exits 1 when the fallback dir doesn't exist yet — that's a valid
    // empty result (no fallbacks this run), not an audit failure. Only treat
    // docker-level failures (container missing → no stdout AND stderr noise)
    // as unavailable.
    if !out.status.success() && !out.stderr.is_empty() {
        return None;
    }

    let stdout = String::from_utf8_lossy(&out.stdout);
    let report = parse_listing(&stdout);
    Some(report)
}

/// Run lifecycle invariant checks against an audit report.
pub fn checks(report: &S3LifecycleReport) -> Vec<CheckResult> {
    let mut out = Vec::new();
    for shard in &report.per_shard {
        out.extend(shard_checks(shard));
    }
    out
}

fn shard_checks(shard: &ShardLifecycle) -> Vec<CheckResult> {
    let mut results = Vec::new();

    // Check start <= end per file.
    for &(start, end) in &shard.ranges {
        if start > end {
            results.push(CheckResult::fail(
                "S3FallbackRangeValid",
                format!("shard_{}: file has start {} > end {}", shard.shard, start, end),
            ));
        }
    }

    // Check no gaps between consecutive files (sorted by start).
    // Overlaps are allowed; only gaps (end+1 < next_start) are flagged.
    let mut sorted = shard.ranges.clone();
    sorted.sort_by_key(|&(start, _)| start);

    for window in sorted.windows(2) {
        let (_, prev_end) = window[0];
        let (next_start, _) = window[1];
        if prev_end + 1 < next_start {
            results.push(CheckResult::fail(
                "S3FallbackNoGaps",
                format!(
                    "shard_{}: gap between end {} and next start {} (missing range {}..{})",
                    shard.shard,
                    prev_end,
                    next_start,
                    prev_end + 1,
                    next_start - 1,
                ),
            ));
        }
    }

    results
}

/// Parse `find` output lines into a report. Lines not matching the expected
/// path pattern are silently skipped.
fn parse_listing(text: &str) -> S3LifecycleReport {
    use std::collections::BTreeMap;

    // path shape: .../cluster/fallback/shard_{NNN}/batch_{start:09}_{end:09}_{uuid}.bin
    let mut by_shard: BTreeMap<u32, Vec<(u64, u64)>> = BTreeMap::new();

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Some(entry) = parse_fallback_path(line) {
            by_shard.entry(entry.0).or_default().push((entry.1, entry.2));
        }
    }

    let mut per_shard: Vec<ShardLifecycle> = by_shard
        .into_iter()
        .map(|(shard, mut ranges)| {
            ranges.sort_by_key(|&(start, _)| start);
            ShardLifecycle { shard, file_count: ranges.len(), ranges }
        })
        .collect();
    per_shard.sort_by_key(|s| s.shard);

    let total_objects = per_shard.iter().map(|s| s.file_count).sum();
    S3LifecycleReport { per_shard, total_objects }
}

/// Parse one find-output path. Returns `(shard, start, end)` or `None`.
///
/// Expected tail: `cluster/fallback/shard_{NNN}/batch_{start:09}_{end:09}_{uuid}.bin`
fn parse_fallback_path(path: &str) -> Option<(u32, u64, u64)> {
    // Walk from the right: filename, then parent dir shard_{NNN}.
    let filename = path.rsplit('/').next()?;
    let shard_dir = path.rsplit('/').nth(1)?;

    // shard dir: "shard_{NNN}"
    let shard: u32 = shard_dir.strip_prefix("shard_")?.parse().ok()?;

    // filename: "batch_{start}_{end}_{uuid}.bin"
    let without_ext = filename.strip_suffix(".bin")?;
    let without_batch = without_ext.strip_prefix("batch_")?;

    // Split into at most 3 parts: start, end, uuid (uuid may contain '_')
    let mut parts = without_batch.splitn(3, '_');
    let start: u64 = parts.next()?.parse().ok()?;
    let end: u64 = parts.next()?.parse().ok()?;

    Some((shard, start, end))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_path_valid() {
        let path = "/data/celeriant-cluster/cluster/fallback/shard_002/batch_000000001_000000100_abc123.bin";
        assert_eq!(parse_fallback_path(path), Some((2, 1, 100)));
    }

    #[test]
    fn parse_path_with_uuid_underscores() {
        // uuid portion may itself contain underscores — only first two _ after "batch_" are delimiters.
        let path = "/data/celeriant-cluster/cluster/fallback/shard_003/batch_000000200_000000399_some_uuid_val.bin";
        assert_eq!(parse_fallback_path(path), Some((3, 200, 399)));
    }

    #[test]
    fn parse_path_invalid_no_bin() {
        let path = "/data/celeriant-cluster/cluster/fallback/shard_001/batch_000000001_000000100_uuid";
        assert_eq!(parse_fallback_path(path), None);
    }

    #[test]
    fn parse_listing_groups_by_shard() {
        let listing = "\
/data/celeriant-cluster/cluster/fallback/shard_001/batch_000000001_000000100_aaa.bin
/data/celeriant-cluster/cluster/fallback/shard_001/batch_000000101_000000200_bbb.bin
/data/celeriant-cluster/cluster/fallback/shard_002/batch_000000001_000000050_ccc.bin
";
        let report = parse_listing(listing);
        assert_eq!(report.total_objects, 3);
        assert_eq!(report.per_shard.len(), 2);
        assert_eq!(report.per_shard[0].shard, 1);
        assert_eq!(report.per_shard[0].file_count, 2);
        assert_eq!(report.per_shard[1].shard, 2);
        assert_eq!(report.per_shard[1].file_count, 1);
    }

    #[test]
    fn no_gaps_passes() {
        let shard = ShardLifecycle {
            shard: 1,
            file_count: 3,
            ranges: vec![(1, 100), (101, 200), (201, 300)],
        };
        let results = shard_checks(&shard);
        assert!(results.iter().all(|r| r.passed()), "unexpected failures: {:?}", results);
    }

    #[test]
    fn overlap_passes() {
        // Uploaders may overlap legitimately.
        let shard = ShardLifecycle {
            shard: 1,
            file_count: 2,
            // second file starts before first ends
            ranges: vec![(1, 150), (100, 200)],
        };
        let results = shard_checks(&shard);
        assert!(results.iter().all(|r| r.passed()), "overlap should not fail: {:?}", results);
    }

    #[test]
    fn gap_detected() {
        let shard = ShardLifecycle {
            shard: 1,
            file_count: 2,
            ranges: vec![(1, 100), (150, 200)],
        };
        let results = shard_checks(&shard);
        let gap_fail = results.iter().find(|r| r.name == "S3FallbackNoGaps" && !r.passed());
        assert!(gap_fail.is_some(), "expected gap failure, got: {:?}", results);
        assert!(gap_fail.unwrap().detail.contains("shard_1"));
    }

    #[test]
    fn invalid_range_detected() {
        let shard = ShardLifecycle {
            shard: 2,
            file_count: 1,
            ranges: vec![(200, 100)],
        };
        let results = shard_checks(&shard);
        let bad = results.iter().find(|r| r.name == "S3FallbackRangeValid" && !r.passed());
        assert!(bad.is_some(), "expected range failure: {:?}", results);
    }

    #[test]
    fn gap_check_uses_sorted_order() {
        // Provide ranges out of order; after sorting there should be no gap.
        let shard = ShardLifecycle {
            shard: 1,
            file_count: 2,
            ranges: vec![(101, 200), (1, 100)],
        };
        let results = shard_checks(&shard);
        let gap_fail = results.iter().find(|r| r.name == "S3FallbackNoGaps" && !r.passed());
        assert!(gap_fail.is_none(), "sorting not applied: {:?}", results);
    }

    #[test]
    fn empty_listing_produces_empty_report() {
        let report = parse_listing("");
        assert_eq!(report.total_objects, 0);
        assert!(report.per_shard.is_empty());
    }
}
