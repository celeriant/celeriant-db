use std::fs;

const AGGREGATE_SNAPSHOTS_RATIO: f64 = 0.09;
const AGGREGATE_CLIENT_SNAPSHOTS_RATIO: f64 = 0.09;
const SCHEMA_CACHE_RATIO: f64 = 0.09;
const LIST_WAL_SEQ_RATIO: f64 = 0.015;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardMemoryBudget {
    pub recent_write_cache_bytes: u64,
    pub aggregate_snapshots_cache_bytes: u64,
    pub aggregate_client_snapshots_cache_bytes: u64,
    pub schema_cache_bytes: u64,
    pub list_wal_seq_cache_bytes: u64,
}

/// Detects available system memory by checking physical RAM and cgroup limits.
/// Returns the minimum of physical RAM and cgroup limit if cgroup exists.
pub fn detect_available_memory() -> Result<u64, String> {
    let physical_ram = read_meminfo_total()?;
    let cgroup_limit = read_cgroup_limit();

    match cgroup_limit {
        Some(limit) if limit < physical_ram => Ok(limit),
        _ => Ok(physical_ram),
    }
}

/// Computes per-shard cache budgets from total budget and number of shards.
/// Recent write cache absorbs the residual after fixed-ratio allocations.
/// Replication queue memory is governed by `internode_max_request_size`
/// (a wire-protocol cap, not a memory ratio) and is small enough relative to
/// per-shard budgets that we don't subtract it from the residual here.
pub fn compute_shard_budgets(total_budget: u64, num_shards: u32) -> ShardMemoryBudget {
    assert!(num_shards > 0, "num_shards must be > 0");
    let per_shard_budget = total_budget / num_shards as u64;

    let aggregate_snapshots = (per_shard_budget as f64 * AGGREGATE_SNAPSHOTS_RATIO) as u64;
    let aggregate_client_snapshots = (per_shard_budget as f64 * AGGREGATE_CLIENT_SNAPSHOTS_RATIO) as u64;
    let schema_cache = (per_shard_budget as f64 * SCHEMA_CACHE_RATIO) as u64;
    let list_wal_seq = (per_shard_budget as f64 * LIST_WAL_SEQ_RATIO) as u64;

    let fixed_total = aggregate_snapshots + aggregate_client_snapshots + schema_cache + list_wal_seq;
    let recent_write_cache = per_shard_budget.saturating_sub(fixed_total);

    ShardMemoryBudget {
        recent_write_cache_bytes: recent_write_cache,
        aggregate_snapshots_cache_bytes: aggregate_snapshots,
        aggregate_client_snapshots_cache_bytes: aggregate_client_snapshots,
        schema_cache_bytes: schema_cache,
        list_wal_seq_cache_bytes: list_wal_seq,
    }
}

fn read_meminfo_total() -> Result<u64, String> {
    let content = fs::read_to_string("/proc/meminfo")
        .map_err(|e| format!("Failed to read /proc/meminfo: {}", e))?;

    for line in content.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            let kb_str = rest.trim().split_whitespace().next()
                .ok_or_else(|| "Failed to parse MemTotal value".to_string())?;
            let kb: u64 = kb_str.parse()
                .map_err(|e| format!("Failed to parse MemTotal as integer: {}", e))?;
            return Ok(kb * 1024); // Convert kB to bytes
        }
    }

    Err("MemTotal not found in /proc/meminfo".to_string())
}

fn read_cgroup_limit() -> Option<u64> {
    let content = fs::read_to_string("/sys/fs/cgroup/memory.max").ok()?;
    let trimmed = content.trim();

    // "max" means no limit
    if trimmed == "max" {
        return None;
    }

    trimmed.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_shard_budgets_single_shard() {
        let budget = compute_shard_budgets(1_000_000_000, 1);

        assert_eq!(budget.aggregate_snapshots_cache_bytes, 90_000_000);
        assert_eq!(budget.aggregate_client_snapshots_cache_bytes, 90_000_000);
        assert_eq!(budget.schema_cache_bytes, 90_000_000);
        assert_eq!(budget.list_wal_seq_cache_bytes, 15_000_000);
        let fixed = 90_000_000 + 90_000_000 + 90_000_000 + 15_000_000;
        assert_eq!(budget.recent_write_cache_bytes, 1_000_000_000 - fixed);
    }

    #[test]
    fn test_compute_shard_budgets_large_machine() {
        // 32 GB total, 16 shards = 2 GB/shard
        let budget = compute_shard_budgets(32_000_000_000, 16);
        let per_shard: u64 = 2_000_000_000;

        assert_eq!(budget.aggregate_snapshots_cache_bytes, 180_000_000);
        assert_eq!(budget.aggregate_client_snapshots_cache_bytes, 180_000_000);
        assert_eq!(budget.schema_cache_bytes, 180_000_000);
        assert_eq!(budget.list_wal_seq_cache_bytes, 30_000_000);
        let fixed = 180_000_000 + 180_000_000 + 180_000_000 + 30_000_000;
        assert_eq!(budget.recent_write_cache_bytes, per_shard - fixed);
    }

    #[test]
    fn test_compute_shard_budgets_fixed_ratios_unchanged() {
        let sum = AGGREGATE_SNAPSHOTS_RATIO
            + AGGREGATE_CLIENT_SNAPSHOTS_RATIO
            + SCHEMA_CACHE_RATIO
            + LIST_WAL_SEQ_RATIO;
        // Fixed ratios sum to 28.5%, leaving ~71.5% for recent write cache.
        assert!((sum - 0.285).abs() < 0.001, "Fixed ratios should sum to 0.285, got {}", sum);
    }

    #[test]
    fn test_compute_shard_budgets_zero() {
        let budget = compute_shard_budgets(0, 1);

        assert_eq!(budget.recent_write_cache_bytes, 0);
        assert_eq!(budget.aggregate_snapshots_cache_bytes, 0);
        assert_eq!(budget.aggregate_client_snapshots_cache_bytes, 0);
        assert_eq!(budget.schema_cache_bytes, 0);
        assert_eq!(budget.list_wal_seq_cache_bytes, 0);
    }
}
