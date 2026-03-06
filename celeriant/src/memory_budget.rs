use std::fs;

const RECENT_WRITE_RATIO: f64 = 0.715;
const AGGREGATE_SNAPSHOTS_RATIO: f64 = 0.09;
const AGGREGATE_CLIENT_SNAPSHOTS_RATIO: f64 = 0.09;
const SCHEMA_CACHE_RATIO: f64 = 0.09;
const LIST_WAL_INDEX_RATIO: f64 = 0.015;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardMemoryBudget {
    pub recent_write_cache_bytes: u64,
    pub aggregate_snapshots_cache_bytes: u64,
    pub aggregate_client_snapshots_cache_bytes: u64,
    pub schema_cache_bytes: u64,
    pub list_wal_index_cache_bytes: u64,
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
pub fn compute_shard_budgets(total_budget: u64, num_shards: u32) -> ShardMemoryBudget {
    assert!(num_shards > 0, "num_shards must be > 0");
    let per_shard_budget = total_budget / num_shards as u64;

    ShardMemoryBudget {
        recent_write_cache_bytes: (per_shard_budget as f64 * RECENT_WRITE_RATIO) as u64,
        aggregate_snapshots_cache_bytes: (per_shard_budget as f64 * AGGREGATE_SNAPSHOTS_RATIO) as u64,
        aggregate_client_snapshots_cache_bytes: (per_shard_budget as f64 * AGGREGATE_CLIENT_SNAPSHOTS_RATIO) as u64,
        schema_cache_bytes: (per_shard_budget as f64 * SCHEMA_CACHE_RATIO) as u64,
        list_wal_index_cache_bytes: (per_shard_budget as f64 * LIST_WAL_INDEX_RATIO) as u64,
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

        assert_eq!(budget.recent_write_cache_bytes, 715_000_000);
        assert_eq!(budget.aggregate_snapshots_cache_bytes, 90_000_000);
        assert_eq!(budget.aggregate_client_snapshots_cache_bytes, 90_000_000);
        assert_eq!(budget.schema_cache_bytes, 90_000_000);
        assert_eq!(budget.list_wal_index_cache_bytes, 15_000_000);
    }

    #[test]
    fn test_compute_shard_budgets_four_shards() {
        let budget = compute_shard_budgets(4_000_000_000, 4);

        // Each shard gets 1B, then ratios applied
        assert_eq!(budget.recent_write_cache_bytes, 715_000_000);
        assert_eq!(budget.aggregate_snapshots_cache_bytes, 90_000_000);
        assert_eq!(budget.aggregate_client_snapshots_cache_bytes, 90_000_000);
        assert_eq!(budget.schema_cache_bytes, 90_000_000);
        assert_eq!(budget.list_wal_index_cache_bytes, 15_000_000);
    }

    #[test]
    fn test_compute_shard_budgets_ratios_sum() {
        // Verify ratios sum to 1.0 (within floating point precision)
        let sum = RECENT_WRITE_RATIO
            + AGGREGATE_SNAPSHOTS_RATIO
            + AGGREGATE_CLIENT_SNAPSHOTS_RATIO
            + SCHEMA_CACHE_RATIO
            + LIST_WAL_INDEX_RATIO;

        assert!((sum - 1.0).abs() < 0.001, "Ratios should sum to 1.0, got {}", sum);
    }

    #[test]
    fn test_compute_shard_budgets_zero() {
        let budget = compute_shard_budgets(0, 1);

        assert_eq!(budget.recent_write_cache_bytes, 0);
        assert_eq!(budget.aggregate_snapshots_cache_bytes, 0);
        assert_eq!(budget.aggregate_client_snapshots_cache_bytes, 0);
        assert_eq!(budget.schema_cache_bytes, 0);
        assert_eq!(budget.list_wal_index_cache_bytes, 0);
    }

    #[test]
    fn test_compute_shard_budgets_many_shards() {
        let budget = compute_shard_budgets(32_000_000_000, 16);

        // Each shard gets 2B
        assert_eq!(budget.recent_write_cache_bytes, 1_430_000_000);
        assert_eq!(budget.aggregate_snapshots_cache_bytes, 180_000_000);
        assert_eq!(budget.aggregate_client_snapshots_cache_bytes, 180_000_000);
        assert_eq!(budget.schema_cache_bytes, 180_000_000);
        assert_eq!(budget.list_wal_index_cache_bytes, 30_000_000);
    }
}
