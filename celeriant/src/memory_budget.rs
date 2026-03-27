use std::fs;

const AGGREGATE_SNAPSHOTS_RATIO: f64 = 0.09;
const AGGREGATE_CLIENT_SNAPSHOTS_RATIO: f64 = 0.09;
const SCHEMA_CACHE_RATIO: f64 = 0.09;
const LIST_WAL_INDEX_RATIO: f64 = 0.015;

/// Replication high water mark: 10% of per-shard budget, floored at 128 MB.
/// This is the threshold at which the replication queue is considered pressured
/// and S3 fallback is triggered. Scaled with machine memory so large machines
/// can absorb transient stalls (e.g. TLS handshake storms) without falling back.
const REPLICATION_HIGH_WATER_RATIO: f64 = 0.10;
const REPLICATION_HIGH_WATER_FLOOR_BYTES: u64 = 128 * 1024 * 1024;

/// Catchup gap is 3× the high water mark. This is NOT resident memory — it's
/// the threshold for "how much data can accumulate before we give up on streaming
/// and fall back to S3". Generous to avoid unnecessary S3 round-trips.
const CATCHUP_GAP_MULTIPLIER: u64 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardMemoryBudget {
    pub recent_write_cache_bytes: u64,
    pub aggregate_snapshots_cache_bytes: u64,
    pub aggregate_client_snapshots_cache_bytes: u64,
    pub schema_cache_bytes: u64,
    pub list_wal_index_cache_bytes: u64,
    pub replication_high_water_bytes: u64,
    pub max_catchup_gap_bytes: u64,
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
///
/// The replication high water mark scales with per-shard memory (10%, min 128 MB)
/// so large machines can absorb transient replication stalls without S3 fallback.
/// The recent write cache absorbs the variable cost as the residual allocation.
pub fn compute_shard_budgets(total_budget: u64, num_shards: u32) -> ShardMemoryBudget {
    assert!(num_shards > 0, "num_shards must be > 0");
    let per_shard_budget = total_budget / num_shards as u64;

    let aggregate_snapshots = (per_shard_budget as f64 * AGGREGATE_SNAPSHOTS_RATIO) as u64;
    let aggregate_client_snapshots = (per_shard_budget as f64 * AGGREGATE_CLIENT_SNAPSHOTS_RATIO) as u64;
    let schema_cache = (per_shard_budget as f64 * SCHEMA_CACHE_RATIO) as u64;
    let list_wal_index = (per_shard_budget as f64 * LIST_WAL_INDEX_RATIO) as u64;

    let replication_high_water = std::cmp::max(
        REPLICATION_HIGH_WATER_FLOOR_BYTES,
        (per_shard_budget as f64 * REPLICATION_HIGH_WATER_RATIO) as u64,
    );
    let max_catchup_gap = replication_high_water * CATCHUP_GAP_MULTIPLIER;

    let fixed_total = aggregate_snapshots + aggregate_client_snapshots + schema_cache + list_wal_index + replication_high_water;
    let recent_write_cache = per_shard_budget.saturating_sub(fixed_total);

    ShardMemoryBudget {
        recent_write_cache_bytes: recent_write_cache,
        aggregate_snapshots_cache_bytes: aggregate_snapshots,
        aggregate_client_snapshots_cache_bytes: aggregate_client_snapshots,
        schema_cache_bytes: schema_cache,
        list_wal_index_cache_bytes: list_wal_index,
        replication_high_water_bytes: replication_high_water,
        max_catchup_gap_bytes: max_catchup_gap,
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
    fn test_compute_shard_budgets_single_shard_floor_applies() {
        // 1 GB per shard: 10% = 100 MB < 128 MB floor, so floor applies
        let budget = compute_shard_budgets(1_000_000_000, 1);

        assert_eq!(budget.aggregate_snapshots_cache_bytes, 90_000_000);
        assert_eq!(budget.aggregate_client_snapshots_cache_bytes, 90_000_000);
        assert_eq!(budget.schema_cache_bytes, 90_000_000);
        assert_eq!(budget.list_wal_index_cache_bytes, 15_000_000);
        assert_eq!(budget.replication_high_water_bytes, REPLICATION_HIGH_WATER_FLOOR_BYTES); // 128 MB floor
        assert_eq!(budget.max_catchup_gap_bytes, REPLICATION_HIGH_WATER_FLOOR_BYTES * CATCHUP_GAP_MULTIPLIER);
        // Recent write = residual: 1B - 90M - 90M - 90M - 15M - 128M = 587M
        let fixed = 90_000_000 + 90_000_000 + 90_000_000 + 15_000_000 + REPLICATION_HIGH_WATER_FLOOR_BYTES;
        assert_eq!(budget.recent_write_cache_bytes, 1_000_000_000 - fixed);
    }

    #[test]
    fn test_compute_shard_budgets_large_machine_ratio_applies() {
        // 32 GB total, 16 shards = 2 GB/shard: 10% = 200 MB > 128 MB floor
        let budget = compute_shard_budgets(32_000_000_000, 16);
        let per_shard: u64 = 2_000_000_000;

        assert_eq!(budget.replication_high_water_bytes, 200_000_000); // 10% of 2 GB
        assert_eq!(budget.max_catchup_gap_bytes, 600_000_000); // 3× high water
        assert_eq!(budget.aggregate_snapshots_cache_bytes, 180_000_000);
        assert_eq!(budget.aggregate_client_snapshots_cache_bytes, 180_000_000);
        assert_eq!(budget.schema_cache_bytes, 180_000_000);
        assert_eq!(budget.list_wal_index_cache_bytes, 30_000_000);
        let fixed = 180_000_000 + 180_000_000 + 180_000_000 + 30_000_000 + 200_000_000;
        assert_eq!(budget.recent_write_cache_bytes, per_shard - fixed);
    }

    #[test]
    fn test_compute_shard_budgets_i4i_16xlarge() {
        // 512 GB at 60%, 64 shards = 4.8 GB/shard
        let total = (512_u64 * 1024 * 1024 * 1024 * 60) / 100;
        let budget = compute_shard_budgets(total, 64);
        let per_shard = total / 64;

        // 10% of 4.8 GB ≈ 480 MB, well above 128 MB floor
        let expected_high_water = (per_shard as f64 * 0.10) as u64;
        assert_eq!(budget.replication_high_water_bytes, expected_high_water);
        assert!(budget.replication_high_water_bytes > 400_000_000, "should be ~480 MB");
        assert_eq!(budget.max_catchup_gap_bytes, expected_high_water * 3);

        // Recent write cache should still be the majority
        assert!(budget.recent_write_cache_bytes > per_shard / 2, "recent write cache should be > 50% of per-shard");
    }

    #[test]
    fn test_compute_shard_budgets_fixed_ratios_unchanged() {
        // Verify the fixed ratios haven't drifted
        let sum = AGGREGATE_SNAPSHOTS_RATIO
            + AGGREGATE_CLIENT_SNAPSHOTS_RATIO
            + SCHEMA_CACHE_RATIO
            + LIST_WAL_INDEX_RATIO
            + REPLICATION_HIGH_WATER_RATIO;
        // Fixed allocations: 28.5% + 10% replication = 38.5%, leaving ~61.5% for recent write cache
        assert!((sum - 0.385).abs() < 0.001, "Fixed ratios should sum to 0.385, got {}", sum);
    }

    #[test]
    fn test_compute_shard_budgets_zero() {
        let budget = compute_shard_budgets(0, 1);

        // Floor still applies for replication, everything else zero
        assert_eq!(budget.recent_write_cache_bytes, 0);
        assert_eq!(budget.aggregate_snapshots_cache_bytes, 0);
        assert_eq!(budget.aggregate_client_snapshots_cache_bytes, 0);
        assert_eq!(budget.schema_cache_bytes, 0);
        assert_eq!(budget.list_wal_index_cache_bytes, 0);
        assert_eq!(budget.replication_high_water_bytes, REPLICATION_HIGH_WATER_FLOOR_BYTES);
    }
}
