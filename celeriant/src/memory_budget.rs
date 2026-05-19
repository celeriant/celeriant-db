use std::fs;

// Relative weights for the three "fixed" caches. They split whatever the
// recent-write cache leaves behind in this ratio (1:1:1 today). The absolute
// fraction each one consumes depends on recent_write_cache_ratio.
const AGGREGATE_SNAPSHOTS_WEIGHT: f64 = 1.0;
const AGGREGATE_CLIENT_SNAPSHOTS_WEIGHT: f64 = 1.0;
const SCHEMA_CACHE_WEIGHT: f64 = 1.0;
const FIXED_WEIGHTS_SUM: f64 =
    AGGREGATE_SNAPSHOTS_WEIGHT + AGGREGATE_CLIENT_SNAPSHOTS_WEIGHT + SCHEMA_CACHE_WEIGHT;

pub const DEFAULT_RECENT_WRITE_CACHE_RATIO: f64 = 0.73;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardMemoryBudget {
    pub recent_write_cache_bytes: u64,
    pub aggregate_snapshots_cache_bytes: u64,
    pub aggregate_client_snapshots_cache_bytes: u64,
    pub schema_cache_bytes: u64,
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

/// Computes per-shard cache budgets. The recent-write cache claims
/// `recent_write_cache_ratio` of per-shard memory; the remainder is split
/// among the three fixed caches in their relative weights. The four buckets
/// always sum to the per-shard budget (modulo float rounding).
pub fn compute_shard_budgets(
    total_budget: u64,
    num_shards: u32,
    recent_write_cache_ratio: f64,
) -> ShardMemoryBudget {
    assert!(num_shards > 0, "num_shards must be > 0");
    assert!(
        (0.0..=1.0).contains(&recent_write_cache_ratio),
        "recent_write_cache_ratio {} out of range [0.0, 1.0]",
        recent_write_cache_ratio,
    );
    let per_shard_budget = total_budget / num_shards as u64;
    let recent_write = (per_shard_budget as f64 * recent_write_cache_ratio) as u64;
    let remaining = per_shard_budget.saturating_sub(recent_write) as f64;

    ShardMemoryBudget {
        recent_write_cache_bytes: recent_write,
        aggregate_snapshots_cache_bytes: (remaining * AGGREGATE_SNAPSHOTS_WEIGHT / FIXED_WEIGHTS_SUM) as u64,
        aggregate_client_snapshots_cache_bytes: (remaining * AGGREGATE_CLIENT_SNAPSHOTS_WEIGHT / FIXED_WEIGHTS_SUM) as u64,
        schema_cache_bytes: (remaining * SCHEMA_CACHE_WEIGHT / FIXED_WEIGHTS_SUM) as u64,
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

    /// Verify the four buckets always sum to per_shard_budget (modulo float rounding).
    fn assert_sums_to_budget(budget: &ShardMemoryBudget, per_shard: u64) {
        let total = budget.recent_write_cache_bytes
            + budget.aggregate_snapshots_cache_bytes
            + budget.aggregate_client_snapshots_cache_bytes
            + budget.schema_cache_bytes;
        assert!(total <= per_shard, "total {} > per_shard {}", total, per_shard);
        assert!(
            per_shard - total <= 4,
            "rounding slack {} bytes too high (per_shard {})",
            per_shard - total,
            per_shard,
        );
    }

    #[test]
    fn test_compute_shard_budgets_single_shard_default_ratio() {
        let budget = compute_shard_budgets(1_000_000_000, 1, DEFAULT_RECENT_WRITE_CACHE_RATIO);
        assert_eq!(budget.recent_write_cache_bytes, 730_000_000);
        assert_eq!(budget.aggregate_snapshots_cache_bytes, 90_000_000);
        assert_eq!(budget.aggregate_client_snapshots_cache_bytes, 90_000_000);
        assert_eq!(budget.schema_cache_bytes, 90_000_000);
        assert_sums_to_budget(&budget, 1_000_000_000);
    }

    #[test]
    fn test_compute_shard_budgets_large_machine_default_ratio() {
        let budget = compute_shard_budgets(32_000_000_000, 16, DEFAULT_RECENT_WRITE_CACHE_RATIO);
        assert_eq!(budget.recent_write_cache_bytes, 1_460_000_000);
        assert_eq!(budget.aggregate_snapshots_cache_bytes, 180_000_000);
        assert_eq!(budget.aggregate_client_snapshots_cache_bytes, 180_000_000);
        assert_eq!(budget.schema_cache_bytes, 180_000_000);
        assert_sums_to_budget(&budget, 2_000_000_000);
    }

    #[test]
    fn test_compute_shard_budgets_lower_ratio_expands_fixed_caches() {
        // Recent-write cache cut to 40%; the remaining 60% splits 1:1:1 across the others.
        let budget = compute_shard_budgets(1_000_000_000, 1, 0.40);
        assert_eq!(budget.recent_write_cache_bytes, 400_000_000);
        assert_eq!(budget.aggregate_snapshots_cache_bytes, 200_000_000);
        assert_eq!(budget.aggregate_client_snapshots_cache_bytes, 200_000_000);
        assert_eq!(budget.schema_cache_bytes, 200_000_000);
        assert_sums_to_budget(&budget, 1_000_000_000);
    }

    #[test]
    fn test_compute_shard_budgets_zero_ratio_gives_all_to_fixed_caches() {
        let budget = compute_shard_budgets(900_000_000, 1, 0.0);
        assert_eq!(budget.recent_write_cache_bytes, 0);
        assert_eq!(budget.aggregate_snapshots_cache_bytes, 300_000_000);
        assert_eq!(budget.aggregate_client_snapshots_cache_bytes, 300_000_000);
        assert_eq!(budget.schema_cache_bytes, 300_000_000);
        assert_sums_to_budget(&budget, 900_000_000);
    }

    #[test]
    fn test_compute_shard_budgets_one_ratio_starves_fixed_caches() {
        let budget = compute_shard_budgets(1_000_000_000, 1, 1.0);
        assert_eq!(budget.recent_write_cache_bytes, 1_000_000_000);
        assert_eq!(budget.aggregate_snapshots_cache_bytes, 0);
        assert_eq!(budget.aggregate_client_snapshots_cache_bytes, 0);
        assert_eq!(budget.schema_cache_bytes, 0);
    }

    #[test]
    fn test_compute_shard_budgets_zero_budget() {
        let budget = compute_shard_budgets(0, 1, DEFAULT_RECENT_WRITE_CACHE_RATIO);
        assert_eq!(budget.recent_write_cache_bytes, 0);
        assert_eq!(budget.aggregate_snapshots_cache_bytes, 0);
        assert_eq!(budget.aggregate_client_snapshots_cache_bytes, 0);
        assert_eq!(budget.schema_cache_bytes, 0);
    }

    #[test]
    #[should_panic(expected = "recent_write_cache_ratio")]
    fn test_compute_shard_budgets_panics_on_negative_ratio() {
        compute_shard_budgets(1_000_000_000, 1, -0.1);
    }

    #[test]
    #[should_panic(expected = "recent_write_cache_ratio")]
    fn test_compute_shard_budgets_panics_on_above_one_ratio() {
        compute_shard_budgets(1_000_000_000, 1, 1.1);
    }
}
