//! S3 path conventions for cluster coordination files.

/// Base prefix for all cluster coordination data in S3.
pub const CLUSTER_PREFIX: &str = "cluster";

/// Path to the lease file.
pub const LEASE_PATH: &str = "cluster/lease.json";

/// Path to the membership file.
pub const MEMBERSHIP_PATH: &str = "cluster/membership.json";

/// Prefix for S3 fallback replication data.
pub const FALLBACK_PREFIX: &str = "cluster/fallback";

/// Generate the S3 path for a fallback batch.
///
/// Format: `cluster/fallback/shard_{shard_id:03}/batch_{start_index:09}_{end_index:09}.bin`
/// Zero-padded to ensure lexicographic ordering = temporal ordering.
pub fn fallback_batch_path(shard_id: u32, start_index: u64, end_index: u64) -> String {
    format!(
        "{}/shard_{:03}/batch_{:09}_{:09}.bin",
        FALLBACK_PREFIX, shard_id, start_index, end_index
    )
}

/// Generate the S3 prefix for listing all fallback batches for a shard.
pub fn fallback_shard_prefix(shard_id: u32) -> String {
    format!("{}/shard_{:03}/", FALLBACK_PREFIX, shard_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fallback_paths() {
        assert_eq!(
            fallback_batch_path(0, 1, 5),
            "cluster/fallback/shard_000/batch_000000001_000000005.bin"
        );
        assert_eq!(
            fallback_batch_path(15, 100, 999999999),
            "cluster/fallback/shard_015/batch_000000100_999999999.bin"
        );

        // Verify lexicographic ordering (same shard, increasing start indices)
        let p1 = fallback_batch_path(0, 1, 3);
        let p2 = fallback_batch_path(0, 10, 15);
        let p3 = fallback_batch_path(0, 100, 200);
        assert!(p1 < p2);
        assert!(p2 < p3);
    }

    #[test]
    fn test_fallback_prefix() {
        assert_eq!(fallback_shard_prefix(0), "cluster/fallback/shard_000/");
        assert_eq!(fallback_shard_prefix(7), "cluster/fallback/shard_007/");
    }
}
