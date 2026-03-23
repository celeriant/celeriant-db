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
/// Format: `cluster/fallback/shard_{shard_id:03}/batch_{start_index:09}_{end_index:09}_{node_id_uuid}.bin`
/// Zero-padded to ensure lexicographic ordering = temporal ordering.
/// `node_id` is formatted as a standard UUID (fixed-width, 36 chars) for consistency with logs and config.
pub fn fallback_batch_path(shard_id: u32, start_index: u64, end_index: u64, node_id: u128) -> String {
    format!(
        "{}/shard_{:03}/batch_{:09}_{:09}_{}.bin",
        FALLBACK_PREFIX, shard_id, start_index, end_index,
        uuid::Uuid::from_u128(node_id)
    )
}

/// Generate the S3 prefix for listing all fallback batches for a shard.
pub fn fallback_shard_prefix(shard_id: u32) -> String {
    format!("{}/shard_{:03}/", FALLBACK_PREFIX, shard_id)
}

/// Parse a fallback batch path to extract shard_id, start_index, end_index, and node_id.
/// Returns None if the path doesn't match the expected format (including old format without node_id).
pub fn parse_fallback_path(path: &str) -> Option<(u32, u64, u64, u128)> {
    let shard_part = path.split('/').find(|p| p.starts_with("shard_"))?;
    let batch_part = path.split('/').find(|p| p.starts_with("batch_"))?;

    let shard_id: u32 = shard_part.strip_prefix("shard_")?.parse().ok()?;
    let batch_name = batch_part.strip_prefix("batch_")?.strip_suffix(".bin")?;

    let (start_str, rest) = batch_name.split_once('_')?;
    let (end_str, node_uuid) = rest.split_once('_')?;

    let start_index: u64 = start_str.parse().ok()?;
    let end_index: u64 = end_str.parse().ok()?;
    let node_id: u128 = uuid::Uuid::parse_str(node_uuid).ok()?.as_u128();

    Some((shard_id, start_index, end_index, node_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fallback_paths() {
        assert_eq!(
            fallback_batch_path(0, 1, 5, 42),
            "cluster/fallback/shard_000/batch_000000001_000000005_00000000-0000-0000-0000-00000000002a.bin"
        );
        assert_eq!(
            fallback_batch_path(15, 100, 999999999, 255),
            "cluster/fallback/shard_015/batch_000000100_999999999_00000000-0000-0000-0000-0000000000ff.bin"
        );

        // Verify lexicographic ordering (same shard, increasing start indices, same node)
        let p1 = fallback_batch_path(0, 1, 3, 1);
        let p2 = fallback_batch_path(0, 10, 15, 1);
        let p3 = fallback_batch_path(0, 100, 200, 1);
        assert!(p1 < p2);
        assert!(p2 < p3);
    }

    #[test]
    fn test_fallback_prefix() {
        assert_eq!(fallback_shard_prefix(0), "cluster/fallback/shard_000/");
        assert_eq!(fallback_shard_prefix(7), "cluster/fallback/shard_007/");
    }

    #[test]
    fn test_parse_fallback_path() {
        assert_eq!(
            parse_fallback_path("cluster/fallback/shard_002/batch_000000005_000000010_00000000-0000-0000-0000-00000000002a.bin"),
            Some((2, 5, 10, 42))
        );
        assert_eq!(
            parse_fallback_path("cluster/fallback/shard_015/batch_123456789_123456799_00000000-0000-0000-0000-0000000000ff.bin"),
            Some((15, 123456789, 123456799, 255))
        );
        assert_eq!(parse_fallback_path("cluster/lease.json"), None);
        assert_eq!(parse_fallback_path("invalid"), None);
        assert_eq!(parse_fallback_path("cluster/fallback/shard_002/batch_000000005_000000010.bin"), None);
        assert_eq!(parse_fallback_path("cluster/fallback/shard_002/batch_000000005.bin"), None);
    }

    #[test]
    fn test_parse_roundtrip() {
        let path = fallback_batch_path(2, 5, 10, 42);
        assert_eq!(parse_fallback_path(&path), Some((2, 5, 10, 42)));
    }
}
