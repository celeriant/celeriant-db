//! S3 fallback replication for when follower is unreachable.
//!
//! When the follower becomes unreachable, the leader writes batches to S3
//! instead. A new leader must consume these batches before accepting writes.

use bincode::{Decode, Encode};
use celeriant_wal::datablocks::datablock::Datablock;
use celeriant_wal::metablocks::metablock::Metablock;

use crate::paths;

/// A batch of writes stored in S3 during fallback mode.
#[derive(Debug, Clone, Encode, Decode)]
pub struct FallbackBatch {
    pub fallback_index: u64,
    pub shard_id: u32,
    pub items: Vec<FallbackItem>,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct FallbackItem {
    pub metablock: Metablock,
    pub datablock: Option<Datablock>,
}

impl FallbackBatch {
    /// Create a new fallback batch.
    pub fn new(
        fallback_index: u64,
        shard_id: u32,
    ) -> Self {
        Self {
            fallback_index,
            shard_id,
            items: Vec::new(),
        }
    }

    /// Add an item to the batch and update tracking.
    pub fn push_item(&mut self, item: FallbackItem) {
        self.items.push(item);
    }

    /// Check if the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Number of items in the batch.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Get the S3 path for this batch.
    pub fn s3_path(&self) -> String {
        paths::fallback_batch_path(self.shard_id, self.fallback_index)
    }
}

/// Parse a fallback batch path to extract shard_id and fallback_index.
/// Returns None if the path doesn't match the expected format.
pub fn parse_fallback_path(path: &str) -> Option<(u32, u64)> {
    // Expected format: cluster/fallback/shard_XX/batch_XXXXXXXXX.bin
    let parts: Vec<&str> = path.split('/').collect();
    if parts.len() < 3 {
        return None;
    }

    let shard_part = parts.iter().find(|p| p.starts_with("shard_"))?;
    let batch_part = parts.iter().find(|p| p.starts_with("batch_"))?;

    let shard_id: u32 = shard_part.strip_prefix("shard_")?.parse().ok()?;
    let batch_name = batch_part.strip_prefix("batch_")?.strip_suffix(".bin")?;
    let fallback_index: u64 = batch_name.parse().ok()?;

    Some((shard_id, fallback_index))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fallback_batch_s3_path() {
        let batch = FallbackBatch::new(5, 2);
        assert_eq!(batch.s3_path(), "cluster/fallback/shard_002/batch_000000005.bin");
    }

    #[test]
    fn test_parse_fallback_path() {
        assert_eq!(
            parse_fallback_path("cluster/fallback/shard_002/batch_000000005.bin"),
            Some((2, 5))
        );
        assert_eq!(
            parse_fallback_path("cluster/fallback/shard_015/batch_123456789.bin"),
            Some((15, 123456789))
        );
        assert_eq!(parse_fallback_path("cluster/lease.bin"), None);
        assert_eq!(parse_fallback_path("invalid"), None);
    }
}
