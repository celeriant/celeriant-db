use crate::{pending_cache_item::PendingCacheItem};
use celeriant_rotating_log::log_segment_file_metadata::LogSegmentFileMetadata;
use celeriant_wal::constants::FIXED_BLOCK_SIZE_BYTES;

/// Data needed to complete a commit after successful replication
pub struct PendingCommitData {
    /// Log segment file metadata with updated write cursor
    pub log_metadata: LogSegmentFileMetadata,
    /// Queue items to process (for cache updates and watch events)
    pub pending_queue: Vec<PendingCacheItem>,
}

impl PendingCommitData {
    pub fn log_id(&self) -> u64 {
        self.log_metadata.log_id
    }

    /// Approximate size in bytes for memory tracking
    pub fn size_bytes(&self) -> u64 {
        let size: u64 = self.pending_queue.iter().map(|item| item.size_bytes()).sum();
        size.saturating_add(FIXED_BLOCK_SIZE_BYTES as u64) // overhead estimate
    }
}

