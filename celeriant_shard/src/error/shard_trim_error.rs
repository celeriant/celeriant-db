use crate::error::{replication_error::ReplicationError, shard_cache_load_error::ShardCacheLoadError, shard_fsync_error::ShardFsyncError};

#[derive(Debug, Clone)]
pub enum ShardTrimError {
    AggregateNotExists,
    AggregateExistsAndCacheError(ShardCacheLoadError),
    ReplicationError(ReplicationError),
    ShardFsyncError(ShardFsyncError),
    TrimIndexOutOfRange { requested: u64, max_event_batch_index: u64 },
    ShardCannotAcceptWrites,
}

impl From<ShardFsyncError> for ShardTrimError {
    fn from(e: ShardFsyncError) -> Self {
        Self::ShardFsyncError(e)
    }
}

impl From<ReplicationError> for ShardTrimError {
    fn from(e: ReplicationError) -> Self {
        Self::ReplicationError(e)
    }
}

impl From<ShardCacheLoadError> for ShardTrimError {
    fn from(e: ShardCacheLoadError) -> Self {
        Self::AggregateExistsAndCacheError(e)
    }
}