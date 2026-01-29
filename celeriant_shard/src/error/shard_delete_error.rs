use crate::error::{replication_error::ReplicationError, shard_cache_load_error::ShardCacheLoadError, shard_fsync_error::ShardFsyncError};

#[derive(Debug, Clone)]
pub enum ShardDeleteError {
    AggregateNotExists,
    EmptyDeleteList,
    OptimisticConcurrencyViolation {
        expected_event_batch_index: u64,
        current_event_batch_index: u64,
    },
    AggregateExistsAndCacheError(ShardCacheLoadError),
    ReplicationError(ReplicationError),
    ShardFsyncError(ShardFsyncError),
    InvalidLeaseIndex,
}

impl From<ShardFsyncError> for ShardDeleteError {
    fn from(e: ShardFsyncError) -> Self {
        Self::ShardFsyncError(e)
    }
}

impl From<ReplicationError> for ShardDeleteError {
    fn from(e: ReplicationError) -> Self {
        Self::ReplicationError(e)
    }
}

impl From<ShardCacheLoadError> for ShardDeleteError {
    fn from(e: ShardCacheLoadError) -> Self {
        Self::AggregateExistsAndCacheError(e)
    }
}