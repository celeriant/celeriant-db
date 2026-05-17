use crate::error::{replication_error::ReplicationError, shard_cache_load_error::ShardCacheLoadError, shard_fsync_error::ShardFsyncError};

#[derive(Debug, Clone)]
pub enum ShardDeleteError {
    AggregateNotExists,
    EmptyDeleteList,
    OptimisticConcurrencyViolation {
        expected_version: u64,
        current_aggregate_version: u64,
    },
    AggregateExistsAndCacheError(ShardCacheLoadError),
    ReplicationError(ReplicationError),
    ShardFsyncError(ShardFsyncError),
    ShardCannotAcceptWrites { leader_address: Option<String> },
}

impl ShardDeleteError {
    pub fn error_code(&self) -> &'static str {
        match self {
            Self::AggregateNotExists => "aggregate_not_exists",
            Self::EmptyDeleteList => "empty_delete_list",
            Self::OptimisticConcurrencyViolation { .. } => "optimistic_concurrency_violation",
            Self::AggregateExistsAndCacheError(_) => "cache_load_error",
            Self::ReplicationError(_) => "replication_error",
            Self::ShardFsyncError(_) => "fsync_error",
            Self::ShardCannotAcceptWrites { .. } => "shard_cannot_accept_writes",
        }
    }
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