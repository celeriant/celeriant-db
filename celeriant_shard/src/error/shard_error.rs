use crate::error::{follower_replication_write_error::FollowerReplicationWriteError, shard_delete_error::ShardDeleteError, shard_exists_error::ShardAggregateDetailsError, shard_listing_error::ShardListingError, shard_read_error::ShardReadError, shard_schema_error::ShardSchemaError, shard_trim_error::ShardTrimError, shard_write_error::ShardWriteError};

#[derive(Debug, Clone)]
pub enum ShardError {
    Read(ShardReadError),
    Write(ShardWriteError),
    TrimStart(ShardTrimError),
    Delete(ShardDeleteError),
    ListAggregateTypes(ShardListingError),
    ListAggregates(ShardListingError),
    ReplicationBatch(FollowerReplicationWriteError),
    WatchRequestInvalid,
    ListOrgs(ShardListingError),
    AggregateDetails(ShardAggregateDetailsError),
    RegisterSchema(ShardSchemaError),
}

impl ShardError {
    pub fn error_code(&self) -> &'static str {
        match self {
            Self::Read(e) => e.error_code(),
            Self::Write(e) => e.error_code(),
            Self::TrimStart(e) => e.error_code(),
            Self::Delete(e) => e.error_code(),
            Self::ListAggregateTypes(e) => e.error_code(),
            Self::ListAggregates(e) => e.error_code(),
            Self::ReplicationBatch(e) => e.error_code(),
            Self::WatchRequestInvalid => "watch_request_invalid",
            Self::ListOrgs(e) => e.error_code(),
            Self::AggregateDetails(e) => e.error_code(),
            Self::RegisterSchema(e) => e.error_code(),
        }
    }
}