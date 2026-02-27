use crate::error::{follower_replication_write_error::FollowerReplicationWriteError, shard_delete_error::ShardDeleteError, shard_exists_error::ShardAggregateDetailsError, shard_listing_error::ShardListingError, shard_read_error::ShardReadError, shard_trim_error::ShardTrimError, shard_write_error::ShardWriteError};

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
}