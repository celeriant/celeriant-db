// use celeriant_msg::response::responses::FollowerRejection;
// use celeriant_rotating_log::rwlock_timeout::LockTimeoutError;

// use crate::error::{rollback_error::RollbackError, shard_read_error::ShardReadError};

// /// Network/transport errors - may be transient, retry or S3 fallback makes sense.
// #[derive(Debug, Clone, PartialEq, Eq)]
// pub enum NetworkError {
//     ConnectionFailed(String),
//     Timeout(String),
//     SendFailed(String),
// }

use crate::error::{fetch_catchup_entries_error::FetchCatchupEntriesError, replication_rollback_failure::ReplicationRollbackFailure, replication_to_s3_error::ReplicateToS3Error};

// /// Errors that can occur during replication operations.
#[derive(Debug, Clone)]
pub enum ReplicationError {
//     /// Network/transport errors - transient, may retry or failover to S3.
//     Network(NetworkError),

//     /// Follower explicitly rejected the batch - indicates state mismatch.
//     FollowerRejected(FollowerRejection),

//     /// Failed to acquire lock within timeout.
//     LockTimeout(String),

    /// Pending replication batches are empty due to rollback.
    RollbackInProgress,
    RollbackFailed(ReplicationRollbackFailure),
    ReplicationClientLockTimeoutError,
    ReplicateToS3Error(ReplicateToS3Error),
    ExtendedCatchupFailure(FetchCatchupEntriesError),

//     /// S3 sidecar is unavailable or returned error.
//     S3Unavailable,

//     /// Replication failed AND subsequent rollback failed (CRITICAL).
//     RollbackFailed(RollbackError),

//     /// Gap between follower and leader exceeds maximum catchup threshold.
//     GapTooLarge { gap_bytes: u64, threshold_bytes: u64 },

//     /// Requested WAL entries are no longer available (compacted).
//     WalEntriesUnavailable { requested_index: u64 },

//     ExtendedCatchupFailure(ShardReadError),
}

// impl ReplicationError {
//     pub fn is_network_error(&self) -> bool {
//         matches!(self, Self::Network(_))
//     }

//     pub fn is_follower_rejection(&self) -> bool {
//         matches!(self, Self::FollowerRejected(_))
//     }

//     /// Network errors may be transient; follower rejections need catchup.
//     pub fn should_try_s3_fallback(&self) -> bool {
//         self.is_network_error()
//     }
// }

// impl From<celeriant_client_glommio::ClientError> for ReplicationError {
//     fn from(e: celeriant_client_glommio::ClientError) -> Self {
//         ReplicationError::Network(NetworkError::ConnectionFailed(e.to_string()))
//     }
// }

// impl From<LockTimeoutError> for ReplicationError {
//     fn from(e: LockTimeoutError) -> Self {
//         ReplicationError::LockTimeout(e.to_string())
//     }
// }
