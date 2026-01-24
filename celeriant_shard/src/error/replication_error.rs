use celeriant_rotating_log::rwlock_timeout::LockTimeoutError;

use crate::error::rollback_error::RollbackError;

/// Errors that can occur during replication operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicationError {
    /// Failed to acquire lock within timeout.
    LockTimeout(String),

    /// pending replication batches are empty
    RollbackInProgress,

    /// Network failure communicating with follower or S3.
    NetworkFailure(String),

    /// Follower's log has diverged from leader (hash mismatch).
    FollowerDiverged,

    /// S3 sidecar is unavailable or returned error.
    S3Unavailable,

    /// Replication failed AND subsequent rollback failed (CRITICAL).
    RollbackFailed(RollbackError),
}

impl From<celeriant_client_glommio::ClientError> for ReplicationError {
    fn from(e: celeriant_client_glommio::ClientError) -> Self {
        ReplicationError::NetworkFailure(e.to_string())
    }
}

impl From<LockTimeoutError> for ReplicationError {
    fn from(e: LockTimeoutError) -> Self {
        ReplicationError::LockTimeout(e.to_string())
    }
}