use crate::error::rollback_error::RollbackError;

/// Errors that can occur during replication operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicationError {
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