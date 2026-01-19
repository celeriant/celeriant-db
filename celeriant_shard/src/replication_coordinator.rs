
pub type ReplicationResult = Result<(), ReplicationError>;

/// Errors that can occur during rollback operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RollbackError {
    /// Failed to reset write cursor to read cursor.
    LogPositionRollbackFailed(String),

    /// Failed to revert in-memory aggregate snapshots.
    MemcacheRollbackFailed(String),

    /// Failed to fsync the rolled-back header (CRITICAL - durability loss).
    HeaderFsyncFailed(String),
}

/// Errors that can occur during replication operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicationError {
    /// Network failure communicating with follower or S3.
    NetworkFailure(String),

    /// Follower's log has diverged from leader (hash mismatch).
    FollowerDiverged,

    /// S3 sidecar is unavailable or returned error.
    S3Unavailable,

    /// Replication failed AND subsequent rollback failed (CRITICAL).
    RollbackFailed(RollbackError),
}