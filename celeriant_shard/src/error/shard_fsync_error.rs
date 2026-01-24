use celeriant_rotating_log::{rotating_log_error::RotatingLogError, rwlock_timeout::LockTimeoutError};
use celeriant_wire::wire_format_error::WireFormatError;
use glommio::GlommioError;

use crate::error::replication_error::ReplicationError;

/// Storage/infrastructure errors—may be transient.
#[derive(Debug, Clone)]
pub enum ShardFsyncError {
    /// Disk I/O failure.
    IoError(String),

    /// Serialization or deserialization failure.
    WireFormat(WireFormatError),

    /// DMA file handle not initialized (startup issue).
    DmaFileNotInitialized,

    /// Log file header corrupted beyond recovery.
    HeaderCorrupted { log_id: Option<u64> },

    /// Requested log file doesn't exist.
    LogFileNotFound { log_id: u64 },

    DatablocksCarryOverBufferNotPresent,

    NotEnoughLogFreeSpace {
        required: u64,
        available: u64,
    },

    /// A rollback occurred and invalidated pending writes.
    /// Writers should retry their operation.
    RollbackInvalidatedWrites,
}

impl From<LockTimeoutError> for ShardFsyncError {
    fn from(e: LockTimeoutError) -> Self {
        Self::IoError(e.to_string())
    }
}

impl From<GlommioError<()>> for ShardFsyncError {
    fn from(e: GlommioError<()>) -> Self {
        Self::IoError(e.to_string())
    }
}

impl From<WireFormatError> for ShardFsyncError {
    fn from(e: WireFormatError) -> Self {
        Self::WireFormat(e)
    }
}

impl From<std::io::Error> for ShardFsyncError {
    fn from(e: std::io::Error) -> Self {
        Self::IoError(e.to_string())
    }
}

impl From<RotatingLogError> for ShardFsyncError {
    fn from(e: RotatingLogError) -> Self {
        match e {
            RotatingLogError::InvalidPreallocatedBytes(b) => {
                Self::IoError(format!("Invalid preallocated bytes: {}", b))
            }
            RotatingLogError::BatchesTooLarge(b) => {
                Self::IoError(format!("Batches too large for log segment file of preallocated bytes: {}", b))
            }
            RotatingLogError::IoError(msg) => Self::IoError(msg),
            RotatingLogError::WireFormat(e) => Self::WireFormat(e),
            RotatingLogError::HeaderCorrupted { log_id } => Self::HeaderCorrupted { log_id },
            RotatingLogError::LogFileNotFound { log_id } => Self::LogFileNotFound { log_id },
        }
    }
}

impl From<ReplicationError> for ShardFsyncError {
    fn from(e: ReplicationError) -> Self {
        match e {
            ReplicationError::LockTimeout(msg) => Self::IoError(msg),
            ReplicationError::RollbackInProgress => Self::IoError("Rollback in progress".into()),
            ReplicationError::NetworkFailure(msg) => Self::IoError(msg),
            ReplicationError::FollowerDiverged => Self::IoError("Follower log diverged from leader".into()),
            ReplicationError::S3Unavailable => Self::IoError("S3 sidecar unavailable".into()),
            ReplicationError::RollbackFailed(rb_err) => Self::IoError(format!("Rollback failed: {:?}", rb_err)),
        }
    }
}