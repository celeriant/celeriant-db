use celeriant_rotating_log::{rotating_log_error::RotatingLogError, rwlock_timeout::LockTimeoutError};
use celeriant_wire::wire_format_error::WireFormatError;
use glommio::GlommioError;

use crate::error::{shard_cache_load_error::ShardCacheError, shard_fsync_error::ShardFsyncError};

/// Storage/infrastructure errors—may be transient.
#[derive(Debug, Clone)]
pub enum ShardWriteError {
    /// Disk I/O failure.
    IoError(String),
    
    /// Serialization or deserialization failure.
    WireFormat(WireFormatError),

    /// Write request contained no events.
    EmptyEventsList,
    
    /// Event type 0 is reserved as a sentinel value.
    ZeroEventType { client_event_index: u64 },
    
    /// Client already wrote an event with this or higher client_event_index.
    ClientIdempotencyViolation {
        last_client_event_index: u64,
        attempted_client_event_index: u64,
    },
    
    /// Expected event_batch_index doesn't match current aggregate state.
    OptimisticConcurrencyViolation {
        expected_event_batch_index: u64,
        current_event_batch_index: u64,
    },
    
    /// Write request requires a valid lease index.
    InvalidLeaseIndex,

    AggregateNotExists,

    // There is a soft delete entry in the queue that hasn't been committed yet
    AggregatePendingDelete,

    /// Aggregate was deleted with allow_recreate=false and cannot be recreated
    AggregateRecreateNotAllowed,

    TrimIndexOutOfRange {
        requested: u64,
        max_event_batch_index: u64,
    },
}

impl From<ShardCacheError> for ShardWriteError {
    fn from(value: ShardCacheError) -> Self {
        match value {
            ShardCacheError::IoError(error) => ShardWriteError::IoError(error.to_string()),
        }
    }
}

impl From<RotatingLogError> for ShardWriteError {
    fn from(e: RotatingLogError) -> Self {
        Self::IoError(e.to_string())
    }
}

impl From<LockTimeoutError> for ShardWriteError {
    fn from(e: LockTimeoutError) -> Self {
        Self::IoError(e.to_string())
    }
}

impl From<ShardFsyncError> for ShardWriteError {
    fn from(e: ShardFsyncError) -> Self {
        match e {
            ShardFsyncError::IoError(msg) => Self::IoError(msg),
            ShardFsyncError::WireFormat(wire_err) => Self::WireFormat(wire_err),
            ShardFsyncError::DmaFileNotInitialized => {
                Self::IoError("DMA file handle not initialized".to_string())
            }
            ShardFsyncError::HeaderCorrupted { log_id } => {
                Self::IoError(format!("Log header corrupted: log_id={:?}", log_id))
            }
            ShardFsyncError::LogFileNotFound { log_id } => {
                Self::IoError(format!("Log file not found: log_id={}", log_id))
            }
            ShardFsyncError::SyncFailurePending => {
                Self::IoError("Previous sync failure pending".to_string())
            }
            ShardFsyncError::DatablocksCarryOverBufferNotPresent => {
                Self::IoError("Datablocks carry-over buffer not present".to_string())
            }
            ShardFsyncError::NotEnoughLogFreeSpace { required, available } => {
                Self::IoError(format!("Not enough free log space, required: {required} but available: {available}"))
            }
        }
    }
}

impl From<GlommioError<()>> for ShardWriteError {
    fn from(e: GlommioError<()>) -> Self {
        Self::IoError(e.to_string())
    }
}

impl From<WireFormatError> for ShardWriteError {
    fn from(e: WireFormatError) -> Self {
        Self::WireFormat(e)
    }
}

impl From<std::io::Error> for ShardWriteError {
    fn from(e: std::io::Error) -> Self {
        Self::IoError(e.to_string())
    }
}