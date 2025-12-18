use celeriant_wire::wire_format_error::WireFormatError;
use glommio::GlommioError;

use celeriant_rotating_log::rotating_log_error::RotatingLogError;

#[derive(Debug, Clone)]
pub enum ShardLogWriteError {
    DmaFileNotInitialized,
    DatablocksCarryOverBufferNotPresent,
    IoError(String),
    SerializationError(WireFormatError),
    EmptyEventsList,
    ZeroEventType {
        client_event_index: u64,
    },
    ClientIdempotencyViolation {
        client_id: u128,
        last_client_event_index: u64,
        attempted_client_event_index: u64,
    },
    OptimisticConcurrencyViolation {
        expected_event_batch_index: u64,
        current_event_batch_index: u64,
    },
    NoEventsToAppend {
        client_id: u128,
        existing_event_index: u64,
    },
    CacheMiss {
        missing_from_event_batch_index: u64,        
        missing_to_event_batch_index: Option<u64>,
    },
    PrependCreatesEventBatchIndexGap { 
        provided_last_batch_index: u64,
        current_first_event_batch_index: u64,
    },
    PrependNonContiguousBatches {
        from_event_batch_index: u64,
        to_event_batch_index: u64,
    },
    FileRenameFailure {
        from: String,
        to: String,
    },
    MaxBytesTooSmall {
        current_max_bytes: u64,
        required_max_bytes: u64,
    },
    InvalidLeaseIndex,
}

impl From<RotatingLogError> for ShardLogWriteError {
    fn from(error: RotatingLogError) -> Self {
        match error {
            RotatingLogError::IoError(e) => ShardLogWriteError::IoError(e),
            RotatingLogError::WireFormat(e) => ShardLogWriteError::SerializationError(e),
            RotatingLogError::HeaderCorrupted { log_id } => ShardLogWriteError::IoError(format!("Header corrupted for log {:?}", log_id)),
            RotatingLogError::LogFileNotFound { log_id } => ShardLogWriteError::IoError(format!("Expected log file not found: {:?}", log_id)),
            RotatingLogError::InvalidPreallocatedBytes(b) => ShardLogWriteError::IoError(format!("Invalid preallocated bytes {:?}", b)),
        }
    }
}

impl From<WireFormatError> for ShardLogWriteError {
    fn from(error: WireFormatError) -> Self {
        ShardLogWriteError::SerializationError(error)
    }
}

impl From<GlommioError<()>> for ShardLogWriteError {
    fn from(error: GlommioError<()>) -> Self {
        ShardLogWriteError::IoError(error.to_string())
    }
}

impl From<std::io::Error> for ShardLogWriteError {
    fn from(error: std::io::Error) -> Self {
        ShardLogWriteError::IoError(error.to_string())
    }
}