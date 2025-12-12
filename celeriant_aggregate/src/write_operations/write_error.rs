use celeriant_wire::wire_format_error::WireFormatError;
use glommio::GlommioError;

#[derive(Debug, Clone)]
pub enum WriteError {
    DmaFileNotInitialized,
    IoError(String),
    SerializationError(WireFormatError),
    OptimisticConcurrencyViolation {
        client_id: u128,
        expected_event_batch_index: u64,
        current_event_batch_index: u64,
    },
    ClientIdempotencyViolation {
        client_id: u128,
        last_client_event_index: u64,
        attempted_client_event_index: u64,
    },
    EmptyEventsList,
    NoEventsToAppend {
        client_id: u128,
        existing_event_index: u64,
    },
    ZeroEventType {
        client_event_index: u64,
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

impl From<WireFormatError> for WriteError {
    fn from(error: WireFormatError) -> Self {
        WriteError::SerializationError(error)
    }
}

impl From<GlommioError<()>> for WriteError {
    fn from(error: GlommioError<()>) -> Self {
        WriteError::IoError(error.to_string())
    }
}

impl From<std::io::Error> for WriteError {
    fn from(error: std::io::Error) -> Self {
        WriteError::IoError(error.to_string())
    }
}