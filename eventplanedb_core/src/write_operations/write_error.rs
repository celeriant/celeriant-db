use eventplanedb_structures::wire_format::WireFormatError;
use glommio::GlommioError;

#[derive(Debug)]
pub enum WriteError {
    DmaFileNotInitialized,
    GlommioError(GlommioError<()>),
    IoError(std::io::Error),
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
        error: std::io::Error,
    },
    MaxBytesTooSmall {
        current_max_bytes: u64,
        required_max_bytes: u64,
    },
}

impl From<WireFormatError> for WriteError {
    fn from(error: WireFormatError) -> Self {
        WriteError::SerializationError(error)
    }
}

impl From<GlommioError<()>> for WriteError {
    fn from(error: GlommioError<()>) -> Self {
        WriteError::GlommioError(error)
    }
}

impl From<std::io::Error> for WriteError {
    fn from(error: std::io::Error) -> Self {
        WriteError::IoError(error)
    }
}