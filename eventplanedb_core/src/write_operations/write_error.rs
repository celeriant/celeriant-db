use eventplanedb_structures::wire_format::WireFormatError;
use glommio::GlommioError;

#[derive(Debug)]
pub enum WriteError {
    IoError(GlommioError<()>),
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
    EmptyEventsList(),
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
        error: std::io::Error,
    }
}

impl From<WireFormatError> for WriteError {
    fn from(error: WireFormatError) -> Self {
        WriteError::SerializationError(error)
    }
}

impl From<GlommioError<()>> for WriteError {
    fn from(error: GlommioError<()>) -> Self {
        WriteError::IoError(error)
    }
}