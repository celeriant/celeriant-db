use celeriant_wire::wire_format_error::WireFormatError;
use glommio::GlommioError;

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