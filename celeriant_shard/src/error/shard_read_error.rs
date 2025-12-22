use celeriant_disk::files::read_fixed_records_visit_const::ReadVisitError;
use celeriant_watch::aggregate_reader::WatchReadError;
use celeriant_wire::wire_format_error::WireFormatError;
use glommio::GlommioError;

#[derive(Debug, Clone)]
pub enum ShardReadError {
    NotExists,
    IoError(String),
    CannotCreateFolders(String),
    MaxBytesTooSmall {
        current_max_bytes: u64,
        required_max_bytes: u64,
    },
    SerializationError(WireFormatError),
    UnavailableBatchIndex {
        minimum_available_event_batch_index: u64,
        requested_event_batch_index: u64,
    },
    WatchLatencyTooHigh {
        latency_ms: u64,
        max_latency_ms: u64,
    },
    CorruptMetadata {
        file_pos_metadata: u64,
    },
    CorruptEventBatch {
        expected_crc: u32,
        actual_crc: u32,
        event_batch_index: u64,
        file_pos_event_batch: u64,
    },
}

impl From<std::io::Error> for ShardReadError {
    fn from(error: std::io::Error) -> Self {
        ShardReadError::IoError(error.to_string())
    }
}

impl From<WireFormatError> for ShardReadError {
    fn from(error: WireFormatError) -> Self {
        ShardReadError::SerializationError(error)
    }
}

impl From<GlommioError<()>> for ShardReadError {
    fn from(error: GlommioError<()>) -> Self {
        ShardReadError::IoError(error.to_string())
    }
}

/// Push the ReadVisitError (io or deserialisation errors) into ReadError
impl From<ReadVisitError<ShardReadError>> for ShardReadError {
    fn from(error: ReadVisitError<ShardReadError>) -> Self {
        match error {
            ReadVisitError::Io(glommio_error) => ShardReadError::IoError(glommio_error.to_string()),
            ReadVisitError::Visitor(e) => e,
        }
    }
}

impl From<WatchReadError> for ShardReadError {
    fn from(error: WatchReadError) -> Self {
        match error {
            WatchReadError::Io(msg) => ShardReadError::IoError(msg),
            WatchReadError::Serialization(e) => ShardReadError::SerializationError(e),
            WatchReadError::Other(msg) => ShardReadError::IoError(msg),
        }
    }
}