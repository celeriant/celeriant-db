use celeriant_disk::files::read_fixed_records_visit_const::ReadVisitError;
use celeriant_wire::wire_format_error::WireFormatError;
use glommio::GlommioError;

#[derive(Debug, Clone)]
pub enum ShardLogReadError {
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
    CorruptMetadata {
        file_pos_metadata: u64,
    },
    CorruptEventBatch {
        expected_crc: u32,
        actual_crc: u32,
        event_batch_index: u64,
        file_pos_event_batch: u64,
    },
    WatchLatencyTooHigh {
        latency_ms: u64,
        max_latency_ms: u64,
    },
}

impl From<std::io::Error> for ShardLogReadError {
    fn from(error: std::io::Error) -> Self {
        ShardLogReadError::IoError(error.to_string())
    }
}

impl From<WireFormatError> for ShardLogReadError {
    fn from(error: WireFormatError) -> Self {
        ShardLogReadError::SerializationError(error)
    }
}

impl From<GlommioError<()>> for ShardLogReadError {
    fn from(error: GlommioError<()>) -> Self {
        ShardLogReadError::IoError(error.to_string())
    }
}

/// Push the ReadVisitError (io or deserialisation errors) into ReadError
impl From<ReadVisitError<ShardLogReadError>> for ShardLogReadError {
    fn from(error: ReadVisitError<ShardLogReadError>) -> Self {
        match error {
            ReadVisitError::Io(glommio_error) => ShardLogReadError::IoError(glommio_error.to_string()),
            ReadVisitError::Visitor(e) => e,
        }
    }
}
