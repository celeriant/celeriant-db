use celeriant_disk::files::read_fixed_records_visit_const::ReadVisitError;
use celeriant_wire::wire_format_error::WireFormatError;
use glommio::GlommioError;


#[derive(Debug, Clone)]
pub enum ReadError {
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
        max_latency_mx: u64,
    }
}

impl From<walkdir::Error> for ReadError {
    fn from(error: walkdir::Error) -> Self {
        ReadError::IoError(error.to_string())
    }
}

impl From<std::io::Error> for ReadError {
    fn from(error: std::io::Error) -> Self {
        ReadError::IoError(error.to_string())
    }
}

impl From<WireFormatError> for ReadError {
    fn from(error: WireFormatError) -> Self {
        ReadError::SerializationError(error)
    }
}

impl From<GlommioError<()>> for ReadError {
    fn from(error: GlommioError<()>) -> Self {
        ReadError::IoError(error.to_string())
    }
}

/// Push the ReadVisitError (io or deserialisation errors) into ReadError
impl From<ReadVisitError<ReadError>> for ReadError {
    fn from(error: ReadVisitError<ReadError>) -> Self {
        match error {
            ReadVisitError::Io(glommio_error) => ReadError::IoError(glommio_error.to_string()),
            ReadVisitError::Visitor(e) => e,
        }
    }
}
