use eventplanedb_structures::wire_format::WireFormatError;
use glommio::GlommioError;

use crate::files::read_fixed_records_visit_const::ReadVisitError;

#[derive(Debug)]
pub enum ReadError {
    NotExists,
    CreateFile(std::io::Error),
    IoError(GlommioError<()>),
    CannotCreateFolders {
        path: String,
        error: std::io::Error,
    },
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
        file_pos_metadata: u64,
        file_pos_event_batch: u64,
    },
}

impl From<std::io::Error> for ReadError {
    fn from(error: std::io::Error) -> Self {
        ReadError::CreateFile(error)
    }
}

impl From<WireFormatError> for ReadError {
    fn from(error: WireFormatError) -> Self {
        ReadError::SerializationError(error)
    }
}

impl From<GlommioError<()>> for ReadError {
    fn from(error: GlommioError<()>) -> Self {
        ReadError::IoError(error)
    }
}

/// Push the ReadVisitError (io or deserialisation errors) into ReadError
impl From<ReadVisitError<ReadError>> for ReadError {
    fn from(error: ReadVisitError<ReadError>) -> Self {
        match error {
            ReadVisitError::Io(glommio_error) => ReadError::IoError(glommio_error),
            ReadVisitError::Visitor(e) => e,
        }
    }
}
