use celeriant_wire::disk::disk_format_error::DiskFormatError;

use crate::errors::write_dual_header_error::WriteDualHeaderError;

#[derive(Debug, Clone)]
pub enum OpenOrCreateError {
    UnableToCreateLogSegmentFile {
        log_id: u64,
        path: String,
        preallocate_bytes: u64,
        source: String,
    },
    UnableToOpenExistingFile {
        log_id: u64,
        path: String,
        source: String,
    },
    UnableToDuplicateWriterFD {
        log_id: u64,
        path: String,
        source: String,
    },
    LogSegmentFileReadError {
        log_id: u64,
        source: String,
        step: String,
    },
    LogSegmentFileCorrupted {
        log_id: u64,
        source: DiskFormatError,
    },
    LogSegmentFileHeaderWriteFailure {
        log_id: u64,
        source: WriteDualHeaderError,
    },
    FSyncErrorOnNewFile {
        log_id: u64,
        source: String,
    },
    DirectoryFSyncErrorOnNewFile {
        log_id: u64,
        source: String,
        path: String,
        step: String,
    },
}