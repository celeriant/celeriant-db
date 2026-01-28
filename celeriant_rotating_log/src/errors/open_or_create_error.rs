use celeriant_wire::disk::disk_format_error::DiskFormatError;
use glommio::GlommioError;

use crate::errors::write_dual_header_error::WriteDualHeaderError;

#[derive(Debug)]
pub enum OpenOrCreateError {
    UnableToCreateLogSegmentFile {
        log_id: u64,
        path: String,
        preallocate_bytes: u64,
        source: GlommioError<()>,
    },
    UnableToOpenExistingFile {
        log_id: u64,
        path: String,
        source: GlommioError<()>,
    },
    UnableToDuplicateWriterFD {
        log_id: u64,
        path: String,
        source: GlommioError<()>,
    },
    LogSegmentFileReadError {
        log_id: u64,
        source: GlommioError<()>,
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
        source: GlommioError<()>,
    },
    DirectoryFSyncErrorOnNewFile {
        log_id: u64,
        source: GlommioError<()>,
        path: String,
        step: String,
    },
}