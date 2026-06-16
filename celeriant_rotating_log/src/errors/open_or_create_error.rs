use std::fmt;

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
    RotationTargetUnsafe {
        log_id: u64,
        path: String,
        source: String,
    },
    /// Filesystem returned ENOSPC during create+preallocate of a new segment.
    /// Surface to the writer so the shard stays alive (existing reads keep
    /// working); writes that need rotation fail until disk space is recovered.
    OutOfSpace {
        log_id: u64,
        path: String,
        preallocate_bytes: u64,
    },
}

impl fmt::Display for OpenOrCreateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnableToCreateLogSegmentFile {
                log_id,
                path,
                preallocate_bytes,
                source,
            } => {
                write!(
                    f,
                    "Unable to create log segment file: log_id={log_id}, path={path}, preallocate_bytes={preallocate_bytes}, source={source}"
                )
            }
            Self::UnableToOpenExistingFile { log_id, path, source } => {
                write!(f, "Unable to open existing log file: log_id={log_id}, path={path}, source={source}")
            }
            Self::UnableToDuplicateWriterFD { log_id, path, source } => {
                write!(f, "Unable to duplicate writer FD: log_id={log_id}, path={path}, source={source}")
            }
            Self::LogSegmentFileReadError { log_id, source, step } => {
                write!(f, "Log segment read error: log_id={log_id}, step={step}, source={source}")
            }
            Self::LogSegmentFileCorrupted { log_id, source } => {
                write!(f, "Log segment corrupted: log_id={log_id}, source={source:?}")
            }
            Self::LogSegmentFileHeaderWriteFailure { log_id, source } => {
                write!(f, "Log segment header write failure: log_id={log_id}, source={source:?}")
            }
            Self::FSyncErrorOnNewFile { log_id, source } => {
                write!(f, "Fsync error on new file: log_id={log_id}, source={source}")
            }
            Self::DirectoryFSyncErrorOnNewFile { log_id, source, path, step } => {
                write!(
                    f,
                    "Directory fsync error on new file: log_id={log_id}, path={path}, step={step}, source={source}"
                )
            }
            Self::RotationTargetUnsafe { log_id, path, source } => {
                write!(f, "Rotation target unsafe to overwrite: log_id={log_id}, path={path}, source={source}")
            }
            Self::OutOfSpace {
                log_id,
                path,
                preallocate_bytes,
            } => {
                write!(
                    f,
                    "Out of disk space rotating log: log_id={log_id}, path={path}, preallocate_bytes={preallocate_bytes}"
                )
            }
        }
    }
}
