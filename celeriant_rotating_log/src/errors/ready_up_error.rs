use std::fmt;

use crate::errors::open_or_create_error::OpenOrCreateError;

#[derive(Debug)]
pub enum ReadyUpError {
    InvalidPreallocatedBytes(u64),
    ActiveFileError(OpenOrCreateError),
    UnableToAccessDirectory { directory: String, source: std::io::Error },
    UnableToCreateDirectory { directory: String, source: std::io::Error },
    UnableToDeleteOrphanSegment { path: String, source: std::io::Error },
    DictCodecBuildFailed(String),
}

impl fmt::Display for ReadyUpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPreallocatedBytes(bytes) => {
                write!(f, "Invalid preallocated bytes: {bytes}")
            }
            Self::ActiveFileError(e) => {
                write!(f, "Active file error: {e}")
            }
            Self::UnableToAccessDirectory { directory, source } => {
                write!(f, "Unable to access directory: {directory}, source={source}")
            }
            Self::UnableToCreateDirectory { directory, source } => {
                write!(f, "Unable to create directory: {directory}, source={source}")
            }
            Self::UnableToDeleteOrphanSegment { path, source } => {
                write!(f, "Unable to delete orphan segment: {path}, source={source}")
            }
            Self::DictCodecBuildFailed(e) => {
                write!(f, "Failed to build zstd dict codec at shard boot: {e}")
            }
        }
    }
}
