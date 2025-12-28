use celeriant_rotating_log::{rotating_log_error::RotatingLogError, rwlock_timeout::LockTimeoutError};
use glommio::GlommioError;

/// Storage/infrastructure errors—may be transient.
#[derive(Debug, Clone)]
pub enum ShardCacheError {
    /// Disk I/O failure.
    IoError(String),
}

impl From<RotatingLogError> for ShardCacheError {
    fn from(e: RotatingLogError) -> Self {
        Self::IoError(e.to_string())
    }
}

impl From<LockTimeoutError> for ShardCacheError {
    fn from(e: LockTimeoutError) -> Self {
        Self::IoError(e.to_string())
    }
}

impl From<GlommioError<()>> for ShardCacheError {
    fn from(e: GlommioError<()>) -> Self {
        Self::IoError(e.to_string())
    }
}

impl From<std::io::Error> for ShardCacheError {
    fn from(e: std::io::Error) -> Self {
        Self::IoError(e.to_string())
    }
}