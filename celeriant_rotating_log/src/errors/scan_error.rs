use celeriant_disk::files::rwlock_timeout::LockTimeoutError;

use crate::errors::open_or_create_error::OpenOrCreateError;

#[derive(Debug, Clone)]
pub enum ScanError<E> {
    OpenLogSegment(OpenOrCreateError),
    LockTimeout(LockTimeoutError),
    NoFileHandle { log_id: u64 },
    Io { log_id: u64, source: String },
    Visitor(E),
}

impl<E> From<OpenOrCreateError> for ScanError<E> {
    fn from(e: OpenOrCreateError) -> Self {
        ScanError::OpenLogSegment(e)
    }
}

impl<E> From<LockTimeoutError> for ScanError<E> {
    fn from(e: LockTimeoutError) -> Self {
        ScanError::LockTimeout(e)
    }
}
