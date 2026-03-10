use celeriant_rotating_log::errors::scan_error::ScanError;

#[derive(Debug, Clone)]
pub enum ShardListingError {
    ReadFromDiskError(ScanError<()>),
    ListSemaphoreClosed,
}

impl ShardListingError {
    pub fn error_code(&self) -> &'static str {
        match self {
            Self::ReadFromDiskError(_) => "read_from_disk_error",
            Self::ListSemaphoreClosed => "list_semaphore_closed",
        }
    }
}