use celeriant_rotating_log::errors::scan_error::ScanError;

#[derive(Debug, Clone)]
pub enum ShardListingError {
    ReadFromDiskError(ScanError<()>),
}