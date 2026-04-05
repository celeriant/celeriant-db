use celeriant_rotating_log::errors::scan_error::ScanError;

#[derive(Debug, Clone)]
pub enum ShardCacheLoadError {
    AggregateLoadingLockTimeout,
    FileScanningError(ScanError<()>),
    DatablockReadError(String),
}