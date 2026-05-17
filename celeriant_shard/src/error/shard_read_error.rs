use celeriant_rotating_log::errors::scan_error::ScanError;
use celeriant_wire::disk::disk_format_error::DiskFormatError;

use crate::error::{fetch_datablock_error::FetchDatablockError, shard_cache_load_error::ShardCacheLoadError};

#[derive(Debug, Clone)]
pub enum ShardReadError {
    UnavailableBatchIndex {
        minimum_available: u64,
        requested: u64,
    },
    AggregateNotExists,
    ShardCacheLoadError(ShardCacheLoadError),
    FetchDatablocksError(FetchDatablockError),
    FetchMetablocksError(ScanError<DiskFormatError>),
}

impl ShardReadError {
    pub fn error_code(&self) -> &'static str {
        match self {
            Self::UnavailableBatchIndex { .. } => "unavailable_version",
            Self::AggregateNotExists => "aggregate_not_exists",
            Self::ShardCacheLoadError(_) => "cache_load_error",
            Self::FetchDatablocksError(_) => "fetch_datablocks_error",
            Self::FetchMetablocksError(_) => "fetch_metablocks_error",
        }
    }
}

impl From<ScanError<DiskFormatError>> for ShardReadError {
    fn from(e: ScanError<DiskFormatError>) -> Self {
        Self::FetchMetablocksError(e)
    }
}

impl From<FetchDatablockError> for ShardReadError {
    fn from(e: FetchDatablockError) -> Self {
        Self::FetchDatablocksError(e)
    }
}

impl From<ShardCacheLoadError> for ShardReadError {
    fn from(e: ShardCacheLoadError) -> Self {
        Self::ShardCacheLoadError(e)
    }
}