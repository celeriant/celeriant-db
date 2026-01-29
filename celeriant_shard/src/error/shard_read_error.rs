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