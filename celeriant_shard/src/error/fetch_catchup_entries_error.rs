use celeriant_rotating_log::errors::scan_error::ScanError;
use celeriant_wire::disk::disk_format_error::DiskFormatError;

use crate::error::fetch_datablock_error::FetchDatablockError;

#[derive(Debug, Clone)]
pub enum FetchCatchupEntriesError {
    MetablockDiscoveryError(ScanError<DiskFormatError>),
    FollowerTooFarBehind,
    FetchDatablockError(FetchDatablockError),
}