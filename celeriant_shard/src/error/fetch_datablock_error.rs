use celeriant_rotating_log::errors::open_or_create_error::OpenOrCreateError;
use celeriant_wire::disk::disk_format_error::DiskFormatError;

#[derive(Debug, Clone)]
pub enum FetchDatablockError {
    DatablockError { log_id: u64, wal_seq: u64, source: DiskFormatError, is_inline: bool },
    LogSegmentFileError(OpenOrCreateError),
    LogSegmentFileReaderContention,
    LogSegmentFileUnavailable { log_id: u64 },
    DatablockReadError(String),
    MissingDatablocksOnDisk,
}