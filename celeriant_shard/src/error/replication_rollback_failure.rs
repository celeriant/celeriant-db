use celeriant_rotating_log::errors::write_dual_header_error::WriteDualHeaderError;

#[derive(Debug, Clone)]
pub enum ReplicationRollbackFailure {
    FsyncAmortisedBatchLockTimeout,
    WriteLockTimeout { log_id: u64 },
    LogSegmentFileUnavailable { log_id: u64 },
    WriteDualHeaderError {
        source: WriteDualHeaderError,
        log_id: u64,
    },
    HeaderFsyncFailed {
        log_id: u64,
    }
}