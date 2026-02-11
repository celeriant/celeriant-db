use crate::error::apply_batch_error::ApplyBatchError;
use crate::error::shard_fsync_error::ShardFsyncError;

#[derive(Debug, Clone)]
pub enum S3CatchupError {
    SidecarUnavailable,
    S3ListFailed { prefix: String, message: String },
    S3GetFailed { path: String, message: String },
    S3DeleteFailed { path: String, message: String },
    DeserializationFailed { path: String, source: celeriant_wire::disk::disk_format_error::DiskFormatError },
    WalIndexGap { expected: u64, got: u64 },
    ApplyFailed(ApplyBatchError),
    FsyncFailed(ShardFsyncError),
}

impl S3CatchupError {
    pub fn is_retriable(&self) -> bool {
        matches!(self,
            Self::S3ListFailed { .. } | Self::S3GetFailed { .. } | Self::S3DeleteFailed { .. }
        ) || matches!(self, Self::FsyncFailed(e) if e.is_retriable())
    }
}
