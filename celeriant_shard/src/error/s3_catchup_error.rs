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
    TruncationFailed(ShardFsyncError),
}

impl S3CatchupError {
    pub fn is_retriable(&self) -> bool {
        matches!(self,
            Self::S3ListFailed { .. } | Self::S3GetFailed { .. } | Self::S3DeleteFailed { .. }
        ) || matches!(self, Self::FsyncFailed(e) if e.is_retriable())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncation_failed_is_not_retriable() {
        let err = S3CatchupError::TruncationFailed(
            ShardFsyncError::ActiveWriteFileUnavailable
        );
        assert!(!err.is_retriable());
    }

    #[test]
    fn s3_errors_are_retriable() {
        let errors = vec![
            S3CatchupError::S3ListFailed { prefix: "test".to_string(), message: "msg".to_string() },
            S3CatchupError::S3GetFailed { path: "test".to_string(), message: "msg".to_string() },
            S3CatchupError::S3DeleteFailed { path: "test".to_string(), message: "msg".to_string() },
        ];
        for err in errors {
            assert!(err.is_retriable(), "Expected {:?} to be retriable", err);
        }
    }

    #[test]
    fn fsync_failed_with_retriable_inner_error_is_retriable() {
        let err = S3CatchupError::FsyncFailed(
            ShardFsyncError::RollbackInvalidatedWrites
        );
        assert!(err.is_retriable());
    }

    #[test]
    fn fsync_failed_with_non_retriable_inner_error_is_not_retriable() {
        let err = S3CatchupError::FsyncFailed(
            ShardFsyncError::ActiveWriteFileUnavailable
        );
        assert!(!err.is_retriable());
    }
}
