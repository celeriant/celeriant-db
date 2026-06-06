use crate::error::apply_batch_error::ApplyBatchError;
use crate::error::shard_fsync_error::ShardFsyncError;

#[derive(Debug, Clone)]
pub enum S3CatchupError {
    SidecarUnavailable,
    S3ListFailed { prefix: String, message: String },
    S3GetFailed { path: String, message: String },
    S3DeleteFailed { path: String, message: String },
    DeserializationFailed { path: String, source: celeriant_wire::disk::disk_format_error::DiskFormatError },
    WalSeqGap { expected: u64, got: u64 },
    ApplyFailed(ApplyBatchError),
    FsyncFailed(ShardFsyncError),
    TruncationFailed(ShardFsyncError),
}

impl S3CatchupError {
    pub fn is_retriable(&self) -> bool {
        matches!(self,
            Self::S3ListFailed { .. } | Self::S3GetFailed { .. } | Self::S3DeleteFailed { .. }
        )
        || matches!(self, Self::FsyncFailed(e) if e.is_retriable())
        || matches!(self, Self::TruncationFailed(e) if e.is_retriable())
    }

    pub fn is_disk_full(&self) -> bool {
        matches!(self, Self::FsyncFailed(e) | Self::TruncationFailed(e) if e.is_disk_full())
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

    #[test]
    fn enospc_rotation_is_disk_full_not_fatal() {
        use celeriant_rotating_log::errors::open_or_create_error::OpenOrCreateError;
        let inner = ShardFsyncError::UnableToRotateToNewLogSegmentFile(
            OpenOrCreateError::OutOfSpace { log_id: 2, path: "log_2.wal".into(), preallocate_bytes: 1 << 27 },
        );
        assert!(inner.is_disk_full());
        for err in [S3CatchupError::FsyncFailed(inner.clone()), S3CatchupError::TruncationFailed(inner)] {
            assert!(err.is_disk_full(), "{err:?}");
        }
        // Other rotation failures stay fatal: corrupt target ≠ transient.
        let other = ShardFsyncError::UnableToRotateToNewLogSegmentFile(
            OpenOrCreateError::RotationTargetUnsafe { log_id: 2, path: "log_2.wal".into(), source: "x".into() },
        );
        assert!(!other.is_disk_full());
        assert!(!S3CatchupError::FsyncFailed(other).is_disk_full());
    }
}
