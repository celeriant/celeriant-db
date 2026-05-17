use celeriant_wire::codec::codec_error::CodecError;

use crate::error::shard_fsync_error::ShardFsyncError;

#[derive(Debug, Clone)]
pub enum FollowerReplicationWriteError {
    ShardFSyncError(ShardFsyncError),
    FailedToSerialiseDatablocks(CodecError),
    BlockBecameInline,
    BatchWalSeqGap { index: usize, expected: u64, actual: u64 },
}

impl FollowerReplicationWriteError {
    pub fn error_code(&self) -> &'static str {
        match self {
            Self::ShardFSyncError(_) => "fsync_error",
            Self::FailedToSerialiseDatablocks(_) => "serialise_datablocks_failed",
            Self::BlockBecameInline => "block_became_inline",
            Self::BatchWalSeqGap { .. } => "batch_wal_seq_gap",
        }
    }
}