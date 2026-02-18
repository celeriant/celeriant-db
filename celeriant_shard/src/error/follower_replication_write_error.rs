use celeriant_wire::codec::codec_error::CodecError;

use crate::error::shard_fsync_error::ShardFsyncError;

#[derive(Debug, Clone)]
pub enum FollowerReplicationWriteError {
    ShardFSyncError(ShardFsyncError),
    FailedToSerialiseDatablocks(CodecError),
    BlockBecameInline,
    BatchWalIndexGap { index: usize, expected: u64, actual: u64 },
}