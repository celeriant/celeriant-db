use celeriant_wire::codec::codec_error::CodecError;

use crate::error::shard_fsync_error::ShardFsyncError;

#[derive(Debug, Clone)]
pub enum FollowerReplicationWriteError {
    ShardFSyncError(ShardFsyncError),
    FailedToSerialiseDatablocks(CodecError),
}