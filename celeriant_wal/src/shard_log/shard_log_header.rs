
use bincode::{Decode, Encode};

#[derive(Debug, Clone, Encode, Decode)]
pub struct ShardLogHeader {
    pub shard_log_version: u32,
    pub shard_log_checkpoint_start_pos: u64,
}