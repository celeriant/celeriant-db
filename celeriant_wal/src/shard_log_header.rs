
use bincode::{Decode, Encode};

use crate::constants::{FIXED_BLOCK_SIZE_BYTES};

pub const CURRENT_VERSION: u32 = 1;

/// The header is written at the start and end of the 1GB fixed size file
/// Writing both, protected by crc checks, allows recovery on torn writes
#[derive(Debug, Clone, Encode, Decode)]
pub struct ShardLogHeader {
    /// A metablock is 512 byte fixed size, written from the start of the file
    /// This position indicates the end of the last written metablock entry
    pub metablocks_position: u64,

    /// The position where new variable length payloads can be written to
    /// Note that event batches are written to end of the file
    /// so this position indicates the start of the most recently written batches
    pub datablocks_position: u64,
}

impl ShardLogHeader {
    pub fn new(file_len: u64) -> Self {
        Self {
            metablocks_position: FIXED_BLOCK_SIZE_BYTES as u64,
            datablocks_position: file_len.saturating_sub(FIXED_BLOCK_SIZE_BYTES as u64),
        }
    }

    pub fn available_space(&self) -> u64 {
        self.datablocks_position.saturating_sub(self.metablocks_position)
    }

    pub fn has_space_for(&self, metablock_size: u64, datablock_size: u64) -> bool {
        self.available_space() >= metablock_size.saturating_add(datablock_size)
    }

    pub fn append_event_batches(
        &mut self,
        metablock_size: u64,
        datablock_size: u64,
    ) {
        self.metablocks_position = self
            .metablocks_position
            .saturating_add(metablock_size);
        self.datablocks_position = self
            .datablocks_position
            .saturating_sub(datablock_size);
    }
}