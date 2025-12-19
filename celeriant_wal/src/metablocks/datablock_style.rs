use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;
use crate::constants::MINIBATCH_SIZE_BYTES;

#[derive(Debug, Clone, Encode, Decode)]
pub enum DatablockStyle {
    Inline {
        /// If the event batch fits within 256 bytes we can fit it directly
        /// within the 512 byte metablock payload. This avoids reading an
        /// additional block from disk.
        minibatch: [u8; MINIBATCH_SIZE_BYTES],
    },
    Block {
        /// Datablock crc check, 0 if no datablock
        crc32c: u32,
        /// Where the datablock variable payload is located in the shard log, 0 if no datablock
        datablock_position: u64,
    }
}

impl DeepSizeOf for DatablockStyle {
    fn deep_size_of_children(&self, _context: &mut deepsize::Context) -> usize {
        // No heap allocations, just stack data
        0
    }
}