use crate::constants::MINIBATCH_SIZE_BYTES;
use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;

#[derive(Debug, Clone, Encode, Decode)]
pub struct DatablockInlineData {
    /// If the event batch fits within 256 bytes we can fit it directly
    /// within the 512 byte metablock payload. This avoids reading an
    /// additional block from disk.
    pub minibatch: [u8; MINIBATCH_SIZE_BYTES],
}

impl DeepSizeOf for DatablockInlineData {
    fn deep_size_of_children(&self, _context: &mut deepsize::Context) -> usize {
        // No heap allocations, just stack data
        0
    }
}
