use crate::constants::MINIBATCH_SIZE_BYTES;
use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;
use serde::{Deserialize, Serialize};
use crate::serde::serde_minibatch_bytes;

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct DatablockInlineData {
    /// If the event batch fits within MINIBATCH_SIZE_BYTES we store it
    /// inline in the metablock, avoiding an additional disk read.
    #[serde(with = "serde_minibatch_bytes")]
    pub minibatch: [u8; MINIBATCH_SIZE_BYTES],
}

impl DeepSizeOf for DatablockInlineData {
    fn deep_size_of_children(&self, _context: &mut deepsize::Context) -> usize {
        // No heap allocations, just stack data
        0
    }
}
