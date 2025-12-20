use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;

use crate::metablocks::{datablock_block_ref::DatablockBlockRef, datablock_inline_data::DatablockInlineData};

/// Datablocks which are small enough get stored inline within the metablock
/// Otherwise they are stored as a reference to a separate datablock payload
#[derive(Debug, Clone, Encode, Decode, DeepSizeOf)]
pub enum DatablockStorageKind {
    None,
    Inline(DatablockInlineData),
    Block(DatablockBlockRef),
}