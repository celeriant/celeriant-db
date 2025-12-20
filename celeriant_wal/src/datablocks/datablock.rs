use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;

use crate::datablocks::datablock_kind::DatablockKind;

/// Variable length payload block, stored at the end of the wal growing forward into
/// the middle of the file, eventually meeting with metablocks, then continuing to a new wal file
/// Each datablock relies on the linked metablock for version + crc at the front 
/// and for upgradability and protect against corruption/bitrot
#[derive(Debug, Clone, Encode, Decode, DeepSizeOf)]
pub struct Datablock {
    pub datablock_kind: DatablockKind,
}