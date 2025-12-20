use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;

#[derive(Debug, Clone, Encode, Decode, DeepSizeOf)]
pub struct DatablockBlockRef {
    /// Datablock crc check
    pub crc32c: u32,
    /// Absolute position where the datablock variable payload is located in the shard log
    pub datablock_position: u64,
    /// Datablock version for deserialisation
    pub version: u32,
    /// Size of the uncompressed event batch data in bytes
    pub uncompressed_size: u64,
    /// Length of the compressed event batch data
    pub compressed_size: u64,
    /// Compression algorithm used
    pub compression_type: u8,
}