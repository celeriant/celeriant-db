pub const MAGIC_NUMBER: u32 = 0xDEADBEEF;

pub const BLOOM_BYTES: usize = 32;
pub const BLOOM_HASH_COUNT: u32 = 4;
pub const BLOOM_BITS: usize = BLOOM_BYTES * 8;

pub const HEAD_BATCH_START_SIZE: usize = size_of::<u64>();
pub const TAIL_BATCH_METADATA_SIZE: usize = UNCOMPRESSED_BATCH_SIZE_OFFSET;

pub const UNCOMPRESSED_BATCH_SIZE_OFFSET: usize = BLOOM_OFFSET + size_of::<u64>();
pub const BLOOM_OFFSET: usize = USE_BLOOM_OFFSET + BLOOM_BYTES;
pub const USE_BLOOM_OFFSET: usize = LOCAL_INDEX_OFFSET + size_of::<u8>();
pub const LOCAL_INDEX_OFFSET: usize = SERVER_ID_OFFSET + size_of::<u64>();
pub const SERVER_ID_OFFSET: usize = CLIENT_ID_OFFSET + size_of::<u64>();
pub const CLIENT_ID_OFFSET: usize = USER_ID_OFFSET + size_of::<u128>();
pub const USER_ID_OFFSET: usize = SERVER_TIME_OFFSET + size_of::<u128>();
pub const SERVER_TIME_OFFSET: usize = TAIL_COMPRESSED_BATCH_SIZE_OFFSET + size_of::<u64>();
pub const TAIL_COMPRESSED_BATCH_SIZE_OFFSET: usize = COMPRESSION_TYPE_OFFSET + size_of::<u64>();
pub const COMPRESSION_TYPE_OFFSET: usize = CHECKSUM_OFFSET + size_of::<u8>();
pub const CHECKSUM_OFFSET: usize = MAGIC_NUMBER_OFFSET + size_of::<u32>();
pub const MAGIC_NUMBER_OFFSET: usize = size_of::<u32>();
