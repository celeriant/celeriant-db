pub const BLOOM_BYTES: usize = 32;
pub const BLOOM_HASH_COUNT: u32 = 4;
pub const BLOOM_BITS: usize = BLOOM_BYTES * 8;
pub const METADATA_BATCH_SIZE_BYTES: usize = 153;

pub static BINCODE_CONFIG_FIXED: bincode::config::Configuration<bincode::config::LittleEndian, bincode::config::Fixint> = 
    bincode::config::standard()
        .with_fixed_int_encoding()  // Force fixed-length integers
        .with_little_endian();

pub static BINCODE_CONFIG_VARIABLE: bincode::config::Configuration<bincode::config::LittleEndian, bincode::config::Varint> = 
    bincode::config::standard()
        .with_variable_int_encoding()  // Force fixed-length integers
        .with_little_endian();