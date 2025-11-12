pub const BLOOM_BYTES: usize = 32;
pub const BLOOM_HASH_COUNT: u32 = 4;
pub const BLOOM_HASH_SEED: u128 = 123456789012345678901234567890123456u128;
pub const BLOOM_BITS: usize = BLOOM_BYTES * 8;
pub const METADATA_BATCH_SIZE_BYTES: usize = 256;
pub const PROTOCOL_VERSION_V2: u32 = 2;
pub const WIRE_HEADER_SIZE: usize = 17;
pub const WIRE_FIXED_BODY_SIZE: usize = 1024;

pub static BINCODE_CONFIG_FIXED: bincode::config::Configuration<
    bincode::config::LittleEndian,
    bincode::config::Fixint,
> = bincode::config::standard()
    .with_fixed_int_encoding() // Force fixed-length integers
    .with_little_endian();

pub static BINCODE_CONFIG_VARIABLE: bincode::config::Configuration<
    bincode::config::LittleEndian,
    bincode::config::Varint,
> = bincode::config::standard()
    .with_variable_int_encoding() // Force fixed-length integers
    .with_little_endian();
