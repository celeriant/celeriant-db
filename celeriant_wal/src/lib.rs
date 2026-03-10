pub mod serde;
pub mod constants;
pub mod compression_type;
pub mod aggregate_key;
pub mod datablocks;
pub mod metablocks;
pub mod shard_log_header;
pub mod aggregate_type_key;
pub mod aggregate_client_key;
pub mod buffer_read;
pub mod s3;
pub mod schema_key;
pub mod schema_type;

pub use schema_type::SchemaType;

/// Formats a u128 as a UUID string (8-4-4-4-12 hex).
pub fn format_uuid(val: u128) -> String {
    let b = val.to_be_bytes();
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15],
    )
}