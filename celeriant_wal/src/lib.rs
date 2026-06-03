pub mod builtin_dict;
pub mod serde;
pub mod constants;
pub mod compression_type;
pub mod aggregate_key;
pub mod sbbf;
pub mod datablocks;
pub mod metablocks;
pub mod shard_log_header;
pub mod aggregate_type_key;
pub mod aggregate_client_key;
pub mod buffer_read;
pub mod s3;
pub mod schema_key;
pub mod schema_type;
pub mod segment_summary;

pub use schema_type::SchemaType;
pub use builtin_dict::resolve_builtin_dict;

/// Formats a u128 as a UUID string (8-4-4-4-12 hex).
pub fn format_uuid(val: u128) -> String {
    uuid::Uuid::from_u128(val).to_string()
}