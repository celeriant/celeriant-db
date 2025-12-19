use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;

/// Track the schemas for each event type.
#[derive(Debug, Clone, Encode, Decode, DeepSizeOf)]
pub struct EventTypeSchema {
    /// Could be json, avro, protobuf, msgpack, etc.
    pub schema_type: u8,

    /// The actual schemas. Requires full transitive compatibility to avoid breaking existing clients
    /// Array index is the minor version of the schema. If None, the minor version is no longer supported
    pub minor_version_schemas: Vec<Option<String>>,
}