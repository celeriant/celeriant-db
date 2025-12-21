use celeriant_wire::wire_format_error::WireFormatError;

/// Storage/infrastructure errors—may be transient.
#[derive(Debug, Clone)]
pub enum ShardReadError {
    /// Disk I/O failure.
    Io(String),
    
    /// Serialization or deserialization failure.
    Serialization(WireFormatError),
}