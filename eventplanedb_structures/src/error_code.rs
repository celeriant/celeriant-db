use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

// Unit-only enum for easy matching in all languages
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    // Read errors
    IoError,
    MaxBytesTooSmall,
    SerializationError,
    UnavailableBatchIndex,
    CorruptEventBatch,
    CorruptMetadata,
    
    // Write errors
    OptimisticConcurrencyViolation,
    ClientIdempotencyViolation,
    EmptyEventsList,
    NoEventsToAppend,
    ZeroEventType,
    WriteError,
    CacheMiss,
    PrependCreatesEventBatchIndexGap,
    PrependNonContiguousBatches,
    
    // Protocol/Transport errors
    MessageTooLarge,
    UnsupportedProtocolVersion,
    InvalidWireFormat,
    
    // General errors
    NotFound,
    AlreadyExists,
    PermissionDenied,
    InvalidArgument,
    ResourceExhausted,
    Internal,
    InvalidRequest,
}
