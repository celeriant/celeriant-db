use crate::error_code::ErrorCode;
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};


// Structured error with optional typed fields
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct EventPlaneDBError {
    pub code: ErrorCode,
    pub message: String,
    
    // Optional typed fields for structured error data
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<u128>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_event_batch_index: Option<u64>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_event_batch_index: Option<u64>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_available_event_batch_index: Option<u64>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requested_event_batch_index: Option<u64>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_client_event_index: Option<u64>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempted_client_event_index: Option<u64>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_max_bytes: Option<u64>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    pub required_max_bytes: Option<u64>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_crc: Option<u32>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actual_crc: Option<u32>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_batch_index: Option<u64>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_size: Option<u64>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_message_size: Option<u64>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol_version: Option<u32>,
}

impl EventPlaneDBError {

    pub fn invalid_request() -> Self {
        Self {
            code: ErrorCode::InvalidRequest,
            message: "Failed to deserialize request".to_string(),
            ..Default::default()
        }
    }

    // Helper constructors for each error type
    pub fn optimistic_concurrency_violation(
        client_id: u128,
        expected: u64,
        current: u64,
    ) -> Self {
        Self {
            code: ErrorCode::OptimisticConcurrencyViolation,
            message: format!(
                "Expected event batch index {} but current is {}",
                expected, current
            ),
            client_id: Some(client_id),
            expected_event_batch_index: Some(expected),
            current_event_batch_index: Some(current),
            ..Default::default()
        }
    }
    
    pub fn client_idempotency_violation(
        client_id: u128,
        last: u64,
        attempted: u64,
    ) -> Self {
        Self {
            code: ErrorCode::ClientIdempotencyViolation,
            message: format!(
                "Client event index {} already processed (last: {})",
                attempted, last
            ),
            client_id: Some(client_id),
            last_client_event_index: Some(last),
            attempted_client_event_index: Some(attempted),
            ..Default::default()
        }
    }
    
    pub fn unavailable_batch_index(min_available: u64, requested: u64) -> Self {
        Self {
            code: ErrorCode::UnavailableBatchIndex,
            message: format!(
                "Requested batch {} is not available (min: {})",
                requested, min_available
            ),
            min_available_event_batch_index: Some(min_available),
            requested_event_batch_index: Some(requested),
            ..Default::default()
        }
    }
    
    pub fn max_bytes_too_small(current: u64, required: u64) -> Self {
        Self {
            code: ErrorCode::MaxBytesTooSmall,
            message: format!(
                "max_bytes {} too small, need at least {}",
                current, required
            ),
            current_max_bytes: Some(current),
            required_max_bytes: Some(required),
            ..Default::default()
        }
    }
    
    pub fn corrupt_event_batch(
        expected_crc: u32,
        actual_crc: u32,
        batch_index: u64,
    ) -> Self {
        Self {
            code: ErrorCode::CorruptEventBatch,
            message: format!(
                "Corrupt batch {} - CRC mismatch (expected: {}, actual: {})",
                batch_index, expected_crc, actual_crc
            ),
            expected_crc: Some(expected_crc),
            actual_crc: Some(actual_crc),
            event_batch_index: Some(batch_index),
            ..Default::default()
        }
    }
    
    pub fn io_error(e: impl std::fmt::Display) -> Self {
        Self {
            code: ErrorCode::IoError,
            message: format!("IO error: {}", e),
            ..Default::default()
        }
    }
    
    pub fn serialization_error(e: impl std::fmt::Display) -> Self {
        Self {
            code: ErrorCode::SerializationError,
            message: format!("Serialization error: {}", e),
            ..Default::default()
        }
    }
    
    pub fn write_error(msg: impl Into<String>) -> Self {
        Self {
            code: ErrorCode::WriteError,
            message: msg.into(),
            ..Default::default()
        }
    }
    
    pub fn message_too_large(size: u64, max_size: u64) -> Self {
        Self {
            code: ErrorCode::MessageTooLarge,
            message: format!(
                "Message size {} bytes exceeds maximum of {} bytes",
                size, max_size
            ),
            message_size: Some(size),
            max_message_size: Some(max_size),
            ..Default::default()
        }
    }
    
    pub fn unsupported_protocol_version(version: u32) -> Self {
        Self {
            code: ErrorCode::UnsupportedProtocolVersion,
            message: format!("Unsupported protocol version: {}", version),
            protocol_version: Some(version),
            ..Default::default()
        }
    }
    
    pub fn invalid_wire_format(msg: impl Into<String>) -> Self {
        Self {
            code: ErrorCode::InvalidWireFormat,
            message: msg.into(),
            ..Default::default()
        }
    }
    
    pub fn not_found(msg: impl Into<String>) -> Self {
        Self {
            code: ErrorCode::NotFound,
            message: msg.into(),
            ..Default::default()
        }
    }
    
    pub fn already_exists(msg: impl Into<String>) -> Self {
        Self {
            code: ErrorCode::AlreadyExists,
            message: msg.into(),
            ..Default::default()
        }
    }
    
    pub fn permission_denied(msg: impl Into<String>) -> Self {
        Self {
            code: ErrorCode::PermissionDenied,
            message: msg.into(),
            ..Default::default()
        }
    }
    
    pub fn invalid_argument(msg: impl Into<String>) -> Self {
        Self {
            code: ErrorCode::InvalidArgument,
            message: msg.into(),
            ..Default::default()
        }
    }
    
    pub fn resource_exhausted(msg: impl Into<String>) -> Self {
        Self {
            code: ErrorCode::ResourceExhausted,
            message: msg.into(),
            ..Default::default()
        }
    }
    
    pub fn internal(msg: impl Into<String>) -> Self {
        Self {
            code: ErrorCode::Internal,
            message: msg.into(),
            ..Default::default()
        }
    }
}

impl Default for EventPlaneDBError {
    fn default() -> Self {
        Self {
            code: ErrorCode::Internal,
            message: String::new(),
            client_id: None,
            expected_event_batch_index: None,
            current_event_batch_index: None,
            min_available_event_batch_index: None,
            requested_event_batch_index: None,
            last_client_event_index: None,
            attempted_client_event_index: None,
            current_max_bytes: None,
            required_max_bytes: None,
            expected_crc: None,
            actual_crc: None,
            event_batch_index: None,
            message_size: None,
            max_message_size: None,
            protocol_version: None,
        }
    }
}