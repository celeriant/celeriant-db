use eventplanedb_structures::{error_code::ErrorCode, eventplanedb_error::EventPlaneDBError};

use crate::{read_operations::read_error::ReadError, write_operations::write_error::WriteError};

impl From<ReadError> for EventPlaneDBError {
    fn from(e: ReadError) -> Self {
        match e {
            ReadError::IoError(_io_err) => EventPlaneDBError::io_error(),
            ReadError::MaxBytesTooSmall { current_max_bytes, required_max_bytes } => {
                EventPlaneDBError::max_bytes_too_small(current_max_bytes, required_max_bytes)
            }
            ReadError::SerializationError(_wire_format_error) => EventPlaneDBError::serialization_error(),
            ReadError::UnavailableBatchIndex { minimum_available_event_batch_index, requested_event_batch_index } => {
                EventPlaneDBError::unavailable_batch_index(minimum_available_event_batch_index, requested_event_batch_index)
            }
            ReadError::CorruptEventBatch { expected_crc, actual_crc, event_batch_index, .. } => {
                EventPlaneDBError::corrupt_event_batch(expected_crc, actual_crc, event_batch_index)
            }
            ReadError::CannotCreateFolders { path: _path, error: _error } => EventPlaneDBError::io_error(),
        }
    }
}

impl From<WriteError> for EventPlaneDBError {
    fn from(e: WriteError) -> Self {        
        match e {
            WriteError::IoError(_io_err) => EventPlaneDBError::io_error(),
            WriteError::OptimisticConcurrencyViolation { client_id, expected_event_batch_index, current_event_batch_index } => {
                EventPlaneDBError::optimistic_concurrency_violation(client_id, expected_event_batch_index, current_event_batch_index)
            }
            WriteError::ClientIdempotencyViolation { client_id, last_client_event_index, attempted_client_event_index } => {
                EventPlaneDBError::client_idempotency_violation(client_id, last_client_event_index, attempted_client_event_index)
            }
            WriteError::EmptyEventsList => EventPlaneDBError {
                code: ErrorCode::EmptyEventsList,
                client_id: None,
                ..Default::default()
            },
            WriteError::NoEventsToAppend { client_id, existing_event_index } => EventPlaneDBError {
                code: ErrorCode::NoEventsToAppend,
                client_id: Some(client_id),
                last_client_event_index: Some(existing_event_index),
                ..Default::default()
            },
            WriteError::SerializationError(_wire_format_error) => EventPlaneDBError::serialization_error(),
            WriteError::CacheMiss { missing_from_event_batch_index, missing_to_event_batch_index } => {
                EventPlaneDBError::cache_miss(missing_from_event_batch_index, missing_to_event_batch_index)
            }
            WriteError::PrependCreatesEventBatchIndexGap { provided_last_batch_index, current_first_event_batch_index } => {
                EventPlaneDBError::prepend_creates_gap(provided_last_batch_index, current_first_event_batch_index)
            }
            WriteError::PrependNonContiguousBatches { from_event_batch_index, to_event_batch_index } => {
                EventPlaneDBError::prepend_non_contiguous(from_event_batch_index, to_event_batch_index)
            }
            WriteError::FileRenameFailure { from: _from, to: _to, error: _error } => EventPlaneDBError::io_error(),
        }
    }
}