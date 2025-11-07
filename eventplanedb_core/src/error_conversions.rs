use eventplanedb_structures::eventplanedb_error::EventPlaneDBError;

impl From<crate::files::read_operations::ReadError> for EventPlaneDBError {
    fn from(e: crate::files::read_operations::ReadError) -> Self {
        use crate::files::read_operations::ReadError;
        match e {
            ReadError::IoError(io_err) => EventPlaneDBError::io_error(),
            ReadError::MaxBytesTooSmall { current_max_bytes, required_max_bytes } => {
                EventPlaneDBError::max_bytes_too_small(current_max_bytes, required_max_bytes)
            }
            ReadError::SerializationError { message } => EventPlaneDBError::serialization_error(),
            ReadError::UnavailableBatchIndex { minimum_available_event_batch_index, requested_event_batch_index } => {
                EventPlaneDBError::unavailable_batch_index(minimum_available_event_batch_index, requested_event_batch_index)
            }
            ReadError::CorruptEventBatch { expected_crc, actual_crc, event_batch_index, .. } => {
                EventPlaneDBError::corrupt_event_batch(expected_crc, actual_crc, event_batch_index)
            }
        }
    }
}

impl From<crate::files::write_operations::AppendError> for EventPlaneDBError {
    fn from(e: crate::files::write_operations::AppendError) -> Self {
        use crate::files::write_operations::AppendError;
        use eventplanedb_structures::error_code::ErrorCode;
        
        match e {
            AppendError::IoError(_io_err) => EventPlaneDBError::io_error(),
            AppendError::OptimisticConcurrencyViolation { client_id, expected_event_batch_index, current_event_batch_index } => {
                EventPlaneDBError::optimistic_concurrency_violation(client_id, expected_event_batch_index, current_event_batch_index)
            }
            AppendError::ClientIdempotencyViolation { client_id, last_client_event_index, attempted_client_event_index } => {
                EventPlaneDBError::client_idempotency_violation(client_id, last_client_event_index, attempted_client_event_index)
            }
            AppendError::EmptyEventsList { client_id } => EventPlaneDBError {
                code: ErrorCode::EmptyEventsList,
                client_id: Some(client_id),
                ..Default::default()
            },
            AppendError::NoEventsToAppend { client_id, existing_event_index } => EventPlaneDBError {
                code: ErrorCode::NoEventsToAppend,
                client_id: Some(client_id),
                last_client_event_index: Some(existing_event_index),
                ..Default::default()
            },
            AppendError::SerializationError { message } => EventPlaneDBError::serialization_error(),
            AppendError::WriteError { message } => EventPlaneDBError::write_error(),
        }
    }
}