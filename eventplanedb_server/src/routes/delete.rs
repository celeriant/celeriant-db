use crate::{app_state::AppState, json_formatter::CompactJson};
use axum::{
    extract::Path,
    http::{HeaderMap, StatusCode},
};
use event_storage::event_batch_item::EventBatchItem;
use event_storage_threads::queue_jobs::delete_async;
use eventplanedb_access::job_error::JobError;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteResponse {
    si: u64,
    event_batches: Vec<EventBatchItem>,
    server_time: u64,
}

pub async fn delete(
    Path(pi): Path<String>,
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: HeaderMap,
) -> Result<CompactJson<DeleteResponse>, (StatusCode, String)> {
    let cb = match state.validate_auth_headers(&headers) {
        Ok(cb) => cb,
        Err(e) => return Err(e),
    };

    // Get the file path from the ID
    let file_path = state.get_file_path(&pi);
    let server_time = chrono::Utc::now().timestamp_millis() as u64;

    match delete_async(&state.workers, file_path, cb, server_time).await {
        Ok(write_result) => Ok(CompactJson(DeleteResponse {
            si: write_result.si,
            event_batches: write_result.events,
            server_time,
        })),
        Err(e) => {
            let (status, message) = match e {
                JobError::PermissionDenied(msg) => (StatusCode::FORBIDDEN, msg),
                JobError::NotFound(msg) => (StatusCode::NOT_FOUND, msg),
                JobError::Other(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
            };
            Err((status, format!("Failed to delete: {message}")))
        }
    }
}
