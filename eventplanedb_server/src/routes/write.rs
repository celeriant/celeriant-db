use axum::{extract::Query, http::StatusCode, Json};
use event_storage::{event_batch_item::EventBatchItem, event_item::EventItem};
use event_storage_threads::{job_error::JobError, queue_jobs::write_async};
use serde::{Deserialize, Serialize};
use crate::{app_state::AppState, crypto::Crypto};

#[derive(Debug, Deserialize)]
pub struct WriteQuery {
    pi: String,
    public_key: String,
    nonce: String,
    sign: String,
    create_if_not_exist: bool,
    share_key: Option<String>
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WriteResponse {
    #[serde(rename = "si")]
    server_id: u64,
    #[serde(rename = "sd")]
    server_time: u64,
}

pub async fn write_events(
    Query(params): Query<WriteQuery>,
    axum::extract::State(state): axum::extract::State<AppState>,
    Json(events): Json<Vec<EventItem>>,
) -> Result<Json<WriteResponse>, (StatusCode, String)> {
    if events.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "No events provided".to_string()));
    }

    let cb = match Crypto::validate_with_public_key(&params.public_key, &params.nonce, &params.sign) {
        Ok(cb) => cb, 
        Err(e) => return Err((StatusCode::UNAUTHORIZED, e.to_string())),
    };

    let file_path = state.get_file_path(&params.pi);
    let server_time = chrono::Utc::now().timestamp_millis() as u64;
    
    // Create an EventBatchItem from the events
    let event_batch = EventBatchItem {
        si: 0, // Will be assigned by the storage system
        cb: Some(cb),
        sd: server_time,
        events: events,
    };

    match write_async(&state.workers, file_path, params.create_if_not_exist, params.share_key, event_batch).await {
        Ok(server_id) => {
            Ok(Json(WriteResponse {
                server_id,
                server_time,
            }))
        }
        Err(e) => {
            let (status, message) = match e {
                JobError::PermissionDenied(msg) => (StatusCode::FORBIDDEN, msg),
                JobError::NotFound(msg) => (StatusCode::NOT_FOUND, msg),
                JobError::Other(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
            };
            Err((status, format!("Failed to write events: {}", message)))
        }
    }
}