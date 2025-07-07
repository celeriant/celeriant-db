use axum::{extract::Query, http::{HeaderMap, StatusCode}, Json};
use event_storage::{event_batch_item::EventBatchItem};
use event_storage_threads::{queue_jobs::share_async};
use eventplanedb_access::{access_level::{AccessLevel}, job_error::JobError};
use serde::{Deserialize, Serialize};
use crate::{app_state::AppState, crypto::Crypto};

#[derive(Debug, Deserialize)]
pub struct ShareQuery {
    pi: String,
    access_level: u64,
    is_single_use: bool,
    iv: Option<Vec<u8>>,
    description: Option<String>,
    expires_on: u64,
}

#[derive(Debug, Serialize)]
pub struct ShareResponse {
    share_key: String,
    share_event: EventBatchItem,
}

pub async fn share(
    Query(params): Query<ShareQuery>,
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: HeaderMap,
) -> Result<Json<ShareResponse>, (StatusCode, String)> {

    let cb = match state.validate_auth_headers(&headers) {
        Ok(cb) => cb,
        Err(e) => return Err(e),
    };

    let file_path = state.get_file_path(&params.pi);
    let share_key = nanoid::nanoid!();
    let share_hash = Crypto::generate_short_client_identity(share_key.as_bytes());
    let access_level = AccessLevel::from(params.access_level);

    match share_async(&state.workers, file_path, cb, share_hash, access_level, params.is_single_use, params.iv, params.description, params.expires_on).await {
        Ok(share_event) => {
            Ok(Json(ShareResponse {
                share_key,
                share_event,
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
