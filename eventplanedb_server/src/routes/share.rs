use axum::{extract::Query, http::StatusCode, Json};
use event_storage::{event_batch_item::EventBatchItem, event_item::EventItem};
use event_storage_threads::{queue_jobs::share_async};
use eventplanedb_access::{access_level::{self, AccessLevel}, job_error::JobError};
use serde::{Deserialize, Serialize};
use crate::{app_state::AppState, crypto::Crypto};

#[derive(Debug, Deserialize)]
pub struct ShareQuery {
    pi: String,
    public_key: String,
    nonce: String,
    sign: String,
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
) -> Result<Json<ShareResponse>, (StatusCode, String)> {

    let cb = match Crypto::validate_with_public_key(&params.public_key, &params.nonce, &params.sign) {
        Ok(cb) => cb, 
        Err(e) => return Err((StatusCode::UNAUTHORIZED, e.to_string())),
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
