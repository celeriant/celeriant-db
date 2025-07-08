use crate::{app_state::AppState, crypto::Crypto};
use axum::{
    Json,
    extract::Path,
    http::{HeaderMap, StatusCode},
};
use event_storage::event_batch_item::EventBatchItem;
use event_storage_threads::queue_jobs::share_async;
use eventplanedb_access::{access_level::AccessLevel, job_error::JobError};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct ShareQuery {
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
    Path(id): Path<String>,
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: HeaderMap,
    Json(share_body): Json<ShareQuery>,
) -> Result<Json<ShareResponse>, (StatusCode, String)> {
    let cb = match state.validate_auth_headers(&headers) {
        Ok(cb) => cb,
        Err(e) => return Err(e),
    };

    let file_path = state.get_file_path(&id);
    let share_key = nanoid::nanoid!();
    let share_hash = Crypto::generate_short_client_identity(share_key.as_bytes());
    let access_level = AccessLevel::from(share_body.access_level);

    match share_async(
        &state.workers,
        file_path,
        cb,
        share_hash,
        access_level,
        share_body.is_single_use,
        share_body.iv,
        share_body.description,
        share_body.expires_on,
    )
    .await
    {
        Ok(share_event) => Ok(Json(ShareResponse { share_key, share_event })),
        Err(e) => {
            let (status, message) = match e {
                JobError::PermissionDenied(msg) => (StatusCode::FORBIDDEN, msg),
                JobError::NotFound(msg) => (StatusCode::NOT_FOUND, msg),
                JobError::Other(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
            };
            Err((status, format!("Failed to share: {message}")))
        }
    }
}
