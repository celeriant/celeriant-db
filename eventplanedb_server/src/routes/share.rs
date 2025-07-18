use crate::{app_state::AppState, error_response::RouteError, json_formatter::CompactJson};
use axum::{
    Json,
    extract::Path,
    http::{HeaderMap},
};
use eventplanedb_crypto::Crypto;
use eventplanedb_storage::event_batch_item::EventBatchItem;
use eventplanedb_thread_worker::queue_jobs::share_async;
use eventplanedb_access::{access_level::AccessLevel};
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
) -> Result<CompactJson<ShareResponse>, RouteError> {
    let server_time = state.server_time();
    let (current_user_hash, current_user_claims) = state.validate_auth_headers(&headers).await?;
    let file_path = state.get_file_path(&id);
    let share_key = nanoid::nanoid!();
    let share_hash = Crypto::generate_short_client_identity(share_key.as_bytes());
    let access_level = AccessLevel::from(share_body.access_level);

    let share_event = share_async(
        &state.workers,
        file_path,
        current_user_hash,
        current_user_claims,
        server_time,
        share_hash,
        access_level,
        share_body.is_single_use,
        share_body.iv,
        share_body.description,
        share_body.expires_on).await?;

    Ok(CompactJson(ShareResponse { share_key, share_event }))
}
