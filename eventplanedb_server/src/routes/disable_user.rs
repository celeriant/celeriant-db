use crate::{app_state::AppState, error_response::RouteError, json_formatter::CompactJson};
use axum::{
    extract::Path,
    http::{HeaderMap},
};
use eventplanedb_storage::event_batch_item::EventBatchItem;
use eventplanedb_thread_worker::queue_jobs::disable_user_async;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct DisableUserResponse {
    si: u64,
    event_batches: Vec<EventBatchItem>,
    server_time: u64,
}

pub async fn disable_user(
    Path((id, user_hash)): Path<(String, String)>,
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: HeaderMap,
) -> Result<CompactJson<DisableUserResponse>, RouteError> {
    let server_time = state.server_time();
    let current_user_hash = state.validate_auth_headers(&headers).await?;
    let file_path = state.get_file_path(&id);

    let result = disable_user_async(&state.workers, file_path, current_user_hash, server_time, user_hash).await?;

    Ok(CompactJson(DisableUserResponse {
        si: result.si,
        event_batches: result.events,
        server_time,
    }))
}
