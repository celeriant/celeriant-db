use crate::{app_state::AppState, error_response::RouteError, json_formatter::CompactJson};
use axum::{extract::Path, http::HeaderMap};
use eventplanedb_storage::event_batch_item::EventBatchItem;
use eventplanedb_thread_worker::queue_jobs::disable_share_async;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct DisableShareResponse {
    si: u64,
    event_batches: Vec<EventBatchItem>,
    server_time: u64,
}

pub async fn disable_share(
    Path((id, share_hash)): Path<(String, String)>,
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: HeaderMap,
) -> Result<CompactJson<DisableShareResponse>, RouteError> {
    let server_time = state.server_time();
    let current_user_hash = state.validate_auth_headers(&headers)?;
    let file_path = state.get_file_path(&id);

    let result = disable_share_async(&state.workers, file_path, current_user_hash, server_time, share_hash).await?;

    Ok(CompactJson(DisableShareResponse {
        si: result.si,
        event_batches: result.events,
        server_time,
    }))
}
