use crate::{app_state::AppState, error_response::RouteError, json_formatter::CompactJson};
use axum::{
    extract::Path,
    http::{HeaderMap},
};
use eventplanedb_thread_worker::queue_jobs::delete_async;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteResponse {
    server_time: u64,
}

pub async fn delete(
    Path(id): Path<String>,
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: HeaderMap,
) -> Result<CompactJson<DeleteResponse>, RouteError> {
    let server_time = state.server_time();
    let current_user_hash = state.validate_auth_headers(&headers).await?;
    let file_path = state.get_file_path(&id);

    delete_async(&state.workers, file_path, current_user_hash, server_time).await?;

    Ok(CompactJson(DeleteResponse {
        server_time,
    }))
}
