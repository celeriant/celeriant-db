use crate::{app_state::AppState, error_response::RouteError, json_formatter::CompactJson};
use axum::{
    Json,
    extract::{Path, Query},
    http::{HeaderMap},
};
use eventplanedb_storage::{event_batch_item::EventBatchItem, event_item::EventItem};
use eventplanedb_thread_worker::queue_jobs::write_async;
use eventplanedb_access::job_error::JobError;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct WriteQuery {
    create_if_not_exist: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WriteResponse {
    si: u64,
    event_batches: Vec<EventBatchItem>,
    server_time: u64,
}

pub async fn write_events(
    Path(id): Path<String>,
    Query(params): Query<WriteQuery>,
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: HeaderMap,
    Json(events): Json<Vec<EventItem>>,
) -> Result<CompactJson<WriteResponse>, RouteError> {
    if events.is_empty() {
        return Err(RouteError::JobError(JobError::InvalidParameters("No events provided".to_string())));
    }

    let server_time = state.server_time();
    let current_user_hash = state.validate_auth_headers(&headers)?;
    let file_path = state.get_file_path(&id);
    let allow_create = params.create_if_not_exist.unwrap_or(false);

    let result = write_async(&state.workers, file_path, current_user_hash, server_time, allow_create, events).await?;

    Ok(CompactJson(WriteResponse {
        si: result.si,
        event_batches: result.events,
        server_time,
    }))
}
