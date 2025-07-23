use crate::{app_state::AppState, error_response::RouteError, json_formatter::CompactJson};
use axum::{
    Json,
    extract::{Path, Query},
    http::HeaderMap,
};
use eventplanedb_access::job_error::JobError;
use eventplanedb_storage::{event_batch_item::EventBatchItem, event_item::EventItem};
use eventplanedb_thread_worker::{job_context::JobContext, queue_jobs::write_async};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct WriteQuery {
    create_if_not_exist: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WriteResponse {
    server_id: u64,
    event_batches: Vec<EventBatchItem>,
    server_time: u64,
}

pub async fn write_events(
    Path(aggregate_id): Path<String>,
    Query(params): Query<WriteQuery>,
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: HeaderMap,
    Json(events): Json<Vec<EventItem>>,
) -> Result<CompactJson<WriteResponse>, RouteError> {
    if events.is_empty() {
        return Err(RouteError::JobError(JobError::InvalidParameters("No events provided".to_string())));
    }

    let current_user_claims = state.get_claims(&headers).await?;

    let server_time = state.server_time();

    let context = JobContext {
        file_path: state.get_file_path(&aggregate_id),
        current_client_id: state.get_client_id(&headers)?,
        current_user_id: current_user_claims.map(|claims| claims.sub),
        server_time,
    };

    let result = write_async(&state.workers, context, params.create_if_not_exist.unwrap_or(false), events).await?;

    let response = WriteResponse {
        server_id: result.server_id,
        event_batches: result.events,
        server_time,
    };

    Ok(CompactJson(response))
}
