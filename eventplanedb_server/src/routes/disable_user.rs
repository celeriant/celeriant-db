use crate::{app_state::AppState, error_response::RouteError, json_formatter::CompactJson};
use axum::{extract::Path, http::HeaderMap};
use eventplanedb_storage::event_batch_item::EventBatchItem;
use eventplanedb_thread_worker::{job_context::JobContext, queue_jobs::disable_user_async};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct DisableUserResponse {
    server_id: u64,
    event_batches: Vec<EventBatchItem>,
    server_time: u64,
}

pub async fn disable_user(
    Path((aggregate_id, user_id)): Path<(String, String)>,
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: HeaderMap,
) -> Result<CompactJson<DisableUserResponse>, RouteError> {
    let current_user_claims = state.get_claims(&headers).await?;

    let server_time = state.server_time();

    let (current_user_id, current_org_id) = current_user_claims.map(|claims| (Some(claims.sub), claims.org_id)).unwrap_or((None, None));

    let context = JobContext {
        file_path: state.get_file_path(&aggregate_id),
        current_client_id: state.get_client_id(&headers)?,
        current_user_id,
        current_org_id,
        server_time,
    };

    let result = disable_user_async(&state.workers, context, None, Some(user_id)).await?;

    let response = DisableUserResponse {
        server_id: result.server_id,
        event_batches: result.events,
        server_time,
    };

    Ok(CompactJson(response))
}
