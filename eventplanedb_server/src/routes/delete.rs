use crate::{app_state::AppState, error_response::RouteError, json_formatter::CompactJson};
use axum::{extract::Path, http::HeaderMap};
use eventplanedb_thread_worker::{job_context::JobContext, queue_jobs::delete_async};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteResponse {
    server_time: u64,
}

pub async fn delete(
    Path(aggregate_id): Path<String>,
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: HeaderMap,
) -> Result<CompactJson<DeleteResponse>, RouteError> {
    let current_user_claims = state.get_claims(&headers).await?;

    let server_time = state.server_time();

    let context = JobContext {
        file_path: state.get_file_path(&aggregate_id),
        current_client_id: state.get_client_id(&headers)?,
        current_user_id: current_user_claims.map(|claims| claims.sub),
        server_time,
    };

    delete_async(&state.workers, context).await?;

    let response = DeleteResponse { server_time };

    Ok(CompactJson(response))
}
