use crate::{app_state::AppState, error_response::RouteError, json_formatter::CompactJson};
use axum::{
    extract::{Path, Query},
    http::HeaderMap,
};
use eventplanedb_storage::catchup_result::CatchupResult;
use eventplanedb_thread_worker::{job_context::JobContext, queue_jobs::read_async};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct ReadQuery {
    from_server_id: Option<u64>,
    share_id: Option<u128>,
    own_events: Option<bool>,
}

pub async fn read_events(
    Path(aggregate_id): Path<String>,
    Query(params): Query<ReadQuery>,
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: HeaderMap,
) -> Result<CompactJson<CatchupResult>, RouteError> {
    let current_user_claims = state.get_claims(&headers).await?;

    let context = JobContext {
        file_path: state.get_file_path(&aggregate_id),
        current_client_id: state.get_client_id(&headers)?,
        current_user_id: current_user_claims.map(|claims| claims.sub),
        server_time: state.server_time(),
    };

    let response = read_async(
        &state.workers,
        context,
        params.share_id,
        params.from_server_id.map_or(0, |f| f),
        state.read_max_bytes,
        params.own_events.unwrap_or(false),
    )
    .await?;

    Ok(CompactJson(response))
}
