use crate::{app_state::AppState, error_response::RouteError, json_formatter::CompactJson};
use axum::{
    extract::{Path, Query},
    http::{HeaderMap},
};
use eventplanedb_storage::catchup_result::CatchupResult;
use eventplanedb_thread_worker::queue_jobs::read_async;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct ReadQuery {
    from_si: Option<u64>,
    share_key: Option<String>,
    own_events: Option<bool>,
}

pub async fn read_events(
    Path(id): Path<String>,
    Query(params): Query<ReadQuery>,
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: HeaderMap,
) -> Result<CompactJson<CatchupResult>, RouteError> {
    let server_time = state.server_time();
    let current_user_hash = state.validate_auth_headers(&headers)?;
    let file_path = state.get_file_path(&id);
    let from_si = params.from_si.map_or(0, |f| f);

    let result = read_async(
        &state.workers,
        file_path,
        current_user_hash,
        server_time,
        params.share_key,
        from_si,
        state.read_max_bytes,
        params.own_events.unwrap_or(false)).await?;

    Ok(CompactJson(result))
}
