use crate::{app_state::AppState, json_formatter::CompactJson};
use axum::{
    extract::{Path, Query},
    http::{HeaderMap, StatusCode},
};
use eventplanedb_storage::catchup_result::CatchupResult;
use eventplanedb_thread_worker::queue_jobs::read_async;
use eventplanedb_access::job_error::JobError;
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
) -> Result<CompactJson<CatchupResult>, (StatusCode, String)> {
    let cb = match state.validate_auth_headers(&headers) {
        Ok(cb) => cb,
        Err(e) => return Err(e),
    };

    let file_path = state.get_file_path(&id);
    let from_si = params.from_si.map_or(0, |f| f);
    let max_bytes = usize::MAX; //TODO: Implement proper max_bytes logic

    match read_async(
        &state.workers,
        file_path,
        cb,
        params.share_key,
        from_si,
        max_bytes,
        params.own_events.unwrap_or(false),
    )
    .await
    {
        Ok(result) => Ok(CompactJson(result)),
        Err(e) => {
            let (status, message) = match e {
                JobError::PermissionDenied(msg) => (StatusCode::FORBIDDEN, msg),
                JobError::NotFound(msg) => (StatusCode::NOT_FOUND, msg),
                JobError::Other(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
            };
            Err((status, format!("Failed to read: {message}")))
        }
    }
}
