use axum::{extract::Query, http::StatusCode, Json};
use event_storage::catchup_result::CatchupResult;
use event_storage_threads::{job_error::JobError, queue_jobs::read_async};
use serde::Deserialize;
use crate::{app_state::AppState, crypto::Crypto};

#[derive(Debug, Deserialize)]
pub struct ReadQuery {
    pi: String,
    public_key: String,
    nonce: String,
    sign: String,
    from_si: Option<u64>,
    share_key: Option<String>
}

pub async fn read_events(
    Query(params): Query<ReadQuery>,
    axum::extract::State(state): axum::extract::State<AppState>,
) -> Result<Json<CatchupResult>, (StatusCode, String)> {

    let cb = match Crypto::validate_with_public_key(&params.public_key, &params.nonce, &params.sign) {
        Ok(cb) => cb, 
        Err(e) => return Err((StatusCode::UNAUTHORIZED, e.to_string())),
    };
    
    let file_path = state.get_file_path(&params.pi);
    let from_si = params.from_si.map_or(0, |f| f);
    let max_bytes = usize::MAX; //TODO: Implement proper max_bytes logic
    
    match read_async(&state.workers, file_path, cb, params.share_key, from_si, max_bytes).await {
        Ok(result) => {
            Ok(Json(result))
        }
        Err(e) => {
            let (status, message) = match e {
                JobError::PermissionDenied(msg) => (StatusCode::FORBIDDEN, msg),
                JobError::NotFound(msg) => (StatusCode::NOT_FOUND, msg),
                JobError::Other(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
            };
            Err((status, format!("Failed to write events: {}", message)))
        }
    }
}
