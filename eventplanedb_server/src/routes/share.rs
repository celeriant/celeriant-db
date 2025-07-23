use crate::{app_state::AppState, error_response::RouteError, json_formatter::CompactJson};
use axum::{Json, extract::Path, http::HeaderMap};
use eventplanedb_access::access_level::AccessLevel;
use eventplanedb_crypto::Crypto;
use eventplanedb_storage::event_batch_item::EventBatchItem;
use eventplanedb_thread_worker::{job_context::JobContext, queue_jobs::share_async};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct ShareQuery {
    access_level: u64,
    is_single_use: bool,
    iv: Option<[u8; 12]>,
    description: Option<String>,
    expires_on: u64,
}

#[derive(Debug, Serialize)]
pub struct ShareResponse {
    share_key: String,
    share_event: EventBatchItem,
}

pub async fn share(
    Path(aggregate_id): Path<String>,
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: HeaderMap,
    Json(share_body): Json<ShareQuery>,
) -> Result<CompactJson<ShareResponse>, RouteError> {
    let current_user_claims = state.get_claims(&headers).await?;

    let context = JobContext {
        file_path: state.get_file_path(&aggregate_id),
        current_client_id: state.get_client_id(&headers)?,
        current_user_id: current_user_claims.map(|claims| claims.sub),
        server_time: state.server_time(),
    };

    let share_key = nanoid::nanoid!();

    let share_event = share_async(
        &state.workers,
        context,
        Crypto::generate_short_client_identity(share_key.as_bytes()),
        AccessLevel::from(share_body.access_level),
        share_body.is_single_use,
        share_body.iv,
        share_body.description,
        share_body.expires_on,
    )
    .await?;

    let response = ShareResponse { share_key, share_event };

    Ok(CompactJson(response))
}
