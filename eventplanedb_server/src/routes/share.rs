use crate::{app_state::AppState, error_response::RouteError, json_formatter::CompactJson, routes::utils::record_span_fields};
use axum::{Json, extract::Path, http::HeaderMap};
use eventplanedb_access::access_level::AccessLevel;
use eventplanedb_crypto::Crypto;
use eventplanedb_storage::event_batch_item::EventBatchItem;
use eventplanedb_thread_worker::{queue_jobs::share_async};
use serde::{Deserialize, Serialize};
use tracing::{info, instrument};

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

#[instrument(
    name = "share",
    skip(state, headers, share_body), 
    fields(
        aggregate_id = %aggregate_id,
        client_id, 
        server_time,
        user_id, 
        org_id,
    )
)]
pub async fn share(
    Path(aggregate_id): Path<String>,
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: HeaderMap,
    Json(share_body): Json<ShareQuery>,
) -> Result<CompactJson<ShareResponse>, RouteError> {
    // Establish the request context from headers and aggregate ID
    let context = state.create_job_context(aggregate_id.clone(), &headers).await?;
    record_span_fields(&context);

    // Send the job context and additional parameters to the worker for processing
    info!(
        is_single_use = share_body.is_single_use,
        access_level = share_body.access_level,
        expires_on = share_body.expires_on,
        "Processing share request",
    );
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

    // Log completion and return the response to the client
    info!("Completed share operation");
    let response = ShareResponse { share_key, share_event };
    Ok(CompactJson(response))
}
