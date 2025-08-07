use crate::{app_state::AppState, error_response::RouteError, json_formatter::CompactJson, routes::utils::record_span_fields};
use axum::{extract::Path, http::HeaderMap};
use eventplanedb_storage::event_batch_item::EventBatchItem;
use eventplanedb_thread_worker::{queue_jobs::disable_user_async};
use serde::{Deserialize, Serialize};
use tracing::{info, instrument};

#[derive(Debug, Serialize, Deserialize)]
pub struct DisableUserResponse {
    server_id: u64,
    event_batches: Vec<EventBatchItem>,
    server_time: u64,
}

#[instrument(
    name = "disable_user",
    skip(state, headers), 
    fields(
        aggregate_id = %aggregate_id,
        client_id, 
        server_time,
        user_id, 
        org_id,
    )
)]
pub async fn disable_user(
    Path((aggregate_id, for_user_id)): Path<(String, String)>,
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: HeaderMap,
) -> Result<CompactJson<DisableUserResponse>, RouteError> {
    // Establish the request context from headers and aggregate ID
    let context = state.create_job_context(aggregate_id.clone(), &headers).await?;
    record_span_fields(&context);

    // Send the job context and additional parameters to the worker for processing
    info!(for_user_id = &for_user_id, "Processing disable_user request");
    let server_time = context.server_time;
    let result = disable_user_async(&state.workers, context, None, Some(for_user_id.clone())).await?;

    // Log completion and return the response to the client
    info!(
        return_event_batch_count = result.events.len(),
        server_id = result.server_id,
        for_user_id = &for_user_id,
        "Completed disable_user operation"
    );
    let response = DisableUserResponse {
        server_id: result.server_id,
        event_batches: result.events,
        server_time,
    };
    Ok(CompactJson(response))
}
