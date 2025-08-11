use std::sync::Arc;

use crate::{app_state::AppState, error_response::RouteError, internal_aggregates::client_aggregate_name, json_formatter::CompactJson, routes::utils::record_span_fields};
use axum::{
    extract::{Path, Query},
    http::{response, HeaderMap},
};
use eventplanedb_crypto::Crypto;
use eventplanedb_storage::{catchup_result::CatchupResult, event_batch_item::EventBatchItem};
use eventplanedb_thread_worker::{queue_jobs::read_async};
use serde::{Deserialize, Serialize};
use tracing::{debug, instrument};

#[derive(Debug, Deserialize)]
pub struct ReadQuery {
    from_server_id: Option<u64>,
    share_id: Option<String>,
    own_events: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct ReadResponse {
    event_batches: Vec<Arc<EventBatchItem>>,
    next_server_id: Option<u64>,
}

#[instrument(
    name = "read_events",
    skip(state, headers, params), 
    fields(
        aggregate_id = %aggregate_id,
        client_id, 
        server_time,
        user_id, 
        org_id,
    )
)]
pub async fn read_events(
    Path(aggregate_id): Path<String>,
    Query(params): Query<ReadQuery>,
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: HeaderMap,
) -> Result<CompactJson<ReadResponse>, RouteError> {
    // Establish the request context from headers and aggregate ID
    let mut context = state.create_job_context(aggregate_id.clone(), &headers).await?;
    record_span_fields(&context);

    // Send the job context and additional parameters to the worker for processing
    let include_own_events = params.own_events.unwrap_or(false);
    let from_server_id = params.from_server_id.map_or(0, |f| f);
    debug!(
        include_own_events = include_own_events,
        from_server_id = from_server_id,
        has_share_id = params.share_id.is_some(),
        "Processing read_events request"
    );
    let share_id = match params.share_id {
        Some(s) => Some(Crypto::decode_base64_u128_from_path(s.as_ref())?),
        None => None,
    };
    let result = read_async(&state.workers, context.clone(), share_id, from_server_id, state.read_max_bytes, include_own_events).await?;

    // if let Some(client_id) = result.special_aggregates.client_id && !result.special_aggregates.client_aggregate.is_empty() {
    //     context.aggregate_id = client_aggregate_name(client_id);
    //     context.file_path = state.get_file_path(&context.aggregate_id);
    //     write_as

    // }
    let response = ReadResponse {
        event_batches: result.events,
        next_server_id: result.next_server_id,
    };
    // Log completion and return the response to the client
    debug!(
        return_event_batch_count = response.event_batches.len(),
        next_server_id = response.next_server_id,
        "Completed read_events operation"
    );
    Ok(CompactJson(response))
}
