use crate::{app_state::{AppState, READ_ACCESS_LEVEL}, error_response::RouteError, job_error::JobError, json_formatter::CompactJson, routes::utils::record_span_fields, wrap_nanoid};
use axum::{
    extract::{Path, Query},
    http::{HeaderMap},
};
use eventplanedb_crypto::Crypto;
use eventplanedb_storage_structures::{event_batch_item::EventBatchItem, read_filters::ReadFilters};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, instrument, warn};

#[derive(Debug, Deserialize)]
pub struct ReadQuery {
    from_server_id: Option<u64>,
    share_id: Option<String>,
    own_events: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct ReadResponse {
    event_batches: Vec<EventBatchItem>,
    next_event_batch_index: Option<u64>,
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
    Path((aggregate_type_id, aggregate_id)): Path<(String, String)>,
    Query(params): Query<ReadQuery>,
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: HeaderMap,
) -> Result<CompactJson<ReadResponse>, RouteError> {
    // Establish the request context from headers and aggregate ID
    let aggregate_type_id = wrap_nanoid::nanoid_to_u128(&aggregate_type_id)
        .map_err(|e| RouteError::BadRequest(format!("Invalid aggregate type ID: {e}")))?;
    let aggregate_id = wrap_nanoid::nanoid_to_u128(&aggregate_id)
        .map_err(|e| RouteError::BadRequest(format!("Invalid aggregate ID: {e}")))?;
    let context = state.create_job_context(aggregate_type_id, aggregate_id, &headers).await?;
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

    state.check_access(&context, READ_ACCESS_LEVEL, share_id).await?;
    
    let mut filters = ReadFilters::new(from_server_id);
    if !include_own_events {
        filters = filters.exclude_client_id(context.client_id);
    }
    let result = state.threaded_engine.read_filtered(
        context.org_id,
        context.aggregate_type_id,
        context.aggregate_id,
        filters,
    ).await?;

    let response = ReadResponse {
        event_batches: result.event_batches,
        next_event_batch_index: result.next_event_batch_index,
    };
    // Log completion and return the response to the client
    debug!(
        return_event_batch_count = response.event_batches.len(),
        next_server_id = response.next_event_batch_index,
        "Completed read_events operation"
    );
    Ok(CompactJson(response))
}
