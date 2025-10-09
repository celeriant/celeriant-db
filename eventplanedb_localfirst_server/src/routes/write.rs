use crate::{app_state::AppState, error_response::RouteError, job_error::JobError, json_formatter::CompactJson, routes::utils::record_span_fields, wrap_nanoid};
use eventplanedb_crypto::Crypto;
use eventplanedb_storage_stateful::aggregate_key::AggregateKey;
use tower_http::follow_redirect::policy::PolicyExt;
use tracing::{debug, error, instrument};
use axum::{
    Json,
    extract::{Path, Query},
    http::HeaderMap,
};
use eventplanedb_storage_structures::{event_batch_item::EventBatchItem, event_item::EventItem};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct WriteQuery {
    client_last_event_batch_index: Option<u64>,
    create_if_not_exist: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WriteResponse {
    event_batch_index: u64,
}

#[instrument(
    name = "write_events",
    skip(state, headers, params, events), 
    fields(
        aggregate_id = %aggregate_id,
        client_id, 
        server_time,
        user_id, 
        org_id,
    )
)]
pub async fn write_events(
    Path((aggregate_type_id, aggregate_id)): Path<(String, String)>,
    Query(params): Query<WriteQuery>,
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: HeaderMap,
    Json(events): Json<Vec<EventItem>>,
) -> Result<CompactJson<WriteResponse>, RouteError> {
    
    // Establish the request context from headers and aggregate ID
    let aggregate_type_id = wrap_nanoid::nanoid_to_u128(&aggregate_type_id)
        .map_err(|e| RouteError::BadRequest(format!("Invalid aggregate type ID: {e}")))?;
    let aggregate_id = wrap_nanoid::nanoid_to_u128(&aggregate_id)
        .map_err(|e| RouteError::BadRequest(format!("Invalid aggregate ID: {e}")))?;
    let context = state.create_job_context(aggregate_type_id, aggregate_id, &headers).await?;
    record_span_fields(&context);
    
    // Basic validation of the request parameters
    if events.is_empty() {
        error!("No events provided for write operation");
        return Err(RouteError::JobError(JobError::InvalidParameters("No events provided".to_string())));
    }

    // Send the job context and additional parameters to the worker for processing
    debug!("Processing write request with {} events", events.len());
    let create_if_not_exist = params.create_if_not_exist.unwrap_or(false);

    //TODO: Permissions, create_if_not_exist handling
    
    let result = state.threaded_engine.append_events(
        context.org_id, 
        context.aggregate_type_id,
        aggregate_id, 
        context.client_id, 
        context.user_id, 
        events, 
        params.client_last_event_batch_index, 
        true,
    ).await?;

    let aggregate_key = AggregateKey::new(context.org_id, context.aggregate_type_id, aggregate_id);
    state.event_notifier.notify(&aggregate_key, context.client_id);
    
    // Log completion and return the response to the client
    debug!(
        server_id = result.event_batch_index,
        "Completed write_events operation"
    );
    let response = WriteResponse {
        event_batch_index: result.event_batch_index,
    };
    Ok(CompactJson(response))
}