use crate::{app_state::AppState, error_response::RouteError, json_formatter::CompactJson, routes::utils::record_span_fields};
use tracing::{debug, error, instrument};
use axum::{
    Json,
    extract::{Path, Query},
    http::HeaderMap,
};
use eventplanedb_access::job_error::JobError;
use eventplanedb_storage::{event_batch_item::EventBatchItem, event_item::EventItem};
use eventplanedb_thread_worker::{queue_jobs::write_async};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct WriteQuery {
    create_if_not_exist: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WriteResponse {
    server_id: u64,
    event_batches: Vec<EventBatchItem>,
    server_time: u64,
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
    Path(aggregate_id): Path<String>,
    Query(params): Query<WriteQuery>,
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: HeaderMap,
    Json(events): Json<Vec<EventItem>>,
) -> Result<CompactJson<WriteResponse>, RouteError> {
    
    // Establish the request context from headers and aggregate ID
    let context = state.create_job_context(aggregate_id.clone(), &headers).await?;
    record_span_fields(&context);
    
    // Basic validation of the request parameters
    if events.is_empty() {
        error!("No events provided for write operation");
        return Err(RouteError::JobError(JobError::InvalidParameters("No events provided".to_string())));
    }

    // Validate event types - reject if any event_type <= 50
    for event in &events {
        if event.event_type <= 50 {
            error!("Invalid event_type {} - must be greater than 50", event.event_type);
            return Err(RouteError::JobError(JobError::PermissionDenied("Client passing in reserved event types is not allowed".to_string())));
        }
    }

    // Send the job context and additional parameters to the worker for processing
    debug!("Processing write request with {} events", events.len());
    let create_if_not_exist = params.create_if_not_exist.unwrap_or(false);
    let server_time = context.server_time;
    let result = write_async(&state.workers, context, create_if_not_exist, events).await?;
    
    // Log completion and return the response to the client
    debug!(
        return_event_batch_count = result.events.len(),
        server_id = result.server_id,
        "Completed write_events operation"
    );
    let response = WriteResponse {
        server_id: result.server_id,
        event_batches: result.events,
        server_time,
    };
    Ok(CompactJson(response))
}