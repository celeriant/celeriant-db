use crate::{app_state::AppState, error_response::RouteError, json_formatter::CompactJson, routes::record_span_fields};
use tracing::{debug, error, instrument, warn, Span};
use axum::{
    Json,
    extract::{Path, Query},
    http::HeaderMap,
};
use eventplanedb_access::job_error::JobError;
use eventplanedb_storage::{event_batch_item::EventBatchItem, event_item::EventItem};
use eventplanedb_thread_worker::{job_context::JobContext, queue_jobs::write_async};
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
    skip(state, headers, events), 
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
    
    let current_client_id = state.get_client_id(&headers)?;
    let server_time = state.server_time();
    let (current_user_id, current_org_id) = state.get_claims(&headers).await?.map(|claims| (Some(claims.sub), claims.org_id)).unwrap_or((None, None));

    let context = JobContext {
        aggregate_id: aggregate_id.clone(),
        file_path: state.get_file_path(&aggregate_id),
        current_client_id,
        current_user_id,
        current_org_id,
        server_time,
    };
    
    record_span_fields(&context);
        
    if events.is_empty() {
        error!("No events provided for write operation");
        return Err(RouteError::JobError(JobError::InvalidParameters("No events provided".to_string())));
    }

    debug!("Processing write request with {} events", events.len());

    let create_if_not_exist = params.create_if_not_exist.unwrap_or(false);

    let result = write_async(&state.workers, context, create_if_not_exist, events).await?;
    
    debug!(
        return_event_batch_count = result.events.len(),
        server_id = result.server_id,
        "Successfully wrote events"
    );

    let response = WriteResponse {
        server_id: result.server_id,
        event_batches: result.events,
        server_time,
    };

    Ok(CompactJson(response))
}
