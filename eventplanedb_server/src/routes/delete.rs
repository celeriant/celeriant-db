use crate::{app_state::AppState, error_response::RouteError, json_formatter::CompactJson, routes::utils::record_span_fields};
use axum::{extract::Path, http::HeaderMap};
use eventplanedb_thread_worker::{queue_jobs::delete_async};
use serde::{Deserialize, Serialize};
use tracing::{info, instrument};

#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteResponse {
    server_time: u64,
}

#[instrument(
    name = "delete",
    skip(state, headers), 
    fields(
        aggregate_id = %aggregate_id,
        client_id, 
        server_time,
        user_id, 
        org_id,
    )
)]
pub async fn delete(
    Path(aggregate_id): Path<String>,
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: HeaderMap,
) -> Result<CompactJson<DeleteResponse>, RouteError> {
    // Establish the request context from headers and aggregate ID
    let context = state.create_job_context(aggregate_id.clone(), &headers).await?;
    record_span_fields(&context);

    // Send the job context and additional parameters to the worker for processing
    info!("Processing delete request");
    let server_time = context.server_time;
    delete_async(&state.workers, context).await?;

    // Log completion and return the response to the client
    info!("Completed delete operation");
    let response = DeleteResponse { server_time };
    Ok(CompactJson(response))
}
