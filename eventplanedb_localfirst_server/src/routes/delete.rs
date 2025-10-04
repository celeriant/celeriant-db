use crate::{app_state::AppState, error_response::RouteError, json_formatter::CompactJson, routes::utils::record_span_fields, wrap_nanoid};
use axum::{extract::Path, http::HeaderMap};
use eventplanedb_crypto::Crypto;
use eventplanedb_storage_stateful::aggregate_key::AggregateKey;
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
    let aggregate_id = wrap_nanoid::nanoid_to_u128(&aggregate_id)
        .map_err(|e| RouteError::BadRequest(format!("Invalid aggregate ID: {e}")))?;
    let context = state.create_job_context(aggregate_id, &headers).await?;
    record_span_fields(&context);

    // Send the job context and additional parameters to the worker for processing
    info!("Processing delete request");
    let server_time = context.server_time;

    //TODO: Delete the aggregate if the requester has access to do so
    state.threaded_engine.delete(context.org_id, context.aggregate_type_id, aggregate_id).await?;

    let aggregate_key = AggregateKey::new(context.org_id, context.aggregate_type_id, aggregate_id);
    state.event_notifier.notify(&aggregate_key, context.client_id);

    // Log completion and return the response to the client
    info!("Completed delete operation");
    let response = DeleteResponse { server_time };
    Ok(CompactJson(response))
}
