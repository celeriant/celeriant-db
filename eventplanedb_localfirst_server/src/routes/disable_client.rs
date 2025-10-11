use crate::{app_state::{AppState, OWNER_ACCESS_LEVEL}, error_response::RouteError, job_error::JobError, json_formatter::CompactJson, routes::utils::record_span_fields, wrap_nanoid};
use axum::{extract::Path, http::HeaderMap};
use eventplanedb_crypto::Crypto;
use serde::{Deserialize, Serialize};
use tracing::{info, instrument};

#[derive(Debug, Serialize, Deserialize)]
pub struct DisableClientResponse {
}

#[instrument(
    name = "disable_client",
    skip(state, headers), 
    fields(
        aggregate_id = %aggregate_id,
        client_id, 
        server_time,
        user_id, 
        org_id,
    )
)]
pub async fn disable_client(
    Path((aggregate_type_id, aggregate_id, for_client_id)): Path<(String, String, String)>,
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: HeaderMap,
) -> Result<CompactJson<DisableClientResponse>, RouteError> {
    let aggregate_type_id = wrap_nanoid::nanoid_to_u128(&aggregate_type_id)
        .map_err(|e| RouteError::BadRequest(format!("Invalid aggregate type ID: {e}")))?;
    let aggregate_id = wrap_nanoid::nanoid_to_u128(&aggregate_id)
        .map_err(|e| RouteError::BadRequest(format!("Invalid aggregate ID: {e}")))?;
    // Establish the request context from headers and aggregate ID
    let context = state.create_job_context(aggregate_type_id, aggregate_id, &headers).await?;
    record_span_fields(&context);

    // Send the job context and additional parameters to the worker for processing
    let for_client_id = Crypto::decode_base64_u128_from_path(&for_client_id)?;
    info!(for_client_id = for_client_id, "Processing disable_client request");
    
    state.check_access(&context, OWNER_ACCESS_LEVEL, None).await?;

    // Disable the client
    let disabled = state.metadata_store
        .disable_client(
            context.org_id,
            aggregate_type_id,
            aggregate_id,
            for_client_id,
        )
        .await?;

    if !disabled {
        return Err(RouteError::JobError(JobError::NotFound("Client not found or already disabled".to_string())));
    }

    // Log completion and return the response to the client
    info!(
        for_client_id = for_client_id,
        "Completed disable_client operation"
    );
    let response = DisableClientResponse {
    };

    Ok(CompactJson(response))
}
