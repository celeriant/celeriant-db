use crate::{app_state::AppState, error_response::RouteError, json_formatter::CompactJson, routes::utils::record_span_fields, wrap_nanoid};
use axum::{extract::Path, http::HeaderMap};
use serde::{Deserialize, Serialize};
use tracing::{info, instrument};

#[derive(Debug, Serialize, Deserialize)]
pub struct DisableUserResponse {
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
    let aggregate_id = wrap_nanoid::nanoid_to_u128(&aggregate_id)
        .map_err(|e| RouteError::BadRequest(format!("Invalid aggregate ID: {e}")))?;
    // Establish the request context from headers and aggregate ID
    let context = state.create_job_context(aggregate_id, &headers).await?;
    record_span_fields(&context);

    // Send the job context and additional parameters to the worker for processing
    info!(for_user_id = &for_user_id, "Processing disable_user request");
    
    //TODO: Disable the user if the requester has access to do so

    // Log completion and return the response to the client
    info!(
        for_user_id = &for_user_id,
        "Completed disable_user operation"
    );
    let response = DisableUserResponse {
    };
    Ok(CompactJson(response))
}
