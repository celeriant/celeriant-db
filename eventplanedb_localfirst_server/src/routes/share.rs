use crate::{app_state::AppState, error_response::RouteError, json_formatter::CompactJson, routes::utils::record_span_fields, wrap_nanoid};
use axum::{Json, extract::Path, http::HeaderMap};
use serde::{Deserialize, Serialize};
use tracing::{info, instrument};

#[derive(Debug, Deserialize)]
pub struct ShareQuery {
    access_level: u64,
    is_single_use: bool,
    expires_on: u64,
}

#[derive(Debug, Serialize)]
pub struct ShareResponse {
    share_key: String,
}

#[instrument(
    name = "share",
    skip(state, headers, share_body), 
    fields(
        aggregate_id = %aggregate_id,
        client_id, 
        server_time,
        user_id, 
        org_id,
    )
)]
pub async fn share(
    Path(aggregate_id): Path<String>,
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: HeaderMap,
    Json(share_body): Json<ShareQuery>,
) -> Result<CompactJson<ShareResponse>, RouteError> {

    // Establish the request context from headers and aggregate ID
    let aggregate_id = wrap_nanoid::nanoid_to_u128(&aggregate_id)
        .map_err(|e| RouteError::BadRequest(format!("Invalid aggregate ID: {e}")))?;
    let context = state.create_job_context(aggregate_id, &headers).await?;
    record_span_fields(&context);

    // Send the job context and additional parameters to the worker for processing
    info!(
        is_single_use = share_body.is_single_use,
        access_level = share_body.access_level,
        expires_on = share_body.expires_on,
        "Processing share request",
    );
    let share_key = nanoid::nanoid!();

    //TODO: Create the share link if the requester has access to do so

    // Log completion and return the response to the client
    info!("Completed share operation");
    let response = ShareResponse { share_key };
    Ok(CompactJson(response))
}
