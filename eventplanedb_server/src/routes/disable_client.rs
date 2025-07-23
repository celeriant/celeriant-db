use crate::{app_state::AppState, error_response::RouteError, json_formatter::CompactJson};
use axum::{extract::Path, http::HeaderMap};
use base64::{Engine as _, engine::general_purpose};
use eventplanedb_access::job_error::JobError;
use eventplanedb_storage::event_batch_item::EventBatchItem;
use eventplanedb_thread_worker::{job_context::JobContext, queue_jobs::disable_user_async};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct DisableClientResponse {
    server_id: u64,
    event_batches: Vec<EventBatchItem>,
    server_time: u64,
}

pub async fn disable_client(
    Path((aggregate_id, client_id_b64)): Path<(String, String)>,
    axum::extract::State(state): axum::extract::State<AppState>,
    headers: HeaderMap,
) -> Result<CompactJson<DisableClientResponse>, RouteError> {
    // Decode the base64 client ID from the URL-safe format
    let client_id = decode_client_id_from_path(&client_id_b64)?;

    let current_user_claims = state.get_claims(&headers).await?;

    let server_time = state.server_time();

    let context = JobContext {
        file_path: state.get_file_path(&aggregate_id),
        current_client_id: state.get_client_id(&headers)?,
        current_user_id: current_user_claims.map(|claims| claims.sub),
        server_time,
    };

    let result = disable_user_async(&state.workers, context, Some(client_id), None).await?;

    let response = DisableClientResponse {
        server_id: result.server_id,
        event_batches: result.events,
        server_time,
    };

    Ok(CompactJson(response))
}

// Helper function to decode the client ID from URL-safe base64
fn decode_client_id_from_path(client_id_b64: &str) -> Result<u128, JobError> {
    // Replace URL-safe characters back to standard base64
    let fixed_b64 = client_id_b64.replace('-', "+").replace('_', "/");

    // Add padding if needed
    let padded_b64 = match fixed_b64.len() % 4 {
        0 => fixed_b64,
        n => fixed_b64 + &"=".repeat(4 - n),
    };

    // Decode from base64
    let bytes = general_purpose::STANDARD
        .decode(padded_b64)
        .map_err(|_| JobError::InvalidParameters("Invalid client ID format".to_string()))?;

    if bytes.len() != 16 {
        return Err(JobError::InvalidParameters("Invalid client ID length".to_string()));
    }

    let mut array = [0u8; 16];
    array.copy_from_slice(&bytes);
    Ok(u128::from_le_bytes(array))
}
