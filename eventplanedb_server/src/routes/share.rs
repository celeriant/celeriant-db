use axum::{extract::Query, http::StatusCode, Json};
use event_storage::{event_item::EventItem};
use eventplanedb_access::share_links_cache::AccessLevel;
use serde::{Deserialize, Serialize};
use crate::{app_state::AppState};

#[derive(Debug, Deserialize)]
pub struct ShareQuery {
    pi: String,
    public_key: String,
    nonce: String,
    sign: String,
    access_level: AccessLevel,
    is_single_use: bool,
    iv: Option<String>,
    description: Option<String>,
    expires_on: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct ShareResponse {
    share_key: String,
    share_event: EventItem,
}

pub async fn share(
    Query(params): Query<ShareQuery>,
    axum::extract::State(state): axum::extract::State<AppState>,
) -> Result<Json<ShareResponse>, (StatusCode, String)> {
    // TODO: Implement authentication logic using:
    // - params.pi (project identifier)
    // - params.public_key
    // - params.nonce
    // - params.sign
    // - params.is_owner
    
    let share_key = nanoid::nanoid!();
    
    // Create EventItem with tp == 43 (share event type)
    let share_event = EventItem {
        tp: 43,
        ed: chrono::Utc::now().timestamp_millis() as u64,
        iv: None,
        int_values: None,
        uint_values: None,
        f32_values: None,
        f64_values: None,
        bool_values: None,
        string_values: None,
        byte_arrays: None,
    };

    Ok(Json(ShareResponse {
        share_key: share_key.clone(),
        share_event,
    }))
}
