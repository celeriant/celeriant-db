use crate::app_state::AppState;
use axum::{
    extract::{Path, Query},
    http::StatusCode,
    response::sse::{Event, Sse},
};
use futures::stream::{self, Stream};
use serde::Deserialize;
use std::{convert::Infallible, time::Duration};

#[derive(Deserialize)]
pub struct AuthParams {
    pub public_key: Option<String>,
    pub nonce: Option<String>,
    pub signature: Option<String>,
}

pub async fn subscribe_events(
    Path(id): Path<String>,
    Query(params): Query<AuthParams>,
    axum::extract::State(state): axum::extract::State<AppState>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, (StatusCode, String)> {
    // Get the current user's hash for filtering notifications
    let current_user_hash =
        if let (Some(public_key), Some(nonce), Some(signature)) = (params.public_key.as_deref(), params.nonce.as_deref(), params.signature.as_deref()) {
            match state.validate_auth_params(public_key, nonce, signature) {
                Ok(cb) => cb,
                Err(e) => return Err(e),
            }
        } else {
            return Err((StatusCode::BAD_REQUEST, "Missing authentication parameters".to_string()));
        };

    let file_path = state.get_file_path(&id);

    // Subscribe to event notifications for this file path
    let receiver = state.event_notifier.subscribe(&file_path);

    // Create an SSE stream that sends events when notifications are received
    let stream = stream::unfold((receiver, current_user_hash), move |(mut receiver, current_user)| async move {
        loop {
            tokio::select! {
                result = receiver.recv() => {
                    match result {
                        Ok(notifier_user_hash) => {
                            // Only send notification if the event was created by a different user
                            if notifier_user_hash != current_user {
                                let event = Event::default().data("new_events");
                                return Some((Ok(event), (receiver, current_user)));
                            }
                            // If it's the same user, just continue the loop without sending anything
                            continue;
                        }
                        Err(_) => {
                            // On channel error, continue the loop
                            continue;
                        }
                    }
                }
                _ = tokio::time::sleep(Duration::from_secs(30)) => {
                    // Send a keep-alive comment every 30 seconds
                    let event = Event::default().comment("keep-alive");
                    return Some((Ok(event), (receiver, current_user)));
                }
            }
        }
    });

    Ok(Sse::new(stream))
}
