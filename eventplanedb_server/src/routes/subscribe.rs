use crate::app_state::AppState;
use axum::{
    extract::{Path, Query},
    http::StatusCode,
    response::sse::{Event, Sse},
};
use eventplanedb_thread_worker::queue_jobs::access_check_async;
use eventplanedb_access::job_error::JobError;
use futures::stream::{self, Stream};
use serde::Deserialize;
use std::{
    convert::Infallible,
    time::{Duration, Instant},
};

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

    match access_check_async(
        &state.workers,
        file_path.clone(),
        current_user_hash.clone(),
        eventplanedb_access::access_level::AccessLevel::Viewer,
    )
    .await
    {
        Ok(_) => {}
        Err(e) => {
            let (status, message) = match e {
                JobError::PermissionDenied(msg) => (StatusCode::FORBIDDEN, msg),
                JobError::NotFound(msg) => (StatusCode::NOT_FOUND, msg),
                JobError::Other(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
            };
            return Err((status, format!("Failed to subscribe: {message}")));
        }
    }

    // Subscribe to event notifications for this file path
    let receiver = state.event_notifier.subscribe(&file_path);

    // Define cooldown period
    let cooldown_period = Duration::from_millis(200);

    // Create an SSE stream with cooldown mechanism
    let stream = stream::unfold(
        (
            receiver,
            current_user_hash,
            Instant::now().checked_sub(cooldown_period).unwrap_or_else(Instant::now),
        ),
        move |(mut receiver, current_user, last_notification)| async move {
            loop {
                tokio::select! {
                    result = receiver.recv() => {
                        match result {
                            Ok(notifier_user_hash) => {
                                // Only send notification if the event was created by a different user
                                if notifier_user_hash != current_user {
                                    let now = Instant::now();
                                    let time_since_last = now.duration_since(last_notification);

                                    // Check if we're outside the cooldown period
                                    if time_since_last >= cooldown_period {
                                        let event = Event::default().data("ne");
                                        return Some((Ok(event), (receiver, current_user, now)));
                                    }
                                    // If inside cooldown period, ignore this notification
                                    // and continue waiting
                                }
                                // If it's the same user or within cooldown, just continue the loop
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
                        let event = Event::default().comment("ka");
                        return Some((Ok(event), (receiver, current_user, last_notification)));
                    }
                }
            }
        },
    );

    Ok(Sse::new(stream))
}
