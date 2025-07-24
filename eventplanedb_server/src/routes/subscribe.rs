use crate::{app_state::AppState, error_response::RouteError};
use axum::{
    extract::{Path, Query},
    response::sse::{Event, Sse},
};
use eventplanedb_thread_worker::job_context::JobContext;
use eventplanedb_thread_worker::queue_jobs::access_check_async;
use futures::stream::{self, Stream};
use serde::Deserialize;
use std::{
    convert::Infallible,
    time::{Duration, Instant},
};

#[derive(Deserialize)]
pub struct AuthParams {
    pub token: Option<String>,
    pub public_key: String,
    pub nonce: String,
    pub signature: String,
}

pub async fn subscribe_events(
    Path(aggregate_id): Path<String>,
    Query(params): Query<AuthParams>,
    axum::extract::State(state): axum::extract::State<AppState>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, RouteError> {
    let current_client_id = state.get_client_id_direct(params.public_key.as_str(), params.nonce.as_str(), params.signature.as_str())?;
    let current_user_claims = state.get_claims_direct(params.token.as_deref()).await?;
    let file_path = state.get_file_path(&aggregate_id);

    let context = JobContext {
        file_path: file_path.clone(),
        current_client_id,
        current_user_id: current_user_claims.map(|claims| claims.sub),
        server_time: state.server_time(),
    };

    access_check_async(&state.workers, context, eventplanedb_access::access_level::AccessLevel::Viewer).await?;

    // Subscribe to event notifications for this file path
    let receiver = state.event_notifier.subscribe(&file_path);

    // Define cooldown period
    let cooldown_period = Duration::from_millis(state.subscribe_cooldown_period_ms);

    // Create an SSE stream with cooldown mechanism
    let stream = stream::unfold(
        (
            receiver,
            current_client_id,
            Instant::now().checked_sub(cooldown_period).unwrap_or_else(Instant::now),
        ),
        move |(mut receiver, current_client_id, last_notification)| async move {
            loop {
                tokio::select! {
                    result = receiver.recv() => {
                        match result {
                            Ok(notifier_client_id) => {
                                // Only send notification if the event was created by a different user
                                if notifier_client_id != current_client_id {
                                    let now = Instant::now();
                                    let time_since_last = now.duration_since(last_notification);

                                    // Check if we're outside the cooldown period
                                    if time_since_last >= cooldown_period {
                                        let event = Event::default().data("ne");
                                        return Some((Ok(event), (receiver, current_client_id, now)));
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
                        return Some((Ok(event), (receiver, current_client_id, last_notification)));
                    }
                }
            }
        },
    );

    Ok(Sse::new(stream))
}
