use crate::{app_state::AppState, error_response::RouteError, routes::utils::record_span_fields};
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
use tracing::{info, trace, warn};

#[derive(Deserialize)]
pub struct SubscribeParams {
    pub token: Option<String>,
    pub public_key: String,
    pub nonce: String,
    pub signature: String,
}

pub async fn subscribe_events(
    Path(aggregate_id): Path<String>,
    Query(params): Query<SubscribeParams>,
    axum::extract::State(state): axum::extract::State<AppState>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, RouteError> {
    // Establish the request context from headers and aggregate ID
    let current_client_id = state.get_client_id_direct(params.public_key.as_str(), params.nonce.as_str(), params.signature.as_str())?;
    let current_user_claims = state.get_claims_direct(params.token.as_deref()).await?;
    let (current_user_id, current_org_id) = current_user_claims.map(|claims| (Some(claims.sub), claims.org_id)).unwrap_or((None, None));
    let file_path = state.get_file_path(&aggregate_id);
    let context = JobContext {
        aggregate_id: aggregate_id.clone(),
        file_path: file_path.clone(),
        current_client_id,
        current_user_id,
        current_org_id,
        server_time: state.server_time(),
    };
    record_span_fields(&context);

    // Send the job context and additional parameters to the worker for processing
    access_check_async(&state.workers, context, eventplanedb_access::access_level::AccessLevel::Viewer).await?;

    // Subscribe to event notifications for this aggregate
    // Create an SSE stream with cooldown mechanism
    info!("Subscribing to event stream using SSE");
    let receiver = state.event_notifier.subscribe(&aggregate_id);
    let cooldown_period = Duration::from_millis(state.subscribe_cooldown_period_ms);
    let stream = stream::unfold(
        (
            receiver,
            current_client_id,
            Instant::now().checked_sub(cooldown_period).unwrap_or_else(Instant::now),
            false, // Add a flag to track missed notifications
        ),
        move |(mut receiver, current_client_id, last_notification, mut missed_notification)| async move {
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
                                        trace!("Sending 'ne' event notification to client_id: {}", current_client_id);
                                        missed_notification = false; // Clear the missed notification flag
                                        return Some((Ok(event), (receiver, current_client_id, now, missed_notification)));
                                    } else {
                                        // Mark that a notification was missed during the cooldown
                                        missed_notification = true;
                                    }
                                }
                                // If it's the same user or within cooldown, just continue the loop
                                continue;
                            }
                            Err(_) => {
                                // On channel error, continue the loop
                                warn!("Event notification channel closed, continuing to wait for new events");
                                continue;
                            }
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_secs(30)) => {
                        // Send a keep-alive comment every 30 seconds

                        // Check if a notification was missed during the cooldown
                        if missed_notification {
                            let now = Instant::now();
                            let time_since_last = now.duration_since(last_notification);
                            if time_since_last >= cooldown_period {
                                let event = Event::default().data("ne");
                                trace!("Sending delayed 'ne' event notification to client_id: {}", current_client_id);
                                missed_notification = false; // Clear the missed notification flag
                                return Some((Ok(event), (receiver, current_client_id, now, missed_notification)));
                            }
                        }

                        let event = Event::default().comment("ka");
                        trace!("Sending keep-alive 'ka' comment to client_id: {}", current_client_id);
                        return Some((Ok(event), (receiver, current_client_id, last_notification, missed_notification)));
                    }
                }
            }
        },
    );

    Ok(Sse::new(stream))
}
