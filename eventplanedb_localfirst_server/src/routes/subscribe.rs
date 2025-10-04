use crate::{
    app_state::AppState, error_response::RouteError, job_context::JobContext,
    routes::utils::record_span_fields, wrap_nanoid,
};
use axum::{
    extract::{Path, Query},
    response::sse::{Event, Sse},
};
use eventplanedb_crypto::Crypto;
use eventplanedb_storage_stateful::aggregate_key::AggregateKey;
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
    Path((aggregate_type_id, aggregate_id)): Path<(String, String)>,
    Query(params): Query<SubscribeParams>,
    axum::extract::State(state): axum::extract::State<AppState>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, RouteError> {
    let aggregate_id = wrap_nanoid::nanoid_to_u128(&aggregate_id)
        .map_err(|e| RouteError::BadRequest(format!("Invalid aggregate ID: {e}")))?;
    let aggregate_type_id = wrap_nanoid::nanoid_to_u128(&aggregate_type_id)
        .map_err(|e| RouteError::BadRequest(format!("Invalid aggregate type ID: {e}")))?;

    // Establish the request context from headers and aggregate ID
    let current_client_id = state.get_client_id_direct(
        params.public_key.as_str(),
        params.nonce.as_str(),
        params.signature.as_str(),
    )?;
    let current_user_claims = state.get_claims_direct(params.token.as_deref()).await?;
    let (current_user_id, current_org_id) = current_user_claims
        .map(|claims| (Some(claims.sub), claims.org_id))
        .unwrap_or((None, None));
    let current_user_id =
        current_user_id.map(|uid| Crypto::generate_short_client_identity(uid.as_bytes()));

    let current_org_id = current_org_id
        .map(|org_id_str| wrap_nanoid::nanoid_to_u128(&org_id_str))
        .transpose()
        .map_err(|e| RouteError::BadRequest(format!("Invalid org ID: {e}")))?;

    let context = JobContext {
        org_id: current_org_id.unwrap_or(1),
        aggregate_type_id,
        aggregate_id,
        client_id: current_client_id,
        user_id: current_user_id,
        server_time: state.server_time(),
    };
    record_span_fields(&context);

    // Send the job context and additional parameters to the worker for processing
    // TODO: Validate that the requester has access to subscribe to this aggregate's events

    // Subscribe to event notifications for this aggregate
    // Create an SSE stream with cooldown mechanism
    info!("Subscribing to event stream using SSE");

    let receiver = state.event_notifier.subscribe(AggregateKey::new(
        context.org_id,
        context.aggregate_type_id,
        context.aggregate_id,
    ));
    let cooldown_period = Duration::from_millis(state.subscribe_cooldown_period_ms);
    let stream = stream::unfold(
        (
            receiver,
            current_client_id,
            Instant::now()
                .checked_sub(cooldown_period)
                .unwrap_or_else(Instant::now),
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
