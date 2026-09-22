//! Watch-options invariants against a real server.
//!
//! - An overflowing `max_shard_hint` is rejected as a protocol error instead
//!   of wrapping to zero shards and connecting a watch that silently delivers
//!   nothing (watch-max-shard-overflow).
//! - The pool threads its configured `max_response_size` into watch
//!   connections. Before the fix the watch stream read frames under a
//!   hardcoded 10MB bound, so a pool configured with a tight response cap
//!   still delivered arbitrarily large watch frames
//!   (watch-max-response-size-not-threaded-from-pool). The observable: a
//!   burst of creates coalesced into one latency window produces a watch
//!   frame far past the pool's cap, which must surface as an error, never be
//!   delivered whole.
//!
//! Whether the burst coalesces into a single oversized frame is a timing race
//! against the latency window, so the burst is retried on fresh orgs; each
//! attempt is decisive when a frame trips the cap or when every event arrives
//! in one response, and merely inconclusive otherwise.

use std::collections::HashSet;
use std::time::Duration;

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use celeriant_client_tokio::client_operations::WriteEventsOptions;
use celeriant_client_tokio::pool::{CeleriantPool, PoolOptions};
use celeriant_client_tokio::watch_connection::{WatchConnection, WatchOptions};
use celeriant_msg::request::requests::WatchRequest;
use celeriant_wal::aggregate_key::AggregateKey;

use crate::TestServer;
use crate::common::{R, event, port_for};

/// Far below the burst's coalesced frame (~200 events, ~60+ bytes each).
const TIGHT_RESPONSE_CAP: u64 = 4096;
const BURST_AGGREGATES: u64 = 200;
const LATENCY_WINDOW_MS: u64 = 1000;
const ATTEMPTS: u32 = 3;

fn watch_request(orgs: Option<HashSet<u128>>, aggregates: Option<HashSet<u128>>) -> WatchRequest {
    WatchRequest {
        correlation_id: None,
        requested_latency_ms: Some(LATENCY_WINDOW_MS),
        shard_id: None,
        orgs,
        aggregate_types: None,
        aggregates,
        operation_types: None,
    }
}

/// One burst attempt against a fresh org. Returns:
/// - `Ok(Some(true))` — decisive pass: an oversized frame errored;
/// - `Ok(Some(false))` — decisive fail: every event was delivered whole;
/// - `Ok(None)` — inconclusive: frames stayed small or the window split.
async fn burst_attempt(
    pool: &CeleriantPool,
    server_address: &str,
    org: u128,
) -> Result<Option<bool>, Box<dyn std::error::Error>> {
    let mut watch = pool
        .watch(watch_request(Some(HashSet::from([org])), None), WatchOptions::default())
        .await
        .map_err(|e| format!("the watch handshake itself is small and must succeed: {e:?}"))?;

    let mut writer = CeleriantClient::connect(server_address).await?;
    for agg in 0..BURST_AGGREGATES {
        writer
            .write_events_with(
                AggregateKey::new(org, 1, agg as u128 + 1),
                vec![event(1, 100, 1001, "{\"b\":1}")],
                0,
                WriteEventsOptions { allow_create: true, ..Default::default() },
            )
            .await?;
    }

    let mut delivered = 0u64;
    let mut max_frame_events = 0u64;
    let deadline = std::time::Instant::now() + Duration::from_millis(4 * LATENCY_WINDOW_MS);
    while std::time::Instant::now() < deadline {
        match watch.next_timeout(Duration::from_secs(1)).await {
            Err(e) => {
                let msg = format!("{e:?}");
                if msg.contains("MessageTooLarge") {
                    return Ok(Some(true));
                }
                return Err(format!("watch died for another reason than the cap: {msg}").into());
            }
            Ok(None) => continue,
            Ok(Some(resp)) => {
                let n = resp.events.len() as u64;
                max_frame_events = max_frame_events.max(n);
                delivered += n;
                // ~60+ bytes per event: a frame of 100+ events is certainly
                // past the 4096-byte cap, so delivering one means the cap was
                // ignored. Full delivery in only small frames is inconclusive
                // (the window split), handled after the loop.
                if max_frame_events >= 100 {
                    return Ok(Some(false));
                }
                if delivered >= BURST_AGGREGATES {
                    break;
                }
            }
        }
    }
    println!(
        "  inconclusive burst: delivered {delivered}/{BURST_AGGREGATES}, largest frame {max_frame_events} events"
    );
    Ok(None)
}

pub async fn options_are_enforced() -> R {
    let server =
        TestServer::start_with_port(port_for("invariant_watch_options_enforced")).await?;

    // Overflowing shard hint: explicit error, never a silently empty watch.
    let overflow = WatchOptions {
        max_shard_hint: Some(u64::MAX),
        timeout: Some(Duration::from_secs(5)),
        ..Default::default()
    };
    match WatchConnection::connect(
        server.address(),
        watch_request(None, Some(HashSet::from([601_001u128]))),
        overflow,
    )
    .await
    {
        Err(ClientError::InvalidShardRange { max_shard_hint, .. }) if max_shard_hint == u64::MAX => {}
        Ok(_) => {
            return Err(
                "a max_shard_hint of u64::MAX connected; wrapped to zero shards this is a \
                 watch that can never deliver an event"
                    .into(),
            );
        }
        Err(other) => {
            return Err(
                format!("expected InvalidShardRange for the overflowing hint, got {other:?}").into()
            );
        }
    }

    // The pool's response cap must reach the watch stream.
    let mut capped_options = PoolOptions::new(server.address().to_string());
    capped_options.max_response_size = TIGHT_RESPONSE_CAP;
    let pool = CeleriantPool::new(capped_options);

    for attempt in 0..ATTEMPTS {
        match burst_attempt(&pool, server.address(), 700_001 + attempt as u128).await? {
            Some(true) => return Ok(()),
            Some(false) => {
                return Err(format!(
                    "the watch delivered all {BURST_AGGREGATES} burst events through a pool \
                     capped at max_response_size={TIGHT_RESPONSE_CAP} — the pool's cap is \
                     not threaded into the watch connection"
                )
                .into());
            }
            None => continue,
        }
    }
    Err("every burst attempt was inconclusive: no frame either tripped the cap or carried the full burst".into())
}
