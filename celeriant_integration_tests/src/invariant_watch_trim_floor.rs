//! Watch invariant: trim floors are monotonic. Two trims of one aggregate
//! coalesced into a single fsync window must notify watchers with the highest
//! floor, never only a stale lower one.
//!
//! The fsync coordinator only amortises (and so only coalesces) under load: an
//! idle server fast-paths every request into its own instant cycle. Background
//! writers keep the sync gate busy so the trims ride one wide fsync window.
//! Landing both trims in one window is still a timing race against the window
//! boundary, so the trim pair is retried on fresh aggregates; each attempt is
//! decisive when it coalesces and merely inconclusive when it does not.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_operations::WriteEventsOptions;
use celeriant_client_tokio::watch_connection::{WatchConnection, WatchOptions};
use celeriant_msg::request::requests::{TrimStartRequest, WatchRequest};
use celeriant_wal::aggregate_key::AggregateKey;

use crate::TestServer;
use crate::common::{R, event, port_for};

const TYPE: u64 = 100;
const FSYNC_WINDOW: Duration = Duration::from_millis(500);
const ATTEMPTS: u32 = 3;

fn trim(key: &AggregateKey, keep_from: u64) -> TrimStartRequest {
    TrimStartRequest {
        correlation_id: None,
        aggregate_key: key.clone(),
        keep_from_aggregate_version: keep_from,
        client_id: 1,
        user_id: None,
    }
}

async fn background_writer(address: String, agg_id: u128, stop: Arc<AtomicBool>) -> Result<(), String> {
    let key = AggregateKey::new(1, 1, agg_id);
    let mut c = CeleriantClient::connect(&address).await.map_err(|e| format!("{e:?}"))?;
    let mut seq = 1u64;
    while !stop.load(Ordering::Relaxed) {
        c.write_events_with(
            key.clone(),
            vec![event(seq, TYPE, 1000 + seq, "{}")],
            0,
            WriteEventsOptions { allow_create: seq == 1, ..Default::default() },
        )
        .await
        .map_err(|e| format!("background write {seq}: {e:?}"))?;
        seq += 1;
    }
    Ok(())
}

/// Runs one trim pair (floor 2 then floor 3, 50ms apart) against a fresh
/// aggregate and reports the floors the watch delivered.
async fn trim_pair_floors(server_address: &str, agg_id: u128) -> Result<Vec<u64>, Box<dyn std::error::Error>> {
    let key = AggregateKey::new(1, 1, agg_id);

    let mut writer_low = CeleriantClient::connect(server_address).await?;
    for i in 1..=3u64 {
        writer_low
            .write_events_with(
                key.clone(),
                vec![event(i, TYPE, 1000 + i, "{}")],
                0,
                WriteEventsOptions { allow_create: i == 1, ..Default::default() },
            )
            .await?;
    }
    let mut writer_high = CeleriantClient::connect(server_address).await?;

    let watch_request = WatchRequest {
        correlation_id: None,
        requested_latency_ms: Some(100),
        shard_id: None,
        orgs: None,
        aggregate_types: None,
        aggregates: Some(HashSet::from([agg_id])),
        operation_types: None,
    };
    let mut w =
        WatchConnection::connect(server_address, watch_request, WatchOptions::default()).await?;

    // Floor 2 enqueues ahead of floor 3 inside one fsync window, so a
    // first-wins collector would broadcast only the stale floor 2.
    let low = writer_low.trim_start(trim(&key, 2));
    let high = async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        writer_high.trim_start(trim(&key, 3)).await
    };
    let (low, high) = tokio::join!(low, high);
    low?;
    high?;

    let mut floors = Vec::new();
    let deadline = std::time::Instant::now() + 4 * FSYNC_WINDOW;
    while std::time::Instant::now() < deadline {
        let Some(resp) = w.next_timeout(Duration::from_secs(1)).await? else { continue };
        for e in resp.events {
            if e.aggregate_id != agg_id {
                continue;
            }
            if let Some(floor) = e.keep_from_aggregate_version {
                if floor == 3 {
                    return Ok(vec![3]);
                }
                floors.push(floor);
            }
        }
    }
    Ok(floors)
}

pub async fn monotonic() -> R {
    let config = crate::ServerConfig {
        num_shards: Some(1),
        standalone: true,
        fsync_delay_us: FSYNC_WINDOW.as_micros() as u64,
        ..Default::default()
    };
    let server =
        TestServer::start_with_config(port_for("invariant_watch_trim_floor_monotonic"), config)
            .await?;

    let stop = Arc::new(AtomicBool::new(false));
    let writers: Vec<_> = (0..4u128)
        .map(|i| {
            tokio::spawn(background_writer(
                server.address().to_string(),
                900_000 + i,
                stop.clone(),
            ))
        })
        .collect();
    // Let the load establish the amortised (slow-path) fsync cadence.
    tokio::time::sleep(2 * FSYNC_WINDOW).await;

    let mut result = Ok(());
    for attempt in 0..ATTEMPTS {
        let floors = trim_pair_floors(server.address(), 777_001 + attempt as u128).await?;
        if floors == vec![3] {
            result = Ok(());
            break;
        }
        result = Err(format!(
            "watch never delivered trim floor 3 (attempt {attempt}); floors delivered: {floors:?} \
             (a lower floor alone means the fsync-window collector kept the first trim, not the highest)"
        )
        .into());
        if floors.contains(&2) {
            // Decisive: the trims coalesced and the highest floor was dropped.
            break;
        }
    }

    stop.store(true, Ordering::Relaxed);
    for w in writers {
        w.await??;
    }
    result
}
