//! Phase 5 — watch / subscribe (single node).
//!
//! Oracle: celeriant-docs/docs/concepts/watch.md, guides/subscribing.md,
//! reference/error-codes.md.

use std::collections::HashSet;
use std::time::Duration;

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use celeriant_client_tokio::client_operations::WriteEventsOptions;
use celeriant_client_tokio::server_error::ServerError;
use celeriant_client_tokio::watch_connection::{WatchConnection, WatchOptions};
use celeriant_msg::request::requests::{
    DeleteRequest, SingleAggregateDelete, TrimStartRequest, WatchRequest,
};
use celeriant_msg::response::watch_event::WatchResponseEvent;
use celeriant_wal::aggregate_key::AggregateKey;
use std::collections::HashMap;
use crate::TestServer;

use crate::common::{event, port_for, R};

const TYPE: u64 = 100;

fn watch_aggregates(ids: &[u128]) -> WatchRequest {
    WatchRequest {
        correlation_id: None,
        requested_latency_ms: Some(100),
        shard_id: None,
        orgs: None,
        aggregate_types: None,
        aggregates: Some(ids.iter().copied().collect::<HashSet<u128>>()),
        operation_types: None,
    }
}

/// Drain notifications until one matches `pred` or the deadline passes.
async fn await_event<F: Fn(&WatchResponseEvent) -> bool>(
    w: &mut WatchConnection,
    pred: F,
) -> Result<WatchResponseEvent, Box<dyn std::error::Error>> {
    let deadline = std::time::Instant::now() + Duration::from_secs(8);
    while std::time::Instant::now() < deadline {
        match w.next_timeout(Duration::from_secs(2)).await? {
            Some(resp) => {
                for e in resp.events {
                    if pred(&e) {
                        return Ok(e);
                    }
                }
            }
            None => continue,
        }
    }
    Err("timed out waiting for a matching watch notification".into())
}

/// 5.1 A watch delivers a change notification for a matching write, naming the
/// aggregate and the affected version range (watch "What a watch delivers";
/// subscribing).
pub async fn watch_notifies_on_write() -> R {
    let server = TestServer::start_with_port(port_for("watch_notifies_on_write")).await?;
    let agg_id = 314u128;
    let key = AggregateKey::new(1, 1, agg_id);

    // Pre-create the aggregate so the event under observation is a plain append
    // (operation "write"), whose notification the docs say carries the affected
    // version range. (A first-write "create" is reported as a distinct operation
    // and omits the range — see FINDINGS F3.)
    let mut c = CeleriantClient::connect(server.address()).await?;
    c.write_events_with(key.clone(), vec![event(1, TYPE, 1000, "{}")], 0, WriteEventsOptions { allow_create: true, ..Default::default() })
        .await?;

    let mut w = WatchConnection::connect(server.address(), watch_aggregates(&[agg_id]), WatchOptions::default()).await?;

    // Append after the watch is live.
    c.write_events_with(key.clone(), vec![event(2, TYPE, 1001, "{}")], 0, WriteEventsOptions { allow_create: false, ..Default::default() })
        .await?;

    // The notification for the append must name the aggregate and the affected
    // batch-index range (watch.md: "the range of batch indexes affected";
    // "ToAggregateVersion only advances").
    let e = await_event(&mut w, |e| e.aggregate_id == agg_id && e.to_aggregate_version.is_some()).await?;
    if e.org_id != 1 || e.aggregate_type_id != 1 {
        return Err(format!("notification identified wrong aggregate: {e:?}").into());
    }
    match e.to_aggregate_version {
        Some(v) if v >= 2 => Ok(()),
        other => Err(format!("notification to_aggregate_version {other:?}, expected >=2 for the appended batch").into()),
    }
}

/// 5.6 When an aggregate is first created under an active watch, the consumer
/// receives a notification carrying the affected version range. Per watch.md
/// (clarified — see FINDINGS F3), a create emits both a `create` operation event
/// (no range) AND a `write` event carrying `to_aggregate_version = 1`; a consumer
/// advancing on `to_aggregate_version` sees the new aggregate at version 1. This
/// test asserts that a ranged notification for the create does arrive (it does
/// NOT require the `create` operation event itself to carry the range).
pub async fn watch_create_notification_carries_version_range() -> R {
    let server =
        TestServer::start_with_port(port_for("watch_create_notification_carries_version_range")).await?;
    let agg_id = 316u128;
    let key = AggregateKey::new(1, 1, agg_id);

    let mut w = WatchConnection::connect(server.address(), watch_aggregates(&[agg_id]), WatchOptions::default()).await?;

    // The FIRST write creates the aggregate.
    let mut c = CeleriantClient::connect(server.address()).await?;
    c.write_events_with(key.clone(), vec![event(1, TYPE, 1000, "{}")], 0, WriteEventsOptions { allow_create: true, ..Default::default() })
        .await?;

    // Wait for the notification that carries the range (the accompanying write
    // event); it must report the new aggregate at version 1.
    let e = await_event(&mut w, |e| e.aggregate_id == agg_id && e.to_aggregate_version.is_some()).await?;
    match e.to_aggregate_version {
        Some(v) if v >= 1 => Ok(()),
        other => Err(format!(
            "create produced no ranged notification: to_aggregate_version is {other:?}, expected Some(>=1)"
        )
        .into()),
    }
}

/// 5.2 A watch whose filters don't match the routing rule is rejected by the
/// server with IncompatibleFilters (9002) (watch "Scope and latency"; error-codes
/// 9002). We send the watch through the raw request path rather than
/// `WatchConnection`, whose documented behavior is to *recover* from a shard
/// routing error by fanning out per-shard (see FINDINGS F4) — so the server's
/// enforcement is only visible on the raw protocol surface.
pub async fn watch_incompatible_filter_rejected() -> R {
    // Multiple shards so an org-only filter under aggregate_id routing is a real
    // mismatch the server must reject (with 1 shard everything trivially routes
    // to shard 0).
    let config = crate::ServerConfig { num_shards: Some(4), standalone: true, ..Default::default() };
    let server = TestServer::start_with_config(port_for("watch_incompatible_filter_rejected"), config).await?;

    let req = WatchRequest {
        correlation_id: None,
        requested_latency_ms: Some(100),
        // No shard_id: the server must route by filters, and an org-only filter
        // under aggregate_id routing has no aggregate to route by.
        shard_id: None,
        orgs: Some([1u128].into_iter().collect()),
        aggregate_types: None,
        aggregates: None,
        operation_types: None,
    };
    let mut c = CeleriantClient::connect(server.address()).await?;
    let res = c
        .send_request(&celeriant_msg::process_client_requests::ClientRequest::Watch(req))
        .await;
    match res {
        Err(ClientError::Server(ServerError::ShardRouting { error_code: 9002, .. })) => Ok(()),
        Err(e) => Err(format!("expected IncompatibleFilters (9002), got error {e:?}").into()),
        Ok(_) => Err("org-only watch under aggregate_id routing was accepted; expected IncompatibleFilters (9002)".into()),
    }
}

/// 5.3 RequestedLatency over the server cap -> LatencyTooHigh (8001)
/// (watch; error-codes 8001). Default cap is --max-requested-latency-ms = 2000.
pub async fn watch_latency_too_high_rejected() -> R {
    let server = TestServer::start_with_port(port_for("watch_latency_too_high_rejected")).await?;

    let mut req = watch_aggregates(&[1]);
    req.requested_latency_ms = Some(60_000); // far above the 2000ms default cap
    let res = WatchConnection::connect(server.address(), req, WatchOptions::default()).await;
    match res {
        Err(ClientError::Server(ServerError::Watch {
            kind: celeriant_client_tokio::server_error::WatchError::LatencyTooHigh, ..
        })) => Ok(()),
        Err(e) => Err(format!("expected WatchError::LatencyTooHigh, got error {e:?}").into()),
        Ok(_) => Err("oversized requested_latency_ms was accepted; expected LatencyTooHigh (8001)".into()),
    }
}

/// 5.4 Coalescing never drops: re-reading from the cursor on each notification
/// sees every event exactly once even under a burst (watch "Coalescing merges;
/// it never drops. The notification's ToAggregateVersion only advances").
pub async fn watch_cursor_misses_no_events() -> R {
    let server = TestServer::start_with_port(port_for("watch_cursor_misses_no_events")).await?;
    let agg_id = 271u128;
    let key = AggregateKey::new(1, 1, agg_id);

    let mut req = watch_aggregates(&[agg_id]);
    req.requested_latency_ms = Some(500); // encourage coalescing of the burst
    let mut w = WatchConnection::connect(server.address(), req, WatchOptions::default()).await?;

    // Burst of writes after the watch is live.
    let n = 30u64;
    let mut writer = CeleriantClient::connect(server.address()).await?;
    for i in 1..=n {
        writer
            .write_events_with(key.clone(), vec![event(i, TYPE, 1000 + i, &format!("{{\"n\":{i}}}"))],
                0, WriteEventsOptions { allow_create: i == 1, ..Default::default() })
            .await?;
    }

    // On each notification, drain from our cursor; assert we observe 1..=n with
    // no gaps and no duplicates.
    let mut reader = CeleriantClient::connect(server.address()).await?;
    let mut cursor = 1u64;
    let mut seen: Vec<u64> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(12);
    while (seen.len() as u64) < n && std::time::Instant::now() < deadline {
        let got = w.next_timeout(Duration::from_secs(3)).await?;
        if got.is_none() {
            continue;
        }
        // Drain everything from the cursor forward.
        let batches = crate::common::read_all_from(&mut reader, &key, cursor).await?;
        for b in batches {
            seen.push(b.aggregate_version);
            cursor = b.aggregate_version + 1;
        }
    }
    let expected: Vec<u64> = (1..=n).collect();
    if seen != expected {
        return Err(format!("cursor-drain saw {seen:?}, expected contiguous 1..={n}").into());
    }
    Ok(())
}

/// 5.5 The notification distinguishes operations: a write, a delete and a trim
/// on the same aggregate produce notifications whose `operation` codes differ
/// from each other (watch: "which operation (a write, a delete, a trim, a
/// create)"). The docs do not publish the numeric codes, so we assert only the
/// documented property — distinctness — not specific values.
pub async fn watch_operations_are_distinct() -> R {
    let server = TestServer::start_with_port(port_for("watch_operations_are_distinct")).await?;
    let agg_id = 999_001u128;
    let key = AggregateKey::new(1, 1, agg_id);

    let mut c = CeleriantClient::connect(server.address()).await?;
    // Seed a stream BEFORE watching (so trim/delete have something to act on and
    // we only observe the three operations under test on the live tail).
    for i in 1..=4u64 {
        c.write_events_with(key.clone(), vec![event(i, TYPE, 1000 + i, "{}")],
            0, WriteEventsOptions { allow_create: i == 1, ..Default::default() }).await?;
    }

    let mut w = WatchConnection::connect(server.address(), watch_aggregates(&[agg_id]), WatchOptions::default()).await?;

    // Operation 1: a write (append one more event).
    c.write_events_with(key.clone(), vec![event(5, TYPE, 2000, "{}")], 0, WriteEventsOptions { allow_create: false, ..Default::default() }).await?;
    let write_op = await_event(&mut w, |e| e.aggregate_id == agg_id).await?.operation;

    // Operation 2: a trim. Its notification differs from the write's op code.
    c.trim_start(TrimStartRequest { correlation_id: None, aggregate_key: key.clone(), keep_from_aggregate_version: 3, client_id: 1, user_id: None }).await?;
    let trim_op = await_event(&mut w, |e| e.aggregate_id == agg_id && e.operation != write_op).await?.operation;

    // Operation 3: a delete.
    let mut deletes = HashMap::new();
    deletes.insert(key.clone(), SingleAggregateDelete { allow_recreate: false, allow_sequence_continuation: false, expected_version: None });
    c.delete(DeleteRequest { correlation_id: None, client_id: 1, user_id: None, deletes }).await?;
    let delete_op = await_event(&mut w, |e| e.aggregate_id == agg_id && e.operation != write_op && e.operation != trim_op).await?.operation;

    if write_op == trim_op || write_op == delete_op || trim_op == delete_op {
        return Err(format!("operations not distinct: write={write_op}, trim={trim_op}, delete={delete_op}").into());
    }
    Ok(())
}
