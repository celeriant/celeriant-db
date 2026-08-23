//! Connection Pool Integration Tests
//!
//! Tests CeleriantPool basic operations, connection lifecycle, idle eviction,
//! pool configuration, and list operations against a standalone server.
//!
//! Run with: cargo run --bin pool_test_main

use std::collections::HashMap;
use std::sync::Arc;

use celeriant_client_tokio::{
    CeleriantPool, ClientError, PoolOptions,
    list_operations::ListOptions,
};
use futures::FutureExt;
use crate::TestServer;
use celeriant_msg::request::{
    read_filters::ReadFilters,
    requests::{
        AggregateDetailsRequest, DeleteRequest, ReadRequest, SingleAggregateDelete,
        SingleAggregateWrite, WriteRequest,
    },
};
use celeriant_wal::{
    aggregate_key::AggregateKey,
    datablocks::datablock_aggregate_event::DatablockAggregateEvent,
};
use tokio::time::Duration;

fn make_event(client_seq: u64, payload: &str) -> DatablockAggregateEvent {
    DatablockAggregateEvent {
        client_seq,
        event_seq: 0,
        event_id: Some(rand::random()),
        event_timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
        event_type_major: 1,
        event_type_minor: 0,
        event_value: Arc::new(payload.as_bytes().to_vec()),
        iv: None,
    }
}

fn pass(name: &str) {
    println!("  PASS: {}", name);
}

/// Every `fail()` is counted so `run()` can exit non-zero. The runner decides
/// pass/fail purely on child exit status, so a printed FAIL that does not reach
/// here is a test that cannot go red.
static FAILURES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

fn fail(name: &str, err: impl std::fmt::Debug) {
    println!("  FAIL: {} -- {:?}", name, err);
    FAILURES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

// ─── Test 1: pool.write() ─────────────────────────────────────────────────────

async fn test_pool_write(pool: &CeleriantPool) {
    let agg = AggregateKey::new(20, 1, 2001);
    let mut writes = HashMap::new();
    writes.insert(
        agg,
        SingleAggregateWrite {
            events: vec![make_event(0, "pool write")],
            allow_create: true,
            expected_version: Some(0),
            enforce_client_idempotency: false,
        },
    );
    let req = WriteRequest { correlation_id: Some(1), client_id: 1, user_id: None, writes };
    match pool.write(req).await {
        Ok(_) => pass("pool.write()"),
        Err(e) => fail("pool.write()", e),
    }
}

// ─── Test 2: pool.write_events(, 0) ─────────────────────────────────────────────

async fn test_pool_write_events(pool: &CeleriantPool) {
    let agg = AggregateKey::new(20, 1, 2002);
    let events = vec![make_event(0, "pool write_events")];
    match pool.write_events(agg.clone(), events, 0).await {
        Ok(_) => pass("pool.write_events(, 0)"),
        Err(e) => fail("pool.write_events(, 0)", e),
    }
}

// ─── Test 3: pool.read() ─────────────────────────────────────────────────────

async fn test_pool_read(pool: &CeleriantPool) {
    let agg = AggregateKey::new(20, 1, 2003);

    // Write first
    let _ = pool.write_events(agg.clone(), vec![make_event(0, "read me back via pool")], 0).await;

    let req = ReadRequest {
        correlation_id: None,
        aggregate_key: agg,
        filters: ReadFilters::new(1),
    };
    match pool.read(req).await {
        Ok(resp) if !resp.event_batches.is_empty() => pass("pool.read()"),
        Ok(resp) => fail("pool.read() -- expected batches", resp),
        Err(e) => fail("pool.read()", e),
    }
}

// ─── Test 4: pool.aggregate_details() ────────────────────────────────────────

async fn test_pool_aggregate_details(pool: &CeleriantPool) {
    let agg = AggregateKey::new(20, 1, 2004);

    let _ = pool.write_events(agg.clone(), vec![make_event(0, "details test")], 0).await;

    let req = AggregateDetailsRequest { correlation_id: None, aggregate_key: agg };
    match pool.aggregate_details(req).await {
        Ok(details) if !details.is_deleted => pass("pool.aggregate_details()"),
        Ok(details) => fail("pool.aggregate_details() -- unexpected values", details),
        Err(e) => fail("pool.aggregate_details()", e),
    }
}

// ─── Test 5: pool.delete() ────────────────────────────────────────────────────

async fn test_pool_delete(pool: &CeleriantPool) {
    let agg = AggregateKey::new(20, 1, 2005);

    let _ = pool.write_events(agg.clone(), vec![make_event(0, "to delete via pool")], 0).await;

    let mut deletes = HashMap::new();
    deletes.insert(
        agg.clone(),
        SingleAggregateDelete {
            allow_recreate: false,
            allow_sequence_continuation: false,
            expected_version: None,
        },
    );
    let req = DeleteRequest { correlation_id: None, client_id: 1, user_id: None, deletes };

    match pool.delete(req).await {
        Ok(_) => {
            // Verify aggregate is gone
            let details_req = AggregateDetailsRequest {
                correlation_id: None,
                aggregate_key: agg,
            };
            match pool.aggregate_details(details_req).await {
                Ok(details) if details.is_deleted => pass("pool.delete()"),
                Ok(_) => fail("pool.delete() -- aggregate still appears undeleted", ()),
                // AggregateNotExists is also valid after delete
                Err(_) => pass("pool.delete()"),
            }
        }
        Err(e) => fail("pool.delete()", e),
    }
}

// ─── Test 6: pool.read_all() / PooledReadAllIterator ─────────────────────────

async fn test_pool_read_all(pool: &CeleriantPool) {
    let agg = AggregateKey::new(20, 1, 2006);

    // Write 4 batches
    for i in 0u64..4 {
        use celeriant_client_tokio::WriteEventsOptions;
        let opts = WriteEventsOptions {
            allow_create: i == 0,
            expected_version: Some(i),
            enforce_client_idempotency: false,
        };
        let events = vec![make_event(i, &format!("read_all batch {}", i))];
        if let Err(e) = pool.write_events_with(agg.clone(), events, 1, opts).await {
            fail("pool.read_all() setup write", e);
            return;
        }
    }

    match pool.read_all(agg, None).await {
        Ok(iter) => match iter.collect().await {
            Ok(batches) if batches.len() == 4 => pass("pool.read_all() / PooledReadAllIterator"),
            Ok(batches) => fail("pool.read_all() -- expected 4 batches", batches.len()),
            Err(e) => fail("pool.read_all() collect", e),
        },
        Err(e) => fail("pool.read_all()", e),
    }
}

// ─── Test 7: connection reuse — two sequential requests ───────────────────────

async fn test_connection_reuse(pool: &CeleriantPool) {
    let agg = AggregateKey::new(20, 1, 2007);

    // First request creates the aggregate
    match pool.write_events(agg.clone(), vec![make_event(0, "first request")], 0).await {
        Ok(_) => {}
        Err(e) => { fail("connection reuse -- first write", e); return; }
    }

    // Second request reuses the pooled connection
    let req = ReadRequest {
        correlation_id: None,
        aggregate_key: agg,
        filters: ReadFilters::new(1),
    };
    match pool.read(req).await {
        Ok(resp) if !resp.event_batches.is_empty() => pass("connection reuse"),
        Ok(_) => fail("connection reuse -- empty read response", ()),
        Err(e) => fail("connection reuse -- second request", e),
    }
}

// ─── Test 8: pool.get_connection() ───────────────────────────────────────────

async fn test_get_connection(pool: &CeleriantPool) {
    let agg = AggregateKey::new(20, 1, 2008);

    let _ = pool.write_events(agg.clone(), vec![make_event(0, "get_connection test")], 0).await;

    match pool.get_connection().await {
        Ok(mut conn) => {
            let req = ReadRequest {
                correlation_id: None,
                aggregate_key: agg,
                filters: ReadFilters::new(1),
            };
            match conn.client().read(req).await {
                Ok(resp) if !resp.event_batches.is_empty() => pass("pool.get_connection()"),
                Ok(_) => fail("pool.get_connection() -- empty read", ()),
                Err(e) => fail("pool.get_connection() read", e),
            }
            // conn drops here, returning to pool
        }
        Err(e) => fail("pool.get_connection()", e),
    }
}

// ─── Test 9: pool.get_leader_connection() ────────────────────────────────────

async fn test_get_leader_connection(pool: &CeleriantPool) {
    let agg = AggregateKey::new(20, 1, 2009);

    match pool.get_leader_connection().await {
        Ok(mut conn) => {
            // Write through the leader connection directly
            let mut writes = HashMap::new();
            writes.insert(
                agg,
                SingleAggregateWrite {
                    events: vec![make_event(0, "leader connection write")],
                    allow_create: true,
                    expected_version: Some(0),
                    enforce_client_idempotency: false,
                },
            );
            let req = WriteRequest { correlation_id: None, client_id: 1, user_id: None, writes };
            match conn.client().write(req).await {
                Ok(_) => pass("pool.get_leader_connection()"),
                Err(e) => fail("pool.get_leader_connection() write", e),
            }
        }
        Err(e) => fail("pool.get_leader_connection()", e),
    }
}

// ─── Test 10: custom pool options — non-default request_timeout ───────────────

async fn test_custom_pool_options(address: &str) {
    let options = PoolOptions::new(address)
        .with_request_timeout(Duration::from_secs(60))
        .with_max_connections(5);
    let pool = CeleriantPool::new(options);

    let agg = AggregateKey::new(20, 1, 2010);
    match pool.write_events(agg, vec![make_event(0, "custom options write")], 0).await {
        Ok(_) => pass("custom pool options (request_timeout=60s)"),
        Err(e) => fail("custom pool options", e),
    }
}

// ─── Test 11: auto-compression via pool ──────────────────────────────────────

async fn test_pool_auto_compression(pool: &CeleriantPool) {
    let agg = AggregateKey::new(20, 1, 2011);
    // Payload above the default 1024-byte auto-compression threshold
    let large_payload = "x".repeat(2048);
    let events = vec![make_event(0, &large_payload)];
    match pool.write_events(agg, events, 0).await {
        Ok(_) => pass("pool auto-compression -- large payload accepted"),
        Err(e) => fail("pool auto-compression", e),
    }
}

// ─── Test 12: pool.list_orgs() ────────────────────────────────────────────────

async fn test_pool_list_orgs(pool: &CeleriantPool) {
    // Ensure org 20 exists from prior writes in this test run
    match pool.list_orgs(ListOptions::default()).await {
        Ok(iter) => match iter.collect().await {
            Ok(orgs) => {
                if orgs.iter().any(|o| o.org_id == 20) {
                    pass("pool.list_orgs()");
                } else {
                    fail("pool.list_orgs() -- org_id 20 not found", orgs.len());
                }
            }
            Err(e) => fail("pool.list_orgs() collect", e),
        },
        Err(e) => fail("pool.list_orgs()", e),
    }
}

// ─── Test 13: pool.list_aggregate_types() ────────────────────────────────────

async fn test_pool_list_aggregate_types(pool: &CeleriantPool) {
    match pool.list_aggregate_types(Some(20), ListOptions::default()).await {
        Ok(iter) => match iter.collect().await {
            Ok(types) => {
                if types.iter().any(|t| t.aggregate_type_id == 1) {
                    pass("pool.list_aggregate_types()");
                } else {
                    fail("pool.list_aggregate_types() -- aggregate_type_id 1 not found", types.len());
                }
            }
            Err(e) => fail("pool.list_aggregate_types() collect", e),
        },
        Err(e) => fail("pool.list_aggregate_types()", e),
    }
}

// ─── Test 14: pool.list_aggregates() ─────────────────────────────────────────

async fn test_pool_list_aggregates(pool: &CeleriantPool) {
    match pool.list_aggregates(Some(20), None, ListOptions::default()).await {
        Ok(iter) => match iter.collect().await {
            Ok(aggs) => {
                // Several aggregates with org_id=20 were written in prior tests
                if !aggs.is_empty() {
                    pass("pool.list_aggregates()");
                } else {
                    fail("pool.list_aggregates() -- expected at least one aggregate", ());
                }
            }
            Err(e) => fail("pool.list_aggregates() collect", e),
        },
        Err(e) => fail("pool.list_aggregates()", e),
    }
}

// ─── Cancellation Safety ──────────────────────────────────────────────────────
//
// A request future dropped after its bytes are written but before its response
// frame is read leaves an undrained frame on the socket. Contract: a connection
// whose request/response exchange did not complete must never be reused, so the
// next borrower can never receive the cancelled request's answer.
//
// Cancellation is modelled with a single synchronous poll, not a wall-clock
// timeout: one poll drives the client through write_frame and parks it on the
// response read, and no server can round-trip inside that poll.

/// Writes `versions` single-event batches so the aggregate ends up at
/// `max_aggregate_version == versions`.
async fn seed_versions(
    pool: &CeleriantPool,
    agg: &AggregateKey,
    versions: u64,
) -> Result<(), ClientError> {
    use celeriant_client_tokio::WriteEventsOptions;
    for i in 0..versions {
        let opts = WriteEventsOptions {
            allow_create: i == 0,
            expected_version: Some(i),
            enforce_client_idempotency: false,
        };
        let events = vec![make_event(i, "cancellation seed")];
        pool.write_events_with(agg.clone(), events, 2, opts).await?;
    }
    Ok(())
}

fn details_req(agg: &AggregateKey) -> AggregateDetailsRequest {
    AggregateDetailsRequest { correlation_id: None, aggregate_key: agg.clone() }
}

/// Polls an `aggregate_details` for `agg` exactly once on a pooled lease, then
/// drops the lease. Returns false if the caller's assertion would be vacuous:
/// either the poll resolved (nothing was cancelled) or the request never
/// actually reached the socket.
async fn cancel_after_one_poll(pool: &CeleriantPool, agg: &AggregateKey) -> Result<(), String> {
    let mut conn = pool.get_connection().await.map_err(|e| format!("no lease: {e:?}"))?;
    if conn.client().aggregate_details(details_req(agg)).now_or_never().is_some() {
        return Err("request resolved inside one poll -- nothing was cancelled".into());
    }
    // The client refuses to write on a stream it knows is one frame behind, so
    // this Err is proof the request went out and was abandoned mid-flight —
    // without it, a poll that parked before writing would pass silently.
    match conn.client().aggregate_details(details_req(agg)).await {
        Err(ClientError::ProtocolError) => Ok(()),
        other => Err(format!(
            "the cancelled request never reached the socket -- reuse gave {other:?}"
        )),
    }
}

// ─── Test 15: a cancelled request must not leak its response to the next borrower ───

async fn test_cancelled_request_does_not_leak_response(address: &str) {
    let name = "cancelled request -- next borrower gets its own response";
    let pool = CeleriantPool::new(PoolOptions::new(address).with_max_connections(1));

    let cancelled_agg = AggregateKey::new(21, 1, 3001);
    let next_agg = AggregateKey::new(21, 1, 3002);
    if let Err(e) = seed_versions(&pool, &cancelled_agg, 5).await {
        fail(name, e);
        return;
    }
    if let Err(e) = seed_versions(&pool, &next_agg, 1).await {
        fail(name, e);
        return;
    }

    // Warm the pool: a completed request leaves a connection in the free list so
    // the single poll below starts at write_frame, not at connect.
    if let Err(e) = pool.aggregate_details(details_req(&cancelled_agg)).await {
        fail(name, e);
        return;
    }

    if let Err(why) = cancel_after_one_poll(&pool, &cancelled_agg).await {
        fail(name, why);
        return;
    }

    match pool.aggregate_details(details_req(&next_agg)).await {
        Ok(d) if d.max_aggregate_version == 1 => pass(name),
        Ok(d) => fail(
            name,
            format!(
                "asked for aggregate 3002 (version 1), got version {} -- the cancelled request's aggregate 3001 is version 5",
                d.max_aggregate_version
            ),
        ),
        Err(e) => fail(name, e),
    }
}

// ─── Test 16: the pool keeps working after a cancellation ────────────────────

async fn test_pool_usable_after_cancellation(address: &str) {
    let name = "pool serves correct responses to every borrower after a cancellation";
    let pool = CeleriantPool::new(PoolOptions::new(address).with_max_connections(1));

    let cancelled_agg = AggregateKey::new(21, 1, 3010);
    let expected: [(AggregateKey, u64); 4] = [
        (AggregateKey::new(21, 1, 3011), 1),
        (AggregateKey::new(21, 1, 3012), 2),
        (AggregateKey::new(21, 1, 3013), 3),
        (AggregateKey::new(21, 1, 3014), 4),
    ];

    if let Err(e) = seed_versions(&pool, &cancelled_agg, 9).await {
        fail(name, e);
        return;
    }
    for (agg, versions) in &expected {
        if let Err(e) = seed_versions(&pool, agg, *versions).await {
            fail(name, e);
            return;
        }
    }

    if let Err(e) = pool.aggregate_details(details_req(&cancelled_agg)).await {
        fail(name, e);
        return;
    }

    if let Err(why) = cancel_after_one_poll(&pool, &cancelled_agg).await {
        fail(name, why);
        return;
    }

    // The bug cascades: every borrower after the first inherits the offset.
    for (agg, versions) in &expected {
        match pool.aggregate_details(details_req(agg)).await {
            Ok(d) if d.max_aggregate_version == *versions => {}
            Ok(d) => {
                fail(
                    name,
                    format!(
                        "asked for aggregate {} (version {}), got version {}",
                        agg.aggregate_id, versions, d.max_aggregate_version
                    ),
                );
                return;
            }
            Err(e) => {
                fail(name, e);
                return;
            }
        }
    }
    pass(name);
}

// ─── Test 17: a cancelled request must not crosstalk within its own lease ────
//
// `PooledConnection::drop` is not enough on its own. `PooledReadAllIterator` and
// the pooled list iterators own one lease across an entire pagination, so a
// cancellation there is never seen by the drop guard.

async fn test_same_lease_reuse_after_cancellation(address: &str) {
    let name = "same-lease reuse after cancellation";
    let pool = CeleriantPool::new(PoolOptions::new(address).with_max_connections(1));

    let cancelled_agg = AggregateKey::new(21, 1, 3101);
    let next_agg = AggregateKey::new(21, 1, 3102);
    if let Err(e) = seed_versions(&pool, &cancelled_agg, 7).await {
        fail(name, e);
        return;
    }
    if let Err(e) = seed_versions(&pool, &next_agg, 1).await {
        fail(name, e);
        return;
    }

    let mut conn = match pool.get_connection().await {
        Ok(c) => c,
        Err(e) => { fail(name, e); return; }
    };
    if let Err(e) = conn.client().aggregate_details(details_req(&cancelled_agg)).await {
        fail(name, e);
        return;
    }

    let polled = conn.client().aggregate_details(details_req(&cancelled_agg)).now_or_never();
    if polled.is_some() {
        fail(name, "request resolved inside one poll -- nothing was cancelled");
        return;
    }

    // A refusal, not an answer. Answering at all means reading 3101's response.
    // Twice: a guard that clears the flag on its way out would answer the first
    // refusal and then silently crosstalk on the second.
    if !matches!(
        conn.client().aggregate_details(details_req(&next_agg)).await,
        Err(ClientError::ProtocolError)
    ) {
        fail(name, "first reuse after cancellation was not refused");
        return;
    }
    match conn.client().aggregate_details(details_req(&next_agg)).await {
        Err(ClientError::ProtocolError) => pass(name),
        Ok(d) => fail(
            name,
            format!(
                "same-lease crosstalk: asked for 3102 (version 1), got version {} -- 3101 is version 7",
                d.max_aggregate_version
            ),
        ),
        Err(e) => fail(name, format!("expected ProtocolError, got {:?}", e)),
    }
}

// ─── Main ─────────────────────────────────────────────────────────────────────


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Connection Pool Integration Tests ===\n");

    let port = 10600 + (std::process::id() % 100) as u16;

    println!("Starting test server...");
    let server = TestServer::start_with_port(port).await?;
    println!("Server started at {}\n", server.address());

    let pool = CeleriantPool::new(PoolOptions::new(server.address()));

    println!("--- Basic Pool Operations ---");
    test_pool_write(&pool).await;
    test_pool_write_events(&pool).await;
    test_pool_read(&pool).await;
    test_pool_aggregate_details(&pool).await;
    test_pool_delete(&pool).await;
    test_pool_read_all(&pool).await;

    println!("\n--- Connection Lifecycle ---");
    test_connection_reuse(&pool).await;
    test_get_connection(&pool).await;
    test_get_leader_connection(&pool).await;

    println!("\n--- Pool Configuration ---");
    test_custom_pool_options(server.address()).await;
    test_pool_auto_compression(&pool).await;

    println!("\n--- List Operations ---");
    test_pool_list_orgs(&pool).await;
    test_pool_list_aggregate_types(&pool).await;
    test_pool_list_aggregates(&pool).await;

    println!("\n--- Cancellation Safety ---");
    test_cancelled_request_does_not_leak_response(server.address()).await;
    test_pool_usable_after_cancellation(server.address()).await;
    test_same_lease_reuse_after_cancellation(server.address()).await;


    let failures = FAILURES.load(std::sync::atomic::Ordering::Relaxed);
    if failures > 0 {
        println!("\n=== {} test(s) FAILED ===", failures);
        return Err(format!("{} pool test(s) failed", failures).into());
    }

    println!("\n=== All tests completed ===");
    Ok(())
}
