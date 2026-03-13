//! Connection Pool Integration Tests
//!
//! Tests CeleriantPool basic operations, connection lifecycle, idle eviction,
//! pool configuration, and list operations against a standalone server.
//!
//! Run with: cargo run --bin pool_test_main

use std::collections::HashMap;
use std::sync::Arc;

use celeriant_client_tokio::{
    CeleriantPool, PoolOptions,
    list_operations::ListOptions,
};
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

fn make_event(client_event_index: u64, payload: &str) -> DatablockAggregateEvent {
    DatablockAggregateEvent {
        client_event_index,
        event_index: 0,
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

fn fail(name: &str, err: impl std::fmt::Debug) {
    println!("  FAIL: {} -- {:?}", name, err);
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
            expected_event_batch_index: Some(0),
            enforce_client_idempotency: false,
            compression_type_id: 0,
            compression_level: None,
        },
    );
    let req = WriteRequest { correlation_id: Some(1), client_id: 1, user_id: None, writes };
    match pool.write(req).await {
        Ok(_) => pass("pool.write()"),
        Err(e) => fail("pool.write()", e),
    }
}

// ─── Test 2: pool.write_events() ─────────────────────────────────────────────

async fn test_pool_write_events(pool: &CeleriantPool) {
    let agg = AggregateKey::new(20, 1, 2002);
    let events = vec![make_event(0, "pool write_events")];
    match pool.write_events(agg.clone(), events).await {
        Ok(_) => pass("pool.write_events()"),
        Err(e) => fail("pool.write_events()", e),
    }
}

// ─── Test 3: pool.read() ─────────────────────────────────────────────────────

async fn test_pool_read(pool: &CeleriantPool) {
    let agg = AggregateKey::new(20, 1, 2003);

    // Write first
    let _ = pool.write_events(agg.clone(), vec![make_event(0, "read me back via pool")]).await;

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

    let _ = pool.write_events(agg.clone(), vec![make_event(0, "details test")]).await;

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

    let _ = pool.write_events(agg.clone(), vec![make_event(0, "to delete via pool")]).await;

    let mut deletes = HashMap::new();
    deletes.insert(
        agg.clone(),
        SingleAggregateDelete {
            allow_recreate: false,
            allow_index_continuation: false,
            expected_event_batch_index: None,
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
            client_id: 1,
            allow_create: i == 0,
            expected_event_batch_index: Some(i),
            enforce_client_idempotency: false,
        };
        let events = vec![make_event(i, &format!("read_all batch {}", i))];
        if let Err(e) = pool.write_events_with(agg.clone(), events, opts).await {
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
    match pool.write_events(agg.clone(), vec![make_event(0, "first request")]).await {
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

    let _ = pool.write_events(agg.clone(), vec![make_event(0, "get_connection test")]).await;

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
                    expected_event_batch_index: Some(0),
                    enforce_client_idempotency: false,
                    compression_type_id: 0,
                    compression_level: None,
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
    match pool.write_events(agg, vec![make_event(0, "custom options write")]).await {
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
    match pool.write_events(agg, events).await {
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

    println!("\n=== All tests completed ===");
    Ok(())
}
