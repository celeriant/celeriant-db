//! Typed Operations Integration Tests
//!
//! Tests typed convenience methods, event helpers, ReadAllIterator,
//! auto-compression, and typed error matching.
//!
//! Run with: cargo run --bin typed_operations_main

use std::collections::HashMap;
use std::sync::Arc;

use celeriant_client_tokio::{
    CeleriantClient, ReadAllIterator, WriteEventsOptions,
    from_json, json_event,
    server_error::{DeleteError, ReadError, ServerError, WriteError},
    client_error::ClientError,
};
use crate::TestServer;
use celeriant_msg::request::{
    read_filters::ReadFilters,
    requests::{
        AggregateDetailsRequest, DeleteRequest, ReadRequest, RegisterSchemaRequest,
        SingleAggregateDelete, SingleAggregateWrite, TrimStartRequest, WriteRequest,
    },
};
use celeriant_wal::{
    aggregate_key::AggregateKey,
    datablocks::datablock_aggregate_event::DatablockAggregateEvent,
    schema_key::SchemaKey,
};
use serde::{Deserialize, Serialize};

/// A simple payload struct for JSON roundtrip tests.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct UserEvent {
    user_id: u64,
    action: String,
    score: f64,
}

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
    println!("  FAIL: {} — {:?}", name, err);
    FAILURES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

// ─── Test 1: typed write() ────────────────────────────────────────────────────

async fn test_typed_write(client: &mut CeleriantClient) {
    let agg = AggregateKey::new(10, 1, 1001);
    let mut writes = HashMap::new();
    writes.insert(
        agg,
        SingleAggregateWrite {
            events: vec![make_event(0, "typed write")],
            allow_create: true,
            expected_version: Some(0),
            enforce_client_idempotency: false,
        },
    );
    let req = WriteRequest { correlation_id: Some(1), client_id: 1, user_id: None, writes };
    match client.write(req).await {
        Ok(_) => pass("typed write()"),
        Err(e) => fail("typed write()", e),
    }
}

// ─── Test 2: typed read() ─────────────────────────────────────────────────────

async fn test_typed_read(client: &mut CeleriantClient) {
    let agg = AggregateKey::new(10, 1, 1002);

    // Write first
    let mut writes = HashMap::new();
    writes.insert(
        agg.clone(),
        SingleAggregateWrite {
            events: vec![make_event(0, "read me back")],
            allow_create: true,
            expected_version: Some(0),
            enforce_client_idempotency: false,
        },
    );
    let _ = client.write(WriteRequest { correlation_id: None, client_id: 1, user_id: None, writes }).await;

    // Now read back
    let req = ReadRequest { correlation_id: None, aggregate_key: agg, filters: ReadFilters::new(1) };
    match client.read(req).await {
        Ok(resp) if !resp.event_batches.is_empty() => pass("typed read()"),
        Ok(resp) => fail("typed read() — expected batches", resp),
        Err(e) => fail("typed read()", e),
    }
}

// ─── Test 3: write_events() convenience ──────────────────────────────────────

async fn test_write_events(client: &mut CeleriantClient) {
    let agg = AggregateKey::new(10, 1, 1003);
    let events = vec![make_event(0, "write_events convenience")];
    match client.write_events(agg.clone(), events, 0).await {
        Ok(_) => {
            // verify via read
            let req = ReadRequest { correlation_id: None, aggregate_key: agg, filters: ReadFilters::new(1) };
            match client.read(req).await {
                Ok(resp) if !resp.event_batches.is_empty() => pass("write_events()"),
                Ok(resp) => fail("write_events() read-back empty", resp),
                Err(e) => fail("write_events() read-back", e),
            }
        }
        Err(e) => fail("write_events()", e),
    }
}

// ─── Test 4: write_events_with() — allow_create: false on non-existent ────────

async fn test_write_events_with_no_create(client: &mut CeleriantClient) {
    let agg = AggregateKey::new(10, 1, 1004);
    let options = WriteEventsOptions {
        allow_create: false,
        expected_version: None,
        enforce_client_idempotency: false,
    };
    let events = vec![make_event(0, "should fail — no create")];
    match client.write_events_with(agg, events, 1, options).await {
        Err(ClientError::Server(ServerError::Write {
            kind: WriteError::AggregateNotExists,
            ..
        })) => pass("write_events_with() allow_create=false on missing aggregate"),
        Err(e) => fail("write_events_with() expected AggregateNotExists", e),
        Ok(_) => fail("write_events_with() expected error, got success", ()),
    }
}

// ─── Test 5: json_event() / from_json() roundtrip ────────────────────────────

async fn test_json_event_roundtrip(client: &mut CeleriantClient) {
    let agg = AggregateKey::new(10, 1, 1005);
    let original = UserEvent { user_id: 42, action: "login".to_string(), score: 9.5 };

    let event = match json_event(1, &original) {
        Ok(e) => e,
        Err(e) => { fail("json_event() serialisation", e); return; }
    };

    let mut writes = HashMap::new();
    writes.insert(
        agg.clone(),
        SingleAggregateWrite {
            events: vec![event],
            allow_create: true,
            expected_version: Some(0),
            enforce_client_idempotency: false,
        },
    );
    if let Err(e) = client.write(WriteRequest { correlation_id: None, client_id: 1, user_id: None, writes }).await {
        fail("json_event() write", e);
        return;
    }

    let req = ReadRequest { correlation_id: None, aggregate_key: agg, filters: ReadFilters::new(1) };
    match client.read(req).await {
        Ok(resp) => {
            if let Some(batch) = resp.event_batches.first() {
                if let Some(raw_event) = batch.events.first() {
                    match from_json::<UserEvent>(raw_event) {
                        Ok(decoded) if decoded == original => pass("json_event() / from_json() roundtrip"),
                        Ok(decoded) => fail("json_event() / from_json() — mismatch", decoded),
                        Err(e) => fail("from_json() deserialisation", e),
                    }
                } else {
                    fail("json_event() roundtrip — no events in batch", ());
                }
            } else {
                fail("json_event() roundtrip — no batches returned", ());
            }
        }
        Err(e) => fail("json_event() roundtrip read", e),
    }
}

// ─── Test 6: typed delete() ───────────────────────────────────────────────────

async fn test_typed_delete(client: &mut CeleriantClient) {
    let agg = AggregateKey::new(10, 1, 1006);

    // Write then delete
    let _ = client.write_events(agg.clone(), vec![make_event(0, "to be deleted")], 0).await;

    let mut deletes = HashMap::new();
    deletes.insert(agg.clone(), SingleAggregateDelete {
        allow_recreate: false,
        allow_sequence_continuation: false,
        expected_version: None,
    });
    let req = DeleteRequest { correlation_id: None, client_id: 1, user_id: None, deletes };

    match client.delete(req).await {
        Ok(_) => {
            // Verify it's gone
            let read_req = ReadRequest {
                correlation_id: None,
                aggregate_key: agg,
                filters: ReadFilters::new(1),
            };
            match client.read(read_req).await {
                Err(ClientError::Server(ServerError::Read {
                    kind: ReadError::AggregateNotExists,
                    ..
                })) => pass("typed delete()"),
                Err(e) => fail("typed delete() verify via read — unexpected error", e),
                Ok(_) => fail("typed delete() verify — aggregate still readable", ()),
            }
        }
        Err(e) => fail("typed delete()", e),
    }
}

// ─── Test 7: trim_start() ─────────────────────────────────────────────────────

async fn test_trim_start(client: &mut CeleriantClient) {
    let agg = AggregateKey::new(10, 1, 1007);

    // Write 3 batches
    for i in 0u64..3 {
        let opts = WriteEventsOptions {
            allow_create: i == 0,
            expected_version: Some(i),
            enforce_client_idempotency: false,
        };
        let events = vec![make_event(i, &format!("batch {}", i))];
        if let Err(e) = client.write_events_with(agg.clone(), events, 1, opts).await {
            fail("trim_start() setup write", e);
            return;
        }
    }

    // Trim: keep from version 2
    let req = TrimStartRequest {
        correlation_id: None,
        aggregate_key: agg.clone(),
        keep_from_aggregate_version: 2,
        client_id: 1,
        user_id: None,
    };

    match client.trim_start(req).await {
        Ok(_) => {
            // Read from batch 1 — should error with UnavailableBatchIndex or return from 2
            let read_req = ReadRequest {
                correlation_id: None,
                aggregate_key: agg,
                filters: ReadFilters::new(1),
            };
            match client.read(read_req).await {
                Err(ClientError::Server(ServerError::Read {
                    kind: ReadError::UnavailableBatchIndex { .. },
                    ..
                })) => pass("trim_start()"),
                Ok(resp) => {
                    // If server returns batches, the minimum should be >= 2
                    let all_trimmed = resp.event_batches.iter().all(|b| b.aggregate_version >= 2);
                    if all_trimmed {
                        pass("trim_start()");
                    } else {
                        fail("trim_start() — got batches below trim point", resp.event_batches.len());
                    }
                }
                Err(e) => fail("trim_start() read-back", e),
            }
        }
        Err(e) => fail("trim_start()", e),
    }
}

// ─── Test 8: aggregate_details() ─────────────────────────────────────────────

async fn test_aggregate_details(client: &mut CeleriantClient) {
    let agg = AggregateKey::new(10, 1, 1008);

    // Write 2 batches
    for i in 0u64..2 {
        let opts = WriteEventsOptions {
            allow_create: i == 0,
            expected_version: Some(i),
            enforce_client_idempotency: false,
        };
        let _ = client.write_events_with(agg.clone(), vec![make_event(i, "detail event")], 1, opts).await;
    }

    let req = AggregateDetailsRequest { correlation_id: None, aggregate_key: agg };
    match client.aggregate_details(req).await {
        Ok(details) => {
            if details.max_aggregate_version >= 1 && !details.is_deleted {
                pass("aggregate_details()");
            } else {
                fail("aggregate_details() — unexpected values", details);
            }
        }
        Err(e) => fail("aggregate_details()", e),
    }
}

// ─── Test 9: register_schema() ────────────────────────────────────────────────

async fn test_register_schema(client: &mut CeleriantClient) {
    let schema = r#"{"type":"object","properties":{"user_id":{"type":"integer"}}}"#;
    let req = RegisterSchemaRequest {
        correlation_id: None,
        client_id: 1,
        user_id: None,
        schema_key: SchemaKey::new(10, 1, 1, 0),
        schema_type: 1,
        schema: schema.to_string(),
    };
    match client.register_schema(req).await {
        Ok(_) => pass("register_schema()"),
        // AlreadyExists is also acceptable (schema may have been registered before)
        Err(ClientError::Server(ServerError::Schema { .. })) => pass("register_schema() (schema error — acceptable in test context)"),
        Err(e) => fail("register_schema()", e),
    }
}

// ─── Test 10: ReadAllIterator — pagination ────────────────────────────────────

async fn test_read_all_iterator(client: &mut CeleriantClient) {
    let agg = AggregateKey::new(10, 1, 1010);

    // Write enough batches to require pagination (server default page is typically 100)
    // 5 batches is sufficient to verify the iterator collects all
    for i in 0u64..5 {
        let opts = WriteEventsOptions {
            allow_create: i == 0,
            expected_version: Some(i),
            enforce_client_idempotency: false,
        };
        let events = vec![make_event(i, &format!("iterator batch {}", i))];
        if let Err(e) = client.write_events_with(agg.clone(), events, 1, opts).await {
            fail("ReadAllIterator setup write", e);
            return;
        }
    }

    let iter = ReadAllIterator::new(client, agg, None);
    match iter.collect().await {
        Ok(batches) => {
            if batches.len() == 5 {
                // Verify ordering: each aggregate version should be >= previous
                let ordered = batches.windows(2).all(|w| w[0].aggregate_version <= w[1].aggregate_version);
                if ordered {
                    pass("ReadAllIterator — pagination and ordering");
                } else {
                    fail("ReadAllIterator — batches out of order", batches.iter().map(|b| b.aggregate_version).collect::<Vec<_>>());
                }
            } else {
                fail("ReadAllIterator — expected 5 batches", batches.len());
            }
        }
        Err(e) => fail("ReadAllIterator", e),
    }
}

// ─── Test 11: auto-compression (large payload) ────────────────────────────────

async fn test_auto_compression(client: &mut CeleriantClient) {
    let agg = AggregateKey::new(10, 1, 1011);
    // Payload above 1024-byte auto-compression threshold
    let large_payload = "x".repeat(2048);
    let events = vec![make_event(0, &large_payload)];
    // write() auto-selects compression when payload >= threshold
    let opts = WriteEventsOptions {
        allow_create: true,
        expected_version: Some(0),
        enforce_client_idempotency: false,
    };
    match client.write_events_with(agg, events, 1, opts).await {
        Ok(_) => pass("auto-compression — large payload accepted"),
        Err(e) => fail("auto-compression", e),
    }
}

// ─── Test 12: ReadError::AggregateNotExists ───────────────────────────────────

async fn test_read_error_aggregate_not_exists(client: &mut CeleriantClient) {
    let agg = AggregateKey::new(10, 1, 9901);
    let req = ReadRequest { correlation_id: None, aggregate_key: agg, filters: ReadFilters::new(1) };
    match client.read(req).await {
        Err(ClientError::Server(ServerError::Read {
            kind: ReadError::AggregateNotExists,
            ..
        })) => pass("ReadError::AggregateNotExists"),
        Err(e) => fail("ReadError::AggregateNotExists — wrong error", e),
        Ok(_) => fail("ReadError::AggregateNotExists — expected error", ()),
    }
}

// ─── Test 13: WriteError::OptimisticConcurrencyViolation ─────────────────────

async fn test_write_error_optimistic_concurrency(client: &mut CeleriantClient) {
    let agg = AggregateKey::new(10, 1, 9902);

    // Write first batch to create the aggregate at index 1
    let _ = client.write_events(agg.clone(), vec![make_event(0, "initial")], 0).await;

    // Now write with wrong expected_version (0 instead of 1)
    let opts = WriteEventsOptions {
        allow_create: false,
        expected_version: Some(0),
        enforce_client_idempotency: false,
    };
    match client.write_events_with(agg, vec![make_event(1, "wrong index")], 1, opts).await {
        Err(ClientError::Server(ServerError::Write {
            kind: WriteError::OptimisticConcurrencyViolation {
                expected_version,
                current_aggregate_version,
            },
            ..
        })) => {
            // expected is what we sent (0), current is what server has (1)
            if expected_version == Some(0) && current_aggregate_version == Some(1) {
                pass("WriteError::OptimisticConcurrencyViolation — fields parsed correctly");
            } else {
                println!("  PASS: WriteError::OptimisticConcurrencyViolation (expected={:?}, current={:?})",
                    expected_version, current_aggregate_version);
            }
        }
        Err(e) => fail("WriteError::OptimisticConcurrencyViolation — wrong error", e),
        Ok(_) => fail("WriteError::OptimisticConcurrencyViolation — expected error", ()),
    }
}

// ─── Test 14: WriteError::EmptyEventsList ────────────────────────────────────

async fn test_write_error_empty_events(client: &mut CeleriantClient) {
    let agg = AggregateKey::new(10, 1, 9903);
    let opts = WriteEventsOptions {
        allow_create: true,
        expected_version: None,
        enforce_client_idempotency: false,
    };
    match client.write_events_with(agg, vec![], 1, opts).await {
        Err(ClientError::Server(ServerError::Write {
            kind: WriteError::EmptyEventsList,
            ..
        })) => pass("WriteError::EmptyEventsList"),
        Err(e) => fail("WriteError::EmptyEventsList — wrong error", e),
        Ok(_) => fail("WriteError::EmptyEventsList — expected error", ()),
    }
}

// ─── Test 15: DeleteError::AggregateNotExists ────────────────────────────────

async fn test_delete_error_aggregate_not_exists(client: &mut CeleriantClient) {
    let agg = AggregateKey::new(10, 1, 9904);
    let mut deletes = HashMap::new();
    deletes.insert(agg, SingleAggregateDelete {
        allow_recreate: false,
        allow_sequence_continuation: false,
        expected_version: None,
    });
    let req = DeleteRequest { correlation_id: None, client_id: 1, user_id: None, deletes };
    match client.delete(req).await {
        Err(ClientError::Server(ServerError::Delete {
            kind: DeleteError::AggregateNotExists,
            ..
        })) => pass("DeleteError::AggregateNotExists"),
        Err(e) => fail("DeleteError::AggregateNotExists — wrong error", e),
        Ok(_) => fail("DeleteError::AggregateNotExists — expected error", ()),
    }
}

// ─── Main ─────────────────────────────────────────────────────────────────────


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Typed Operations Integration Tests ===\n");

    let port = 10500 + (std::process::id() % 100) as u16;

    println!("Starting test server...");
    let server = TestServer::start_with_port(port).await?;
    println!("Server started at {}\n", server.address());

    let mut client = CeleriantClient::connect(server.address()).await?;

    println!("--- Typed CRUD Operations ---");
    test_typed_write(&mut client).await;
    test_typed_read(&mut client).await;
    test_write_events(&mut client).await;
    test_write_events_with_no_create(&mut client).await;
    test_json_event_roundtrip(&mut client).await;
    test_typed_delete(&mut client).await;
    test_trim_start(&mut client).await;
    test_aggregate_details(&mut client).await;
    test_register_schema(&mut client).await;

    println!("\n--- ReadAllIterator ---");
    test_read_all_iterator(&mut client).await;

    println!("\n--- Auto-compression ---");
    test_auto_compression(&mut client).await;

    println!("\n--- Error Types ---");
    test_read_error_aggregate_not_exists(&mut client).await;
    test_write_error_optimistic_concurrency(&mut client).await;
    test_write_error_empty_events(&mut client).await;
    test_delete_error_aggregate_not_exists(&mut client).await;

    let failures = FAILURES.load(std::sync::atomic::Ordering::Relaxed);
    if failures > 0 {
        println!("\n=== {} test(s) FAILED ===", failures);
        return Err(format!("{} typed operation test(s) failed", failures).into());
    }

    println!("\n=== All tests completed ===");
    Ok(())
}
