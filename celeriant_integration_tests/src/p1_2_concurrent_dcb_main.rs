//! P1-2: Concurrent Multi-Aggregate Writes with Shared Aggregate (DCB Contention)
//!
//! Two clients race multi-aggregate writes that share a common aggregate.
//! Both expect the same version on the shared aggregate. Exactly one wins;
//! the loser's entire write (including their unique aggregate) is rejected.
//!
//! This validates that the two-phase commit logic correctly handles OCC
//! conflicts when multiple multi-aggregate writes race for the same aggregate.

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use celeriant_integration_tests::{count_events, ServerConfig, TestServer};
use celeriant_msg::process_requests::Request;
use celeriant_msg::request::requests::{SingleAggregateWrite, WriteRequest};
use celeriant_msg::process_responses::Response;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::compression_type::CompressionType;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Barrier;

const PORT_BASE: u16 = 18900;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== P1-2: Concurrent DCB Writes with Shared Aggregate ===\n");

    let port = PORT_BASE + (std::process::id() % 100) as u16;

    let config = ServerConfig {
        num_shards: Some(1),
        log_level: "warn".to_string(),
        standalone: true,
        ..Default::default()
    };
    let _server = TestServer::start_with_config(port, config).await?;

    let aggregate_x = AggregateKey::new(1, 1, 100); // Shared aggregate
    let aggregate_a = AggregateKey::new(1, 1, 101); // Client 1's unique aggregate
    let aggregate_b = AggregateKey::new(1, 1, 102); // Client 2's unique aggregate

    let addr = format!("127.0.0.1:{}", port);

    // Create all three aggregates with initial events
    println!("Creating shared aggregate X and unique aggregates A, B...");
    let mut init_client = CeleriantClient::connect(&addr).await?;

    for (key, label) in [
        (&aggregate_x, "X"),
        (&aggregate_a, "A"),
        (&aggregate_b, "B"),
    ] {
        let event = create_event(0, format!("Initial event for aggregate {}", label));
        let mut writes = HashMap::new();
        writes.insert(
            key.clone(),
            SingleAggregateWrite {
                events: vec![event],
                allow_create: true,
                expected_event_batch_index: Some(0),
                enforce_client_idempotency: false,
                compression_type: CompressionType::None,
            },
        );
        let request = Request::Write(WriteRequest {
            correlation_id: None,
            client_id: 999,
            user_id: None,
            writes,
        });
        init_client
            .send_request(&request, CompressionType::None)
            .await?;
    }

    // Verify initial state: all aggregates have 1 event
    let x_count = count_events(&mut init_client, &aggregate_x).await?;
    let a_count = count_events(&mut init_client, &aggregate_a).await?;
    let b_count = count_events(&mut init_client, &aggregate_b).await?;
    assert_eq!(x_count, 1, "Aggregate X should have 1 event initially");
    assert_eq!(a_count, 1, "Aggregate A should have 1 event initially");
    assert_eq!(b_count, 1, "Aggregate B should have 1 event initially");
    println!("  X: {} event, A: {} event, B: {} event", x_count, a_count, b_count);

    // Prepare two clients for concurrent writes
    println!("\nEstablishing 2 connections...");
    let mut client1 = CeleriantClient::connect(&addr).await?;
    let mut client2 = CeleriantClient::connect(&addr).await?;
    println!("  Connected");

    let barrier = Arc::new(Barrier::new(2));
    let client1_won = Arc::new(AtomicBool::new(false));
    let client2_won = Arc::new(AtomicBool::new(false));

    let bar1 = barrier.clone();
    let bar2 = barrier.clone();
    let won1 = client1_won.clone();
    let won2 = client2_won.clone();

    let agg_x1 = aggregate_x.clone();
    let agg_a1 = aggregate_a.clone();
    let agg_x2 = aggregate_x.clone();
    let agg_b2 = aggregate_b.clone();

    println!("\nRacing two multi-aggregate writes (both expecting version 1 on X)...");

    // Client 1: writes to [X, A]
    let task1 = tokio::spawn(async move {
        bar1.wait().await;

        let event_x = create_event(1, "Client 1 write to X".to_string());
        let event_a = create_event(1, "Client 1 write to A".to_string());

        let mut writes = HashMap::new();
        writes.insert(
            agg_x1,
            SingleAggregateWrite {
                events: vec![event_x],
                allow_create: false,
                expected_event_batch_index: Some(1), // Expect version 1
                enforce_client_idempotency: false,
                compression_type: CompressionType::None,
            },
        );
        writes.insert(
            agg_a1,
            SingleAggregateWrite {
                events: vec![event_a],
                allow_create: false,
                expected_event_batch_index: Some(1), // Expect version 1
                enforce_client_idempotency: false,
                compression_type: CompressionType::None,
            },
        );

        let request = Request::Write(WriteRequest {
            correlation_id: Some(1),
            client_id: 1,
            user_id: None,
            writes,
        });

        match client1.send_request(&request, CompressionType::None).await {
            Ok(Response::Write(_)) => {
                won1.store(true, Ordering::Relaxed);
                println!("  Client 1: SUCCESS");
            }
            Ok(Response::GenericError(e)) if e.error_code == 2003 => {
                println!("  Client 1: OCC FAILURE (error {})", e.error_code);
            }
            Err(ClientError::CeleriantError(e)) if e.error_code == 2003 => {
                println!("  Client 1: OCC FAILURE (error {})", e.error_code);
            }
            other => {
                panic!("Client 1: unexpected response: {:?}", other);
            }
        }
    });

    // Client 2: writes to [X, B]
    let task2 = tokio::spawn(async move {
        bar2.wait().await;

        let event_x = create_event(1, "Client 2 write to X".to_string());
        let event_b = create_event(1, "Client 2 write to B".to_string());

        let mut writes = HashMap::new();
        writes.insert(
            agg_x2,
            SingleAggregateWrite {
                events: vec![event_x],
                allow_create: false,
                expected_event_batch_index: Some(1), // Expect version 1
                enforce_client_idempotency: false,
                compression_type: CompressionType::None,
            },
        );
        writes.insert(
            agg_b2,
            SingleAggregateWrite {
                events: vec![event_b],
                allow_create: false,
                expected_event_batch_index: Some(1), // Expect version 1
                enforce_client_idempotency: false,
                compression_type: CompressionType::None,
            },
        );

        let request = Request::Write(WriteRequest {
            correlation_id: Some(2),
            client_id: 2,
            user_id: None,
            writes,
        });

        match client2.send_request(&request, CompressionType::None).await {
            Ok(Response::Write(_)) => {
                won2.store(true, Ordering::Relaxed);
                println!("  Client 2: SUCCESS");
            }
            Ok(Response::GenericError(e)) if e.error_code == 2003 => {
                println!("  Client 2: OCC FAILURE (error {})", e.error_code);
            }
            Err(ClientError::CeleriantError(e)) if e.error_code == 2003 => {
                println!("  Client 2: OCC FAILURE (error {})", e.error_code);
            }
            other => {
                panic!("Client 2: unexpected response: {:?}", other);
            }
        }
    });

    task1.await?;
    task2.await?;

    let c1_won = client1_won.load(Ordering::Relaxed);
    let c2_won = client2_won.load(Ordering::Relaxed);

    println!("\nVerifying exactly one winner...");
    assert!(
        c1_won ^ c2_won,
        "Exactly one client should succeed (c1={}, c2={})",
        c1_won,
        c2_won
    );
    println!("  Exactly one client won: c1={}, c2={}", c1_won, c2_won);

    // Verify final state
    println!("\nVerifying final event counts...");
    let mut read_client = CeleriantClient::connect(&addr).await?;
    let x_final = count_events(&mut read_client, &aggregate_x).await?;
    let a_final = count_events(&mut read_client, &aggregate_a).await?;
    let b_final = count_events(&mut read_client, &aggregate_b).await?;

    println!("  X: {} events", x_final);
    println!("  A: {} events", a_final);
    println!("  B: {} events", b_final);

    // X should have 2 events (initial + winner's write)
    assert_eq!(
        x_final, 2,
        "Aggregate X should have 2 events (initial + winner)"
    );

    // Exactly one of A or B should have 2 events (the other still has 1)
    if c1_won {
        assert_eq!(
            a_final, 2,
            "Client 1 won, so A should have 2 events (initial + write)"
        );
        assert_eq!(
            b_final, 1,
            "Client 1 won, so B should still have 1 event (write rejected)"
        );
    } else {
        assert_eq!(
            b_final, 2,
            "Client 2 won, so B should have 2 events (initial + write)"
        );
        assert_eq!(
            a_final, 1,
            "Client 2 won, so A should still have 1 event (write rejected)"
        );
    }

    println!("\n=== All Tests Passed ===");
    Ok(())
}

fn create_event(client_event_index: u64, message: String) -> DatablockAggregateEvent {
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
        event_value: Arc::new(message.into_bytes()),
        iv: None,
    }
}
