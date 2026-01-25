//! Replication Catchup Integration Tests
//!
//! Tests the flexible replication catchup feature that allows the leader to
//! send missing WAL entries when a follower falls behind.
//!
//! Scenario:
//! 1. Leader and follower start together, initial writes replicate normally
//! 2. Follower is stopped
//! 3. Leader continues writing (falls back to S3, continues without follower)
//! 4. Follower restarts
//! 5. Next write triggers WalIndexMismatch, leader catches up follower
//! 6. Verify follower has all events
//!
//! Run with: cargo run --bin replication_catchup_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_integration_tests::{ConfigClusterRole, ServerConfig, TestServer};
use celeriant_msg::{
    process_requests::Request,
    request::requests::{ReadRequest, SingleAggregateWrite, WriteRequest},
};
use celeriant_runtimes::RoutingRule;
use celeriant_wal::{
    aggregate_key::AggregateKey, compression_type::CompressionType,
    datablocks::datablock_aggregate_event::DatablockAggregateEvent,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

struct ReplicatedServers {
    leader: TestServer,
    follower: TestServer,
}

impl ReplicatedServers {
    async fn start(base_port: u16) -> Result<Self, Box<dyn std::error::Error>> {
        // Start follower first
        let follower_port = base_port + 100;
        let follower_config = ServerConfig {
            log_level: "info".to_string(),
            cluster_role: ConfigClusterRole::Follower,
            routing_rule: RoutingRule::AggregateTypeId,
            ..Default::default()
        };
        println!("Starting follower on port {}...", follower_port);
        let follower = TestServer::start_with_config(follower_port, follower_config).await?;

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Start leader with follower address
        let follower_replication_port = follower_port + 1;
        let leader_config = ServerConfig {
            log_level: "info".to_string(),
            cluster_role: ConfigClusterRole::Leader,
            follower_address: Some(format!("127.0.0.1:{}", follower_replication_port)),
            routing_rule: RoutingRule::AggregateTypeId,
            ..Default::default()
        };
        println!(
            "Starting leader on port {} (replicating to 127.0.0.1:{})...",
            base_port, follower_replication_port
        );
        let leader = TestServer::start_with_config(base_port, leader_config).await?;

        Ok(Self { leader, follower })
    }
}

async fn write_event(
    client: &mut CeleriantClient,
    aggregate_key: &AggregateKey,
    event_num: u64,
    allow_create: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let event = DatablockAggregateEvent {
        client_event_index: event_num,
        event_index: 0,
        event_id: None,
        event_timestamp: 1000 + event_num,
        event_type_major: 100,
        event_type_minor: 0,
        event_value: Arc::new(format!("{{\"event\":{}}}", event_num).into_bytes()),
        iv: None,
    };

    let mut writes = HashMap::new();
    writes.insert(
        aggregate_key.clone(),
        SingleAggregateWrite {
            events: vec![event],
            allow_create,
            expected_event_batch_index: if event_num == 1 { Some(0) } else { None },
            enforce_client_idempotency: false,
            compression_type: CompressionType::None,
        },
    );

    let write_req = WriteRequest {
        correlation_id: Some(event_num as u128),
        client_id: 999,
        user_id: Some(888),
        writes,
    };

    let response = client
        .send_request(&Request::Write(write_req), CompressionType::None)
        .await?;

    match response {
        celeriant_msg::process_responses::Response::Write(_) => Ok(()),
        other => Err(format!("Write failed: {:?}", other).into()),
    }
}

async fn count_events(
    client: &mut CeleriantClient,
    aggregate_key: &AggregateKey,
) -> Result<usize, Box<dyn std::error::Error>> {
    let read_req = ReadRequest {
        correlation_id: Some(999),
        aggregate_key: aggregate_key.clone(),
        filters: celeriant_msg::request::read_filters::ReadFilters::new(1),
    };

    let response = client
        .send_request(&Request::Read(read_req), CompressionType::None)
        .await?;

    match response {
        celeriant_msg::process_responses::Response::Read(read_resp) => {
            let total: usize = read_resp
                .event_batches
                .iter()
                .map(|b| b.events.len())
                .sum();
            Ok(total)
        }
        celeriant_msg::process_responses::Response::GenericError(_) => {
            // Aggregate doesn't exist yet
            Ok(0)
        }
        other => Err(format!("Unexpected response: {:?}", other).into()),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Replication Catchup Integration Tests ===\n");

    let port = 10300 + (std::process::id() % 100) as u16;

    println!("Starting replicated cluster...");
    let mut servers = ReplicatedServers::start(port).await?;
    println!(
        "Cluster started: leader at {}, follower at {}\n",
        servers.leader.address(),
        servers.follower.address()
    );

    let mut leader_client = CeleriantClient::connect(servers.leader.address()).await?;
    let aggregate_key = AggregateKey::new(1, 1, 1);

    // ========================================
    // Phase 1: Normal replication (follower is up)
    // ========================================
    println!("PHASE 1: Normal replication with follower online");
    println!("------------------------------------------------");

    println!("  Writing events 1-3 to leader...");
    for i in 1..=3 {
        write_event(&mut leader_client, &aggregate_key, i, i == 1).await?;
    }

    println!("  Waiting for replication...");
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut follower_client = CeleriantClient::connect(servers.follower.address()).await?;
    let follower_count = count_events(&mut follower_client, &aggregate_key).await?;
    assert_eq!(follower_count, 3, "Follower should have 3 events after normal replication");
    println!("  ✓ Follower has {} events\n", follower_count);

    // ========================================
    // Phase 2: Follower goes down, leader continues
    // ========================================
    println!("PHASE 2: Follower goes down, leader continues writing");
    println!("------------------------------------------------------");

    println!("  Stopping follower...");
    drop(follower_client); // Close connection before stopping
    servers.follower.stop();
    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("  Writing events 4-6 to leader (follower is down, S3 fallback)...");
    for i in 4..=6 {
        write_event(&mut leader_client, &aggregate_key, i, false).await?;
    }
    println!("  ✓ Leader continued writing while follower was down\n");

    // ========================================
    // Phase 3: Follower restarts, catchup happens
    // ========================================
    println!("PHASE 3: Follower restarts, catchup on next write");
    println!("-------------------------------------------------");

    println!("  Restarting follower...");
    servers.follower.restart().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Reconnect to follower
    let mut follower_client = CeleriantClient::connect(servers.follower.address()).await?;

    // Check follower state - should still have old data (3 events)
    let follower_count = count_events(&mut follower_client, &aggregate_key).await?;
    println!("  Follower currently has {} events (behind leader)", follower_count);
    assert_eq!(follower_count, 3, "Follower should still have only 3 events after restart");

    println!("  Writing event 7 to leader (should trigger catchup)...");
    write_event(&mut leader_client, &aggregate_key, 7, false).await?;

    println!("  Waiting for catchup replication...");
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // ========================================
    // Phase 4: Verify catchup worked
    // ========================================
    println!("\nPHASE 4: Verify follower caught up");
    println!("----------------------------------");

    let follower_count = count_events(&mut follower_client, &aggregate_key).await?;
    println!("  Follower now has {} events", follower_count);

    if follower_count == 7 {
        println!("  ✓ Catchup successful! Follower has all 7 events");
    } else {
        println!("  ✗ Catchup failed! Expected 7 events, got {}", follower_count);
        return Err(format!("Catchup verification failed: expected 7, got {}", follower_count).into());
    }

    // Verify leader also has all events
    let leader_count = count_events(&mut leader_client, &aggregate_key).await?;
    assert_eq!(leader_count, 7, "Leader should have 7 events");
    println!("  ✓ Leader has {} events", leader_count);

    println!("\n=== All Tests Passed ===\n");

    Ok(())
}
