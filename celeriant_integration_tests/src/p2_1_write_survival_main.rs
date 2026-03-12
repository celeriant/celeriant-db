//! P2-1. Acknowledged Write Survival Under SIGKILL
//!
//! Tests that every acknowledged write survives leader SIGKILL.
//!
//! Scenario:
//! 1. Start 2-node cluster with S3
//! 2. Write N events in a loop, tracking which ones succeed (acked)
//! 3. SIGKILL leader via leader.stop()
//! 4. Wait for follower takeover (5-8s)
//! 5. For each acked write:
//!    - Read the event from new leader (former follower)
//!    - Assert event exists with correct data
//! 6. Verify total count matches acked count
//!
//! Key insight: writes that return Ok have completed fsync + replication,
//! so they MUST survive SIGKILL. If this test fails, we found a durability bug.

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_integration_tests::{write_event, MinioContainer, TestServer, s3_cluster_config};
use celeriant_msg::{
    process_client_requests::ClientRequest,
    process_client_responses::ClientResponse,
    request::requests::ReadRequest,
};
use celeriant_wal::{
    aggregate_key::AggregateKey, compression_type::CompressionType,
    datablocks::datablock_aggregate_event::DatablockAggregateEvent,
};
use std::time::Duration;

const PORT_BASE: u16 = 20100;
const NUM_EVENTS: u64 = 100;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== P2-1: Acknowledged Write Survival Under SIGKILL ===\n");

    let leader_port = PORT_BASE;
    let follower_port = PORT_BASE + 100;
    let minio_port = PORT_BASE + 10;

    println!("Starting MinIO container on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-write-survival").await?;
    let (region, bucket_name, access_key, secret_key, minio_endpoint, allow_http) = minio.s3_config_fields();
    println!("MinIO ready at {}\n", minio_endpoint);

    let num_shards = 4;
    let aggregate_key = AggregateKey::new(1, 1, 1);

    // ========================================
    // PHASE 1: Start cluster
    // ========================================
    println!("PHASE 1: Start 2-node cluster");
    println!("------------------------------");

    let leader_config = s3_cluster_config(
        num_shards,
        &region,
        &bucket_name,
        &access_key,
        &secret_key,
        &minio_endpoint,
        allow_http,
    );

    println!("  Starting leader on port {}...", leader_port);
    let mut leader = TestServer::start_with_config(leader_port, leader_config).await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let follower_config = s3_cluster_config(
        num_shards,
        &region,
        &bucket_name,
        &access_key,
        &secret_key,
        &minio_endpoint,
        allow_http,
    );

    println!("  Starting follower on port {}...", follower_port);
    let follower = TestServer::start_with_config(follower_port, follower_config).await?;

    println!("  Waiting for election and heartbeat establishment...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    let mut leader_client = CeleriantClient::connect(leader.address()).await?;
    println!("  ✓ Cluster ready\n");

    // ========================================
    // PHASE 2: Write events, track acks
    // ========================================
    println!("PHASE 2: Write {} events and track acknowledged writes", NUM_EVENTS);
    println!("--------------------------------------------------------");

    // Track which client_event_index values were acked
    let mut acked_events: Vec<u64> = Vec::new();

    for i in 1..=NUM_EVENTS {
        let result = write_event(&mut leader_client, &aggregate_key, i, i == 1).await;
        if result.is_ok() {
            acked_events.push(i);
        }
    }

    println!("  ✓ Write phase complete: {}/{} events acknowledged", acked_events.len(), NUM_EVENTS);

    assert!(
        !acked_events.is_empty(),
        "No events were acknowledged - cluster may be unhealthy"
    );

    // ========================================
    // PHASE 3: SIGKILL leader
    // ========================================
    println!("\nPHASE 3: SIGKILL leader (simulate hard crash)");
    println!("----------------------------------------------");

    println!("  Stopping leader process...");
    drop(leader_client);
    leader.stop();
    println!("  ✓ Leader SIGKILL'd\n");

    // ========================================
    // PHASE 4: Wait for follower takeover
    // ========================================
    println!("PHASE 4: Wait for follower takeover");
    println!("-----------------------------------");

    println!("  Waiting for follower to detect heartbeat loss and take over...");
    println!("  (heartbeat timeout ~2s + S3 race ~1s = ~5s total)");
    tokio::time::sleep(Duration::from_secs(6)).await;

    let mut new_leader_client = CeleriantClient::connect(follower.address()).await?;

    // Probe that new leader is ready
    println!("  Verifying new leader is ready...");
    write_event(&mut new_leader_client, &aggregate_key, NUM_EVENTS + 1, false).await?;
    println!("  ✓ New leader is ready (former follower)\n");

    // ========================================
    // PHASE 5: Verify all acked events survived
    // ========================================
    println!("PHASE 5: Verify all {} acked events survived SIGKILL", acked_events.len());
    println!("--------------------------------------------------------");

    for client_event_idx in &acked_events {
        let event = read_event_by_client_index(
            &mut new_leader_client,
            &aggregate_key,
            *client_event_idx,
        )
        .await?;

        assert_eq!(
            event.client_event_index, *client_event_idx,
            "Event client_event_index mismatch: expected {}, got {}",
            client_event_idx, event.client_event_index
        );

        // Verify data integrity
        let expected_payload = format!("{{\"event\":{}}}", client_event_idx);
        let actual_payload = String::from_utf8_lossy(&event.event_value);
        assert_eq!(
            actual_payload, expected_payload,
            "Event data corrupted: expected {}, got {}",
            expected_payload, actual_payload
        );
    }

    println!("  ✓ All {} acked events survived with correct data", acked_events.len());

    // Verify total event count matches acked count (plus the probe write)
    let total_count = count_events_all(&mut new_leader_client, &aggregate_key).await?;
    let expected_count = acked_events.len() + 1; // +1 for the probe write
    assert_eq!(
        total_count, expected_count,
        "Total event count mismatch: expected {}, got {}",
        expected_count, total_count
    );
    println!("  ✓ Total event count matches: {}", total_count);

    println!("\n=== All Tests Passed ===");
    println!("Proof: Every acknowledged write survived leader SIGKILL\n");

    Ok(())
}

/// Read a specific event by its client_event_index.
/// Scans all events until the matching client_event_index is found.
async fn read_event_by_client_index(
    client: &mut CeleriantClient,
    aggregate_key: &AggregateKey,
    target_client_event_index: u64,
) -> Result<DatablockAggregateEvent, Box<dyn std::error::Error>> {
    let mut from_batch = 1u64;

    loop {
        let read_req = ReadRequest {
            correlation_id: Some(999),
            aggregate_key: aggregate_key.clone(),
            filters: celeriant_msg::request::read_filters::ReadFilters::new(from_batch),
        };

        let response = client
            .send_request(&ClientRequest::Read(read_req), CompressionType::None)
            .await?;

        match response {
            ClientResponse::Read(read_resp) => {
                for batch in read_resp.event_batches {
                    for event in batch.events {
                        if event.client_event_index == target_client_event_index {
                            return Ok(event);
                        }
                    }
                }
                match read_resp.next_event_batch_index {
                    Some(next) => from_batch = next,
                    None => {
                        return Err(format!(
                            "Event with client_event_index {} not found",
                            target_client_event_index
                        )
                        .into())
                    }
                }
            }
            other => return Err(format!("Unexpected response: {:?}", other).into()),
        }
    }
}

/// Count all events in an aggregate.
async fn count_events_all(
    client: &mut CeleriantClient,
    aggregate_key: &AggregateKey,
) -> Result<usize, Box<dyn std::error::Error>> {
    let mut total = 0usize;
    let mut from_batch = 1u64;

    loop {
        let read_req = ReadRequest {
            correlation_id: Some(999),
            aggregate_key: aggregate_key.clone(),
            filters: celeriant_msg::request::read_filters::ReadFilters::new(from_batch),
        };

        let response = client
            .send_request(&ClientRequest::Read(read_req), CompressionType::None)
            .await;

        match response {
            Ok(ClientResponse::Read(read_resp)) => {
                total += read_resp
                    .event_batches
                    .iter()
                    .map(|b| b.events.len())
                    .sum::<usize>();
                match read_resp.next_event_batch_index {
                    Some(next) => from_batch = next,
                    None => return Ok(total),
                }
            }
            Ok(other) => return Err(format!("Unexpected response: {:?}", other).into()),
            Err(e) => match &e {
                celeriant_client_tokio::client_error::ClientError::Server(
                    celeriant_client_tokio::server_error::ServerError::Read {
                        kind: celeriant_client_tokio::server_error::ReadError::AggregateNotExists, ..
                    }
                ) => return Ok(total),
                _ => return Err(Box::new(e)),
            },
        }
    }
}
