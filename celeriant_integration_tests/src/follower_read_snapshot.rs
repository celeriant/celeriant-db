//! Integration test: Follower Read Snapshot
//!
//! Tests that replicated data is visible when reading from the follower.
//! Also exercises delete and trim paths to verify they work correctly
//! on the follower side.
//!
//! Run with: cargo run --bin follower_read_snapshot_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{
    count_events, s3_cluster_config, write_event, MinioContainer, TestServer,
};
use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::process_client_responses::ClientResponse;
use celeriant_msg::request::requests::{DeleteRequest, SingleAggregateDelete, TrimStartRequest};
use celeriant_wal::aggregate_key::AggregateKey;
use std::collections::HashMap;
use std::time::Duration;

async fn delete_aggregate(
    client: &mut CeleriantClient,
    key: &AggregateKey,
    allow_recreate: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut deletes = HashMap::new();
    deletes.insert(
        key.clone(),
        SingleAggregateDelete {
            allow_recreate,
            allow_index_continuation: false,
            expected_event_batch_index: None,
        },
    );

    let req = DeleteRequest {
        correlation_id: Some(1),
        client_id: 999,
        user_id: Some(888),
        deletes,
    };

    let response = client
        .send_request(&ClientRequest::Delete(req))
        .await?;

    match response {
        ClientResponse::Delete(_) => Ok(()),
        other => Err(format!("Delete failed: {:?}", other).into()),
    }
}

async fn trim_aggregate(
    client: &mut CeleriantClient,
    key: &AggregateKey,
    keep_from: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let req = TrimStartRequest {
        correlation_id: Some(2),
        aggregate_key: key.clone(),
        keep_from_event_batch_index: keep_from,
        client_id: 999,
        user_id: Some(888),
    };

    let response = client
        .send_request(&ClientRequest::TrimStart(req))
        .await?;

    match response {
        ClientResponse::TrimStart(_) => Ok(()),
        other => Err(format!("Trim failed: {:?}", other).into()),
    }
}


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Follower Read Snapshot Test ===\n");

    // Setup cluster
    let port_base = 11400 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let minio_port = port_base + 10;

    println!("Starting MinIO...");
    let minio = MinioContainer::start(minio_port).await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();

    let num_shards = 1;
    let mut config = s3_cluster_config(
        num_shards, &region, &bucket, &access_key, &secret_key, &endpoint, allow_http,
    );
    config.heartbeat_lease_duration_ms = 30_000;
    config.s3_lease_duration_ms = 30_000;

    println!("Starting cluster...");
    let leader = TestServer::start_with_config_labeled(leader_port, config.clone(), "leader".into()).await?;
    let follower = TestServer::start_with_config_labeled(follower_port, config, "follower".into()).await?;

    println!("Waiting for election + replication...");
    tokio::time::sleep(Duration::from_secs(10)).await;

    let mut leader_client = CeleriantClient::connect(leader.address()).await?;
    let mut follower_client = CeleriantClient::connect(follower.address()).await?;

    // ── Phase 1: Basic write and read ──
    println!("\nPHASE 1: Write to leader, read from follower");
    let key_basic = AggregateKey::new(1, 0, 1);

    for i in 1..=5 {
        write_event(&mut leader_client, &key_basic, i, i == 1).await?;
    }

    let leader_count = count_events(&mut leader_client, &key_basic).await?;
    let follower_count = count_events(&mut follower_client, &key_basic).await?;
    println!("  leader={}, follower={}", leader_count, follower_count);
    assert_eq!(leader_count, 5, "leader should have 5 events");
    assert_eq!(follower_count, 5, "follower should have 5 events");
    println!("  Phase 1 PASSED\n");

    // ── Phase 2: Trim on leader, verify follower ──
    println!("PHASE 2: Trim on leader, verify follower sees trimmed data");
    let key_trim = AggregateKey::new(1, 0, 2);

    for i in 1..=10 {
        write_event(&mut leader_client, &key_trim, i, i == 1).await?;
    }

    // Trim first 5 batches
    trim_aggregate(&mut leader_client, &key_trim, 6).await?;

    let leader_count = count_events(&mut leader_client, &key_trim).await?;
    let follower_count = count_events(&mut follower_client, &key_trim).await?;
    println!("  After trim: leader={}, follower={}", leader_count, follower_count);
    assert_eq!(leader_count, follower_count, "trim should be visible on follower");
    println!("  Phase 2 PASSED\n");

    // ── Phase 3: Delete on leader, verify follower ──
    println!("PHASE 3: Delete on leader, verify follower sees deleted aggregate");
    let key_delete = AggregateKey::new(1, 0, 3);

    for i in 1..=3 {
        write_event(&mut leader_client, &key_delete, i, i == 1).await?;
    }

    // Verify exists before delete
    let follower_count = count_events(&mut follower_client, &key_delete).await?;
    assert_eq!(follower_count, 3, "follower should see 3 events before delete");

    // Delete the aggregate
    delete_aggregate(&mut leader_client, &key_delete, false).await?;

    // After delete, follower should return 0 or an error
    let leader_count = count_events(&mut leader_client, &key_delete).await?;
    let follower_count = count_events(&mut follower_client, &key_delete).await?;
    println!("  After delete: leader={}, follower={}", leader_count, follower_count);
    assert_eq!(leader_count, 0, "leader should show 0 events after delete");
    assert_eq!(follower_count, 0, "follower should show 0 events after delete");
    println!("  Phase 3 PASSED\n");

    // ── Phase 4: Delete with recreate ──
    println!("PHASE 4: Delete with recreate, verify follower");
    let key_recreate = AggregateKey::new(1, 0, 4);

    for i in 1..=3 {
        write_event(&mut leader_client, &key_recreate, i, i == 1).await?;
    }

    delete_aggregate(&mut leader_client, &key_recreate, true).await?;

    // Recreate with new events — use allow_create but don't assert batch index 0
    // since the aggregate's batch index continues from before deletion.
    for i in 1..=2 {
        write_event(&mut leader_client, &key_recreate, 100 + i, i == 1).await?;
    }

    let leader_count = count_events(&mut leader_client, &key_recreate).await?;
    let follower_count = count_events(&mut follower_client, &key_recreate).await?;
    println!("  After recreate: leader={}, follower={}", leader_count, follower_count);
    assert_eq!(leader_count, follower_count, "recreated aggregate should converge");
    println!("  Phase 4 PASSED\n");

    println!("=== All Phases Passed ===");
    Ok(())
}
