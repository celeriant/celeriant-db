//! Compaction Integration Test: Replicated Mode
//!
//! Verifies that leader-side compaction does not corrupt data visible on the
//! follower. After compaction on the leader, both nodes must still return
//! correct event counts for surviving aggregates and not-found for deleted ones.
//!
//! Scenario:
//! 1. Start 2-node cluster with S3 (MinIO), compaction_check_interval_secs=5.
//! 2. Write keepers to leader, write fillers, delete fillers.
//! 3. Trigger rotation, wait for replication to follower.
//! 4. Wait for compaction on leader.
//! 5. Verify keepers on both leader and follower.
//! 6. Verify deleted aggregates on both nodes.

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use crate::{
    count_events, s3_cluster_config, verify_compacted_segment_sizes, write_event,
    write_large_event, MinioContainer, ServerConfig, TestServer,
};
use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::request::requests::{DeleteRequest, SingleAggregateDelete};
use celeriant_wal::{aggregate_key::AggregateKey, compression_type::CompressionType};
use std::collections::HashMap;
use std::time::Duration;

const NUM_KEEPERS: u128 = 10;
const NUM_FILLERS: u128 = 5;
const FILLER_WRITES_PER_AGG: u64 = 20;
const FILLER_PAYLOAD_BYTES: usize = 32768;

async fn delete_aggregate(
    client: &mut CeleriantClient,
    key: &AggregateKey,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut deletes = HashMap::new();
    deletes.insert(
        key.clone(),
        SingleAggregateDelete {
            allow_recreate: false,
            allow_index_continuation: false,
            expected_event_batch_index: None,
        },
    );
    let request = ClientRequest::Delete(DeleteRequest {
        correlation_id: None,
        deletes,
        client_id: 1,
        user_id: None,
    });
    client
        .send_request(&request, CompressionType::None)
        .await?;
    Ok(())
}

fn is_not_found(err: &ClientError) -> bool {
    matches!(
        err,
        ClientError::Server(celeriant_client_tokio::server_error::ServerError::Read {
            kind: celeriant_client_tokio::server_error::ReadError::AggregateNotExists,
            ..
        })
    )
}

async fn verify_keepers(
    client: &mut CeleriantClient,
    label: &str,
    agg_type_id: u128,
    category_id: u128,
) -> Result<(), Box<dyn std::error::Error>> {
    for i in 1..=NUM_KEEPERS {
        let key = AggregateKey::new(agg_type_id, category_id, i);
        let count = count_events(client, &key).await?;
        assert_eq!(
            count, 2,
            "{}: keeper {} expected 2 events, got {}",
            label, i, count
        );
    }
    println!("  {}: all {} keepers have exactly 2 events", label, NUM_KEEPERS);
    Ok(())
}

async fn verify_deleted(
    client: &mut CeleriantClient,
    label: &str,
    agg_type_id: u128,
    category_id: u128,
    filler_start: u128,
) -> Result<(), Box<dyn std::error::Error>> {
    for i in 0..NUM_FILLERS {
        let key = AggregateKey::new(agg_type_id, category_id, filler_start + i);
        match count_events(client, &key).await {
            Ok(0) => {}
            Ok(n) => panic!("{}: deleted filler {} still has {} events", label, i, n),
            Err(e) => {
                if let Some(ce) = e.downcast_ref::<ClientError>() {
                    assert!(
                        is_not_found(ce),
                        "{}: filler {} unexpected error: {:?}",
                        label, i, ce
                    );
                }
            }
        }
    }
    println!("  {}: all {} deleted fillers confirmed gone", label, NUM_FILLERS);
    Ok(())
}


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Compaction Integration Test: Replicated ===\n");

    let port_base = 16600 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let minio_port = port_base + 10;

    println!("Starting MinIO on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-compaction-repl").await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();
    println!("MinIO ready at {}\n", endpoint);

    let base_config = s3_cluster_config(
        1, &region, &bucket, &access_key, &secret_key, &endpoint, allow_http,
    );
    let config = ServerConfig {
        shard_log_preallocate_bytes: 2 * 1024 * 1024,
        compaction_check_interval_secs: 5,
        compaction_min_reclaimable_ratio: 0.20,
        pending_replication_high_water_bytes: 100_000_000,
        ..base_config
    };

    println!("Starting leader on port {}...", leader_port);
    let _leader =
        TestServer::start_with_config_labeled(leader_port, config.clone(), "leader".into())
            .await?;
    println!("Starting follower on port {}...", follower_port);
    let _follower =
        TestServer::start_with_config_labeled(follower_port, config, "follower".into()).await?;

    println!("Waiting for cluster stabilization (8s)...");
    tokio::time::sleep(Duration::from_secs(8)).await;

    let mut leader_client = CeleriantClient::connect(_leader.address()).await?;

    let agg_type_id: u128 = 1;
    let category_id: u128 = 1;
    let filler_start = NUM_KEEPERS + 1;

    // ========================================
    // Phase A: Write keepers to leader
    // ========================================
    println!("\nPHASE A: Creating {} keeper aggregates on leader", NUM_KEEPERS);
    println!("--------------------------------------------------");

    for i in 1..=NUM_KEEPERS {
        let key = AggregateKey::new(agg_type_id, category_id, i);
        write_event(&mut leader_client, &key, 1, true).await?;
        write_event(&mut leader_client, &key, 2, false).await?;
    }
    println!("  {} keepers created", NUM_KEEPERS);

    // ========================================
    // Phase B: Write fillers, then delete them
    // ========================================
    println!("\nPHASE B: Creating and deleting {} filler aggregates", NUM_FILLERS);
    println!("------------------------------------------------------");

    for i in 0..NUM_FILLERS {
        let key = AggregateKey::new(agg_type_id, category_id, filler_start + i);
        write_event(&mut leader_client, &key, 1, true).await?;
        for w in 2..=FILLER_WRITES_PER_AGG {
            write_large_event(&mut leader_client, &key, w, FILLER_PAYLOAD_BYTES).await?;
        }
    }
    println!("  {} fillers created", NUM_FILLERS);

    for i in 0..NUM_FILLERS {
        let key = AggregateKey::new(agg_type_id, category_id, filler_start + i);
        delete_aggregate(&mut leader_client, &key).await?;
    }
    println!("  {} fillers deleted", NUM_FILLERS);

    // ========================================
    // Phase C: Trigger rotation via large writes
    // ========================================
    println!("\nPHASE C: Forcing log rotation");
    println!("-------------------------------");

    let rotation_key = AggregateKey::new(agg_type_id, category_id, 1000);
    write_event(&mut leader_client, &rotation_key, 1, true).await?;
    for w in 2..=50u64 {
        write_large_event(&mut leader_client, &rotation_key, w, 32768).await?;
    }
    println!("  Log rotation triggered");

    // ========================================
    // Phase D: Wait for replication to follower
    // ========================================
    println!("\nPHASE D: Waiting for replication to follower (up to 30s)");
    println!("----------------------------------------------------------");

    let mut follower_client = CeleriantClient::connect(_follower.address()).await?;
    let check_key = AggregateKey::new(agg_type_id, category_id, 1);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(count) = count_events(&mut follower_client, &check_key).await {
            if count >= 2 {
                println!("  Replication confirmed (keeper 1 has {} events on follower)", count);
                break;
            }
        }
        if tokio::time::Instant::now() > deadline {
            panic!("Replication to follower timed out after 30s");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // ========================================
    // Phase E: Wait for compaction on leader
    // ========================================
    println!("\nPHASE E: Waiting 30s for compaction on leader");
    println!("-----------------------------------------------");

    tokio::time::sleep(Duration::from_secs(30)).await;
    println!("  Compaction wait complete");

    // Reconnect both clients — the server's 30s idle timeout closed the previous connections.
    leader_client = CeleriantClient::connect(_leader.address()).await?;
    follower_client = CeleriantClient::connect(_follower.address()).await?;

    // ========================================
    // Phase E.5: Verify sealed segments are < 1 GB on both nodes
    // ========================================
    println!("\nPHASE E.5: Verifying sealed segment sizes on both nodes");
    println!("--------------------------------------------------------");
    let preallocate_bytes = _leader.config().shard_log_preallocate_bytes;
    verify_compacted_segment_sizes(&_leader.config().data_root, "Leader", preallocate_bytes)?;
    verify_compacted_segment_sizes(&_follower.config().data_root, "Follower", preallocate_bytes)?;

    // ========================================
    // Phase F: Verify keepers on both nodes
    // ========================================
    println!("\nPHASE F: Verifying keeper aggregates on both nodes");
    println!("----------------------------------------------------");

    verify_keepers(&mut leader_client, "Leader", agg_type_id, category_id).await?;
    verify_keepers(&mut follower_client, "Follower", agg_type_id, category_id).await?;

    // ========================================
    // Phase G: Verify deleted aggregates on both nodes
    // ========================================
    println!("\nPHASE G: Verifying deleted aggregates on both nodes");
    println!("-----------------------------------------------------");

    verify_deleted(&mut leader_client, "Leader", agg_type_id, category_id, filler_start).await?;
    verify_deleted(&mut follower_client, "Follower", agg_type_id, category_id, filler_start).await?;

    println!("\n=== PASS ===\n");
    Ok(())
}
