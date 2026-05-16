//! Compaction Integration Test: Restart Survival
//!
//! Verifies that data compacted before a server restart is still readable
//! after the server comes back up.
//!
//! Scenario:
//! 1. Start standalone server with 5s compaction interval.
//! 2. Write keepers + fillers, delete fillers, trigger rotation.
//! 3. Wait for compaction to fire.
//! 4. Stop and restart the server.
//! 5. Verify keepers readable with correct event counts.
//! 6. Verify deleted aggregates still deleted.

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use crate::{
    count_events, verify_compacted_segment_sizes, write_event, write_large_event, ServerConfig,
    TestServer,
};
use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::request::requests::{DeleteRequest, SingleAggregateDelete};
use celeriant_wal::{aggregate_key::AggregateKey};
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
        .send_request(&request)
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


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Compaction Integration Test: Restart Survival ===\n");

    let port_base = 16400 + (std::process::id() % 100) as u16;

    let config = ServerConfig {
        num_shards: Some(1),
        standalone: true,
        log_level: "info".to_string(),
        shard_log_preallocate_bytes: 2 * 1024 * 1024,
        compaction_check_interval_secs: 5,
        compaction_min_reclaimable_ratio: 0.20,
        ..Default::default()
    };

    println!("Starting standalone server on port {}...", port_base);
    let mut server =
        TestServer::start_with_config_labeled(port_base, config, "standalone".into()).await?;
    let mut client = CeleriantClient::connect(server.address()).await?;

    let agg_type_id: u128 = 1;
    let category_id: u128 = 1;
    let filler_start = NUM_KEEPERS + 1;

    // ========================================
    // Phase A: Write keepers + fillers
    // ========================================
    println!("\nPHASE A: Creating keepers and fillers");
    println!("--------------------------------------");

    for i in 1..=NUM_KEEPERS {
        let key = AggregateKey::new(agg_type_id, category_id, i);
        write_event(&mut client, &key, 1, true).await?;
        write_event(&mut client, &key, 2, false).await?;
    }
    println!("  {} keepers created (2 events each)", NUM_KEEPERS);

    for i in 0..NUM_FILLERS {
        let key = AggregateKey::new(agg_type_id, category_id, filler_start + i);
        write_event(&mut client, &key, 1, true).await?;
        for w in 2..=FILLER_WRITES_PER_AGG {
            write_large_event(&mut client, &key, w, FILLER_PAYLOAD_BYTES).await?;
        }
    }
    println!("  {} fillers created", NUM_FILLERS);

    // ========================================
    // Phase B: Delete fillers and trigger rotation
    // ========================================
    println!("\nPHASE B: Deleting fillers and triggering rotation");
    println!("---------------------------------------------------");

    for i in 0..NUM_FILLERS {
        let key = AggregateKey::new(agg_type_id, category_id, filler_start + i);
        delete_aggregate(&mut client, &key).await?;
    }
    println!("  {} fillers deleted", NUM_FILLERS);

    let rotation_key = AggregateKey::new(agg_type_id, category_id, 1000);
    write_event(&mut client, &rotation_key, 1, true).await?;
    for w in 2..=50u64 {
        write_large_event(&mut client, &rotation_key, w, 32768).await?;
    }
    println!("  Log rotation triggered");

    // ========================================
    // Phase C: Wait for compaction
    // ========================================
    println!("\nPHASE C: Waiting 30s for compaction to fire");
    println!("---------------------------------------------");

    tokio::time::sleep(Duration::from_secs(30)).await;
    println!("  Compaction wait complete");

    // ========================================
    // Phase C.5: Verify sealed segments are < 1 GB (compaction ran)
    // ========================================
    println!("\nPHASE C.5: Verifying sealed segment sizes");
    println!("------------------------------------------");
    verify_compacted_segment_sizes(
        &server.config().data_root,
        "Standalone",
        server.config().shard_log_preallocate_bytes,
    )?;

    // ========================================
    // Phase D: Restart server
    // ========================================
    println!("\nPHASE D: Stopping and restarting server");
    println!("----------------------------------------");

    drop(client);
    server.stop();
    tokio::time::sleep(Duration::from_secs(1)).await;
    server.restart().await?;
    let mut client = CeleriantClient::connect(server.address()).await?;
    println!("  Server restarted");

    // ========================================
    // Phase E: Verify keepers after restart
    // ========================================
    println!("\nPHASE E: Verifying {} keeper aggregates", NUM_KEEPERS);
    println!("-------------------------------------------");

    for i in 1..=NUM_KEEPERS {
        let key = AggregateKey::new(agg_type_id, category_id, i);
        let count = count_events(&mut client, &key).await?;
        assert_eq!(count, 2, "Keeper {} expected 2 events, got {}", i, count);
    }
    println!("  All {} keepers have exactly 2 events", NUM_KEEPERS);

    // ========================================
    // Phase F: Verify deleted aggregates still deleted
    // ========================================
    println!("\nPHASE F: Verifying {} deleted fillers", NUM_FILLERS);
    println!("----------------------------------------");

    for i in 0..NUM_FILLERS {
        let key = AggregateKey::new(agg_type_id, category_id, filler_start + i);
        match count_events(&mut client, &key).await {
            Ok(0) => {}
            Ok(n) => panic!("Deleted filler {} still has {} events", i, n),
            Err(e) => {
                if let Some(ce) = e.downcast_ref::<ClientError>() {
                    assert!(is_not_found(ce), "Filler {} unexpected error: {:?}", i, ce);
                }
            }
        }
    }
    println!("  All {} deleted fillers confirmed gone", NUM_FILLERS);

    println!("\n=== PASS ===\n");
    Ok(())
}
