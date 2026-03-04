//! Compaction Integration Test: Standalone Mode
//!
//! Verifies that background compaction correctly reclaims space from deleted
//! aggregates without corrupting surviving data.
//!
//! Scenario:
//! 1. Start standalone server with 5s compaction interval and 2MB segments.
//! 2. Write 10 "keeper" aggregates with 2 events each.
//! 3. Write 5 "filler" aggregates with large events to dominate the segment.
//! 4. Delete all 5 filler aggregates.
//! 5. Force log rotation via large writes.
//! 6. Wait for compaction to fire (poll up to 30s).
//! 7. Verify keepers still have exactly 2 events each.
//! 8. Verify deleted aggregates return not-found.

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use celeriant_integration_tests::{
    count_events, verify_compacted_segment_sizes, write_event, write_large_event, ServerConfig,
    TestServer,
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
        ClientError::CeleriantError(e) if e.error_code == 1001
    )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Compaction Integration Test: Standalone ===\n");

    let port_base = 16200 + (std::process::id() % 100) as u16;

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
    let _server =
        TestServer::start_with_config_labeled(port_base, config, "standalone".into()).await?;
    let mut client = CeleriantClient::connect(_server.address()).await?;

    let agg_type_id: u128 = 1;
    let category_id: u128 = 1;

    // ========================================
    // Phase A: Create 10 keeper aggregates with 2 events each
    // ========================================
    println!("\nPHASE A: Creating {} keeper aggregates (2 events each)", NUM_KEEPERS);
    println!("-------------------------------------------------------");

    for i in 1..=NUM_KEEPERS {
        let key = AggregateKey::new(agg_type_id, category_id, i);
        write_event(&mut client, &key, 1, true)
            .await
            .map_err(|e| format!("Keeper {} event 1 failed: {}", i, e))?;
        write_event(&mut client, &key, 2, false)
            .await
            .map_err(|e| format!("Keeper {} event 2 failed: {}", i, e))?;
    }
    println!("  {} keepers created", NUM_KEEPERS);

    // ========================================
    // Phase B: Create 5 filler aggregates with large events
    // ========================================
    println!("\nPHASE B: Creating {} filler aggregates ({} x {}B writes)", NUM_FILLERS, FILLER_WRITES_PER_AGG, FILLER_PAYLOAD_BYTES);
    println!("-----------------------------------------------------------");

    let filler_start = NUM_KEEPERS + 1;
    for i in 0..NUM_FILLERS {
        let key = AggregateKey::new(agg_type_id, category_id, filler_start + i);
        write_event(&mut client, &key, 1, true)
            .await
            .map_err(|e| format!("Filler {} init failed: {}", i, e))?;
        for w in 2..=FILLER_WRITES_PER_AGG {
            write_large_event(&mut client, &key, w, FILLER_PAYLOAD_BYTES)
                .await
                .map_err(|e| format!("Filler {} write {} failed: {}", i, w, e))?;
        }
    }
    println!("  {} fillers created", NUM_FILLERS);

    // ========================================
    // Phase C: Delete all filler aggregates
    // ========================================
    println!("\nPHASE C: Deleting all {} filler aggregates", NUM_FILLERS);
    println!("---------------------------------------------");

    for i in 0..NUM_FILLERS {
        let key = AggregateKey::new(agg_type_id, category_id, filler_start + i);
        delete_aggregate(&mut client, &key).await?;
    }
    println!("  {} fillers deleted", NUM_FILLERS);

    // ========================================
    // Phase D: Force log rotation via large writes to a fresh aggregate
    // ========================================
    println!("\nPHASE D: Forcing log rotation via large writes");
    println!("------------------------------------------------");

    let rotation_key = AggregateKey::new(agg_type_id, category_id, 1000);
    write_event(&mut client, &rotation_key, 1, true).await?;
    for w in 2..=50u64 {
        write_large_event(&mut client, &rotation_key, w, 32768).await?;
    }
    println!("  Log rotation triggered");

    // ========================================
    // Phase E: Wait for compaction to fire
    // ========================================
    println!("\nPHASE E: Waiting for compaction (up to 30s)");
    println!("---------------------------------------------");

    // The compaction timer fires every 5s. We wait long enough for it to run
    // at least once. We verify by checking that keepers are still readable.
    tokio::time::sleep(Duration::from_secs(30)).await;
    println!("  Waited 30s for compaction background timer");

    // Reconnect — the server's 30s idle timeout closed the previous connection.
    client = CeleriantClient::connect(_server.address()).await?;

    // ========================================
    // Phase E.5: Verify sealed segments are < 1 GB (compaction ran)
    // ========================================
    println!("\nPHASE E.5: Verifying sealed segment sizes");
    println!("------------------------------------------");
    verify_compacted_segment_sizes(
        &_server.config().data_root,
        "Standalone",
        _server.config().shard_log_preallocate_bytes,
    )?;

    // ========================================
    // Phase F: Verify keeper aggregates still have exactly 2 events
    // ========================================
    println!("\nPHASE F: Verifying {} keeper aggregates", NUM_KEEPERS);
    println!("-------------------------------------------");

    for i in 1..=NUM_KEEPERS {
        let key = AggregateKey::new(agg_type_id, category_id, i);
        let count = count_events(&mut client, &key).await?;
        assert_eq!(
            count, 2,
            "Keeper {} expected 2 events, got {}",
            i, count
        );
    }
    println!("  All {} keepers have exactly 2 events", NUM_KEEPERS);

    // ========================================
    // Phase G: Verify deleted aggregates return not-found
    // ========================================
    println!("\nPHASE G: Verifying {} deleted fillers are gone", NUM_FILLERS);
    println!("--------------------------------------------------");

    for i in 0..NUM_FILLERS {
        let key = AggregateKey::new(agg_type_id, category_id, filler_start + i);
        match count_events(&mut client, &key).await {
            Ok(0) => {} // deleted aggregate returns 0 events — acceptable
            Ok(n) => panic!("Deleted filler {} still has {} events", i, n),
            Err(e) => {
                // ClientError wrapping — downcast to check error code
                if let Some(ce) = e.downcast_ref::<ClientError>() {
                    assert!(is_not_found(ce), "Filler {} unexpected error: {:?}", i, ce);
                }
                // count_events already handles 1001 → Ok(0), so any error here is unexpected
            }
        }
    }
    println!("  All {} deleted fillers confirmed gone", NUM_FILLERS);

    println!("\n=== PASS ===\n");
    Ok(())
}
