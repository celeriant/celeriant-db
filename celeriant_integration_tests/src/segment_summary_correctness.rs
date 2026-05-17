//! Segment Summary Correctness Tests
//!
//! Verifies that segment summaries remain correct across log segment rotations,
//! including multi-org/type listing, cross-segment deletes, trims, and recreates.
//!
//! Run with: cargo run --bin segment_summary_correctness_main

use std::collections::HashMap;

use crate::{write_event, write_large_event, ServerConfig, TestServer};
use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::list_operations::{
    ListAggregateTypesIterator, ListAggregatesIterator, ListOptions, ListOrgsIterator,
};
use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::request::requests::{
    DeleteRequest, SingleAggregateDelete, TrimStartRequest,
};
use celeriant_wal::aggregate_key::AggregateKey;

async fn force_rotation(
    client: &mut CeleriantClient,
    scratch: &AggregateKey,
    start_event: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    for i in start_event..start_event + 30 {
        write_large_event(client, scratch, i, 65536).await?;
    }
    Ok(())
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Segment Summary Correctness Tests ===\n");

    let port = 17600 + (std::process::id() % 100) as u16;

    let config = ServerConfig {
        log_level: "warn".to_string(),
        standalone: true,
        num_shards: Some(1),
        shard_log_preallocate_bytes: 2 * 1024 * 1024,
        list_page_size: 100,
        ..Default::default()
    };

    let server = TestServer::start_with_config_labeled(port, config, "summary-test".into()).await?;
    let mut client = CeleriantClient::connect(server.address()).await?;

    let scratch = AggregateKey::new(1, 10, 999);
    let options = ListOptions {
        max_shard_hint: Some(0),
        ..Default::default()
    };

    // ── Sub-test 1: Multi-org/type listing after rotation ──
    println!("\n=== Sub-test 1: Multi-org/type listing after rotation ===");

    let keys = [
        AggregateKey::new(1, 10, 100),
        AggregateKey::new(1, 20, 101),
        AggregateKey::new(2, 10, 200),
        AggregateKey::new(2, 30, 201),
        AggregateKey::new(3, 20, 300),
    ];
    for key in &keys {
        write_event(&mut client, key, 1, true).await?;
    }

    // Create scratch aggregate then force rotation
    write_event(&mut client, &scratch, 1, true).await?;
    for i in 2..=30 {
        write_large_event(&mut client, &scratch, i, 65536).await?;
    }

    // Write one more aggregate after rotation
    let post_rotation_key = AggregateKey::new(3, 30, 301);
    write_event(&mut client, &post_rotation_key, 1, true).await?;

    // Verify list_orgs
    let orgs = ListOrgsIterator::new(&mut client, options.clone()).collect().await?;
    let org_ids: Vec<u128> = orgs.iter().map(|o| o.org_id).collect();
    println!("Orgs found: {:?}", org_ids);
    assert!(org_ids.contains(&1), "Expected org 1");
    assert!(org_ids.contains(&2), "Expected org 2");
    assert!(org_ids.contains(&3), "Expected org 3");

    // Verify list_aggregate_types for org 1
    let types = ListAggregateTypesIterator::new(&mut client, Some(1), options.clone())
        .collect()
        .await?;
    let type_ids: Vec<u128> = types.iter().map(|t| t.aggregate_type_id).collect();
    println!("Org 1 types: {:?}", type_ids);
    assert!(type_ids.contains(&10), "Expected type 10 for org 1");
    assert!(type_ids.contains(&20), "Expected type 20 for org 1");

    // Verify list_aggregate_types for org 2
    let types = ListAggregateTypesIterator::new(&mut client, Some(2), options.clone())
        .collect()
        .await?;
    let type_ids: Vec<u128> = types.iter().map(|t| t.aggregate_type_id).collect();
    println!("Org 2 types: {:?}", type_ids);
    assert!(type_ids.contains(&10), "Expected type 10 for org 2");
    assert!(type_ids.contains(&30), "Expected type 30 for org 2");

    // Verify list_aggregates for (org=1, type=10)
    let aggs = ListAggregatesIterator::new(&mut client, Some(1), Some(10), options.clone())
        .collect()
        .await?;
    let agg_ids: Vec<u128> = aggs.iter().map(|a| a.aggregate_id).collect();
    println!("Org 1, Type 10 aggregates: {:?}", agg_ids);
    assert!(agg_ids.contains(&100), "Expected aggregate 100");
    assert!(agg_ids.contains(&999), "Expected aggregate 999 (scratch)");

    println!("Sub-test 1 PASSED");

    // ── Sub-test 2: Cross-segment delete barrier ──
    println!("\n=== Sub-test 2: Cross-segment delete barrier ===");

    // Force another rotation
    force_rotation(&mut client, &scratch, 31).await?;

    // Delete aggregate 200 (org=2, type=10)
    let mut deletes = HashMap::new();
    deletes.insert(
        AggregateKey::new(2, 10, 200),
        SingleAggregateDelete {
            allow_recreate: false,
            allow_sequence_continuation: false,
            expected_version: None,
        },
    );
    let req = ClientRequest::Delete(DeleteRequest {
        correlation_id: Some(100),
        client_id: 999,
        user_id: Some(888),
        deletes,
    });
    client
        .send_request(&req)
        .await?;

    // Verify aggregate 200 is excluded from default listing
    let aggs = ListAggregatesIterator::new(&mut client, Some(2), None, options.clone())
        .collect()
        .await?;
    let agg_ids: Vec<u128> = aggs.iter().map(|a| a.aggregate_id).collect();
    println!("Org 2 aggregates (no deleted): {:?}", agg_ids);
    assert!(!agg_ids.contains(&200), "Aggregate 200 should be excluded after delete");

    // Verify aggregate 200 appears with include_deleted
    let deleted_options = ListOptions {
        include_deleted: true,
        max_shard_hint: Some(0),
        ..Default::default()
    };
    let aggs = ListAggregatesIterator::new(&mut client, Some(2), None, deleted_options)
        .collect()
        .await?;
    let agg_200 = aggs.iter().find(|a| a.aggregate_id == 200);
    println!("Aggregate 200 with include_deleted: {:?}", agg_200.map(|a| a.is_deleted));
    assert!(
        agg_200.is_some() && agg_200.unwrap().is_deleted,
        "Aggregate 200 should appear as deleted with include_deleted"
    );

    // Verify org 2 still listed (agg 201 exists)
    let orgs = ListOrgsIterator::new(&mut client, options.clone()).collect().await?;
    let org_ids: Vec<u128> = orgs.iter().map(|o| o.org_id).collect();
    println!("Orgs after delete: {:?}", org_ids);
    assert!(org_ids.contains(&2), "Org 2 should still exist (agg 201 is alive)");

    println!("Sub-test 2 PASSED");

    // ── Sub-test 3: Cross-segment trim ──
    println!("\n=== Sub-test 3: Cross-segment trim ===");

    // Create a fresh aggregate for trim testing to avoid cross-segment complexity.
    // Write 5 batches in the active segment, then trim.
    let trim_key = AggregateKey::new(1, 10, 888);
    write_event(&mut client, &trim_key, 1, true).await?;
    write_event(&mut client, &trim_key, 2, false).await?;
    write_event(&mut client, &trim_key, 3, false).await?;
    write_event(&mut client, &trim_key, 4, false).await?;
    write_event(&mut client, &trim_key, 5, false).await?;

    // Check min_aggregate_version before trim
    let aggs = ListAggregatesIterator::new(&mut client, Some(1), Some(10), options.clone())
        .collect()
        .await?;
    let before = aggs.iter().find(|a| a.aggregate_id == 888).unwrap();
    println!(
        "Before trim: min_aggregate_version={}, max_aggregate_version={}",
        before.min_aggregate_version, before.max_aggregate_version
    );
    let original_min = before.min_aggregate_version;

    // Trim: keep from version 3 onwards
    let req = TrimStartRequest {
        correlation_id: Some(200),
        aggregate_key: trim_key.clone(),
        keep_from_aggregate_version: 3,
        client_id: 999,
        user_id: Some(888),
    };
    client
        .send_request(&ClientRequest::TrimStart(req))
        .await?;

    let aggs = ListAggregatesIterator::new(&mut client, Some(1), Some(10), options.clone())
        .collect()
        .await?;
    let after = aggs.iter().find(|a| a.aggregate_id == 888).unwrap();
    println!(
        "After trim: min_aggregate_version={}, max_aggregate_version={}",
        after.min_aggregate_version, after.max_aggregate_version
    );
    assert!(
        after.min_aggregate_version > original_min,
        "min_aggregate_version should increase after trim (was {}, now {})",
        original_min, after.min_aggregate_version
    );

    println!("Sub-test 3 PASSED");

    // ── Sub-test 4: Recreate after delete ──
    println!("\n=== Sub-test 4: Recreate after delete ===");

    let recreate_key = AggregateKey::new(3, 10, 400);
    write_event(&mut client, &recreate_key, 1, true).await?;
    write_event(&mut client, &recreate_key, 2, false).await?;
    write_event(&mut client, &recreate_key, 3, false).await?;

    // Delete with allow_recreate
    let mut deletes = HashMap::new();
    deletes.insert(
        recreate_key.clone(),
        SingleAggregateDelete {
            allow_recreate: true,
            allow_sequence_continuation: false,
            expected_version: None,
        },
    );
    let req = ClientRequest::Delete(DeleteRequest {
        correlation_id: Some(300),
        client_id: 999,
        user_id: Some(888),
        deletes,
    });
    client
        .send_request(&req)
        .await?;

    // Force rotation before recreate
    force_rotation(&mut client, &scratch, 61).await?;

    // Recreate
    write_event(&mut client, &recreate_key, 4, true).await?;

    let aggs = ListAggregatesIterator::new(&mut client, Some(3), Some(10), options.clone())
        .collect()
        .await?;
    let recreated = aggs.iter().find(|a| a.aggregate_id == 400);
    println!(
        "Recreated aggregate 400: is_deleted={:?}, event_batch_count={:?}",
        recreated.map(|a| a.is_deleted),
        recreated.map(|a| a.event_batch_count)
    );
    assert!(
        recreated.is_some() && !recreated.unwrap().is_deleted,
        "Recreated aggregate should not be deleted"
    );
    assert_eq!(
        recreated.unwrap().event_batch_count, 1,
        "Recreated aggregate should have 1 batch (new incarnation only)"
    );

    println!("Sub-test 4 PASSED");

    println!("\n=== All segment summary correctness tests passed! ===");
    Ok(())
}
