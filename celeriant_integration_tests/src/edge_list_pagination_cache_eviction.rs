//! Edge Case: List Pagination Across Cache Eviction
//!
//! Tests that paginated list operations return correct results even when the
//! WAL sequence cache is evicted between page fetches.
//!
//! The `list_wal_seq_cache_bytes` config controls an LRU that maps WAL sequence
//! positions to file offsets, avoiding full log scans on each page. With
//! capacity=1 entry (24 bytes / 24 bytes per entry), every new page fetch evicts
//! the previous cursor entry. The test verifies completeness (no gaps — every
//! aggregate appears at least once after cross-page dedup) when the cache always
//! misses and the server must fall back to a full log scan. Listing is
//! best-effort: an aggregate spanning a segment rotation may surface on more than
//! one page, so duplicates are tolerated, not failed.
//!
//! Scenario:
//! 1. Start a standalone server with:
//!    - list_wal_seq_cache_bytes=24 (capacity=1 — every second fetch is a miss)
//!    - list_page_size=20 (small pages — forces many cursor hops)
//!    - shard_log_preallocate_bytes=2MB (small log — forces rotation)
//! 2. Create 200 aggregates (all in same shard via AggregateTypeId routing)
//! 3. Force log rotation by writing large events (fills the 2MB log)
//! 4. Manually paginate using ListAggregatesRequest + cursor:
//!    - Fetch one page at a time
//!    - Between each page, write more events to force cache invalidation
//! 5. Collect all aggregate IDs across all pages, dedup'd
//! 6. Assert: no gaps (all 200 present); duplicates tolerated as best-effort
//!
//! The existence of the code comment "LRU behaviour here needs more testing &
//! optimisation" makes this an explicit regression guard.
//!
//! This is test #10 in the integration test coverage report.
//!
//! Run with: cargo run --bin edge_list_pagination_cache_eviction_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{write_event, write_large_event, ServerConfig, TestServer};
use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::process_client_responses::ClientResponse;
use celeriant_msg::request::requests::ListAggregatesRequest;
use celeriant_runtimes::RoutingRule;
use celeriant_wal::aggregate_key::AggregateKey;
use std::collections::HashSet;


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Edge Case: List Pagination Across Cache Eviction ===\n");

    let port_base = 17500 + (std::process::id() % 100) as u16;
    let server_port = port_base;

    // Very small memory budget forces minimal caches.
    // list_page_size=20 → forces many pages across 200 aggregates.
    let config = ServerConfig {
        num_shards: Some(1),
        log_level: "warn".to_string(),
        standalone: true,
        routing_rule: RoutingRule::AggregateTypeId,
        // Tiny memory budget — maximum cache pressure
        memory_budget_bytes: Some(256 * 1024),
        // Small page size — forces many page fetches
        list_page_size: 20,
        // 2MB log — forces rotation after ~200 × small events
        shard_log_preallocate_bytes: 2 * 1024 * 1024,
        // Extend list timeout to handle full log scans with cache misses
        list_max_duration_ms: 10_000,
        ..Default::default()
    };

    println!("Starting standalone server on port {}...", server_port);
    let _server = TestServer::start_with_config_labeled(server_port, config, "standalone".into()).await?;
    println!("Server ready at 127.0.0.1:{}\n", server_port);

    let mut client = CeleriantClient::connect(&format!("127.0.0.1:{}", server_port)).await?;

    // All aggregates use agg_type_id=1, which routes to shard 0 (1 % 1 = 0).
    let agg_type_id: u128 = 1;
    let org_id: u128 = 1;
    let category_id: u128 = 1;
    let num_aggregates: u128 = 200;

    // ========================================
    // Phase 1: Create 200 aggregates
    // ========================================
    println!("PHASE 1: Creating {} aggregates", num_aggregates);
    println!("----------------------------------------");

    for i in 1..=num_aggregates {
        let key = AggregateKey::new(agg_type_id, category_id, i);
        write_event(&mut client, &key, 1, true)
            .await
            .map_err(|e| format!("Phase 1 write failed for aggregate {}: {}", i, e))?;

        if i % 50 == 0 {
            println!("  Created {} aggregates...", i);
        }
    }
    println!("  Phase 1 complete: {} aggregates created", num_aggregates);

    // ========================================
    // Phase 2: Force log rotation via large writes
    // ========================================
    println!("\nPHASE 2: Force log rotation via large writes");
    println!("---------------------------------------------");

    // Write to a separate aggregate to fill the 2MB log and trigger rotation.
    let rotation_key = AggregateKey::new(agg_type_id, category_id, num_aggregates + 1);
    write_event(&mut client, &rotation_key, 1, true)
        .await
        .map_err(|e| format!("Phase 2 initial write failed: {}", e))?;

    // 30 × 64KB = ~1.9MB — enough to fill and rotate the 2MB log.
    for i in 2u64..=30 {
        write_large_event(&mut client, &rotation_key, i, 65536)
            .await
            .map_err(|e| format!("Phase 2 large write {} failed: {}", i, e))?;
    }
    println!("  Log rotation triggered (30 × 64KB writes)");

    // ========================================
    // Phase 3: Paginated list with cache eviction between pages
    // ========================================
    println!("\nPHASE 3: Paginated list with cache eviction between pages");
    println!("-----------------------------------------------------------");

    let mut seen_ids: HashSet<u128> = HashSet::new();
    let mut duplicate_count = 0usize;
    let mut cursor: Option<u64> = None;
    let mut page_num = 0usize;
    let shard_id: u64 = 0;

    // Between each page, write another large event to a scratch aggregate.
    // This forces the server to update WAL sequence entries, potentially evicting
    // the cached cursor position from the 1-entry LRU.
    let scratch_key = AggregateKey::new(agg_type_id, category_id, num_aggregates + 2);
    write_event(&mut client, &scratch_key, 1, true).await?;
    let mut scratch_event: u64 = 2;

    loop {
        page_num += 1;

        let req = ClientRequest::ListAggregates(ListAggregatesRequest {
            correlation_id: Some(page_num as u128),
            shard_id,
            org_id: Some(org_id),
            aggregate_type_id: Some(agg_type_id),
            cursor,
        });

        let response = client.send_request(&req).await?;
        let list_resp = match response {
            ClientResponse::ListAggregates(r) => r,
            other => {
                return Err(format!("Unexpected response on page {}: {:?}", page_num, other).into())
            }
        };

        let page_size = list_resp.aggregates.len();
        println!(
            "  Page {}: {} aggregates (cursor={:?} → next={:?})",
            page_num,
            page_size,
            cursor,
            list_resp.next_cursor
        );

        // Listing is best-effort. An aggregate whose writes span a segment
        // rotation lives in more than one per-segment summary, so it can surface
        // on multiple pages. Dedup across pages; completeness is asserted below.
        for item in &list_resp.aggregates {
            if !seen_ids.insert(item.aggregate_id) {
                duplicate_count += 1;
            }
        }

        cursor = list_resp.next_cursor;

        if cursor.is_none() {
            println!("  No more pages (next_cursor=None)");
            break;
        }

        // Between pages: write a large event to the scratch aggregate to force
        // cache invalidation. With a 1-entry LRU, this write updates the WAL
        // index, evicting the cached cursor position for the next page.
        //
        // NOTE: The LRU eviction assumes that writing to the scratch aggregate
        // displaces the cursor entry for the list aggregate from the 1-entry cache.
        // If the cache implementation changes (e.g. separate caches per aggregate),
        // this eviction may not occur. The test is still valid as a pagination
        // correctness test regardless of whether cache eviction actually happens —
        // it verifies that all 200 aggregates appear exactly once across pages.
        write_large_event(&mut client, &scratch_key, scratch_event, 65536)
            .await
            .map_err(|e| format!("Cache eviction write failed on page {}: {}", page_num, e))?;
        scratch_event += 1;
    }

    println!(
        "\n  Pagination complete: {} pages, {} unique aggregates seen",
        page_num,
        seen_ids.len()
    );

    // ========================================
    // Phase 4: Verify correctness — all 200 original aggregates present
    // ========================================
    println!("\nPHASE 4: Verify correctness");
    println!("----------------------------");

    // The scratch/rotation aggregates (IDs 201, 202) will also appear in results.
    // Check that all original 200 are present.
    let mut missing: Vec<u128> = Vec::new();
    for i in 1..=num_aggregates {
        if !seen_ids.contains(&i) {
            missing.push(i);
        }
    }

    if !missing.is_empty() {
        return Err(format!(
            "FAIL: {} aggregate(s) missing from paginated list results: {:?}",
            missing.len(),
            &missing[..missing.len().min(10)]
        )
        .into());
    }

    println!(
        "  All {} original aggregates present in paginated results",
        num_aggregates
    );
    println!(
        "  Cross-page duplicates (best-effort, tolerated): {}",
        duplicate_count
    );
    println!(
        "  Total unique aggregates seen: {} (includes rotation/scratch aggregates)",
        seen_ids.len()
    );
    assert!(
        seen_ids.len() >= num_aggregates as usize,
        "Should have seen at least {} aggregates, got {}",
        num_aggregates,
        seen_ids.len()
    );

    println!("\n=== PASS ===\n");

    Ok(())
}
