//! P3-1: Cold Read Latency After Cache Eviction
//!
//! Measures read latency for warm (cached) vs cold (evicted) aggregates to validate
//! the reverse WAL scan + LRU architecture under cache pressure.
//!
//! Scenario:
//! 1. Start standalone server with SMALL cache config (~100 entries)
//! 2. Write to 1000 aggregates (10x cache capacity)
//! 3. Phase 1 (warm reads): Read same aggregate 100 times, collect latencies
//! 4. Phase 2 (cold reads): Read 100 different aggregates, collect latencies
//! 5. Calculate P50/P95/P99 for both
//! 6. Assert cold latency > warm latency (expected behavior)
//!
//! This is test P3-1 in the integration test coverage report (Batch 5).
//!
//! Run with: cargo run --bin p3_1_cold_read_latency_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{write_event, ServerConfig, TestServer};
use celeriant_msg::{process_client_requests::ClientRequest, process_client_responses::ClientResponse, request::requests::ReadRequest};
use celeriant_wal::{aggregate_key::AggregateKey};
use std::time::{Duration, Instant};

const PORT_BASE: u16 = 20500;
const NUM_AGGREGATES: u128 = 1000;
const WARM_READ_ITERATIONS: usize = 100;
const COLD_READ_COUNT: usize = 100;

fn calculate_percentiles(mut latencies: Vec<u64>) -> (u64, u64, u64) {
    latencies.sort_unstable();
    let p50 = latencies[latencies.len() * 50 / 100];
    let p95 = latencies[latencies.len() * 95 / 100];
    let p99 = latencies[latencies.len() * 99 / 100];
    (p50, p95, p99)
}

async fn read_aggregate(
    client: &mut CeleriantClient,
    aggregate_key: &AggregateKey,
) -> Result<usize, Box<dyn std::error::Error>> {
    let mut total_events = 0usize;
    let mut from_batch = 1u64;

    loop {
        let read_req = ReadRequest {
            correlation_id: Some(999),
            aggregate_key: aggregate_key.clone(),
            filters: celeriant_msg::request::read_filters::ReadFilters::new(from_batch),
        };

        let response = client
            .send_request(&ClientRequest::Read(read_req))
            .await?;

        match response {
            ClientResponse::Read(read_resp) => {
                total_events += read_resp
                    .event_batches
                    .iter()
                    .map(|b| b.events.len())
                    .sum::<usize>();
                match read_resp.next_aggregate_version {
                    Some(next) => from_batch = next,
                    None => return Ok(total_events),
                }
            }
            other => return Err(format!("Unexpected response: {:?}", other).into()),
        }
    }
}


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== P3-1: Cold Read Latency After Cache Eviction ===\n");

    // Start standalone server with SMALL cache
    let config = ServerConfig {
        log_level: "warn".to_string(),
        standalone: true,
        memory_budget_bytes: Some(256 * 1024), // ~256KB total — tiny cache
        num_shards: Some(1),
        ..Default::default()
    };

    println!("Starting standalone server on port {} with tiny cache...", PORT_BASE);
    let server = TestServer::start_with_config(PORT_BASE, config).await?;
    println!("Server started at {}\n", server.address());

    let mut client = CeleriantClient::connect(server.address()).await?;

    // ========================================
    // Setup: Write to 1000 aggregates (10x cache capacity)
    // ========================================
    println!("SETUP: Writing to {} aggregates (10x cache capacity)", NUM_AGGREGATES);
    println!("----------------------------------------------------------");

    for i in 1..=NUM_AGGREGATES {
        let key = AggregateKey::new(1, 1, i);
        write_event(&mut client, &key, 1, true).await?;

        if i % 200 == 0 {
            println!("  Wrote {} aggregates...", i);
        }
    }
    println!("  Setup complete: {} aggregates written", NUM_AGGREGATES);

    // Give cache time to settle
    println!("\nWaiting for cache to settle (2s)...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // ========================================
    // Phase 1: Warm reads (same aggregate, repeatedly)
    // ========================================
    println!("\nPHASE 1: Warm reads ({} iterations on same aggregate)", WARM_READ_ITERATIONS);
    println!("----------------------------------------------------------");

    let warm_key = AggregateKey::new(1, 1, 1);
    let mut warm_latencies: Vec<u64> = Vec::with_capacity(WARM_READ_ITERATIONS);

    // Prime cache with first read
    read_aggregate(&mut client, &warm_key).await?;

    for i in 0..WARM_READ_ITERATIONS {
        let start = Instant::now();
        read_aggregate(&mut client, &warm_key).await?;
        warm_latencies.push(start.elapsed().as_micros() as u64);

        if (i + 1) % 25 == 0 {
            println!("  Completed {} warm reads...", i + 1);
        }
    }

    let (warm_p50, warm_p95, warm_p99) = calculate_percentiles(warm_latencies);
    println!("\nWarm Read Latencies:");
    println!("  P50: {}us", warm_p50);
    println!("  P95: {}us", warm_p95);
    println!("  P99: {}us", warm_p99);

    // ========================================
    // Phase 2: Cold reads (different aggregates)
    // ========================================
    println!("\nPHASE 2: Cold reads ({} different aggregates)", COLD_READ_COUNT);
    println!("----------------------------------------------------------");

    let mut cold_latencies: Vec<u64> = Vec::with_capacity(COLD_READ_COUNT);

    // Read aggregates 500-599 (middle of range, unlikely to be cached)
    for i in 500..(500 + COLD_READ_COUNT as u128) {
        let cold_key = AggregateKey::new(1, 1, i);
        let start = Instant::now();
        read_aggregate(&mut client, &cold_key).await?;
        cold_latencies.push(start.elapsed().as_micros() as u64);

        if ((i - 500 + 1) as usize) % 25 == 0 {
            println!("  Completed {} cold reads...", i - 500 + 1);
        }
    }

    let (cold_p50, cold_p95, cold_p99) = calculate_percentiles(cold_latencies);
    println!("\nCold Read Latencies:");
    println!("  P50: {}us", cold_p50);
    println!("  P95: {}us", cold_p95);
    println!("  P99: {}us", cold_p99);

    // ========================================
    // Assertions & Summary
    // ========================================
    println!("\n=== LATENCY COMPARISON ===");
    println!("Warm: P50={}us, P95={}us, P99={}us", warm_p50, warm_p95, warm_p99);
    println!("Cold: P50={}us, P95={}us, P99={}us", cold_p50, cold_p95, cold_p99);

    let cold_vs_warm_ratio = cold_p50 as f64 / warm_p50.max(1) as f64;
    println!("\nCold vs Warm P50 ratio: {:.2}x", cold_vs_warm_ratio);

    // Assert cold reads are slower than warm reads
    assert!(
        cold_p50 > warm_p50,
        "Cold read P50 ({}) should be > warm read P50 ({})",
        cold_p50,
        warm_p50
    );

    println!("\n=== PASS ===");
    println!("Cold reads are measurably slower than warm reads (expected behavior)");

    Ok(())
}
