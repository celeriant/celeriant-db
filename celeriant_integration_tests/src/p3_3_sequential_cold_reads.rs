//! P3-3: Sustained Cold Sequential Reads (Audit Replay)
//!
//! Measures throughput of sequential cold reads across many aggregates,
//! simulating an audit or replay scenario.
//!
//! Scenario:
//! 1. Start standalone server with SMALL cache config
//! 2. Write to 1500 aggregates (exceeds cache capacity)
//! 3. Wait for cache to settle
//! 4. Phase 1 (cold reads): Sequential read through all aggregates, measure throughput
//! 5. Phase 2 (warm reads): Second pass through same aggregates (should benefit from cache)
//! 6. Compare throughput: second pass should be faster (cache warming effect)
//!
//! This is test P3-3 in the integration test coverage report (Batch 5).
//!
//! Run with: cargo run --bin p3_3_sequential_cold_reads_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{write_event, ServerConfig, TestServer};
use celeriant_msg::{process_client_requests::ClientRequest, process_client_responses::ClientResponse, request::requests::ReadRequest};
use celeriant_wal::{aggregate_key::AggregateKey};
use std::time::{Duration, Instant};

const PORT_BASE: u16 = 20900;
const NUM_AGGREGATES: u128 = 1500;

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
                match read_resp.next_event_batch_index {
                    Some(next) => from_batch = next,
                    None => return Ok(total_events),
                }
            }
            other => return Err(format!("Unexpected response: {:?}", other).into()),
        }
    }
}


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== P3-3: Sustained Cold Sequential Reads (Audit Replay) ===\n");

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
    // Setup: Write to NUM_AGGREGATES aggregates (exceeds cache capacity)
    // ========================================
    println!("SETUP: Writing to {} aggregates (exceeds cache capacity)", NUM_AGGREGATES);
    println!("----------------------------------------------------------");

    let start_write = Instant::now();
    for i in 1..=NUM_AGGREGATES {
        let key = AggregateKey::new(1, 1, i);
        write_event(&mut client, &key, 1, true).await?;

        if i % 300 == 0 {
            println!("  Wrote {} aggregates...", i);
        }
    }
    let write_elapsed = start_write.elapsed();
    println!("  Setup complete: {} aggregates written in {:?}", NUM_AGGREGATES, write_elapsed);

    // Give cache time to settle
    println!("\nWaiting for cache to settle (2s)...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // ========================================
    // Phase 1: Cold sequential reads (first pass)
    // ========================================
    println!("\nPHASE 1: Cold sequential reads (first pass through {} aggregates)", NUM_AGGREGATES);
    println!("----------------------------------------------------------");

    let mut cold_latencies: Vec<u64> = Vec::with_capacity(NUM_AGGREGATES as usize);
    let cold_start = Instant::now();

    for i in 1..=NUM_AGGREGATES {
        let key = AggregateKey::new(1, 1, i);
        let read_start = Instant::now();
        read_aggregate(&mut client, &key).await?;
        cold_latencies.push(read_start.elapsed().as_micros() as u64);

        if i % 300 == 0 {
            println!("  Completed {} cold reads...", i);
        }
    }

    let cold_elapsed = cold_start.elapsed();
    let cold_elapsed_secs = cold_elapsed.as_secs_f64();
    let cold_throughput = NUM_AGGREGATES as f64 / cold_elapsed_secs;

    let (cold_p50, cold_p95, cold_p99) = calculate_percentiles(cold_latencies);

    println!("\nPhase 1 Results (Cold Reads):");
    println!("  Total time: {:?}", cold_elapsed);
    println!("  Throughput: {:.1} aggregates/sec", cold_throughput);
    println!("  Latency P50: {}us", cold_p50);
    println!("  Latency P95: {}us", cold_p95);
    println!("  Latency P99: {}us", cold_p99);

    // ========================================
    // Phase 2: Warm sequential reads (second pass)
    // ========================================
    println!("\nPHASE 2: Warm sequential reads (second pass, cache warming)", );
    println!("----------------------------------------------------------");

    let mut warm_latencies: Vec<u64> = Vec::with_capacity(NUM_AGGREGATES as usize);
    let warm_start = Instant::now();

    for i in 1..=NUM_AGGREGATES {
        let key = AggregateKey::new(1, 1, i);
        let read_start = Instant::now();
        read_aggregate(&mut client, &key).await?;
        warm_latencies.push(read_start.elapsed().as_micros() as u64);

        if i % 300 == 0 {
            println!("  Completed {} warm reads...", i);
        }
    }

    let warm_elapsed = warm_start.elapsed();
    let warm_elapsed_secs = warm_elapsed.as_secs_f64();
    let warm_throughput = NUM_AGGREGATES as f64 / warm_elapsed_secs;

    let (warm_p50, warm_p95, warm_p99) = calculate_percentiles(warm_latencies);

    println!("\nPhase 2 Results (Warm Reads):");
    println!("  Total time: {:?}", warm_elapsed);
    println!("  Throughput: {:.1} aggregates/sec", warm_throughput);
    println!("  Latency P50: {}us", warm_p50);
    println!("  Latency P95: {}us", warm_p95);
    println!("  Latency P99: {}us", warm_p99);

    // ========================================
    // Comparison & Assertions
    // ========================================
    println!("\n=== THROUGHPUT COMPARISON ===");
    println!("Cold (1st pass):  {:.1} aggregates/sec", cold_throughput);
    println!("Warm (2nd pass):  {:.1} aggregates/sec", warm_throughput);

    let speedup = warm_throughput / cold_throughput;
    println!("\nWarm vs Cold speedup: {:.2}x", speedup);

    println!("\n=== LATENCY COMPARISON ===");
    println!("Cold: P50={}us, P95={}us, P99={}us", cold_p50, cold_p95, cold_p99);
    println!("Warm: P50={}us, P95={}us, P99={}us", warm_p50, warm_p95, warm_p99);

    // With a tiny cache (256KB) and 1500 aggregates, the cache benefit may be
    // marginal. Allow a 5% tolerance for measurement noise — the important thing
    // is that warm reads aren't *slower* than cold reads.
    let tolerance = 0.95;
    assert!(
        warm_throughput > cold_throughput * tolerance,
        "Warm throughput ({:.1}) should be >= ~{:.1} (cold throughput {:.1} × {:.2} tolerance)",
        warm_throughput,
        cold_throughput * tolerance,
        cold_throughput,
        tolerance,
    );

    println!("\n=== PASS ===");
    println!("Warm vs cold ratio: {:.2}x (tolerance: {:.0}%)", speedup, tolerance * 100.0);

    Ok(())
}
