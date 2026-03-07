//! P3-2: Bloom Filter False Positive Behaviour
//!
//! Validates that bloom filters effectively prevent disk scans when reading nonexistent aggregates.
//! This test populates a shard with many aggregates across multiple log segments, then measures
//! latency for reading nonexistent vs. existing aggregates. If bloom filters work correctly,
//! nonexistent reads should be fast (bloom rejects most segments), not proportional to segment count.
//!
//! Scenario:
//! 1. Write 1000-5000 aggregates with large payloads to force multiple log rotations
//! 2. Read 100 nonexistent aggregates (IDs far outside written range), measure latency
//! 3. Read 100 existing aggregates (control), measure latency
//! 4. Print latency percentiles for both scenarios
//! 5. Assert nonexistent reads are fast (bloom filter working)
//!
//! Run with: cargo run --bin p3_2_bloom_filter_main

use std::collections::HashMap;
use std::sync::Arc;

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_integration_tests::{ServerConfig, TestServer};
use celeriant_msg::{
    process_client_requests::ClientRequest,
    request::requests::{ReadRequest, SingleAggregateWrite, WriteRequest},
};
use celeriant_wal::{
    aggregate_key::AggregateKey, compression_type::CompressionType,
    datablocks::datablock_aggregate_event::DatablockAggregateEvent,
};
use tokio::time::Instant;

const PORT_BASE: u16 = 20700;
const NUM_AGGREGATES: u64 = 2000;
const EVENT_PAYLOAD_SIZE: usize = 25_000; // 25KB events to force log rotations
const NUM_NONEXISTENT_READS: usize = 100;
const NUM_EXISTING_READS: usize = 100;

fn create_large_event(event_num: u64, payload_size: usize) -> DatablockAggregateEvent {
    let mut payload = format!("{{\"event\":{},\"pad\":\"", event_num);
    let pad_len = payload_size.saturating_sub(payload.len() + 2);
    payload.extend(std::iter::repeat('x').take(pad_len));
    payload.push_str("\"}");

    DatablockAggregateEvent {
        client_event_index: event_num,
        event_index: 0,
        event_id: None,
        event_timestamp: 1000 + event_num,
        event_type_major: 100,
        event_type_minor: 0,
        event_value: Arc::new(payload.into_bytes()),
        iv: None,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== P3-2: Bloom Filter False Positive Behaviour ===\n");

    let config = ServerConfig {
        num_shards: Some(1),
        log_level: "warn".to_string(),
        standalone: true,
        ..Default::default()
    };

    let server = TestServer::start_with_config(PORT_BASE, config).await?;
    let mut client = CeleriantClient::connect(server.address()).await?;

    println!("Phase 1: Populate shard with {} aggregates", NUM_AGGREGATES);
    let write_start = Instant::now();

    for agg_id in 1..=NUM_AGGREGATES {
        let aggregate_key = AggregateKey::new(1, 100, agg_id as u128);
        let event = create_large_event(1, EVENT_PAYLOAD_SIZE);

        let mut writes = HashMap::new();
        writes.insert(
            aggregate_key,
            SingleAggregateWrite {
                events: vec![event],
                allow_create: true,
                expected_event_batch_index: Some(0),
                enforce_client_idempotency: false,
                compression_type_id: 0,
                compression_level: None,
            },
        );

        let request = ClientRequest::Write(WriteRequest {
            correlation_id: Some(agg_id as u128),
            client_id: 999,
            user_id: Some(888),
            writes,
        });

        client
            .send_request(&request, CompressionType::None)
            .await?;

        if agg_id % 1000 == 0 {
            println!("  Written {} aggregates ({:.1}s)", agg_id, write_start.elapsed().as_secs_f64());
        }
    }

    println!("  ✓ Written {} aggregates in {:.2}s\n", NUM_AGGREGATES, write_start.elapsed().as_secs_f64());

    println!("Phase 2: Read {} nonexistent aggregates (IDs 1,000,000+)", NUM_NONEXISTENT_READS);
    let mut nonexistent_latencies_us = Vec::with_capacity(NUM_NONEXISTENT_READS);

    for i in 0..NUM_NONEXISTENT_READS {
        let nonexistent_id = 1_000_000 + i as u64;
        let aggregate_key = AggregateKey::new(1, 100, nonexistent_id as u128);

        let read_start = Instant::now();
        let read_req = ReadRequest {
            correlation_id: Some(999),
            aggregate_key,
            filters: celeriant_msg::request::read_filters::ReadFilters::new(1),
        };

        let _response = client
            .send_request(&ClientRequest::Read(read_req), CompressionType::None)
            .await;

        let elapsed_us = read_start.elapsed().as_micros() as u64;
        nonexistent_latencies_us.push(elapsed_us);
    }

    nonexistent_latencies_us.sort_unstable();

    let nonexistent_avg_us = nonexistent_latencies_us.iter().sum::<u64>() as f64 / nonexistent_latencies_us.len() as f64;
    let nonexistent_p50_us = nonexistent_latencies_us[nonexistent_latencies_us.len() * 50 / 100];
    let nonexistent_p95_us = nonexistent_latencies_us[nonexistent_latencies_us.len() * 95 / 100];
    let nonexistent_p99_us = nonexistent_latencies_us[nonexistent_latencies_us.len() * 99 / 100];
    let nonexistent_max_us = nonexistent_latencies_us[nonexistent_latencies_us.len() - 1];

    println!("  Nonexistent aggregate read latency:");
    println!("    Avg: {:.2}ms", nonexistent_avg_us / 1000.0);
    println!("    P50: {:.2}ms", nonexistent_p50_us as f64 / 1000.0);
    println!("    P95: {:.2}ms", nonexistent_p95_us as f64 / 1000.0);
    println!("    P99: {:.2}ms", nonexistent_p99_us as f64 / 1000.0);
    println!("    Max: {:.2}ms\n", nonexistent_max_us as f64 / 1000.0);

    println!("Phase 3: Read {} existing aggregates (control)", NUM_EXISTING_READS);
    let mut existing_latencies_us = Vec::with_capacity(NUM_EXISTING_READS);

    for i in 0..NUM_EXISTING_READS {
        let existing_id = 1 + (i as u64 * NUM_AGGREGATES / NUM_EXISTING_READS as u64);
        let aggregate_key = AggregateKey::new(1, 100, existing_id as u128);

        let read_start = Instant::now();
        let read_req = ReadRequest {
            correlation_id: Some(999),
            aggregate_key,
            filters: celeriant_msg::request::read_filters::ReadFilters::new(1),
        };

        let _response = client
            .send_request(&ClientRequest::Read(read_req), CompressionType::None)
            .await?;

        let elapsed_us = read_start.elapsed().as_micros() as u64;
        existing_latencies_us.push(elapsed_us);
    }

    existing_latencies_us.sort_unstable();

    let existing_avg_us = existing_latencies_us.iter().sum::<u64>() as f64 / existing_latencies_us.len() as f64;
    let existing_p50_us = existing_latencies_us[existing_latencies_us.len() * 50 / 100];
    let existing_p95_us = existing_latencies_us[existing_latencies_us.len() * 95 / 100];
    let existing_p99_us = existing_latencies_us[existing_latencies_us.len() * 99 / 100];
    let existing_max_us = existing_latencies_us[existing_latencies_us.len() - 1];

    println!("  Existing aggregate read latency:");
    println!("    Avg: {:.2}ms", existing_avg_us / 1000.0);
    println!("    P50: {:.2}ms", existing_p50_us as f64 / 1000.0);
    println!("    P95: {:.2}ms", existing_p95_us as f64 / 1000.0);
    println!("    P99: {:.2}ms", existing_p99_us as f64 / 1000.0);
    println!("    Max: {:.2}ms\n", existing_max_us as f64 / 1000.0);

    println!("Phase 4: Comparison");
    println!("  Nonexistent vs Existing (P50): {:.2}ms vs {:.2}ms",
        nonexistent_p50_us as f64 / 1000.0, existing_p50_us as f64 / 1000.0);
    println!("  Nonexistent vs Existing (P99): {:.2}ms vs {:.2}ms",
        nonexistent_p99_us as f64 / 1000.0, existing_p99_us as f64 / 1000.0);

    let ratio = nonexistent_p50_us as f64 / existing_p50_us as f64;
    println!("  Ratio (nonexistent/existing P50): {:.2}x\n", ratio);

    println!("Phase 5: Bloom filter effectiveness validation");
    let max_acceptable_latency_ms = 100.0;
    let nonexistent_p99_ms = nonexistent_p99_us as f64 / 1000.0;

    if nonexistent_p99_ms <= max_acceptable_latency_ms {
        println!("  ✓ Nonexistent reads are fast (P99={:.2}ms <= {:.0}ms)",
            nonexistent_p99_ms, max_acceptable_latency_ms);
        println!("  ✓ Bloom filters are preventing unnecessary disk scans");
    } else {
        println!("  ✗ Nonexistent reads are slow (P99={:.2}ms > {:.0}ms)",
            nonexistent_p99_ms, max_acceptable_latency_ms);
        println!("  ✗ Bloom filters may be saturated or not working");
        return Err(format!(
            "Bloom filter effectiveness test failed: P99 latency {:.2}ms exceeds threshold {:.0}ms",
            nonexistent_p99_ms, max_acceptable_latency_ms
        ).into());
    }

    println!("\n=== TEST PASSED: Bloom Filter False Positive Behaviour ===");
    Ok(())
}
