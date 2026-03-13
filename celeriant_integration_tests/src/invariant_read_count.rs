//! Hypothesis 1: count_events accurately returns the number of events written
//!
//! This test verifies that count_events returns the exact count of events
//! written to each aggregate, testing both single-shard and multi-shard
//! configurations.

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_integration_tests::{count_events, write_event, ServerConfig, TestServer};
use celeriant_runtimes::RoutingRule;
use celeriant_wal::aggregate_key::AggregateKey;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Invariant Test: count_events Accuracy ===\n");

    let port = 10700 + (std::process::id() % 100) as u16;

    println!("Phase 1: Single aggregate, 1 shard");
    test_single_aggregate(port).await?;

    println!("\nPhase 2: Multiple aggregates, 4 shards");
    test_multi_shard(port + 10).await?;

    println!("\n=== All Invariant Tests Passed ===");
    Ok(())
}

async fn test_single_aggregate(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let config = ServerConfig {
        num_shards: Some(1),
        log_level: "warn".to_string(),
        standalone: true,
        ..Default::default()
    };

    let _server = TestServer::start_with_config(port, config).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut client = CeleriantClient::connect(&format!("127.0.0.1:{}", port)).await?;

    let key = AggregateKey::new(1, 1, 1);

    println!("  Writing 500 events...");
    for i in 1..=500 {
        write_event(&mut client, &key, i, i == 1).await?;
    }

    let count = count_events(&mut client, &key).await?;
    assert_eq!(
        count, 500,
        "Expected 500 events after first batch, got {}",
        count
    );
    println!("  Count after 500 writes: {} ✓", count);

    println!("  Writing another 500 events...");
    for i in 501..=1000 {
        write_event(&mut client, &key, i, false).await?;
    }

    let count = count_events(&mut client, &key).await?;
    assert_eq!(
        count, 1000,
        "Expected 1000 events after second batch, got {}",
        count
    );
    println!("  Count after 1000 writes: {} ✓", count);

    Ok(())
}

async fn test_multi_shard(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let config = ServerConfig {
        num_shards: Some(4),
        routing_rule: RoutingRule::AggregateTypeId,
        log_level: "warn".to_string(),
        standalone: true,
        ..Default::default()
    };

    let _server = TestServer::start_with_config(port, config).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut client = CeleriantClient::connect(&format!("127.0.0.1:{}", port)).await?;

    println!("  Writing 200 events to each of 4 aggregates (one per shard)...");
    for type_id in 0..4 {
        let key = AggregateKey::new(1, type_id, 1);
        for i in 1..=200 {
            write_event(&mut client, &key, i, i == 1).await?;
        }
        println!("    Aggregate type_id={} done", type_id);
    }

    println!("  Verifying counts...");
    for type_id in 0..4 {
        let key = AggregateKey::new(1, type_id, 1);
        let count = count_events(&mut client, &key).await?;
        assert_eq!(
            count, 200,
            "Expected 200 events for type_id={}, got {}",
            type_id, count
        );
        println!("    type_id={}: {} events ✓", type_id, count);
    }

    Ok(())
}
