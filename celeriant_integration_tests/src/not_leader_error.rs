//! NotLeader Client Error Integration Test
//!
//! Verifies that writing to a follower returns `ClientError::NotLeader`
//! with the leader's address, so clients can redirect.
//!
//! Run with: cargo run -p celeriant_integration_tests --bin not_leader_error_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use crate::{write_event, MinioContainer, ServerConfig, TestServer};
use celeriant_runtimes::RoutingRule;
use celeriant_wal::aggregate_key::AggregateKey;
use std::time::Duration;


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== NotLeader Client Error Integration Test ===\n");

    let port_base = 12700 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let minio_port = port_base + 10;

    println!("Starting MinIO on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-not-leader").await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();

    let cluster_config = |port_label: &str, port: u16| -> ServerConfig {
        println!("Starting {} on port {}...", port_label, port);
        ServerConfig {
            num_shards: Some(1),
            log_level: "warn".to_string(),
            routing_rule: RoutingRule::AggregateTypeId,
            s3_enabled: true,
            s3_region: Some(region.clone()),
            s3_bucket: Some(bucket.clone()),
            s3_access_key_id: Some(access_key.clone()),
            s3_secret_access_key: Some(secret_key.clone()),
            s3_endpoint_override: Some(endpoint.clone()),
            s3_allow_http: allow_http,
            s3_skip_signature: false,
            ..Default::default()
        }
    };

    let _node_a = TestServer::start_with_config(leader_port, cluster_config("Node A", leader_port)).await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let _node_b = TestServer::start_with_config(follower_port, cluster_config("Node B", follower_port)).await?;

    println!("Waiting for election and heartbeat (3s)...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Determine which node is leader by trying a write to each
    let agg = AggregateKey::new(1, 1, 1);
    let mut client_a = CeleriantClient::connect(_node_a.address()).await?;
    let mut client_b = CeleriantClient::connect(_node_b.address()).await?;

    let (follower_client, follower_name, leader_port_str) =
        match write_event(&mut client_a, &agg, 1, true).await {
            Ok(_) => {
                println!("Node A is leader, Node B is follower");
                (&mut client_b, "Node B", leader_port.to_string())
            }
            Err(_) => {
                println!("Node B is leader, Node A is follower");
                write_event(&mut client_b, &agg, 1, true).await?;
                (&mut client_a, "Node A", follower_port.to_string())
            }
        };

    // ========================================
    // TEST 1: Write to follower returns NotLeader with leader address
    // ========================================
    println!("\nTEST 1: Write to follower returns NotLeader");
    println!("-------------------------------------------");

    let result = write_event(follower_client, &agg, 99, false).await;
    match result {
        Err(ref e) => {
            // The write_event helper wraps ClientError in Box<dyn Error>, so downcast
            let client_err = e.downcast_ref::<ClientError>()
                .expect("expected ClientError");
            match client_err {
                ClientError::NotLeader { leader_address: Some(addr), .. } => {
                    println!("  {} returned NotLeader, leader_address={}", follower_name, addr);
                    // Server binds on 0.0.0.0, so compare by port
                    assert!(
                        addr.ends_with(&format!(":{}", leader_port_str)),
                        "leader_address '{}' should contain leader port {}",
                        addr, leader_port_str
                    );
                    println!("  leader_address port matches actual leader");
                }
                ClientError::NotLeader { leader_address: None, .. } => {
                    panic!("NotLeader returned but leader_address was None — follower should know the leader");
                }
                other => {
                    panic!("Expected NotLeader, got: {:?}", other);
                }
            }
        }
        Ok(_) => panic!("Write to follower should have failed with NotLeader"),
    }
    println!("  PASSED");

    println!("\n=== All Tests Passed ===\n");
    Ok(())
}
