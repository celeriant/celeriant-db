//! Single-Aggregate Integration Tests
//!
//! Tests basic CRUD operations, idempotency, and listing functionality.
//! Creates a temporary data directory and spawns the server automatically.
//!
//! Run with: cargo run --bin single_main

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use celeriant_runtimes::RoutingRule;

use crate::{poll_converged_count, MinioContainer, ServerConfig, TestServer, FOLLOWER_CONVERGENCE_TIMEOUT};
use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use celeriant_client_tokio::list_operations::{
    ListAggregateTypesIterator, ListAggregatesIterator, ListOptions, ListOrgsIterator,
};
use celeriant_msg::{
    process_client_requests::ClientRequest,
    request::{
        read_filters::ReadFilters,
        requests::{
            DeleteRequest, AggregateDetailsRequest, ReadRequest, SingleAggregateDelete, SingleAggregateWrite,
            WriteRequest,
        },
    },
};
use celeriant_wal::{
    aggregate_key::AggregateKey,
    datablocks::datablock_aggregate_event::DatablockAggregateEvent,
};

/// Enable replicated mode: writes to leader, reads from follower
const REPLICATED_MODE: bool = true;

fn s3_cluster_config(
    region: &str,
    bucket: &str,
    access_key: &str,
    secret_key: &str,
    endpoint: &str,
    allow_http: bool,
) -> ServerConfig {
    ServerConfig {
        log_level: "warn".to_string(),
        routing_rule: RoutingRule::AggregateTypeId,
        heartbeat_lease_duration_ms: 10_000,
        s3_enabled: true,
        s3_region: Some(region.to_string()),
        s3_bucket: Some(bucket.to_string()),
        s3_access_key_id: Some(access_key.to_string()),
        s3_secret_access_key: Some(secret_key.to_string()),
        s3_endpoint_override: Some(endpoint.to_string()),
        s3_allow_http: allow_http,
        ..Default::default()
    }
}

struct ReplicatedServers {
    leader: TestServer,
    follower: TestServer,
    _minio: MinioContainer,
}

impl ReplicatedServers {
    async fn start(base_port: u16) -> Result<Self, Box<dyn std::error::Error>> {
        let minio_port = base_port + 10;

        println!("Starting MinIO on port {}...", minio_port);
        let minio = MinioContainer::start_with_bucket(minio_port, "test-single").await?;
        let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();
        println!("MinIO ready at {}\n", endpoint);

        let config = s3_cluster_config(&region, &bucket, &access_key, &secret_key, &endpoint, allow_http);

        // Leader starts first — wins CreateOnly election race
        let follower_port = base_port + 100;
        println!("Starting leader on port {}...", base_port);
        let leader = TestServer::start_with_config_labeled(base_port, config.clone(), "leader".into()).await?;
        println!("Starting follower on port {}...", follower_port);
        let follower = TestServer::start_with_config_labeled(follower_port, config, "follower".into()).await?;

        println!("Waiting for election + discovery + replication connection...");
        tokio::time::sleep(Duration::from_secs(8)).await;

        Ok(Self { leader, follower, _minio: minio })
    }
}


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mode_str = if REPLICATED_MODE {
        "Replicated (writes->leader, reads->follower)"
    } else {
        "Standalone"
    };
    println!("=== Single-Aggregate Integration Tests ({}) ===\n", mode_str);

    let port = 10100 + (std::process::id() % 100) as u16;

    // Start server(s) and create clients
    let (mut write_client, mut read_client, _standalone, _replicated) = if REPLICATED_MODE {
        println!("Starting replicated cluster...");
        let servers = ReplicatedServers::start(port).await?;
        println!(
            "Cluster started: leader at {}, follower at {}\n",
            servers.leader.address(),
            servers.follower.address()
        );

        let write_client = CeleriantClient::connect(servers.leader.address()).await?;
        let read_client = CeleriantClient::connect(servers.follower.address()).await?;
        (write_client, read_client, None, Some(servers))
    } else {
        println!("Starting test server...");
        let server = TestServer::start_with_port(port).await?;
        println!("Server started at {}\n", server.address());

        let client = CeleriantClient::connect(server.address()).await?;
        let read_client = CeleriantClient::connect(server.address()).await?;
        (client, read_client, Some(server), None)
    };

    // Both aggregates must route to the same shard for the atomic multi-write test.
    // With aggregate_type_id routing: shard = type_id % num_shards.
    // Use type_ids that are multiples of num_shards apart so they always land together.
    let num_cpus = std::thread::available_parallelism().map(|n| n.get() as u128).unwrap_or(1);
    let aggregate_1 = AggregateKey::new(1, 2, 101);
    let aggregate_2 = AggregateKey::new(1, 2 + num_cpus, 201);
    let client_id: u128 = 999;

    // Verify nonexistent aggregates return error 7001
    println!("=== Checking nonexistent aggregates return error ===");
    for agg in [&aggregate_1, &aggregate_2] {
        let request = ClientRequest::AggregateDetails(AggregateDetailsRequest {
            aggregate_key: agg.clone(),
            correlation_id: None,
        });
        match read_client
            .send_request(&request)
            .await
        {
            Err(ClientError::Server(celeriant_client_tokio::server_error::ServerError::Details {
                kind: celeriant_client_tokio::server_error::DetailsError::AggregateNotExists, ..
            })) => {
                println!("Aggregate {:?}: correctly returned AggregateNotExists", agg);
            }
            Ok(response) => panic!("Expected error for nonexistent aggregate {:?}, got {:?}", agg, response),
            Err(e) => panic!("Aggregate {:?}: transport error - {:?}", agg, e),
        }
    }

    // Create initial events for both aggregates
    println!("\n=== Creating aggregates with initial writes ===");
    for (i, agg) in [&aggregate_1, &aggregate_2].iter().enumerate() {
        let event = create_event(0, format!("Initial event for aggregate {}", i + 1));
        let mut writes = HashMap::new();
        writes.insert(
            (*agg).clone(),
            SingleAggregateWrite {
                events: vec![event],
                allow_create: true,
                expected_version: Some(0),
                enforce_client_idempotency: true,
            },
        );

        let request = ClientRequest::Write(WriteRequest {
            correlation_id: Some(i as u128),
            client_id,
            user_id: None,
            writes,
        });
        match write_client
            .send_request(&request)
            .await
        {
            Ok(response) => println!("Initial write to aggregate {}: {:?}", i + 1, response),
            Err(e) => println!("Initial write to aggregate {} failed: {:?}", i + 1, e),
        }
    }

    // Atomic multi-aggregate write
    println!("\n=== Performing atomic multi-aggregate write ===");
    let event_1 = create_event(1, "Atomic write event for aggregate 1".to_string());
    let event_2 = create_event(1, "Atomic write event for aggregate 2".to_string());

    let mut writes = HashMap::new();
    writes.insert(
        aggregate_1.clone(),
        SingleAggregateWrite {
            events: vec![event_1],
            allow_create: false,
            expected_version: Some(1),
            enforce_client_idempotency: true,
        },
    );
    writes.insert(
        aggregate_2.clone(),
        SingleAggregateWrite {
            events: vec![event_2],
            allow_create: false,
            expected_version: Some(1),
            enforce_client_idempotency: true,
        },
    );

    let atomic_request = ClientRequest::Write(WriteRequest {
        correlation_id: Some(1000),
        client_id,
        user_id: Some(42),
        writes,
    });
    match write_client
        .send_request(&atomic_request)
        .await
    {
        Ok(response) => println!("Atomic multi-aggregate write: {:?}", response),
        Err(e) => println!("Atomic multi-aggregate write failed: {:?}", e),
    }

    // Test idempotency - retry same write
    println!("\n=== Testing idempotency (retry same write) ===");
    match write_client
        .send_request(&atomic_request)
        .await
    {
        Ok(response) => println!("Idempotent retry succeeded: {:?}", response),
        Err(e) => println!("Idempotent retry result: {:?}", e),
    }

    // Follower commit-notify is paced; wait for both writes to be visible on read_client
    // before the reads and listing assertions below.
    poll_converged_count(&mut read_client, &aggregate_1, 2, FOLLOWER_CONVERGENCE_TIMEOUT).await?;
    poll_converged_count(&mut read_client, &aggregate_2, 2, FOLLOWER_CONVERGENCE_TIMEOUT).await?;

    // Read back events from both aggregates (use read_client - follower in replicated mode)
    println!("\n=== Reading events from both aggregates ===");
    if REPLICATED_MODE {
        println!("(Reading from FOLLOWER)");
    }
    for (i, agg) in [&aggregate_1, &aggregate_2].iter().enumerate() {
        let request = ClientRequest::Read(ReadRequest {
            correlation_id: None,
            aggregate_key: (*agg).clone(),
            filters: ReadFilters::new(1),
        });
        match read_client
            .send_request(&request)
            .await
        {
            Ok(response) => println!("Aggregate {} events: {:?}", i + 1, response),
            Err(e) => println!("Aggregate {} read failed: {:?}", i + 1, e),
        }
    }

    // === List Operations (use read_client - follower in replicated mode) ===
    println!("\n=== Listing Organizations ===");
    if REPLICATED_MODE {
        println!("(Reading from FOLLOWER)");
    }
    let options = ListOptions::default();
    let orgs_iter = ListOrgsIterator::new(&mut read_client, options);
    let orgs = orgs_iter.collect().await?;
    println!("Found {} organizations:", orgs.len());
    for org in &orgs {
        println!("  - Org ID: {}", org.org_id);
    }
    // Verify org_id 1 exists (both aggregates use org_id 1)
    assert!(
        orgs.iter().any(|o| o.org_id == 1),
        "Expected org_id 1 to exist"
    );
    println!("  Verified org_id 1 exists");

    println!("\n=== Listing Aggregate Types ===");
    let options = ListOptions::default();
    let types_iter = ListAggregateTypesIterator::new(&mut read_client, Some(1), options);
    let agg_types = types_iter.collect().await?;
    println!("Found {} aggregate types for org 1:", agg_types.len());
    for agg_type in &agg_types {
        println!(
            "  - Org: {}, Type ID: {}",
            agg_type.org_id, agg_type.aggregate_type_id
        );
    }
    // Verify both aggregate types exist
    let type_id_2 = aggregate_2.aggregate_type_id;
    assert!(
        agg_types.iter().any(|t| t.aggregate_type_id == 2),
        "Expected aggregate_type_id 2 to exist"
    );
    assert!(
        agg_types.iter().any(|t| t.aggregate_type_id == type_id_2),
        "Expected aggregate_type_id {} to exist", type_id_2
    );
    println!("  Verified aggregate types 2 and {} exist", type_id_2);

    println!("\n=== Listing Aggregates (before delete) ===");
    let options = ListOptions::default();
    let aggs_iter = ListAggregatesIterator::new(&mut read_client, Some(1), None, options);
    let aggregates = aggs_iter.collect().await?;
    println!("Found {} aggregates for org 1:", aggregates.len());
    for agg in &aggregates {
        println!(
            "  - Org: {}, Type: {}, ID: {}, Deleted: {}",
            agg.org_id, agg.aggregate_type_id, agg.aggregate_id, agg.is_deleted
        );
    }
    // Verify both aggregates exist and are not deleted
    assert!(
        aggregates
            .iter()
            .any(|a| a.aggregate_id == 101 && !a.is_deleted),
        "Expected aggregate 101 to exist and not be deleted"
    );
    assert!(
        aggregates
            .iter()
            .any(|a| a.aggregate_id == 201 && !a.is_deleted),
        "Expected aggregate 201 to exist and not be deleted"
    );
    println!("  Verified aggregates 101 and 201 exist and are not deleted");

    // === Delete aggregate_1 ===
    println!("\n=== Deleting aggregate 1 ===");
    let mut deletes = HashMap::new();
    deletes.insert(
        aggregate_1.clone(),
        SingleAggregateDelete {
            allow_recreate: false,
            allow_sequence_continuation: false,
            expected_version: Some(2), // We have 2 event batches now (0 and 1)
        },
    );
    let delete_request = ClientRequest::Delete(DeleteRequest {
        correlation_id: Some(3000),
        client_id,
        user_id: Some(42),
        deletes,
    });
    match write_client
        .send_request(&delete_request)
        .await
    {
        Ok(response) => println!("Delete aggregate 1: {:?}", response),
        Err(e) => println!("Delete aggregate 1 failed: {:?}", e),
    }

    // Wait for the delete to be applied on read_client before listing.
    poll_converged_count(&mut read_client, &aggregate_1, 0, FOLLOWER_CONVERGENCE_TIMEOUT).await?;

    // === List Aggregates again to verify delete ===
    println!("\n=== Listing Aggregates (after delete, excluding deleted) ===");
    let options = ListOptions::default();
    let aggs_iter = ListAggregatesIterator::new(&mut read_client, Some(1), None, options);
    let aggregates = aggs_iter.collect().await?;
    println!("Found {} non-deleted aggregates for org 1:", aggregates.len());
    for agg in &aggregates {
        println!(
            "  - Org: {}, Type: {}, ID: {}, Deleted: {}",
            agg.org_id, agg.aggregate_type_id, agg.aggregate_id, agg.is_deleted
        );
    }
    // Verify aggregate 101 is no longer in the list (filtered out as deleted)
    assert!(
        !aggregates.iter().any(|a| a.aggregate_id == 101),
        "Expected aggregate 101 to be filtered out (deleted)"
    );
    assert!(
        aggregates
            .iter()
            .any(|a| a.aggregate_id == 201 && !a.is_deleted),
        "Expected aggregate 201 to still exist and not be deleted"
    );
    println!("  Verified aggregate 101 is filtered out, aggregate 201 still exists");

    println!("\n=== Listing Aggregates (after delete, including deleted) ===");
    let options = ListOptions {
        include_deleted: true,
        ..Default::default()
    };
    let aggs_iter = ListAggregatesIterator::new(&mut read_client, Some(1), None, options);
    let aggregates = aggs_iter.collect().await?;
    println!(
        "Found {} total aggregates for org 1 (including deleted):",
        aggregates.len()
    );
    for agg in &aggregates {
        println!(
            "  - Org: {}, Type: {}, ID: {}, Deleted: {}",
            agg.org_id, agg.aggregate_type_id, agg.aggregate_id, agg.is_deleted
        );
    }
    // Verify aggregate 101 shows as deleted
    assert!(
        aggregates
            .iter()
            .any(|a| a.aggregate_id == 101 && a.is_deleted),
        "Expected aggregate 101 to be marked as deleted"
    );
    assert!(
        aggregates
            .iter()
            .any(|a| a.aggregate_id == 201 && !a.is_deleted),
        "Expected aggregate 201 to still exist and not be deleted"
    );
    println!("  Verified aggregate 101 is marked as deleted, aggregate 201 is not deleted");

    // Test expected_version conflict
    println!("\n=== Testing expected_version conflict ===");
    let conflict_event = create_event(2, "This should fail".to_string());
    let mut writes = HashMap::new();
    writes.insert(
        aggregate_1.clone(),
        SingleAggregateWrite {
            events: vec![conflict_event],
            allow_create: false,
            expected_version: Some(0), // Wrong! Should be 2 now
            enforce_client_idempotency: true,
        },
    );

    let request = ClientRequest::Write(WriteRequest {
        correlation_id: Some(2000),
        client_id,
        user_id: None,
        writes,
    });
    match write_client
        .send_request(&request)
        .await
    {
        Ok(response) => println!("Unexpected success: {:?}", response),
        Err(e) => println!("Expected conflict error: {:?}", e),
    }

    println!("\n=== All tests completed successfully! ===");

    Ok(())
}

fn create_event(client_seq: u64, message: String) -> DatablockAggregateEvent {
    DatablockAggregateEvent {
        client_seq,
        event_seq: 0, // Server will assign
        event_id: Some(rand::random()),
        event_timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
        event_type_major: 1,
        event_type_minor: 0,
        event_value: Arc::new(message.into_bytes()),
        iv: None,
    }
}
