//! Edge Case: Corrupted S3 Batch Data
//!
//! Tests that S3 batch CRC32C checksum validation catches corruption and skips
//! the bad batch, letting leader→follower TCP extended catchup fill the range.
//! Corruption of an S3 fallback batch is no longer fatal — durability rests on
//! leader WAL + TCP, with S3 as a best-effort accelerator.
//!
//! Scenario:
//! 1. Start MinIO + two-node cluster
//! 2. Write events 1-3 with follower online (verify replication)
//! 3. Stop follower
//! 4. Write events 4-13 to leader — S3 fallback creates batch objects
//! 5. Wait for S3 fallback writes to complete
//! 6. Corrupt a batch by overwriting it with garbage bytes via MinioContainer::put_object()
//! 7. Restart follower — S3 catchup detects the bad CRC32C and skips that batch
//! 8. Verify: follower stays alive and converges to leader event count via TCP

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{
    count_events, is_leader, s3_cluster_config, write_event, write_large_event, MinioContainer,
    TestServer,
};
use celeriant_wal::aggregate_key::AggregateKey;
use std::time::Duration;


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Edge Case: Corrupted S3 Batch Data ===\n");

    let port_base = 17100 + (std::process::id() % 100) as u16;
    let leader_port = port_base;
    let follower_port = port_base + 100;
    let minio_port = port_base + 10;

    println!("Starting MinIO on port {}...", minio_port);
    let minio = MinioContainer::start_with_bucket(minio_port, "test-corrupted-s3").await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();
    println!("MinIO ready at {}\n", endpoint);

    let num_shards = 2;
    // Route writes to a known shard: aggregate_type_id=1, shard = 1 % 2 = 1
    let aggregate_key = AggregateKey::new(1, 1, 1);
    let expected_shard = (aggregate_key.aggregate_type_id % num_shards as u128) as u32;
    let shard_prefix = format!("cluster/fallback/shard_{:03}/", expected_shard);

    let base_config = s3_cluster_config(
        num_shards,
        &region,
        &bucket,
        &access_key,
        &secret_key,
        &endpoint,
        allow_http,
    );

    let config = crate::ServerConfig {
        shard_log_preallocate_bytes: 2 * 1024 * 1024,
        ..base_config
    };

    println!("Starting leader on port {}...", leader_port);
    let leader =
        TestServer::start_with_config_labeled(leader_port, config.clone(), "leader".into())
            .await?;
    println!("Starting follower on port {}...", follower_port);
    let follower =
        TestServer::start_with_config_labeled(follower_port, config, "follower".into()).await?;

    println!(
        "  Leader at {}, Follower at {}",
        leader.address(),
        follower.address()
    );

    println!("Waiting for cluster stabilization (8s)...");
    tokio::time::sleep(Duration::from_secs(8)).await;

    // Verify roles — the first-started node wins the CreateOnly election.
    // Swap if the follower ended up as leader (possible under CI timing).
    let (leader, mut follower) = if is_leader(leader.address()).await? {
        (leader, follower)
    } else if is_leader(follower.address()).await? {
        (follower, leader)
    } else {
        return Err("Neither node is leader after 8s — election failed".into());
    };
    println!("  Role check passed\n");

    // ========================================
    // Phase 1: Create aggregate with follower online
    // ========================================
    println!("PHASE 1: Create aggregate with follower online");
    println!("----------------------------------------------");

    let mut leader_client = CeleriantClient::connect(leader.address()).await?;

    for i in 1u64..=3 {
        write_event(&mut leader_client, &aggregate_key, i, i == 1).await?;
    }

    let mut follower_client = CeleriantClient::connect(follower.address()).await?;
    let initial_count = count_events(&mut follower_client, &aggregate_key).await?;
    assert_eq!(
        initial_count, 3,
        "Follower should have 3 events after initial replication, got {}",
        initial_count
    );
    println!("  Follower has {} event(s) — replication working", initial_count);

    // ========================================
    // Phase 2: Stop follower, write large events to trigger S3 fallback
    // ========================================
    println!("\nPHASE 2: Stop follower, write large events to generate S3 batches");
    println!("--------------------------------------------------------------------");

    println!("  Stopping follower...");
    drop(follower_client);
    follower.stop();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // ~5 events × 8KB = 40KB > 32KB batch limit → at least 2 S3 batches.
    println!("  Writing 10 large events (4KB each) to leader...");
    for i in 4u64..=13 {
        write_large_event(&mut leader_client, &aggregate_key, i, 4096).await?;
    }
    println!("  Writes complete");

    println!("  Waiting 5s for S3 fallback writes to complete...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // ========================================
    // Phase 3: Inspect S3 batches, then corrupt one
    // ========================================
    println!("\nPHASE 3: Inspect S3 batches and corrupt one");
    println!("--------------------------------------------");

    let mut objects = minio.list_objects(&shard_prefix).await?;
    println!("  S3 fallback objects found: {}", objects.len());
    for obj in &objects {
        println!("    - {}", obj);
    }

    assert!(
        objects.len() >= 2,
        "Expected at least 2 S3 batches (32KB batch limit with 40KB+ data), got {}. \
         Try increasing event count or reducing batch size.",
        objects.len()
    );

    // Sort by path — lexicographic order matches WAL sequence order.
    objects.sort();

    // Corrupt the last batch. Using the last batch guarantees it was written while the
    // follower was stopped (events 4–13), so it is novel to the follower and cannot
    // have been received via TCP replication. The first batch may overlap with data
    // already replicated before the follower stopped.
    let corrupt_target = objects.last().expect("objects non-empty (checked above)").clone();
    let garbage: Vec<u8> = vec![0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0xFF, 0x42, 0x13];
    println!("  Overwriting '{}' with {} garbage bytes...", corrupt_target, garbage.len());
    minio.put_object(&corrupt_target, garbage).await?;
    println!("  Corruption complete");

    // Verify the corruption landed.
    let corrupted_bytes = minio.get_object(&corrupt_target).await?;
    assert_eq!(
        corrupted_bytes.as_ref(),
        &[0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0xFF, 0x42, 0x13],
        "Corruption did not take effect"
    );
    println!("  Confirmed: corrupted object has garbage bytes");

    // ========================================
    // Phase 4: Restart follower — S3 catchup skips corruption, TCP fills the gap
    // ========================================
    println!("\nPHASE 4: Restart follower — S3 catchup skips corrupt batch, TCP fills it");
    println!("--------------------------------------------------------------------------");

    println!("  Restarting follower...");
    follower.restart().await?;

    // Corrupt batch is skipped with a warning; S3 catchup stops where the chain
    // breaks. TCP extended catchup only fires when the leader attempts a fresh
    // replication that the follower rejects on WAL mismatch — so after the
    // follower has settled, poke the leader with one more write.
    tokio::time::sleep(Duration::from_secs(3)).await;

    let mut leader_client = CeleriantClient::connect(leader.address()).await?;
    println!("  Poking leader with one more write to trigger TCP extended catchup...");
    write_large_event(&mut leader_client, &aggregate_key, 14, 4096).await?;

    let mut follower_client = CeleriantClient::connect(follower.address()).await?;
    let converge_timeout = Duration::from_secs(30);
    let start = std::time::Instant::now();
    let mut converged = false;
    let mut last_count = 0usize;

    while start.elapsed() < converge_timeout {
        follower
            .check_alive()
            .map_err(|e| format!("Follower should have stayed alive across corruption: {}", e))?;
        last_count = count_events(&mut follower_client, &aggregate_key).await?;
        if last_count >= 14 {
            converged = true;
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    // ========================================
    // Phase 5: Verify follower converged via TCP despite corrupted batch
    // ========================================
    println!("\nPHASE 5: Verify follower converged via TCP extended catchup");
    println!("-------------------------------------------------------------");

    let leader_count = count_events(&mut leader_client, &aggregate_key).await?;
    println!("  Leader has {} events, follower has {} events", leader_count, last_count);
    assert_eq!(
        leader_count, 14,
        "Leader should have all 14 events (3 initial + 10 large + 1 poke), got {}",
        leader_count
    );
    assert!(
        converged,
        "Follower should have converged to {} events via TCP despite corrupt S3 batch, got {}",
        leader_count, last_count
    );

    println!("\n=== PASS ===\n");

    Ok(())
}
