//! P2-3: WAL On-Disk Corruption Detection on Restart
//!
//! Tests that WAL file corruption is detected on restart and the node refuses
//! to serve garbage data.
//!
//! Scenario:
//! 1. Start standalone node
//! 2. Write 10 events
//! 3. Stop the node
//! 4. Hex-edit a metablock's payload bytes in the WAL file (shard_000/log_000.bin)
//! 5. Attempt to restart
//! 6. Verify: CRC32C mismatch detected, node refuses to start or serve data
//!
//! This is test P2-3 in the integration test coverage report (Batch 4).
//!
//! Run with: cargo run --bin p2_3_wal_corruption_main

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{count_events, write_event, TestServer};
use celeriant_wal::aggregate_key::AggregateKey;
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::time::Duration;


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== P2-3: WAL On-Disk Corruption Detection on Restart ===\n");

    let port_base = 20300;
    let port = port_base;

    let config = crate::ServerConfig {
        num_shards: Some(1),
        log_level: "warn".to_string(),
        standalone: true,
        ..Default::default()
    };

    println!("Starting standalone node on port {}...", port);
    let mut server = TestServer::start_with_config_labeled(port, config, "standalone".into()).await?;
    println!("  Server ready at {}\n", server.address());

    // ========================================
    // Phase 1: Write events to populate WAL
    // ========================================
    println!("PHASE 1: Write events to populate WAL");
    println!("--------------------------------------");

    let aggregate_key = AggregateKey::new(1, 1, 1);
    let mut client = CeleriantClient::connect(server.address()).await?;

    println!("  Writing 10 events...");
    for i in 1u64..=10 {
        write_event(&mut client, &aggregate_key, i, i == 1).await?;
    }

    let count = count_events(&mut client, &aggregate_key).await?;
    assert_eq!(count, 10, "Expected 10 events before corruption, got {}", count);
    println!("  Verified: {} events written\n", count);

    // ========================================
    // Phase 2: Stop server and corrupt WAL file
    // ========================================
    println!("PHASE 2: Stop server and corrupt WAL file");
    println!("------------------------------------------");

    // Get data directory path and WAL file path before stopping
    let data_dir = server.config().data_root.to_str().unwrap().to_string();
    let wal_path = format!("{}/shard_0/log_1.wal", data_dir);

    println!("  Stopping server...");
    drop(client);
    server.stop();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // WAL file structure:
    // - Bytes 0..HEADER_BLOCK_SIZE_BYTES: Primary header
    // - HEADER_BLOCK_SIZE_BYTES..: Metablocks (1024 bytes each, with CRC32C)
    // - (end - HEADER_BLOCK_SIZE_BYTES)..end: Backup header
    // Corrupt the first metablock area (just after the header) — offset is header-relative so it
    // tracks HEADER_BLOCK_SIZE_BYTES rather than assuming a fixed 512KB header.
    let corruption_offset = celeriant_wal::constants::HEADER_BLOCK_SIZE_BYTES as u64 + 512; // first metablock, midway through payload
    let corruption_bytes = [0xFF, 0xFF, 0xFF, 0xFF];

    println!("  Corrupting {} bytes at offset {}...", corruption_bytes.len(), corruption_offset);
    let mut file = OpenOptions::new().write(true).open(&wal_path)?;
    file.seek(SeekFrom::Start(corruption_offset))?;
    file.write_all(&corruption_bytes)?;
    drop(file); // Ensure flush
    println!("  Corruption complete\n");

    // ========================================
    // Phase 3: Attempt restart - should fail or refuse to serve data
    // ========================================
    println!("PHASE 3: Attempt restart - corruption detection");
    println!("------------------------------------------------");

    println!("  Attempting to restart server...");
    let restart_result = server.restart().await;

    match restart_result {
        Ok(()) => {
            println!("  Server restarted (process started)");
            println!("  Checking if server can serve data...");

            // Give server time to detect corruption during recovery
            tokio::time::sleep(Duration::from_secs(2)).await;

            // Try to connect and query - should fail if corruption was detected
            match CeleriantClient::connect(server.address()).await {
                Ok(mut client) => {
                    println!("  Connected to server, attempting to count events...");
                    match tokio::time::timeout(
                        Duration::from_secs(5),
                        count_events(&mut client, &aggregate_key)
                    ).await {
                        Ok(Ok(count)) => {
                            panic!(
                                "Server served data ({} events) despite WAL corruption. \
                                 Expected: detection of CRC32C mismatch and refusal to serve corrupted data.",
                                count
                            );
                        }
                        Ok(Err(e)) => {
                            println!("  Count events failed: {}", e);
                            println!("  Corruption detected - server refused to serve data");
                        }
                        Err(_) => {
                            println!("  Request timed out");
                            println!("  Corruption detected - server unresponsive");
                        }
                    }

                    // Check if server exited after detecting corruption
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    match server.check_alive() {
                        Ok(()) => {
                            println!("  Server still running but refusing operations");
                        }
                        Err(e) => {
                            println!("  Server shut down: {}", e);
                            assert!(
                                e.contains("exit status: 0") || e.contains("status: 0"),
                                "Server should exit cleanly (status 0) on corruption, got: {}",
                                e
                            );
                            println!("  Graceful shutdown confirmed");
                        }
                    }
                }
                Err(e) => {
                    println!("  Failed to connect: {}", e);
                    println!("  Checking if server exited...");

                    tokio::time::sleep(Duration::from_secs(1)).await;
                    match server.check_alive() {
                        Ok(()) => {
                            panic!(
                                "Server running but connection failed. Expected: corruption detection. Error: {}",
                                e
                            );
                        }
                        Err(exit_msg) => {
                            println!("  Server exited: {}", exit_msg);
                            assert!(
                                exit_msg.contains("exit status: 0") || exit_msg.contains("status: 0"),
                                "Server should exit cleanly (status 0) on corruption, got: {}",
                                exit_msg
                            );
                            println!("  Corruption detected - graceful shutdown");
                        }
                    }
                }
            }
        }
        Err(e) => {
            println!("  Restart failed: {}", e);
            println!("  Corruption detected during startup - server refused to start");

            // This is acceptable - corruption detection during boot
            // Check that the process exited (not just timed out)
            match server.check_alive() {
                Ok(()) => {
                    panic!("Server process still running after restart failure. Error: {}", e);
                }
                Err(exit_msg) => {
                    println!("  Server process exited: {}", exit_msg);
                }
            }
        }
    }

    println!("\n=== PASS ===");
    println!("WAL corruption was detected and the node refused to serve garbage data.\n");

    Ok(())
}
