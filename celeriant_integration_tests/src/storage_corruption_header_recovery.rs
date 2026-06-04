//! Storage corruption: primary header corrupt → recover from backup header
//! (remaining-tests.md item 6, storage_corruption extension (a)).
//!
//! `invariants.md` Durability: "Both the primary header (offset 0) and backup
//! header (offset `file_len - 512KB`) are written on every fsync. If the
//! primary is corrupt on open, the backup is used." `p2_3_wal_corruption`
//! corrupts a *metablock* and asserts the node refuses to serve; this asserts
//! the complementary positive path — corrupt only the primary header, keep
//! the backup intact, and the node must recover and serve every event.
//!
//! Deterministic: no cluster, no timing. Corrupt bytes inside the primary
//! header block (past the 8-byte CRC+version prefix, well within the 512 KB
//! block, so its CRC32C fails on open) while leaving the trailing backup
//! header byte-for-byte intact.

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{count_events, read_all_batches, write_event, TestServer};
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::constants::HEADER_BLOCK_SIZE_BYTES;
use celeriant_wire::disk::versioned_block::deserialise_shard_log_header;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::time::Duration;

const EVENTS: u64 = 12;

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Storage Corruption: primary header → backup recovery ===\n");

    let port = 19500;
    let config = crate::ServerConfig {
        num_shards: Some(1),
        log_level: "warn".to_string(),
        standalone: true,
        ..Default::default()
    };
    let mut server = TestServer::start_with_config_labeled(port, config, "standalone".into()).await?;
    println!("Server ready at {}", server.address());

    let key = AggregateKey::new(1, 1, 1);
    let mut client = CeleriantClient::connect(server.address()).await?;
    println!("Writing {} events...", EVENTS);
    for i in 1..=EVENTS {
        write_event(&mut client, &key, i, i == 1).await?;
    }
    let pre = read_all_batches(&mut client, &key).await?;
    assert_eq!(pre.len() as u64, EVENTS, "expected {EVENTS} events pre-corruption");

    let data_dir = server.config().data_root.to_str().unwrap().to_string();
    let wal_path = format!("{}/shard_0/log_1.wal", data_dir);
    drop(client);
    server.stop();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Snapshot the backup header region so we can prove we left it intact.
    let file_len = std::fs::metadata(&wal_path)?.len();
    let backup_offset = file_len - HEADER_BLOCK_SIZE_BYTES as u64;
    let backup_before = read_region(&wal_path, backup_offset, 4096)?;

    // Corrupt the primary header: 32 bytes at offset 1024 (inside the block,
    // past the CRC/version prefix). Breaks the primary's CRC32C; the backup
    // at `backup_offset` is untouched.
    println!("Corrupting primary header (32 bytes @ 1024); backup @ {backup_offset} left intact...");
    {
        let mut f = OpenOptions::new().write(true).open(&wal_path)?;
        f.seek(SeekFrom::Start(1024))?;
        f.write_all(&[0xFFu8; 32])?;
        f.flush()?;
    }
    let backup_after = read_region(&wal_path, backup_offset, 4096)?;
    if backup_before != backup_after {
        return Err("backup header region changed during primary corruption — test setup invalid".into());
    }

    // Non-vacuity gate: the corruption must make the PRIMARY header fail to
    // deserialize (else recovery never touches the backup and the pass is
    // vacuous), while the BACKUP still parses cleanly.
    let primary_block = read_region(&wal_path, 0, HEADER_BLOCK_SIZE_BYTES)?;
    let backup_block = read_region(&wal_path, backup_offset, HEADER_BLOCK_SIZE_BYTES)?;
    if deserialise_shard_log_header(&primary_block).is_ok() {
        return Err("primary header still parses after corruption — the byte edit didn't break it; pass would be vacuous".into());
    }
    if deserialise_shard_log_header(&backup_block).is_err() {
        return Err("backup header does not parse — cannot attribute recovery to it; test setup invalid".into());
    }
    println!("  Confirmed: primary header now fails CRC, backup header parses cleanly.");

    println!("Restarting — node must recover from the backup header...");
    server.restart().await?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    server.check_alive().map_err(|e| format!("node died on restart instead of recovering from backup header: {e}"))?;

    let mut client = CeleriantClient::connect(server.address()).await?;
    let count = tokio::time::timeout(Duration::from_secs(10), count_events(&mut client, &key))
        .await
        .map_err(|_| "count timed out — node did not recover from backup header")??;
    if count as u64 != EVENTS {
        return Err(format!("recovered {count} events, expected {EVENTS} — backup-header recovery lost data").into());
    }

    // Byte parity: the recovered read must match what was written.
    let post = read_all_batches(&mut client, &key).await?;
    for (i, (a, b)) in pre.iter().zip(post.iter()).enumerate() {
        if a.aggregate_version != b.aggregate_version
            || a.events.len() != b.events.len()
            || a.events.iter().zip(b.events.iter()).any(|(x, y)| x.event_value.as_slice() != y.event_value.as_slice())
        {
            return Err(format!("batch[{i}] differs after backup-header recovery").into());
        }
    }

    println!("\n=== PASS: {EVENTS} events recovered byte-identical from the backup header ===");
    Ok(())
}

fn read_region(path: &str, offset: u64, len: usize) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut f = std::fs::File::open(path)?;
    f.seek(SeekFrom::Start(offset))?;
    let mut buf = vec![0u8; len];
    f.read_exact(&mut buf)?;
    Ok(buf)
}
