//! WAL mid-datablock torn write: garbage in the uncommitted datablock region
//! must be invisible after restart and safely overwritten by new writes.
//!
//! A crash mid-fsync can leave a partially written datablock below the
//! committed header's `datablocks_position` (datablocks grow from the end of
//! the file toward the front). The header never covered those bytes, so
//! recovery must serve exactly the committed events, and the next writes
//! must land over the garbage without harm.
//!
//! Deterministic: no cluster, no timing. Payloads are >512 bytes and
//! incompressible so every event lands as an external datablock, not inline
//! in its metablock.

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::process_client_responses::ClientResponse;
use celeriant_msg::request::requests::{SingleAggregateWrite, WriteRequest};
use crate::{fill_incompressible, read_all_batches, TestServer};
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::constants::HEADER_BLOCK_SIZE_BYTES;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use celeriant_wire::disk::versioned_block::deserialise_shard_log_header;
use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;
use std::time::Duration;

const COMMITTED_EVENTS: u64 = 8;
const POST_EVENTS: u64 = 4;
const PAYLOAD_BYTES: usize = 2048;
const GARBAGE_BYTES: u64 = 4096;

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== WAL mid-datablock torn write: uncommitted garbage is invisible ===\n");

    let port = 19600;
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
    println!("Writing {COMMITTED_EVENTS} events with {PAYLOAD_BYTES}-byte incompressible payloads...");
    for i in 1..=COMMITTED_EVENTS {
        write_big_event(&mut client, &key, i).await?;
    }
    let pre = read_all_batches(&mut client, &key).await?;
    assert_eq!(pre.len() as u64, COMMITTED_EVENTS, "expected {COMMITTED_EVENTS} committed events");

    let data_dir = server.config().data_root.to_str().unwrap().to_string();
    let wal_path = format!("{}/shard_0/log_1.wal", data_dir);
    drop(client);
    server.stop();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Locate the committed cursor from the primary header, then plant a torn
    // "datablock" in the free space just below datablocks_position — exactly
    // where the next datablock would have been mid-written at a crash.
    let header_block = read_region(&wal_path, 0, HEADER_BLOCK_SIZE_BYTES)?;
    let header = deserialise_shard_log_header(&header_block)
        .map_err(|e| format!("primary header must parse before the test can run: {e:?}"))?;
    let d_pos = header.write.datablocks_position;
    let m_pos = header.write.metablocks_position;
    let garbage_start = d_pos
        .checked_sub(GARBAGE_BYTES)
        .ok_or("datablocks_position too low for garbage window")?;
    if garbage_start <= m_pos {
        return Err(format!(
            "no free space between metablocks ({m_pos}) and datablocks ({d_pos}) — grow the file or shrink the garbage window"
        ).into());
    }
    println!(
        "Committed cursor: metablocks={m_pos} datablocks={d_pos}; planting {GARBAGE_BYTES} torn bytes at {garbage_start}..{d_pos}"
    );
    {
        let mut garbage = vec![0u8; GARBAGE_BYTES as usize];
        fill_incompressible(&mut garbage, 0xDEAD_70E5);
        let mut f = std::fs::OpenOptions::new().write(true).open(&wal_path)?;
        f.seek(SeekFrom::Start(garbage_start))?;
        f.write_all(&garbage)?;
        f.flush()?;
    }

    println!("Restarting — torn bytes are beyond the committed header and must be ignored...");
    server.restart().await?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    server.check_alive().map_err(|e| format!("node died on restart with torn datablock bytes: {e}"))?;

    let mut client = CeleriantClient::connect(server.address()).await?;
    let post = tokio::time::timeout(Duration::from_secs(10), read_all_batches(&mut client, &key))
        .await
        .map_err(|_| "read timed out — node did not recover cleanly")??;
    if post.len() as u64 != COMMITTED_EVENTS {
        return Err(format!(
            "recovered {} batches, expected {COMMITTED_EVENTS} — torn bytes leaked into recovery",
            post.len()
        ).into());
    }
    assert_batches_equal(&pre, &post, "after restart")?;

    // New writes land over the garbage region; everything must still read
    // back byte-identical.
    println!("Writing {POST_EVENTS} more events over the torn region...");
    for i in COMMITTED_EVENTS + 1..=COMMITTED_EVENTS + POST_EVENTS {
        write_big_event(&mut client, &key, i).await?;
    }
    let all = read_all_batches(&mut client, &key).await?;
    if all.len() as u64 != COMMITTED_EVENTS + POST_EVENTS {
        return Err(format!(
            "expected {} batches after post-writes, got {}",
            COMMITTED_EVENTS + POST_EVENTS,
            all.len()
        ).into());
    }
    assert_batches_equal(&pre, &all[..COMMITTED_EVENTS as usize], "after post-writes")?;

    println!("\n=== PASS: torn datablock bytes invisible; {POST_EVENTS} new events landed safely over them ===");
    Ok(())
}

async fn write_big_event(
    client: &mut CeleriantClient,
    key: &AggregateKey,
    event_num: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut payload = vec![0u8; PAYLOAD_BYTES];
    fill_incompressible(&mut payload, event_num);
    let event = DatablockAggregateEvent {
        client_seq: event_num,
        event_seq: 0,
        event_id: None,
        event_timestamp: 1000 + event_num,
        event_type_major: 100,
        event_type_minor: 0,
        event_value: Arc::new(payload),
        iv: None,
    };
    let mut writes = HashMap::new();
    writes.insert(
        key.clone(),
        SingleAggregateWrite {
            events: vec![event],
            allow_create: event_num == 1,
            expected_version: None,
            enforce_client_idempotency: false,
        },
    );
    let req = WriteRequest {
        correlation_id: Some(event_num as u128),
        client_id: 999,
        user_id: None,
        writes,
    };
    match client.send_request(&ClientRequest::Write(req)).await? {
        ClientResponse::Write(_) => Ok(()),
        other => Err(format!("write {event_num} failed: {other:?}").into()),
    }
}

fn assert_batches_equal(
    expected: &[celeriant_msg::response::aggregate_event_batch::AggregateEventBatch],
    actual: &[celeriant_msg::response::aggregate_event_batch::AggregateEventBatch],
    when: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    for (i, (a, b)) in expected.iter().zip(actual.iter()).enumerate() {
        if a.aggregate_version != b.aggregate_version
            || a.events.len() != b.events.len()
            || a.events.iter().zip(b.events.iter()).any(|(x, y)| x.event_value.as_slice() != y.event_value.as_slice())
        {
            return Err(format!("batch[{i}] differs {when}").into());
        }
    }
    Ok(())
}

fn read_region(path: &str, offset: u64, len: usize) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut f = std::fs::File::open(path)?;
    f.seek(SeekFrom::Start(offset))?;
    let mut buf = vec![0u8; len];
    f.read_exact(&mut buf)?;
    Ok(buf)
}
