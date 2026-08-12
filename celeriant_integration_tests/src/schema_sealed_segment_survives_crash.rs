//! A schema whose registration lives in a SEALED WAL segment must survive
//! kill -9 + restart on the same data root.
//!
//! Any change that proves "no schema registered" from per-segment metadata may
//! SKIP sealed segments it believes hold no registrations. The failure mode that
//! must never happen is OVER-SKIPPING: the segment holding a registration gets
//! skipped, the server concludes no-schema, and a registered schema silently
//! stops being enforced. This test builds the exact shape where that goes wrong:
//!
//!   - two schemas are registered, then the log rotates twice under bulk load,
//!     sealing the segment that holds both registrations,
//!   - one schema's key COLLIDES with a live aggregate in the segment blooms
//!     (event_type_major 1 == aggregate id 1; SchemaKey and AggregateKey hash
//!     the same 48-byte domain — see recovery_multiseg_read_amplification), so
//!     skip logic sees "maybe present" everywhere,
//!   - the other schema (major 42) collides with nothing, so every bulk-only
//!     segment is legitimately skippable and the one holding the registration
//!     is the only thing standing between enforcement and silence,
//!   - kill -9, restart, and from a fresh client every violating write must
//!     still be rejected, conforming writes accepted, and both schemas still
//!     found (duplicate registration rejected) — for an aggregate that existed
//!     pre-crash AND a brand-new aggregate of the same type.
//!
//! `recovery_multiseg_read_amplification` pins the COST of this shape; this
//! pins the CORRECTNESS. The violating writes run FIRST after restart so the
//! very first cold schema lookup is the one a mis-skip would corrupt.

use std::sync::Arc;
use std::time::Duration;

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use celeriant_client_tokio::client_operations::WriteEventsOptions;
use celeriant_client_tokio::server_error::{SchemaError, ServerError};
use celeriant_msg::request::requests::RegisterSchemaRequest;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use celeriant_wal::schema_key::SchemaKey;

use crate::common::port_for;
use crate::{ServerConfig, TestServer};

const ORG: u128 = 1;
const AGG_TYPE: u128 = 1;
/// Pre-crash aggregates get ids 1..=N_AGGS — small ids, so id 1 exists and the
/// colliding schema below aliases it in every segment bloom.
const N_AGGS: u128 = 4;
const NEW_AGG_A: u128 = 500;
const NEW_AGG_B: u128 = 501;
/// Colliding schema: major == aggregate id 1. Skip logic sees this key as
/// "maybe present" in every segment holding aggregate 1.
const COLLIDE_MAJOR: u64 = 1;
/// Clean schema: collides with nothing, so sealed bulk-only segments are all
/// legitimately skippable — over-skipping erases exactly this one.
const CLEAN_MAJOR: u64 = 42;
/// Bulk writes use an unschema'd type so their random payloads pass through.
const BULK_MAJOR: u64 = 7;
const CLIENT_ID: u128 = 4242;
const PREALLOCATE: u64 = 2 * 1024 * 1024;
const PAYLOAD_BYTES: usize = 16 * 1024;
const MAX_BULK_WRITES: u64 = 4000;

const COLLIDE_SCHEMA: &str =
    r#"{"type":"object","properties":{"name":{"type":"string"},"age":{"type":"integer"}},"required":["name","age"]}"#;
const CLEAN_SCHEMA: &str =
    r#"{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"]}"#;

fn agg_key(id: u128) -> AggregateKey {
    AggregateKey::new(ORG, AGG_TYPE, id)
}

fn ev(major: u64, payload: &[u8]) -> DatablockAggregateEvent {
    DatablockAggregateEvent {
        client_seq: 0,
        event_seq: 0,
        event_id: Some(rand::random()),
        event_timestamp: 1_000,
        event_type_major: major,
        event_type_minor: 0,
        event_value: Arc::new(payload.to_vec()),
        iv: None,
    }
}

/// Incompressible payload (splitmix64 over the write identity) — constant
/// payloads compress to nothing and the log never rotates.
fn bulk_payload(a: u128, seq: u64) -> Vec<u8> {
    let mut x = (a as u64).wrapping_mul(0x9E3779B97F4A7C15) ^ seq.wrapping_mul(0xBF58476D1CE4E5B9);
    let mut out = Vec::with_capacity(PAYLOAD_BYTES);
    while out.len() < PAYLOAD_BYTES {
        x = x.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = x;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        out.extend_from_slice(&(z ^ (z >> 31)).to_le_bytes());
    }
    out.truncate(PAYLOAD_BYTES);
    out
}

fn register_request(major: u64, schema: &str) -> RegisterSchemaRequest {
    RegisterSchemaRequest {
        correlation_id: Some(rand::random()),
        client_id: CLIENT_ID,
        user_id: None,
        schema_key: SchemaKey::new(ORG, AGG_TYPE, major, 0),
        schema_type: 0,
        schema: schema.to_string(),
    }
}

fn shard_dir(data_root: &std::path::Path) -> Option<std::path::PathBuf> {
    let mut dirs: Vec<_> = std::fs::read_dir(data_root)
        .into_iter()
        .flatten()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.is_dir() && p.file_name().is_some_and(|n| n.to_string_lossy().starts_with("shard_")))
        .collect();
    dirs.sort();
    dirs.into_iter().next()
}

fn wal_file_count(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "wal"))
        .count()
}

async fn write(
    c: &mut CeleriantClient,
    agg: u128,
    major: u64,
    payload: &[u8],
    allow_create: bool,
) -> Result<(), ClientError> {
    let opts = WriteEventsOptions { allow_create, enforce_client_idempotency: false, ..Default::default() };
    c.write_events_with(agg_key(agg), vec![ev(major, payload)], CLIENT_ID, opts).await.map(|_| ())
}

async fn expect_rejected(
    c: &mut CeleriantClient,
    agg: u128,
    major: u64,
    payload: &[u8],
    what: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    match write(c, agg, major, payload, false).await {
        Err(ClientError::Server(ServerError::Schema { kind: SchemaError::ValidationFailed, .. })) => Ok(()),
        Ok(()) => Err(format!(
            "{what}: violating write for major {major} was ACCEPTED — the registered schema became invisible (over-skip)"
        )
        .into()),
        Err(other) => Err(format!("{what}: expected SchemaValidationFailed, got {other:?}").into()),
    }
}

async fn expect_still_registered(
    c: &mut CeleriantClient,
    major: u64,
    schema: &str,
    what: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    match c.register_schema(register_request(major, schema)).await {
        Err(ClientError::Server(ServerError::Schema { kind: SchemaError::AlreadyExists, .. })) => Ok(()),
        Ok(_) => Err(format!(
            "{what}: re-registering major {major} SUCCEEDED — the sealed registration was not found (over-skip)"
        )
        .into()),
        Err(other) => Err(format!("{what}: expected SchemaAlreadyExists for major {major}, got {other:?}").into()),
    }
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Schema in a sealed segment survives kill -9 + restart ===\n");

    let config = ServerConfig {
        num_shards: Some(1),
        standalone: true,
        shard_log_preallocate_bytes: PREALLOCATE,
        ..Default::default()
    };
    let mut server =
        TestServer::start_with_config(port_for("schema_sealed_segment_survives_crash"), config).await?;
    let data_root = server.config().data_root.clone();
    let mut c = CeleriantClient::connect(server.address()).await?;

    println!("Creating aggregates 1..={N_AGGS} (id 1 aliases the colliding schema key)");
    for a in 1..=N_AGGS {
        write(&mut c, a, BULK_MAJOR, &bulk_payload(a, 0), true).await?;
    }

    println!("Registering schemas: major {COLLIDE_MAJOR} (bloom-collides with aggregate 1), major {CLEAN_MAJOR} (clean)");
    c.register_schema(register_request(COLLIDE_MAJOR, COLLIDE_SCHEMA)).await?;
    c.register_schema(register_request(CLEAN_MAJOR, CLEAN_SCHEMA)).await?;

    let shard = shard_dir(&data_root).ok_or("no shard_* dir under data root")?;
    let segments_at_registration = wal_file_count(&shard);

    println!("Warm enforcement before any seal");
    write(&mut c, 1, COLLIDE_MAJOR, br#"{"name":"a","age":1}"#, false).await?;
    expect_rejected(&mut c, 1, COLLIDE_MAJOR, br#"{"name":"no-age"}"#, "warm").await?;
    write(&mut c, 2, CLEAN_MAJOR, br#"{"id":1}"#, false).await?;
    expect_rejected(&mut c, 2, CLEAN_MAJOR, br#"{"id":"not-int"}"#, "warm").await?;

    // ── Seal the registrations away: incompressible bulk on an unschema'd type
    // until the log has rotated at least twice past the registration segment.
    let target = segments_at_registration + 2;
    let mut seq = 0u64;
    'bulk: loop {
        for a in 1..=N_AGGS {
            seq += 1;
            write(&mut c, a, BULK_MAJOR, &bulk_payload(a, seq), false).await?;
            if seq >= MAX_BULK_WRITES {
                return Err(format!(
                    "log did not reach {target} segments after {seq} writes ({} wal files) — raise payload or lower preallocate",
                    wal_file_count(&shard)
                )
                .into());
            }
        }
        if wal_file_count(&shard) >= target {
            break 'bulk;
        }
    }
    println!(
        "Rotated: {} wal files after {seq} bulk writes (registrations sealed in segment {segments_at_registration})",
        wal_file_count(&shard)
    );

    // Sealed segments must carry their sidecars before the kill: the skip
    // metadata has to EXIST on disk for over-skipping to be possible at all.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    for id in 1..wal_file_count(&shard) as u64 + 1 {
        let summary = shard.join(format!("log_{id}.summary"));
        let sealed = shard.join(format!("log_{}.wal", id + 1)).exists();
        while sealed && !summary.exists() {
            if std::time::Instant::now() > deadline {
                return Err(format!("sidecar {summary:?} not written 30s after seal").into());
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    println!("Warm enforcement across the seal");
    expect_rejected(&mut c, 1, COLLIDE_MAJOR, br#"{"name":"no-age"}"#, "warm across seal").await?;
    expect_rejected(&mut c, 2, CLEAN_MAJOR, br#"{"id":"not-int"}"#, "warm across seal").await?;
    drop(c);

    println!("\nkill -9, restart on the same data root");
    server.stop();
    tokio::time::sleep(Duration::from_secs(1)).await;
    server.restart().await?;
    let mut c = CeleriantClient::connect(server.address()).await?;

    // Violating writes FIRST: the first cold lookup per schema key is the one
    // an over-skipping absence proof would answer wrongly, and a pass-through
    // here is silent data corruption, not an error.
    println!("Cold enforcement, pre-crash aggregates");
    expect_rejected(&mut c, 1, COLLIDE_MAJOR, br#"{"name":"no-age"}"#, "cold, pre-crash aggregate").await?;
    write(&mut c, 1, COLLIDE_MAJOR, br#"{"name":"b","age":2}"#, false).await?;
    expect_rejected(&mut c, 2, CLEAN_MAJOR, br#"{"id":"not-int"}"#, "cold, pre-crash aggregate").await?;
    write(&mut c, 2, CLEAN_MAJOR, br#"{"id":2}"#, false).await?;

    println!("Cold enforcement, new aggregates of the same type");
    write(&mut c, NEW_AGG_A, COLLIDE_MAJOR, br#"{"name":"c","age":3}"#, true).await?;
    expect_rejected(&mut c, NEW_AGG_A, COLLIDE_MAJOR, br#"{"age":"three"}"#, "cold, new aggregate").await?;
    write(&mut c, NEW_AGG_B, CLEAN_MAJOR, br#"{"id":3}"#, true).await?;
    expect_rejected(&mut c, NEW_AGG_B, CLEAN_MAJOR, b"not json", "cold, new aggregate").await?;

    // The retrievability proof the client API offers: a duplicate registration
    // is rejected only if the server can still FIND the sealed registration.
    println!("Cold retrievability: duplicate registrations rejected");
    expect_still_registered(&mut c, COLLIDE_MAJOR, COLLIDE_SCHEMA, "cold").await?;
    expect_still_registered(&mut c, CLEAN_MAJOR, CLEAN_SCHEMA, "cold").await?;

    println!("\n=== PASS: sealed-segment schemas enforced and findable after kill -9 ===");
    Ok(())
}
