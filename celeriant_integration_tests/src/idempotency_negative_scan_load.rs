//! Highlights the client-idempotency NEGATIVE-lookup cost (engine-idempotency-negative-scan).
//!
//! A first write from a new (aggregate, client) with `enforce_client_idempotency` must
//! prove the client absent by reverse-scanning the aggregate's chain. On a DEEP aggregate
//! (lots of client history) that walk is O(depth), and every new producer pays it again —
//! there's no negative memoization. The per-aggregate backlink already cut it from O(WAL)
//! to O(aggregate), but the per-producer repetition remains.
//!
//! This reproduces the shape: build one deep hot aggregate, then time fresh-producer
//! first-writes to it vs to a shallow control. It reports the cost and asserts the writes
//! succeed; locally the scan is page-cache-bound so the ratio is modest (the production
//! magnitude shows on the RPi repro in the design doc). When negative memoization lands,
//! tighten this to assert hot ≈ shallow.

use std::time::{Duration, Instant};

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_operations::WriteEventsOptions;
use celeriant_wal::aggregate_key::AggregateKey;

use crate::common::{event, port_for};
use crate::{ServerConfig, TestServer};

const TYPE: u64 = 100;
/// Distinct clients of history on the hot aggregate (its negative-scan depth).
const HOT_DEPTH: u64 = 300;
/// Foreign aggregates between each hot write, to bloat the shared WAL.
const FOREIGN_PER: u64 = 8;
/// Fresh producers timed per target.
const PROBES: u64 = 25;

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Idempotency negative-scan load (highlights O(aggregate-depth) cold lookup) ===\n");

    // Single shard so hot + foreign share one WAL (mirrors the production interleaving).
    let config = ServerConfig { num_shards: Some(1), standalone: true, ..Default::default() };
    let server = TestServer::start_with_config(port_for("idempotency_negative_scan_load"), config).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;

    let hot = AggregateKey::new(1, 1, 1);
    let shallow = AggregateKey::new(1, 1, 2);

    // Build deep history on the hot aggregate. Idempotency OFF here so setup itself
    // doesn't trigger negative scans; the metablocks still record each client.
    println!("Building hot aggregate: {HOT_DEPTH} client versions interleaved with {FOREIGN_PER}x foreign");
    let mut foreign_id = 1_000_000u128;
    for i in 0..HOT_DEPTH {
        write_first(&mut c, &hot, 10_000 + i as u128, i == 0).await?;
        for _ in 0..FOREIGN_PER {
            write_first(&mut c, &AggregateKey::new(1, 1, foreign_id), foreign_id, true).await?;
            foreign_id += 1;
        }
    }
    // Shallow control: exists, ~no history.
    write_first(&mut c, &shallow, 20_000, true).await?;

    // Time fresh-producer first-writes (idempotency ON → negative scan) to each target.
    let hot_avg = probe(&mut c, &hot, 100_000).await?;
    let shallow_avg = probe(&mut c, &shallow, 200_000).await?;

    let ratio = hot_avg.as_secs_f64() / shallow_avg.as_secs_f64().max(1e-9);
    println!("\n  hot (depth {HOT_DEPTH}) new-producer first-write avg: {hot_avg:?}");
    println!("  shallow           new-producer first-write avg: {shallow_avg:?}");
    println!("  ratio hot/shallow: {ratio:.1}x  <- negative-scan amplification on a deep aggregate");

    println!("\n=== PASS (repro/highlight; tighten to assert hot ~= shallow once negative memoization lands) ===");
    Ok(())
}

/// First write for a new (aggregate, client) without idempotency (setup path).
async fn write_first(c: &mut CeleriantClient, key: &AggregateKey, client_id: u128, allow_create: bool) -> Result<(), Box<dyn std::error::Error>> {
    let opts = WriteEventsOptions { allow_create, enforce_client_idempotency: false, ..Default::default() };
    c.write_events_with(key.clone(), vec![event(1, TYPE, 1000, "{}")], client_id, opts).await?;
    Ok(())
}

/// Time `PROBES` fresh-producer first-writes (new client_id each, idempotency ON, so each
/// triggers a negative reverse scan of the aggregate) to `key`. Returns the average.
async fn probe(c: &mut CeleriantClient, key: &AggregateKey, client_base: u128) -> Result<Duration, Box<dyn std::error::Error>> {
    let mut total = Duration::ZERO;
    for p in 0..PROBES {
        let opts = WriteEventsOptions { allow_create: false, enforce_client_idempotency: true, ..Default::default() };
        let start = Instant::now();
        c.write_events_with(key.clone(), vec![event(1, TYPE, 1000, "{}")], client_base + p as u128, opts).await?;
        total += start.elapsed();
    }
    Ok(total / PROBES as u32)
}
