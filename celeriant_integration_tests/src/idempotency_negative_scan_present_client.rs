//! Isolates the gap the client_id bloom (A) does NOT close, to decide whether the
//! per-aggregate negative memoization (B) is still worth building.
//!
//! The committed client_id bloom short-circuits a producer that is NEW TO THE SEGMENT to a
//! zero scan. So `idempotency_negative_scan_load` (fresh client_id per probe) is now flat -
//! it measures A's win, not B's gap. B's remaining niche is a client that is ALREADY PRESENT
//! in the segment (so the bloom says "maybe present" and cannot short-circuit) doing its
//! FIRST write to a given deep aggregate: that still pays a full O(aggregate-depth) chain
//! walk to prove the client absent, once per (aggregate, client) pair.
//!
//! Each probe client is first written to a throwaway aggregate (lands it in the segment's
//! client bloom = "present"), THEN timed first-writing to hot vs shallow. A high hot/shallow
//! ratio here = B's gap is real and unaddressed by A. A flat ratio = A already covers it and
//! B is not needed.

use std::time::{Duration, Instant};

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_operations::WriteEventsOptions;
use celeriant_wal::aggregate_key::AggregateKey;

use crate::common::{event, port_for};
use crate::{ServerConfig, TestServer};

const TYPE: u64 = 100;
const HOT_DEPTH: u64 = 300;
const FOREIGN_PER: u64 = 8;
const PROBES: u64 = 25;

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Idempotency negative-scan, PRESENT client (isolates option-B gap past the bloom) ===\n");

    // Small segment so the hot aggregate's history SPANS MULTIPLE segments. A present client is
    // in the active segment's client bloom but absent from older ones; the active-segment-bloom
    // mode-switch must then drop the client bloom and enumerate the full chain so option-B
    // completeness still establishes across segments.
    let config = ServerConfig { num_shards: Some(1), standalone: true, shard_log_preallocate_bytes: 2 * 1024 * 1024, ..Default::default() };
    let server = TestServer::start_with_config(port_for("idempotency_negative_scan_present_client"), config).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;

    let hot = AggregateKey::new(1, 1, 1);
    let shallow = AggregateKey::new(1, 1, 2);
    let warmup = AggregateKey::new(1, 1, 3);

    println!("Building hot aggregate: {HOT_DEPTH} client versions interleaved with {FOREIGN_PER}x foreign");
    let mut foreign_id = 1_000_000u128;
    for i in 0..HOT_DEPTH {
        write_first(&mut c, &hot, 10_000 + i as u128, i == 0).await?;
        for _ in 0..FOREIGN_PER {
            write_first(&mut c, &AggregateKey::new(1, 1, foreign_id), foreign_id, true).await?;
            foreign_id += 1;
        }
    }
    write_first(&mut c, &shallow, 20_000, true).await?;

    // First present-client first-write triggers the establishing scan AND option-B completeness
    // (full chain enumerated, no bloom skip). Untimed: it's the one-time cost, not steady state.
    probe_present(&mut c, &warmup, &hot, 1, 90_000).await?;
    probe_present(&mut c, &warmup, &shallow, 1, 91_000).await?;

    // Steady state: every later present-client first-write should hit the completeness
    // short-circuit (no scan), so hot collapses to the shallow control.
    let hot_avg = probe_present(&mut c, &warmup, &hot, PROBES, 100_000).await?;
    let shallow_avg = probe_present(&mut c, &warmup, &shallow, PROBES, 200_000).await?;

    let ratio = hot_avg.as_secs_f64() / shallow_avg.as_secs_f64().max(1e-9);
    println!("\n  hot (depth {HOT_DEPTH}) present-client first-write avg: {hot_avg:?}");
    println!("  shallow            present-client first-write avg: {shallow_avg:?}");
    println!("  ratio hot/shallow: {ratio:.2}x  <- option-B short-circuit removes the chain walk");

    assert!(ratio < 1.3, "negative-scan amplification not removed: ratio {ratio:.2}x (expected ~1.0x with option-B completeness)");

    println!("\n=== PASS (option-B completeness short-circuits the present-client negative lookup) ===");
    Ok(())
}

async fn write_first(c: &mut CeleriantClient, key: &AggregateKey, client_id: u128, allow_create: bool) -> Result<(), Box<dyn std::error::Error>> {
    let opts = WriteEventsOptions { allow_create, enforce_client_idempotency: false, ..Default::default() };
    c.write_events_with(key.clone(), vec![event(1, TYPE, 1000, "{}")], client_id, opts).await?;
    Ok(())
}

/// Make each probe client present in the segment (write to `warmup`), then time its
/// idempotency-ON first-write to `target` (a negative lookup the bloom can't short-circuit).
async fn probe_present(c: &mut CeleriantClient, warmup: &AggregateKey, target: &AggregateKey, probes: u64, client_base: u128) -> Result<Duration, Box<dyn std::error::Error>> {
    let mut total = Duration::ZERO;
    for p in 0..probes {
        let client = client_base + p as u128;
        write_first(c, warmup, client, true).await?;
        let opts = WriteEventsOptions { allow_create: false, enforce_client_idempotency: true, ..Default::default() };
        let start = Instant::now();
        c.write_events_with(target.clone(), vec![event(1, TYPE, 1000, "{}")], client, opts).await?;
        total += start.elapsed();
    }
    Ok(total / probes as u32)
}
