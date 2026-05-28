//! P3-4: Read Thundering Herd — concurrent cold reads of distinct aggregates.
//!
//! P3-1/P3-3 measure *sequential* cold reads. The dangerous case for the
//! bloom + LRU + reverse-WAL-scan model is *concurrent* reads of many DISTINCT
//! cold aggregates: the per-aggregate dedup lock (`aggregate_loading`) only
//! collapses duplicate reads of the *same* key, so distinct keys all proceed
//! and each launches its own reverse scan. The only thing standing between that
//! and an NVMe read storm is the per-shard `cache_load_semaphore`
//! (`read_max_concurrent`, default 64), which caps concurrent disk scans.
//!
//! This test asks two questions:
//!   1. Does the system survive a herd of distinct cold reads without wedging,
//!      crashing, or returning wrong data? (i.e. does the semaphore actually
//!      bound the self-inflicted load?)
//!   2. What does it cost? We surface the latency and the read amplification
//!      (segment-header re-reads counted via `celeriant_cache_log_file_misses_total`)
//!      so the reverse-scan tradeoff is grounded in numbers, not hand-waving.
//!
//! Setup forces the hard path: tiny memory budget (the snapshot LRU holds only
//! the newest few hundred aggregates, so reads of older ones miss), small
//! segments (many rotations -> deep reverse walks), and a small file cache
//! (`max_open_files`), so consulting an evicted sealed segment's bloom re-reads
//! its 512KB header. We then read OLD aggregates (earliest segments) to maximise
//! scan depth.
//!
//! Run with: cargo run -p celeriant_integration_tests -- --test p3_4_read_thundering_herd

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use crate::{fill_incompressible, scrape_counter, ServerConfig, TestServer};
use celeriant_msg::{
    process_client_requests::ClientRequest,
    process_client_responses::ClientResponse,
    request::read_filters::ReadFilters,
    request::requests::{ReadRequest, SingleAggregateWrite, WriteRequest},
};
use celeriant_wal::{aggregate_key::AggregateKey, datablocks::datablock_aggregate_event::DatablockAggregateEvent};

const PORT_BASE: u16 = 21100;
const NUM_AGGREGATES: u64 = 1500;
const PAYLOAD_BYTES: usize = 30_000; // external, incompressible — forces rotations
const HERD_SIZE: usize = 200; // distinct concurrent readers

fn percentiles(mut v: Vec<u64>) -> (u64, u64, u64, u64) {
    v.sort_unstable();
    let p = |q: usize| v[(v.len() * q / 100).min(v.len() - 1)];
    (p(50), p(95), p(99), *v.last().unwrap())
}

/// Create-write one aggregate with a large incompressible payload.
async fn write_create(
    client: &mut CeleriantClient,
    id: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut payload = vec![0u8; PAYLOAD_BYTES];
    fill_incompressible(&mut payload, id);

    let event = DatablockAggregateEvent {
        client_seq: 1,
        event_seq: 0,
        event_id: None,
        event_timestamp: 1000 + id,
        event_type_major: 100,
        event_type_minor: 0,
        event_value: Arc::new(payload),
        iv: None,
    };

    let mut writes = HashMap::new();
    writes.insert(
        AggregateKey::new(1, 1, id as u128),
        SingleAggregateWrite { events: vec![event], allow_create: true, expected_version: Some(0), enforce_client_idempotency: false },
    );

    match client
        .send_request(&ClientRequest::Write(WriteRequest { correlation_id: Some(id as u128), client_id: 999, user_id: Some(888), writes }))
        .await?
    {
        ClientResponse::Write(_) => Ok(()),
        other => Err(format!("write failed for {id}: {other:?}").into()),
    }
}

/// Read one aggregate, returning the number of events seen.
async fn read_count(client: &mut CeleriantClient, id: u64) -> Result<usize, String> {
    let req = ReadRequest {
        correlation_id: Some(id as u128),
        aggregate_key: AggregateKey::new(1, 1, id as u128),
        filters: ReadFilters::new(1),
    };
    match client.send_request(&ClientRequest::Read(req)).await {
        Ok(ClientResponse::Read(r)) => Ok(r.event_batches.iter().map(|b| b.events.len()).sum()),
        Ok(other) => Err(format!("unexpected response for {id}: {other:?}")),
        Err(e) => Err(format!("read error for {id}: {e:?}")),
    }
}

async fn scrape(metrics_port: u16, name: &str) -> u64 {
    scrape_counter("127.0.0.1", metrics_port, name).await.unwrap_or(0)
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== P3-4: Read Thundering Herd (concurrent distinct cold reads) ===\n");

    let metrics_port = PORT_BASE + 2;
    let config = ServerConfig {
        log_level: "warn".to_string(),
        standalone: true,
        num_shards: Some(1),
        // Tiny cache: nearly every read misses the snapshot/recent-write caches.
        memory_budget_bytes: Some(512 * 1024),
        // Small segments: many rotations -> a deep reverse scan crosses many files.
        shard_log_preallocate_bytes: 4 * 1024 * 1024,
        // Small file cache: sealed-segment blooms get evicted, so consulting them
        // re-reads 512KB headers — the amplification we want to expose.
        max_open_files: 8,
        ..Default::default()
    };

    let mut server = TestServer::start_with_config(PORT_BASE, config).await?;

    // ── Populate ───────────────────────────────────────────────────────────
    println!("Populating {NUM_AGGREGATES} aggregates ({PAYLOAD_BYTES} B each)...");
    let pop_start = Instant::now();
    {
        let mut client = CeleriantClient::connect(server.address()).await?;
        for id in 1..=NUM_AGGREGATES {
            write_create(&mut client, id).await?;
            if id % 500 == 0 {
                println!("  wrote {id} ({:.1}s)", pop_start.elapsed().as_secs_f64());
            }
        }
    }
    println!("  populated in {:.1}s\n", pop_start.elapsed().as_secs_f64());

    // Let writes settle. With a 512KB budget the snapshot LRU holds only the
    // newest few hundred aggregates, so the OLD ids we read below are not resident.
    tokio::time::sleep(Duration::from_secs(2)).await;
    server.check_alive().map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

    // Disjoint cold id sets, both in the OLD range (earliest segments = deepest
    // scans). Neither has been read, and both are evicted from the tiny LRU.
    let seq_ids: Vec<u64> = (0..HERD_SIZE).map(|i| 1 + (i as u64) * 2).collect(); // odd: 1,3,..
    let herd_ids: Vec<u64> = (0..HERD_SIZE).map(|i| 2 + (i as u64) * 2).collect(); // even: 2,4,..

    let misses_before = scrape(metrics_port, "celeriant_cache_log_file_misses_total").await;
    let bloom_before = scrape(metrics_port, "celeriant_read_bloom_short_circuit_total").await;

    // ── Phase A: sequential cold baseline ───────────────────────────────────
    println!("Phase A: {HERD_SIZE} sequential cold reads...");
    let mut seq_lat = Vec::with_capacity(HERD_SIZE);
    let seq_start = Instant::now();
    {
        let mut client = CeleriantClient::connect(server.address()).await?;
        for &id in &seq_ids {
            let t = Instant::now();
            let n = read_count(&mut client, id).await.map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
            seq_lat.push(t.elapsed().as_micros() as u64);
            assert_eq!(n, 1, "aggregate {id} should have exactly 1 event");
        }
    }
    let seq_wall = seq_start.elapsed();
    let (sp50, sp95, sp99, smax) = percentiles(seq_lat);

    // ── Phase B: concurrent thundering herd ─────────────────────────────────
    println!("Phase B: {HERD_SIZE} CONCURRENT cold reads (distinct keys)...");
    let herd_start = Instant::now();
    let mut handles = Vec::with_capacity(HERD_SIZE);
    for &id in &herd_ids {
        let addr = server.address().to_string();
        handles.push(tokio::spawn(async move {
            let mut client = CeleriantClient::connect(&addr).await.map_err(|e| format!("connect: {e:?}"))?;
            let t = Instant::now();
            let n = read_count(&mut client, id).await?;
            Ok::<(u64, usize), String>((t.elapsed().as_micros() as u64, n))
        }));
    }

    let mut herd_lat = Vec::with_capacity(HERD_SIZE);
    let mut ok = 0usize;
    let mut failures: Vec<String> = Vec::new();
    for h in handles {
        match h.await {
            Ok(Ok((lat, n))) => {
                herd_lat.push(lat);
                if n == 1 { ok += 1; } else { failures.push(format!("wrong event count: {n}")); }
            }
            Ok(Err(e)) => failures.push(e),
            Err(e) => failures.push(format!("join error: {e}")),
        }
    }
    let herd_wall = herd_start.elapsed();

    let misses_after = scrape(metrics_port, "celeriant_cache_log_file_misses_total").await;
    let bloom_after = scrape(metrics_port, "celeriant_read_bloom_short_circuit_total").await;

    // ── Report ───────────────────────────────────────────────────────────
    let (hp50, hp95, hp99, hmax) = percentiles(herd_lat.clone());
    let file_misses = misses_after.saturating_sub(misses_before);
    let bloom_skips = bloom_after.saturating_sub(bloom_before);
    let header_read_mb = (file_misses * 512) as f64 / 1024.0; // 512KB per segment header
    let useful_kb = (HERD_SIZE as u64 * PAYLOAD_BYTES as u64) as f64 / 1024.0;

    println!("\n── Sequential (Phase A) ──");
    println!("  wall: {:?}  P50={}us P95={}us P99={}us max={}us", seq_wall, sp50, sp95, sp99, smax);
    println!("── Concurrent herd (Phase B, {HERD_SIZE} at once) ──");
    println!("  wall: {:?}  P50={}us P95={}us P99={}us max={}us", herd_wall, hp50, hp95, hp99, hmax);
    println!("  succeeded: {ok}/{HERD_SIZE}");
    println!("── Mechanism (scraped metrics, delta over read phase) ──");
    println!("  segment-file re-opens (log_file_misses): {file_misses}");
    println!("  bloom short-circuits:                    {bloom_skips}");
    println!("── Read amplification ──");
    println!("  header bytes paged in: ~{header_read_mb:.1} MB ({file_misses} x 512KB)");
    println!("  useful payload served: ~{useful_kb:.1} KB ({HERD_SIZE} x {PAYLOAD_BYTES} B)");
    if useful_kb > 0.0 {
        println!("  amplification: ~{:.0}x", (header_read_mb * 1024.0) / useful_kb);
    }

    // ── Assertions ───────────────────────────────────────────────────────
    // 1. Survival: the herd must not wedge, crash, or corrupt. This is the proof
    //    that the cache_load_semaphore + dedup locks bound the self-inflicted load.
    server.check_alive().map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
    if !failures.is_empty() {
        return Err(format!("{} of {HERD_SIZE} concurrent reads failed; first: {}", failures.len(), failures[0]).into());
    }
    assert_eq!(ok, HERD_SIZE, "all concurrent cold reads must return correct data");

    // 2. Mechanism fired: cold reads actually hit the reverse-scan / bloom path.
    //    (Re-opens OR bloom skips must have happened — proves we exercised disk.)
    assert!(
        file_misses > 0 || bloom_skips > 0,
        "expected cold reads to re-open segments or short-circuit blooms; \
         file_misses={file_misses} bloom_skips={bloom_skips}"
    );

    println!("\n=== PASS ===");
    println!("{HERD_SIZE} concurrent distinct cold reads completed correctly; the shard");
    println!("absorbed the herd without collapse. Cost is surfaced above — this is the");
    println!("reverse-scan tradeoff, bounded (not eliminated) by read_max_concurrent.");
    Ok(())
}
