//! Post-recovery first-touch read amplification across sealed segments.
//!
//! The wedge this pins: SchemaKey's bloom hash occupies the same 48-byte
//! xxh3 domain as AggregateKey's — (org ‖ type ‖ major:8LE ‖ minor:8LE) is
//! byte-identical to (org ‖ type ‖ agg:16LE) when agg == major + (minor<<64) —
//! so a schema-absence proof gated by a SHARED per-segment filter can never
//! bloom-skip a segment holding the aliased aggregate (major 1, aggregate 1 —
//! the fleet-bench shape): every no_schema cache miss walks every sealed
//! metablock region. A fresh server pays that invisibly on a tiny WAL and
//! caches the result; a recovered server pays it against the whole history.
//! On NVMe it hides in latency noise; on ~1 ms storage the scan outlives the
//! client timeout and the retry churn becomes a 100%-read wedge. The dedicated
//! per-segment schema bloom in the `.summary` sidecar is what makes schema
//! absence provable without touching segment bytes; this test holds the cost
//! to that promise.
//!
//! So the assertion here is on BYTES READ, not time: `/proc/<pid>/io`
//! read_bytes is the storage truth the storm is made of (O_DIRECT — every
//! read is a device read). `crash_kill9_under_load_durability` pins
//! correctness across the same shape; this pins the cost. Pass = first-touch
//! reads stay bounded regardless of how many sealed bytes exist.

use std::sync::Arc;
use std::time::Duration;

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_operations::WriteEventsOptions;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;

use crate::common::port_for;
use crate::{ServerConfig, TestServer};

const PREALLOCATE: u64 = 2 * 1024 * 1024;
/// Sealed segments required before the kill. Two is the smallest count where
/// "walked one sealed segment" and "walked them all" are distinguishable.
const MIN_SEALED: usize = 2;
const N_AGGS: u64 = 64;
const PAYLOAD_BYTES: usize = 1024;
/// The one pre-crash writer — matches the fleet bench shape (one client_id per
/// process), so every sealed per-aggregate client set is Exact(1) and a NEW
/// client is definitely-absent everywhere: the cheapest possible consult.
const CLIENT_LOAD: u128 = 7777;
const MAX_LOAD_WRITES: u64 = 8000;

fn agg_key(a: u64) -> AggregateKey {
    AggregateKey::new(1, 1, (a + 1) as u128)
}

/// Incompressible payload (splitmix64 over the write identity) — constant
/// payloads compress to nothing and the log never rotates.
fn payload(a: u64, seq: u64) -> Vec<u8> {
    let mut x = a.wrapping_mul(0x9E3779B97F4A7C15) ^ seq.wrapping_mul(0xBF58476D1CE4E5B9);
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

/// event_type_major 1 with minor 0 is the fleet-bench shape, and it collides
/// byte-for-byte with AggregateKey(org, type, 1) in the segment blooms (both
/// hash org‖type‖(major,minor) == org‖type‖agg over 48 LE bytes). Aggregate id
/// 1 exists here, so the schema-absence check cannot bloom-skip any segment —
/// the write path must stay bounded anyway.
fn ev(seq: u64, a: u64) -> DatablockAggregateEvent {
    DatablockAggregateEvent {
        client_seq: seq,
        event_seq: 0,
        event_id: None,
        event_timestamp: 1_000 + seq,
        event_type_major: 1,
        event_type_minor: 0,
        event_value: Arc::new(payload(a, seq)),
        iv: None,
    }
}

fn shard_dir(data_root: &std::path::Path) -> Result<std::path::PathBuf, String> {
    let mut dirs: Vec<_> = std::fs::read_dir(data_root)
        .map_err(|e| format!("read_dir {data_root:?}: {e}"))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.is_dir() && p.file_name().is_some_and(|n| n.to_string_lossy().starts_with("shard_"))
        })
        .collect();
    dirs.sort();
    dirs.into_iter().next().ok_or_else(|| format!("no shard_* dir under {data_root:?}"))
}

fn wal_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut v: Vec<_> = std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "wal"))
        .collect();
    v.sort();
    v
}

/// Storage-layer bytes this process has read (O_DIRECT: all device reads).
fn proc_read_bytes(pid: u32) -> Result<u64, String> {
    let io = std::fs::read_to_string(format!("/proc/{pid}/io"))
        .map_err(|e| format!("/proc/{pid}/io: {e}"))?;
    io.lines()
        .find_map(|l| l.strip_prefix("read_bytes: "))
        .and_then(|v| v.trim().parse().ok())
        .ok_or_else(|| format!("no read_bytes in /proc/{pid}/io"))
}

/// Wait until the server's read counter stops moving (recovery warm-up and any
/// open-time scans done), so probe deltas measure only the probe.
async fn await_read_quiescence(pid: u32) -> Result<u64, String> {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mut last = proc_read_bytes(pid)?;
    loop {
        tokio::time::sleep(Duration::from_millis(700)).await;
        let now = proc_read_bytes(pid)?;
        if now == last {
            return Ok(now);
        }
        if std::time::Instant::now() > deadline {
            return Err(format!("server still reading after 60s (read_bytes {last} -> {now})"));
        }
        last = now;
    }
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Recovery over sealed segments: first-touch read amplification ===\n");

    let config = ServerConfig {
        num_shards: Some(1),
        standalone: true,
        shard_log_preallocate_bytes: PREALLOCATE,
        ..Default::default()
    };
    let mut server = TestServer::start_with_config(port_for("recovery_multiseg_read_amplification"), config).await?;
    let data_root = server.config().data_root.clone();
    let mut c = CeleriantClient::connect(server.address()).await?;

    // ── Load: one client round-robins the aggregates until the log has rotated
    // at least MIN_SEALED times. Incompressible payloads make volume ≈ bytes.
    let shard = loop {
        match shard_dir(&data_root) {
            Ok(d) => break d,
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    };
    let mut seq = 0u64;
    'load: loop {
        for a in 0..N_AGGS {
            seq += 1;
            let opts = WriteEventsOptions {
                allow_create: seq <= N_AGGS,
                enforce_client_idempotency: true,
                ..Default::default()
            };
            c.write_events_with(agg_key(a), vec![ev(seq, a)], CLIENT_LOAD, opts).await?;
            if seq >= MAX_LOAD_WRITES {
                return Err(format!(
                    "log did not reach {} segments after {seq} writes ({} wal files) — raise payload or lower preallocate",
                    MIN_SEALED + 1,
                    wal_files(&shard).len()
                )
                .into());
            }
        }
        if wal_files(&shard).len() >= MIN_SEALED + 1 {
            break 'load;
        }
    }
    println!("Rotated: {} wal files after {seq} writes", wal_files(&shard).len());

    // Every sealed segment must carry its sidecar before the kill — the point of
    // the test is that the cheap answer EXISTS on disk and goes unused.
    let sealed: Vec<u64> = {
        let files = wal_files(&shard);
        let active = files.len() as u64; // log ids are 1-based and contiguous
        (1..active).collect()
    };
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    for id in &sealed {
        let summary = shard.join(format!("log_{id}.summary"));
        while !summary.exists() {
            if std::time::Instant::now() > deadline {
                return Err(format!("sidecar {summary:?} not written 30s after seal").into());
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
    let sealed_bytes: u64 = sealed
        .iter()
        .map(|id| std::fs::metadata(shard.join(format!("log_{id}.wal"))).map(|m| m.len()).unwrap_or(PREALLOCATE))
        .sum();
    println!("Sealed: {} segments, {} bytes, sidecars present", sealed.len(), sealed_bytes);
    drop(c);

    // ── Crash UNDER LOAD and recover on the same root. The in-flight writes
    // matter: they leave the sealed segments' persisted read-cursor state in
    // the shape the fleet crash produces (the drained-idle shape lets the
    // read-branch scanners skip sealed segments instead of walking them, and
    // the repro disappears).
    println!("kill -9 under load, restart on the same data root");
    let addr = server.address().to_string();
    let writer = tokio::spawn(async move {
        let Ok(mut c) = CeleriantClient::connect(&addr).await else { return };
        let mut seq = 0u64;
        loop {
            seq += 1;
            // Bench-shaped: client_id 0, allow_create, no idempotency.
            if c.write_events(agg_key(seq % N_AGGS), vec![ev(seq, seq % N_AGGS)], 0).await.is_err() {
                return; // server died under us — that's the point
            }
        }
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    server.stop();
    writer.abort();
    tokio::time::sleep(Duration::from_secs(1)).await;
    server.restart().await?;
    let pid = server.pid();
    let baseline = await_read_quiescence(pid).await?;
    println!("Recovery warm-up settled at read_bytes={baseline}");

    // A first touch that uses the sidecars/blooms reads a few chunks (tens of
    // KiB); one that walks the sealed metablock regions reads them wholesale
    // (MiBs here, hundreds of MiB at fleet segment sizes). A fixed absolute
    // budget separates the regimes without scaling with how much data exists —
    // which is the entire point: first-touch cost must not grow with history.
    let budget: u64 = 256 * 1024;
    let mut failures = Vec::new();
    let mut probe = |label: &str, delta: u64| {
        println!("{label}: {delta} bytes read (budget {budget})");
        if delta > budget {
            failures.push(format!(
                "{label} read {delta} bytes of the {sealed_bytes}-byte sealed chain — the schema-absence scan re-walked history the blooms should bound"
            ));
        }
    };

    // Probe A: new client, existing aggregate — the post-crash fan-in shape.
    // Its schema key (1,1,major=1,0) collides with aggregate id 1's bloom hash,
    // so the no_schema proof cannot skip any sealed segment: this is the probe
    // that catches the walk.
    let mut c = CeleriantClient::connect(server.address()).await?;
    let opts = WriteEventsOptions { allow_create: false, enforce_client_idempotency: true, ..Default::default() };
    c.write_events_with(agg_key(0), vec![ev(1, 0)], 99_999, opts).await?;
    let after_a = await_read_quiescence(pid).await?;
    probe("probe A (new client, existing aggregate)", after_a - baseline);

    // Probe B: brand-new aggregate, same schema key — control. Probe A's
    // completed scan cached the no_schema result, so this write must be
    // near-free; a nonzero walk here would mean the cost is not even
    // once-per-key.
    let opts = WriteEventsOptions { allow_create: true, enforce_client_idempotency: true, ..Default::default() };
    c.write_events_with(agg_key(N_AGGS + 100), vec![ev(1, N_AGGS + 100)], 99_998, opts).await?;
    let after_b = await_read_quiescence(pid).await?;
    probe("probe B (new aggregate)", after_b - after_a);

    if !failures.is_empty() {
        for f in &failures {
            eprintln!("  FAIL: {f}");
        }
        return Err(format!("{} probe(s) re-read the sealed chain on first touch after recovery", failures.len()).into());
    }

    println!("\n=== PASS: post-recovery first touches stayed within the sidecar-consult budget ===");
    Ok(())
}
