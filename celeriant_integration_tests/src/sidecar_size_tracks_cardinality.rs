//! Sealed-segment sidecar size must track key cardinality.
//!
//! The segment blooms persisted in `log_N.summary` are right-sized at seal
//! from the segment's true cardinality (~10 bits/key, one-block floor, capped)
//! — never fixed-size copies of the 256 KiB + 128 KiB in-memory blooms, which
//! would put ~393 KB in every sidecar no matter how few keys the segment holds.
//!
//! Contract pinned here: a sealed segment holding a SMALL key population
//! (64 aggregates x 4 clients ≪ 300 keys) produces a `.summary` file under
//! 64 KiB — a budget well above the honest entry cost and well below the
//! fixed-copy size, so any fixed-size shortcut fails loudly.
//!
//! Behavior preservation is pinned elsewhere
//! (`recovery_multiseg_read_amplification`, `schema_sealed_segment_survives_crash`,
//! `idempotency_across_seal`); this test only adds a cheap sanity tail:
//! kill -9, restart, one never-seen client writes to every touched aggregate —
//! the negative-lookup path the shrunken blooms must still serve.

use std::sync::Arc;
use std::time::Duration;

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_operations::WriteEventsOptions;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;

use crate::common::port_for;
use crate::{ServerConfig, TestServer};

const PREALLOCATE: u64 = 2 * 1024 * 1024;
/// Two sealed segments prove the size holds per seal, not once by luck.
const MIN_SEALED: usize = 2;
const N_AGGS: usize = 64;
const N_CLIENTS: usize = 4;
const PAYLOAD_BYTES: usize = 1024;
const MAX_LOAD_WRITES: u64 = 12_000;
/// C1 budget: entries for ≤300 keys cost ~KB; right-sized blooms cost ~KB.
/// 64 KiB is 6x headroom above that and 6x below today's fixed ~400 KB.
const SIDECAR_BUDGET: u64 = 64 * 1024;

fn agg_key(a: usize) -> AggregateKey {
    AggregateKey::new(4_000 + a as u128, 1, 1)
}

fn client_id(c: usize) -> u128 {
    2_000 + c as u128
}

/// Incompressible payload (splitmix64 over the write identity) — constant
/// payloads compress to nothing and the log never rotates.
fn payload(a: usize, client: u128, seq: u64) -> Vec<u8> {
    let mut x = (a as u64).wrapping_mul(0x9E3779B97F4A7C15)
        ^ (client as u64).wrapping_mul(0xBF58476D1CE4E5B9)
        ^ seq.wrapping_mul(0x94D049BB133111EB);
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

fn ev(a: usize, client: u128, seq: u64) -> DatablockAggregateEvent {
    DatablockAggregateEvent {
        client_seq: seq,
        event_seq: 0,
        event_id: None,
        event_timestamp: seq,
        event_type_major: 7,
        event_type_minor: 0,
        event_value: Arc::new(payload(a, client, seq)),
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

fn cleanup() {
    let _ = std::process::Command::new("pkill").args(["-9", "-x", "celeriant"]).status();
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let r = run_inner().await;
    cleanup();
    r
}

async fn run_inner() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Sealed-segment sidecar size tracks key cardinality ===\n");

    let config = ServerConfig {
        num_shards: Some(1),
        standalone: true,
        shard_log_preallocate_bytes: PREALLOCATE,
        ..Default::default()
    };
    let mut server =
        TestServer::start_with_config(port_for("sidecar_size_tracks_cardinality"), config).await?;
    let data_root = server.config().data_root.clone();
    let mut c = CeleriantClient::connect(server.address()).await?;

    // ── Load: a small, fixed key population (64 aggregates x 4 clients) pushed
    // round-robin until the log has rotated at least MIN_SEALED times.
    let shard = loop {
        match shard_dir(&data_root) {
            Ok(d) => break d,
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    };
    let mut seqs = [[0u64; N_CLIENTS]; N_AGGS];
    let mut writes = 0u64;
    let mut round = 0usize;
    'load: loop {
        let cl = round % N_CLIENTS;
        for a in 0..N_AGGS {
            let seq = seqs[a][cl] + 1;
            let opts = WriteEventsOptions {
                allow_create: round == 0,
                enforce_client_idempotency: true,
                ..Default::default()
            };
            c.write_events_with(agg_key(a), vec![ev(a, client_id(cl), seq)], client_id(cl), opts)
                .await?;
            seqs[a][cl] = seq;
            writes += 1;
            if writes >= MAX_LOAD_WRITES {
                return Err(format!(
                    "log did not reach {} segments after {writes} writes ({} wal files) — raise payload or lower preallocate",
                    MIN_SEALED + 1,
                    wal_files(&shard).len()
                )
                .into());
            }
        }
        round += 1;
        if wal_files(&shard).len() >= MIN_SEALED + 1 {
            break 'load;
        }
    }
    println!("Rotated: {} wal files after {writes} writes ({N_AGGS} aggregates x {N_CLIENTS} clients)", wal_files(&shard).len());
    drop(c);

    // ── Every sealed segment must have its sidecar on disk before we measure.
    let sealed: Vec<u64> = {
        let active = wal_files(&shard).len() as u64; // log ids are 1-based and contiguous
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

    // ── C1: with ≪ 300 keys per segment, each sidecar must be well under the
    // fixed-bloom era's ~400 KB — blooms sized by cardinality, not by constant.
    let mut oversized = Vec::new();
    for id in &sealed {
        let summary = shard.join(format!("log_{id}.summary"));
        let size = std::fs::metadata(&summary)?.len();
        println!(
            "log_{id}.summary: {size} bytes (budget {SIDECAR_BUDGET}) — {}",
            if size < SIDECAR_BUDGET { "ok" } else { "OVERSIZED" }
        );
        if size >= SIDECAR_BUDGET {
            oversized.push(format!("log_{id}.summary is {size} bytes"));
        }
    }
    if !oversized.is_empty() {
        return Err(format!(
            "sidecar size does not track cardinality: {} of {} sealed sidecars at/over the {SIDECAR_BUDGET}-byte budget for a ~{}-key segment ({}) — fixed-size blooms still embedded at seal",
            oversized.len(),
            sealed.len(),
            N_AGGS + N_CLIENTS,
            oversized.join(", ")
        )
        .into());
    }
    println!("All {} sealed sidecars under {SIDECAR_BUDGET} bytes", sealed.len());

    // ── Sanity tail: the shrunken sidecars must still serve recovery. Kill -9,
    // restart on the same root, then a never-seen client writes seq 1 to every
    // touched aggregate — each write consults the sealed segments' client sets
    // (negative lookup) and must succeed.
    println!("kill -9, restart on the same data root");
    server.stop(); // Child::kill == SIGKILL: no shutdown path runs
    tokio::time::sleep(Duration::from_secs(1)).await;
    server.restart().await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    let new_client: u128 = 999_999;
    for a in 0..N_AGGS {
        let opts = WriteEventsOptions {
            allow_create: false,
            enforce_client_idempotency: true,
            ..Default::default()
        };
        c.write_events_with(agg_key(a), vec![ev(a, new_client, 1)], new_client, opts)
            .await
            .map_err(|e| format!("post-restart write to aggregate {a} from new client rejected: {e:?}"))?;
    }
    println!("Post-restart: new client landed seq 1 on all {N_AGGS} aggregates");

    println!("\n=== PASS: sealed sidecars sized by cardinality and still serving recovery ===");
    Ok(())
}
