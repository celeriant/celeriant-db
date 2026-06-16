//! Cold-read amplification benchmark.
//!
//! The existing `exists_benchmark` keeps every segment's file handle (and
//! therefore its in-header aggregate-skip bloom) resident in the LRU, so a
//! negative lookup is answered by in-memory bloom checks. That is the *warm*
//! case.
//!
//! In production the file LRU is bounded (`max_open_files`) and starts empty
//! after a restart. A sealed segment's bloom lives inside its `LogSegmentFile`,
//! so consulting the bloom on an evicted segment first re-opens the file — a
//! 512KB header read (`HEADER_BLOCK_SIZE_BYTES`). This benchmark isolates that
//! cost by holding the segment count fixed and flipping the file cache between
//! "warm" (cache >= segments, blooms resident) and "cold" (cache << segments,
//! every lookup re-reads headers).
//!
//! The gap between the two curves is the disk amplification a cache-missed
//! lookup pays — and the thing that multiplies under a concurrent miss storm.
//!
//! Run: cargo bench -p celeriant_shard --bench cold_read_benchmark

use std::collections::HashMap;
use std::hint::black_box;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use celeriant_distributed::validated_node_status::ValidatedNodeStatus;
use celeriant_msg::request::requests::{AggregateDetailsRequest, SingleAggregateWrite, WriteRequest};
use celeriant_shard::internal_shard_config::InternalShardConfig;
use celeriant_shard::replication_client::StubReplicationClient;
use celeriant_shard::s3_downloader::StubS3Downloader;
use celeriant_shard::shard_wal::ShardWal;
use celeriant_shard::timestamp_config::TimestampConfig;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use glommio::{LocalExecutorBuilder, Placement};
use tempfile::tempdir;

criterion_group!(benches, bench_cold_negative_lookup);
criterion_main!(benches);

// 2MB segments: one 512KB header block + ~1MB metablock/datablock arena.
const SEGMENT_SIZE_BYTES: u64 = 2 * 1024 * 1024;
const EVENT_BYTES: usize = 300 * 1024; // external, incompressible — ~3 writes/segment
const NEGATIVE_LOOKUPS_PER_ITER: usize = 10;

/// Segment-count configurations to sweep. More segments = longer reverse walk
/// and (in the cold case) more header re-reads per negative lookup.
fn segment_configs() -> Vec<(&'static str, u64)> {
    vec![("8seg", 8), ("32seg", 32), ("96seg", 96)]
}

fn base_config(shard_dir: PathBuf, max_open_files: u64) -> InternalShardConfig {
    InternalShardConfig {
        node_id: 1,
        shard_id: 1,
        max_open_files,
        shard_log_preallocate_bytes: SEGMENT_SIZE_BYTES,
        fsync_delay: Duration::ZERO,
        replication_delay: Duration::ZERO,
        s3_replication_delay: Duration::from_millis(500),
        replication_rollback_cooldown: Duration::ZERO,
        heartbeat_starve_threshold: Duration::ZERO,
        recent_write_cache_bytes: 8 * 1024 * 1024,
        shard_dir,
        max_response_size: 16 * 1024 * 1024,
        max_request_size: 16 * 1024 * 1024,
        internode_max_request_size: 64 * 1024 * 1024,
        aggregate_snapshots_cache_bytes: 64 * 1024 * 1024,
        aggregate_client_snapshots_cache_bytes: 32 * 1024 * 1024,
        read_max_chunk_size: 32 * 1024,
        chain_read_window_bytes: 1024,
        timestamp_config: TimestampConfig::default(),
        list_max_duration: Duration::from_millis(2000),
        list_page_size: 20000,
        list_max_concurrent: 16,
        read_max_concurrent: 64,
        schema_cache_bytes: 4 * 1024 * 1024,
        max_schema_size_bytes: 16384,
        max_catchup_gap_bytes: Some(104_857_600),
        max_promotion_batch_bytes: None,
        max_clock_drift_ms: 500,
        compaction_check_interval: Duration::from_secs(600),
        compaction_min_reclaimable_ratio: 0.20,
        compaction_temp_dir: std::path::PathBuf::from("/tmp/test_cold_read_bench"),
        // Cold caches on (re)open: the lookup, not the warmup, pays the disk cost.
        cache_warmup_max_duration: Duration::ZERO,
        wal_compression_level: 3,
        dict_bytes: std::sync::Arc::from(celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES),
        s3_lease_duration_ms: 0,
    }
}

/// xorshift fill that resists the zstd dictionary so datablocks land external at full size.
fn incompressible(n: usize, seed: u64) -> Vec<u8> {
    let mut v = vec![0u8; n];
    let mut s = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    for c in v.chunks_mut(8) {
        s = s.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = s;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        let b = z.to_le_bytes();
        c.copy_from_slice(&b[..c.len()]);
    }
    v
}

fn write_req(aggregate_id: u128) -> WriteRequest {
    let event = DatablockAggregateEvent {
        client_seq: 1,
        event_type_major: 1,
        event_value: Arc::new(incompressible(EVENT_BYTES, aggregate_id as u64)),
        ..Default::default()
    };
    let mut writes = HashMap::new();
    writes.insert(
        AggregateKey::new(1, 1, aggregate_id),
        SingleAggregateWrite { events: vec![event], allow_create: true, expected_version: None, enforce_client_idempotency: false },
    );
    WriteRequest { correlation_id: None, client_id: aggregate_id, user_id: None, writes }
}

/// ~3-4 external 300KB datablocks fit in a 2MB segment's arena before it rotates.
const WRITES_PER_SEGMENT_EST: u64 = 4;

/// Count `log_*.wal` segment files on disk (the true segment count).
fn count_segments(shard_dir: &std::path::Path) -> usize {
    std::fs::read_dir(shard_dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .filter(|e| {
                    let name = e.file_name();
                    let n = name.to_string_lossy();
                    n.starts_with("log_") && n.ends_with(".wal")
                })
                .count()
        })
        .unwrap_or(0)
}

/// Populate a WAL with roughly `target_segments` segments by writing a fixed
/// volume. Returns the number of distinct aggregates written.
fn setup_wal(shard_dir: PathBuf, target_segments: u64) -> u128 {
    let writes = target_segments * WRITES_PER_SEGMENT_EST;
    LocalExecutorBuilder::new(Placement::Fixed(0))
        .spawn(move || async move {
            let shard = Rc::new(
                ShardWal::open(base_config(shard_dir, 4096), ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader)
                    .await
                    .unwrap(),
            );
            for id in 1..=writes as u128 {
                shard.write(write_req(id)).await.unwrap();
            }
            shard.close().await;
            writes as u128
        })
        .unwrap()
        .join()
        .unwrap()
}

fn bench_cold_negative_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("cold_negative_lookup");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));
    group.warm_up_time(Duration::from_secs(3));

    for (name, target_segments) in segment_configs() {
        let tempdir = tempdir().unwrap();
        let shard_dir = tempdir.path().to_path_buf();
        eprintln!("\n=== Setup {name}: filling to ~{target_segments} segments ===");
        let written = setup_wal(shard_dir.clone(), target_segments);
        let actual = count_segments(&shard_dir);
        eprintln!("=== Setup {name} complete: {written} aggregates across {actual} segments on disk ===\n");

        // Warm file cache: every segment resident, bloom checks are in-memory.
        bench_variant(&mut group, "warm_file_cache", name, &shard_dir, 4096);
        // Cold file cache: only 4 handles, so each negative lookup re-reads
        // headers for the rest of the segments.
        bench_variant(&mut group, "cold_file_cache", name, &shard_dir, 4);

        drop(tempdir);
    }

    group.finish();
}

fn bench_variant(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    variant: &str,
    size_name: &str,
    shard_dir: &std::path::Path,
    max_open_files: u64,
) {
    group.bench_with_input(BenchmarkId::new(variant, size_name), &shard_dir.to_path_buf(), |b, shard_dir| {
        b.iter_custom(|iters| {
            let shard_dir = shard_dir.clone();
            LocalExecutorBuilder::new(Placement::Fixed(0))
                .spawn(move || async move {
                    let shard = ShardWal::open(base_config(shard_dir, max_open_files), ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader)
                        .await
                        .unwrap();

                    let mut total = Duration::ZERO;
                    let mut probe: u128 = 5_000_000; // far outside the written range
                    for _ in 0..iters {
                        for _ in 0..NEGATIVE_LOOKUPS_PER_ITER {
                            probe += 1;
                            let req = AggregateDetailsRequest { correlation_id: None, aggregate_key: AggregateKey::new(1, 1, probe) };
                            let start = std::time::Instant::now();
                            let result = shard.exists(&req).await;
                            total += start.elapsed();
                            debug_assert!(result.is_err(), "probe aggregate must not exist");
                            black_box(result.is_err());
                        }
                    }
                    shard.close().await;
                    total
                })
                .unwrap()
                .join()
                .unwrap()
        });
    });
}
