//! Boot warm-up cost benchmark.
//!
//! On open, `ShardWal` reverse-scans the WAL to repopulate the snapshot/client
//! caches (`pre_warm_cache`). The scan runs until the caches fill OR
//! `cache_warmup_max_duration` elapses. With the default (no deadline) and a
//! cache large enough to hold the working set, **boot reads every metablock in
//! the shard** — boot time is O(batches written), not O(active segment).
//!
//! This benchmark measures `ShardWal::open()` (≈ warm-up time) as the batch
//! count grows, for two regimes:
//!   - `unbounded`: no warm-up deadline (Duration::MAX) — scans the whole WAL.
//!   - `bounded_50ms`: a 50ms deadline — boot is capped, but only the newest
//!     batches get cached; older aggregates stay cold and pay reverse-scan cost
//!     on first read.
//!
//! Contrast with EventStoreDB, whose persistent index rebuild on unclean
//! shutdown can take hours: celeriant's warm-up is best-effort and bounded, so
//! it never blocks boot for hours — but it also doesn't guarantee a warm cache.
//!
//! Run: cargo bench -p celeriant_shard --bench warmup_benchmark

use std::collections::HashMap;
use std::hint::black_box;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use celeriant_distributed::validated_node_status::ValidatedNodeStatus;
use celeriant_msg::request::requests::{SingleAggregateWrite, WriteRequest};
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

criterion_group!(benches, bench_warmup);
criterion_main!(benches);

/// Distinct-aggregate batch counts to sweep. Each is one metablock to scan.
fn batch_counts() -> Vec<(&'static str, u64)> {
    vec![("5k", 5_000), ("20k", 20_000), ("80k", 80_000)]
}

fn config(shard_dir: PathBuf, warmup: Duration) -> InternalShardConfig {
    InternalShardConfig {
        node_id: 1,
        shard_id: 1,
        max_open_files: 4096,
        // Big segments so warm-up cost is the scan itself, not rotation count.
        shard_log_preallocate_bytes: 512 * 1024 * 1024,
        fsync_delay: Duration::ZERO,
        replication_delay: Duration::ZERO,
        s3_replication_delay: Duration::from_millis(500),
        replication_rollback_cooldown: Duration::ZERO,
        heartbeat_starve_threshold: Duration::ZERO,
        // Caches large enough to hold the whole working set, so warm-up doesn't
        // stop early on cache-full — it scans until the deadline or log_1.
        recent_write_cache_bytes: 256 * 1024 * 1024,
        shard_dir,
        max_response_size: 16 * 1024 * 1024,
        max_request_size: 16 * 1024 * 1024,
        internode_max_request_size: 64 * 1024 * 1024,
        aggregate_snapshots_cache_bytes: 256 * 1024 * 1024,
        aggregate_client_snapshots_cache_bytes: 256 * 1024 * 1024,
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
        compaction_temp_dir: std::path::PathBuf::from("/tmp/test_warmup_bench"),
        cache_warmup_max_duration: warmup,
        wal_compression_level: 3,
        dict_bytes: std::sync::Arc::from(celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES),
        s3_lease_duration_ms: 0,
    }
}

fn write_req(id: u128) -> WriteRequest {
    let event = DatablockAggregateEvent {
        client_seq: 1,
        event_type_major: 1,
        event_value: Arc::new(vec![0xABu8; 8]), // tiny, inline — one metablock per write
        ..Default::default()
    };
    let mut writes = HashMap::new();
    writes.insert(
        AggregateKey::new(1, 1, id),
        SingleAggregateWrite { events: vec![event], allow_create: true, expected_version: None, enforce_client_idempotency: false },
    );
    WriteRequest { correlation_id: None, client_id: id, user_id: None, writes }
}

/// Populate `batches` distinct aggregates (one metablock each).
fn setup_wal(shard_dir: PathBuf, batches: u64) {
    LocalExecutorBuilder::new(Placement::Fixed(0))
        .spawn(move || async move {
            let shard = Rc::new(
                ShardWal::open(config(shard_dir, Duration::MAX), ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader)
                    .await
                    .unwrap(),
            );
            for id in 1..=batches as u128 {
                shard.write(write_req(id)).await.unwrap();
            }
            shard.close().await;
        })
        .unwrap()
        .join()
        .unwrap();
}

fn bench_warmup(c: &mut Criterion) {
    let mut group = c.benchmark_group("boot_warmup");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(12));
    group.warm_up_time(Duration::from_secs(2));

    for (name, batches) in batch_counts() {
        let tempdir = tempdir().unwrap();
        let shard_dir = tempdir.path().to_path_buf();
        eprintln!("\n=== Setup boot_warmup {name}: writing {batches} batches ===");
        setup_wal(shard_dir.clone(), batches);
        eprintln!("=== Setup {name} complete ===\n");

        bench_open(&mut group, "unbounded", name, &shard_dir, Duration::MAX);
        bench_open(&mut group, "bounded_50ms", name, &shard_dir, Duration::from_millis(50));

        drop(tempdir);
    }

    group.finish();
}

fn bench_open(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    variant: &str,
    size_name: &str,
    shard_dir: &std::path::Path,
    warmup: Duration,
) {
    group.bench_with_input(BenchmarkId::new(variant, size_name), &shard_dir.to_path_buf(), |b, shard_dir| {
        b.iter_custom(|iters| {
            let shard_dir = shard_dir.clone();
            LocalExecutorBuilder::new(Placement::Fixed(0))
                .spawn(move || async move {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let start = std::time::Instant::now();
                        let shard = ShardWal::open(config(shard_dir.clone(), warmup), ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader)
                            .await
                            .unwrap();
                        total += start.elapsed();
                        black_box(&shard);
                        shard.close().await;
                    }
                    total
                })
                .unwrap()
                .join()
                .unwrap()
        });
    });
}
