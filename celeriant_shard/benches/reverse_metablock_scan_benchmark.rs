//! Reverse metablock-scan cost for reading OLD aggregate versions.
//!
//! `ShardWal::read` seeks to an aggregate's tip and reverse-scans the WAL until
//! it reaches the requested version. The scan is filtered by aggregate key but
//! today still *reads every metablock* between tip and target — including all
//! the interleaved foreign aggregates' metablocks. So reading the OLDEST version
//! of a backlogged aggregate costs O(all metablocks written since it) disk reads,
//! not O(this aggregate's own chain).
//!
//! This bench writes one target aggregate interleaved with many foreign
//! aggregates, then times reading the target's oldest vs newest version across
//! growing backlogs. Pre-fix: oldest scales ~linearly with the foreign volume.
//! Post-fix (per-aggregate backlink): oldest tracks only the target's own chain.
//!
//! Run: cargo bench -p celeriant_shard --bench reverse_metablock_scan_benchmark

use std::collections::HashMap;
use std::hint::black_box;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use celeriant_distributed::validated_node_status::ValidatedNodeStatus;
use celeriant_msg::request::read_filters::ReadFilters;
use celeriant_msg::request::requests::{ReadRequest, SingleAggregateWrite, WriteRequest};
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

criterion_group!(benches, bench_old_version_read);
criterion_main!(benches);

const SEGMENT_SIZE_BYTES: u64 = 2 * 1024 * 1024;
const TARGET_ID: u128 = 1;
const FOREIGN_BASE_ID: u128 = 1_000_000;
/// Fixed target chain depth across all configs, so the only variable is how many
/// FOREIGN metablocks are interleaved between the target's versions — that is
/// exactly what the per-aggregate backlink lets the reverse scan skip.
const TARGET_VERSIONS: u64 = 100;

/// Distinct foreign aggregates written between each target version. Models one
/// aggregate buried among many others (the production "1 of N queues" shape).
/// The backlink win grows with this ratio; below ~31 the old full-scan reads
/// fewer, larger IOs and can be faster despite reading far more bytes.
fn foreign_configs() -> Vec<(&'static str, u128)> {
    vec![("8x", 8), ("64x", 64), ("256x", 256)]
}

fn base_config(shard_dir: PathBuf, chain_read_window_bytes: u64) -> InternalShardConfig {
    InternalShardConfig {
        node_id: 1,
        shard_id: 1,
        max_open_files: 4096,
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
        negative_lookup_cache_bytes: 2 * 1024 * 1024,
        read_max_chunk_size: 32 * 1024,
        chain_read_window_bytes,
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
        compaction_temp_dir: std::path::PathBuf::from("/tmp/test_reverse_scan_bench"),
        cache_warmup_max_duration: Duration::ZERO,
        wal_compression_level: 3,
        dict_bytes: std::sync::Arc::from(celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES),
        s3_lease_duration_ms: 0,
    }
}

/// Tiny inline event so each write is one dense ~512B metablock, maximising the
/// number of metablocks per segment (the thing the reverse scan walks).
fn write_req(aggregate_id: u128, client_seq: u64) -> WriteRequest {
    let event = DatablockAggregateEvent {
        client_seq,
        event_type_major: 1,
        event_value: Arc::new(vec![0u8; 16]),
        ..Default::default()
    };
    let mut writes = HashMap::new();
    writes.insert(
        AggregateKey::new(1, 1, aggregate_id),
        SingleAggregateWrite { events: vec![event], allow_create: true, expected_version: None, enforce_client_idempotency: false },
    );
    WriteRequest { correlation_id: None, client_id: aggregate_id, user_id: None, writes }
}

/// Write TARGET_VERSIONS target versions, each followed by `foreign_per_round`
/// distinct foreign aggregates, so the target's chain is interleaved across the WAL.
fn setup_wal(shard_dir: PathBuf, foreign_per_round: u128) {
    LocalExecutorBuilder::new(Placement::Fixed(0))
        .spawn(move || async move {
            let shard = Rc::new(
                ShardWal::open(base_config(shard_dir, 1024), ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader)
                    .await
                    .unwrap(),
            );
            let mut foreign = FOREIGN_BASE_ID;
            for round in 0..TARGET_VERSIONS {
                shard.write(write_req(TARGET_ID, round + 1)).await.unwrap();
                for _ in 0..foreign_per_round {
                    shard.write(write_req(foreign, 1)).await.unwrap();
                    foreign += 1;
                }
            }
            shard.close().await;
        })
        .unwrap()
        .join()
        .unwrap();
}

fn bench_old_version_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("old_version_read");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));
    group.warm_up_time(Duration::from_secs(3));

    for (name, foreign_per_round) in foreign_configs() {
        let tempdir = tempdir().unwrap();
        let shard_dir = tempdir.path().to_path_buf();
        eprintln!("\n=== Setup {name}: {TARGET_VERSIONS} target versions interleaved with {foreign_per_round}x foreign ===");
        setup_wal(shard_dir.clone(), foreign_per_round);

        // Oldest version walks the whole chain. Compare the per-block window (1024 —
        // reads only the target's metablocks) against a 32KB window (batches dense
        // in-window hops but reads interleaved foreign bytes).
        bench_read(&mut group, "oldest_perblock", name, &shard_dir, 1, 1024);
        bench_read(&mut group, "oldest_window32k", name, &shard_dir, 1, 32 * 1024);
        // Newest version: near the tip, ~O(1) — the control curve.
        bench_read(&mut group, "newest", name, &shard_dir, TARGET_VERSIONS, 1024);

        drop(tempdir);
    }

    group.finish();
}

fn bench_read(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    variant: &str,
    size_name: &str,
    shard_dir: &std::path::Path,
    target_version: u64,
    chain_window: u64,
) {
    group.bench_with_input(BenchmarkId::new(variant, size_name), &shard_dir.to_path_buf(), |b, shard_dir| {
        b.iter_custom(|iters| {
            let shard_dir = shard_dir.clone();
            LocalExecutorBuilder::new(Placement::Fixed(0))
                .spawn(move || async move {
                    let shard = ShardWal::open(base_config(shard_dir, chain_window), ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader)
                        .await
                        .unwrap();

                    let mk_req = || ReadRequest {
                        correlation_id: None,
                        aggregate_key: AggregateKey::new(1, 1, TARGET_ID),
                        filters: ReadFilters::new(target_version).to_aggregate_version(target_version),
                    };

                    // Warm the aggregate snapshot (tip lookup) so we measure the
                    // version walk, not the one-off exists() scan.
                    shard.read(&mk_req()).await.unwrap();

                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let start = std::time::Instant::now();
                        let resp = shard.read(&mk_req()).await.unwrap();
                        total += start.elapsed();
                        black_box(resp.event_batches.len());
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
