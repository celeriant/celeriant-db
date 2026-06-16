//! Cost of the client-seq idempotency reconstruction scan (`cache_aggregate_client`).
//!
//! On a cold idempotent write, the shard scans the WAL backward for that client's
//! last metablock, walking THIS aggregate's versions past interleaved foreign
//! aggregates. This bench writes one target aggregate by many distinct clients,
//! interleaved with foreign aggregates, then times a cold idempotent write from the
//! OLDEST client (full chain walk) vs the NEWEST (near-tip control).
//!
//! Each iteration runs on a fresh copy of the WAL: the triggering write appends a
//! block for the client, which would short-circuit the next scan otherwise. The copy
//! is outside the timed region.
//!
//! To compare scan modes, flip the single builder line in `cache_aggregate_client`
//! (shard_wal.rs): `.with_bloom_filter(aggregate_key)` (read every block) vs
//! `.with_aggregate_chain(aggregate_key.clone(), self.config.chain_read_window_bytes)`
//! (skip foreign via backlinks). Re-run with `--save-baseline` and `critcmp`.
//!
//! Run: cargo bench -p celeriant_shard --bench client_seq_scan_benchmark

use std::collections::HashMap;
use std::hint::black_box;
use std::path::{Path, PathBuf};
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

criterion_group!(benches, bench_client_seq_scan);
criterion_main!(benches);

const SEGMENT_SIZE_BYTES: u64 = 2 * 1024 * 1024;
const TARGET_ID: u128 = 1;
const FOREIGN_BASE_ID: u128 = 1_000_000;
/// Distinct clients writing the target aggregate, fixed across configs so the only
/// variable is foreign interleaving (what the backlink lets the scan skip).
const TARGET_CLIENTS: u64 = 100;

/// Foreign aggregates written between each target version. Models one aggregate
/// buried among many others; the backlink win grows with this ratio.
fn foreign_configs() -> Vec<(&'static str, u128)> {
    vec![("8x", 8), ("64x", 64), ("256x", 256)]
}

fn base_config(shard_dir: PathBuf) -> InternalShardConfig {
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
        compaction_temp_dir: std::path::PathBuf::from("/tmp/test_client_seq_scan_bench"),
        cache_warmup_max_duration: Duration::ZERO,
        wal_compression_level: 3,
        dict_bytes: std::sync::Arc::from(celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES),
        s3_lease_duration_ms: 0,
    }
}

/// One tiny inline event = one dense ~512B metablock, recording `client_id`/`client_seq`.
fn write_req(aggregate_id: u128, client_id: u128, client_seq: u64, enforce_idempotency: bool) -> WriteRequest {
    let event = DatablockAggregateEvent {
        client_seq,
        event_type_major: 1,
        event_value: Arc::new(vec![0u8; 16]),
        ..Default::default()
    };
    let mut writes = HashMap::new();
    writes.insert(
        AggregateKey::new(1, 1, aggregate_id),
        SingleAggregateWrite {
            events: vec![event],
            allow_create: true,
            expected_version: None,
            enforce_client_idempotency: enforce_idempotency,
        },
    );
    WriteRequest { correlation_id: None, client_id, user_id: None, writes }
}

/// Write the target aggregate once per distinct client, each followed by
/// `foreign_per_round` distinct foreign aggregates. Client 1 is oldest, client
/// TARGET_CLIENTS is newest. Idempotency off here — setup only lays down the chain.
fn setup_wal(shard_dir: PathBuf, foreign_per_round: u128) {
    LocalExecutorBuilder::new(Placement::Fixed(0))
        .spawn(move || async move {
            let shard = Rc::new(
                ShardWal::open(base_config(shard_dir), ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader)
                    .await
                    .unwrap(),
            );
            let mut foreign = FOREIGN_BASE_ID;
            for client in 1..=TARGET_CLIENTS {
                shard.write(write_req(TARGET_ID, client as u128, 1, false)).await.unwrap();
                for _ in 0..foreign_per_round {
                    shard.write(write_req(foreign, foreign, 1, false)).await.unwrap();
                    foreign += 1;
                }
            }
            shard.close().await;
        })
        .unwrap()
        .join()
        .unwrap();
}

/// Copy every file in `src` into a fresh `dst` (flat dir of WAL segments).
fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_file() {
            std::fs::copy(entry.path(), dst.join(entry.file_name())).unwrap();
        }
    }
}

fn bench_client_seq_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("client_seq_scan");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));
    group.warm_up_time(Duration::from_secs(3));

    for (name, foreign_per_round) in foreign_configs() {
        let template = tempdir().unwrap();
        let template_dir = template.path().join("wal");
        eprintln!("\n=== Setup {name}: {TARGET_CLIENTS} clients on target interleaved with {foreign_per_round}x foreign ===");
        setup_wal(template_dir.clone(), foreign_per_round);

        // Oldest client sits at the bottom of the chain → full walk.
        bench_cold_idempotent_write(&mut group, "oldest_client", name, &template_dir, 1);
        // Newest client is near the tip → ~O(1) control curve.
        bench_cold_idempotent_write(&mut group, "newest_client", name, &template_dir, TARGET_CLIENTS as u128);
        // Brand-new client never in the chain: the NEGATIVE lookup. Must walk the
        // aggregate's whole chain to prove absence — the engine-idempotency-negative-scan
        // case. Cost ≈ oldest_client (full walk), and crucially every NEW producer pays it
        // again (no negative memoization), so fan-in workloads multiply this by P.
        bench_cold_idempotent_write(&mut group, "new_client", name, &template_dir, 9_000_000);

        drop(template);
    }

    group.finish();
}

fn bench_cold_idempotent_write(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    variant: &str,
    size_name: &str,
    template_dir: &Path,
    client_id: u128,
) {
    group.bench_with_input(BenchmarkId::new(variant, size_name), &template_dir.to_path_buf(), |b, template_dir| {
        b.iter_custom(|iters| {
            let template_dir = template_dir.clone();
            LocalExecutorBuilder::new(Placement::Fixed(0))
                .spawn(move || async move {
                    let mut total = Duration::ZERO;
                    for i in 0..iters {
                        // Fresh WAL copy per iteration (the triggering write appends a
                        // block for this client; reusing state would short-circuit it).
                        let work = tempdir().unwrap();
                        let work_dir = work.path().join("wal");
                        copy_dir(&template_dir, &work_dir);

                        let shard = ShardWal::open(base_config(work_dir), ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader)
                            .await
                            .unwrap();

                        // Cold idempotent write triggers cache_aggregate_client's reverse scan.
                        // seq 2 > the client's setup seq (1), so it's accepted after reconstruction.
                        let req = write_req(TARGET_ID, client_id, 2 + i, true);
                        let start = std::time::Instant::now();
                        let resp = shard.write(req).await.unwrap();
                        total += start.elapsed();
                        black_box(resp);

                        shard.close().await;
                        drop(work);
                    }
                    total
                })
                .unwrap()
                .join()
                .unwrap()
        });
    });
}
