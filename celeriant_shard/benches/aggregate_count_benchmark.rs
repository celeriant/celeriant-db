use std::collections::HashMap;
use std::hint::black_box;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use celeriant_distributed::validated_node_status::ValidatedNodeStatus;
use celeriant_shard::internal_shard_config::InternalShardConfig;
use celeriant_shard::replication_client::StubReplicationClient;
use celeriant_shard::s3_downloader::StubS3Downloader;
use celeriant_shard::timestamp_config::TimestampConfig;
use celeriant_msg::request::requests::{SingleAggregateWrite, WriteRequest};
use celeriant_shard::shard_wal::ShardWal;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use glommio::timer::sleep;
use glommio::{LocalExecutorBuilder, Placement};
use tempfile::tempdir;

criterion_group!(benches, bench_aggregate_count_impact);
criterion_main!(benches);

// =============================================================================
// CONFIGURATION
// =============================================================================

const EVENT_SIZE_BYTES: usize = 256;
const EVENTS_PER_WRITE: usize = 5;

const TOTAL_WRITES: usize = 100_000;
const WRITES_PER_WAVE: usize = 100;
const INTER_WAVE_DELAY: Duration = Duration::from_micros(500);

const FSYNC_DELAY: Duration = Duration::from_millis(4);
const SEGMENT_SIZE_BYTES: u64 = 128 * 1024 * 1024;

fn aggregate_count_configs() -> Vec<(&'static str, usize)> {
    vec![
        ("agg_1", 1),
        ("agg_10", 10),
        ("agg_100", 100),
        ("agg_500", 500),
        ("agg_1000", 1000),
        ("agg_5000", 5000),
        ("agg_15000", 15000),
        ("agg_30000", 30000),
        ("agg_90000", 90000),
        ("agg_180000", 180000),
        ("agg_360000", 360000),
    ]
}

// =============================================================================
// HELPERS
// =============================================================================

fn create_config(shard_dir: PathBuf) -> InternalShardConfig {
    InternalShardConfig {
        node_id: 1,
        max_open_files: 256,
        shard_log_preallocate_bytes: SEGMENT_SIZE_BYTES,
        fsync_delay: FSYNC_DELAY,
        replication_delay: Duration::from_millis(17),
        recent_write_cache_bytes: 64 * 1024 * 1024,
        shard_dir,
        max_response_size: 16 * 1024 * 1024,
        max_request_size: 16 * 1024 * 1024,
        aggregate_snapshots_cache_bytes: 64 * 1024 * 1024,
        aggregate_client_snapshots_cache_bytes: 32 * 1024 * 1024,
        read_max_chunk_size: 32 * 1024,
        timestamp_config: TimestampConfig::default(),
        list_max_duration: Duration::from_millis(2000),
        list_page_size: 20000,
        list_max_concurrent: 16,
        read_max_concurrent: 64,
        list_wal_index_cache_bytes: 12 * 1024 * 1024,
        schema_cache_bytes: 4 * 1024 * 1024,
        max_schema_size_bytes: 16384,
        pending_replication_high_water_bytes: 67_108_864, // 64MB
        max_catchup_gap_bytes: 104_857_600,
        s3_download_max_rounds: 3,
        max_clock_drift_ms: 500,
        shard_id: 1,
        compaction_check_interval: Duration::from_secs(600),
        compaction_min_reclaimable_ratio: 0.20,
        compaction_temp_dir: std::path::PathBuf::from("/tmp/test_compaction"),
        cache_warmup_max_duration: Duration::MAX,
    }
}

fn create_events(count: usize, size: usize, base_index: u64) -> Vec<DatablockAggregateEvent> {
    (0..count)
        .map(|i| DatablockAggregateEvent {
            client_event_index: base_index + i as u64,
            event_index: 0,
            event_id: None,
            event_timestamp: 1_700_000_000_000 + i as u64,
            event_type_major: 1,
            event_type_minor: 0,
            event_value: Arc::new(vec![0xABu8; size]),
            iv: None,
        })
        .collect()
}

fn create_write_request(
    aggregate_key: AggregateKey,
    events: Vec<DatablockAggregateEvent>,
    client_id: u128,
) -> WriteRequest {
    let mut writes = HashMap::new();
    writes.insert(
        aggregate_key,
        SingleAggregateWrite {
            events,
            allow_create: true,
            expected_event_batch_index: None,
            enforce_client_idempotency: false,
            compression_type_id: 0,
            compression_level: None,
        },
    );

    WriteRequest {
        correlation_id: None,
        client_id,
        user_id: None,
        writes,
    }
}

// =============================================================================
// BENCHMARK
// =============================================================================

/// Benchmark write performance with different aggregate counts.
///
/// Tests how performance scales with the number of distinct aggregates being written to.
/// Uses 4ms fsync delay and 100k total writes.
fn bench_aggregate_count_impact(c: &mut Criterion) {
    let mut group = c.benchmark_group("aggregate_count_impact");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));
    group.warm_up_time(Duration::from_secs(2));

    let bytes_per_iteration = EVENT_SIZE_BYTES * EVENTS_PER_WRITE * TOTAL_WRITES;
    group.throughput(Throughput::Bytes(bytes_per_iteration as u64));

    for (config_name, num_aggregates) in aggregate_count_configs() {
        eprintln!(
            "\n=== Benchmarking aggregate count: {} ({} total writes, {} aggregates) ===",
            config_name, TOTAL_WRITES, num_aggregates
        );

        group.bench_with_input(
            BenchmarkId::new("multi_write", config_name),
            &num_aggregates,
            |b, &num_aggregates| {
                b.iter_custom(|iters| {
                    let mut total_duration = Duration::ZERO;

                    for _ in 0..iters {
                        let tempdir = tempdir().unwrap();
                        let shard_dir = tempdir.path().to_path_buf();

                        let iteration_duration = LocalExecutorBuilder::new(Placement::Fixed(0))
                            .spawn(move || async move {
                                let config = create_config(shard_dir);
                                let shard_wal = Rc::new(ShardWal::open(config, ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader).await.unwrap());

                                let mut all_handles = Vec::with_capacity(TOTAL_WRITES);
                                let num_waves = TOTAL_WRITES / WRITES_PER_WAVE;

                                for wave in 0..num_waves {
                                    for i in 0..WRITES_PER_WAVE {
                                        let write_id = wave * WRITES_PER_WAVE + i;
                                        let aggregate_id = write_id % num_aggregates;
                                        let shard_wal = shard_wal.clone();

                                        let handle = glommio::spawn_local(async move {
                                            let aggregate_key =
                                                AggregateKey::new(1, 1, aggregate_id as u128);
                                            let base_index = (write_id / num_aggregates * EVENTS_PER_WRITE) as u64;
                                            let events =
                                                create_events(EVENTS_PER_WRITE, EVENT_SIZE_BYTES, base_index);
                                            let write_request = create_write_request(
                                                aggregate_key,
                                                events,
                                                write_id as u128,
                                            );

                                            let start = Instant::now();
                                            let result = shard_wal.write(write_request).await;
                                            let elapsed = start.elapsed();

                                            black_box(result.unwrap());
                                            elapsed
                                        });
                                        all_handles.push(handle);
                                    }

                                    if wave < num_waves - 1 {
                                        sleep(INTER_WAVE_DELAY).await;
                                    }
                                }

                                let mut cumulative_write_time = Duration::ZERO;
                                for h in all_handles {
                                    cumulative_write_time += h.await;
                                }

                                shard_wal.close().await;
                                cumulative_write_time / TOTAL_WRITES as u32
                            })
                            .unwrap()
                            .join()
                            .unwrap();

                        total_duration += iteration_duration;
                    }

                    total_duration
                });
            },
        );
    }

    group.finish();
}
