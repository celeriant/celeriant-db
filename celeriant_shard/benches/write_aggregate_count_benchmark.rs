use std::collections::HashMap;
use std::hint::black_box;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use celeriant_memcache::internal_shard_config::InternalShardConfig;
use celeriant_memcache::timestamp_config::TimestampConfig;
use celeriant_msg::request::requests::{SingleAggregateWrite, WriteRequest};
use celeriant_shard::shard_wal::ShardWal;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::compression_type::CompressionType;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use glommio::timer::sleep;
use glommio::{LocalExecutorBuilder, Placement};
use tempfile::tempdir;

criterion_group!(benches, bench_write_aggregate_count);
criterion_main!(benches);

// =============================================================================
// CONFIGURATION
// =============================================================================

const EVENT_SIZE_BYTES: usize = 256;
const EVENTS_PER_WRITE: usize = 5;
const TOTAL_WRITES: usize = 150000;
const WRITES_PER_WAVE: usize = 40;
const INTER_WAVE_DELAY: Duration = Duration::from_micros(500);
const SEGMENT_SIZE_BYTES: u64 = 128 * 1024 * 1024;
const FSYNC_DELAY: Duration = Duration::from_millis(3);

fn aggregate_count_configs() -> Vec<(&'static str, usize)> {
    vec![
        ("agg_1", 1),
        ("agg_100", 100),
        ("agg_500", 500),
        ("agg_1000", 1000),
        ("agg_2000", 2000),
        ("agg_4000", 4000),
        ("agg_8000", 8000),
        ("agg_16000", 16000),
        ("agg_32000", 32000),
        ("agg_64000", 64000),
        ("agg_128000", 128000),
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
        recent_write_cache_bytes: 64 * 1024 * 1024,
        non_durable_writes: false,
        shard_dir,
        max_response_size: 16 * 1024 * 1024,
        aggregate_snapshots_cache_bytes: 64 * 1024 * 1024,
        aggregate_client_snapshots_cache_bytes: 32 * 1024 * 1024,
        read_max_chunk_size: 32 * 1024,
        timestamp_config: TimestampConfig::default(),
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
            compression_type: CompressionType::None,
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

fn bench_write_aggregate_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_aggregate_count");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(3));
    group.warm_up_time(Duration::from_secs(1));

    let bytes_per_iteration = EVENT_SIZE_BYTES * EVENTS_PER_WRITE * TOTAL_WRITES;
    group.throughput(Throughput::Bytes(bytes_per_iteration as u64));

    for (name, nbr_aggregates) in aggregate_count_configs() {
        eprintln!(
            "\n=== Benchmarking aggregate count: {} ({} total writes in waves of {}) ===",
            name, TOTAL_WRITES, WRITES_PER_WAVE
        );

        group.bench_with_input(
            BenchmarkId::new("multi_aggregate", name),
            &nbr_aggregates,
            |b, &nbr_aggregates| {
                b.iter_custom(|iters| {
                    let mut total_duration = Duration::ZERO;

                    for _ in 0..iters {
                        let tempdir = tempdir().unwrap();
                        let shard_dir = tempdir.path().to_path_buf();

                        let iteration_duration = LocalExecutorBuilder::new(Placement::Fixed(0))
                            .spawn(move || async move {
                                let config = create_config(shard_dir);
                                let shard_wal = Rc::new(ShardWal::open(config).await.unwrap());

                                let mut all_handles = Vec::with_capacity(TOTAL_WRITES);
                                let num_waves = TOTAL_WRITES / WRITES_PER_WAVE;

                                for wave in 0..num_waves {
                                    for i in 0..WRITES_PER_WAVE {
                                        let write_id = (wave * WRITES_PER_WAVE + i) % nbr_aggregates;
                                        let shard_wal = shard_wal.clone();

                                        let handle = glommio::spawn_local(async move {
                                            let aggregate_key =
                                                AggregateKey::new(1, 1, write_id as u128);
                                            let events =
                                                create_events(EVENTS_PER_WRITE, EVENT_SIZE_BYTES, 0);
                                            let write_request = create_write_request(
                                                aggregate_key,
                                                events,
                                                write_id as u128,
                                            );

                                            let start = Instant::now();
                                            let result = shard_wal.write(0, write_request).await;
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

                                shard_wal.close().await.unwrap();
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