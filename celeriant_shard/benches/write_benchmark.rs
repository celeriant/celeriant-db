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

criterion_group!(benches, bench_write_fsync_delays, bench_write_cache_impact, bench_write_idle_latency);
criterion_main!(benches);

// =============================================================================
// CONFIGURATION
// =============================================================================

const EVENT_SIZE_BYTES: usize = 256;
const EVENTS_PER_WRITE: usize = 5;

/// Total writes per benchmark iteration - large enough to amortize startup costs
const TOTAL_WRITES: usize = 4000;

/// Writes are submitted in waves to simulate realistic arrival patterns
const WRITES_PER_WAVE: usize = 40;

/// Delay between waves - this controls the "arrival rate" of writes
/// 500µs between waves of 20 = ~40,000 writes/sec arrival rate
const INTER_WAVE_DELAY: Duration = Duration::from_micros(500);

const NBR_AGGREGATE_MULTI: usize = 5000;

const SEGMENT_SIZE_BYTES: u64 = 128 * 1024 * 1024;

/// Fsync delay configurations to test
fn fsync_delay_configs() -> Vec<(&'static str, Duration)> {
    vec![
        ("0ms_sync", Duration::from_millis(0)),
        ("400us", Duration::from_micros(400)),
        ("1ms", Duration::from_millis(1)),
        ("2ms", Duration::from_millis(2)),
        ("3ms", Duration::from_millis(3)),
        ("4ms", Duration::from_millis(4)),
        ("5ms", Duration::from_millis(5)),
        ("10ms", Duration::from_millis(10)),
        ("25ms", Duration::from_millis(25)),
        ("50ms", Duration::from_millis(50)),
    ]
}

/// Cache configurations: (name, cache_bytes)
fn cache_configs() -> Vec<(&'static str, u64)> {
    vec![
        ("cache_off", 0),
        ("cache_64mb", 64 * 1024 * 1024),
    ]
}

// =============================================================================
// HELPERS
// =============================================================================

fn create_config(
    shard_dir: PathBuf,
    fsync_delay: Duration,
    recent_write_cache_bytes: u64,
) -> InternalShardConfig {
    InternalShardConfig {
        node_id: 1,
        max_open_files: 256,
        shard_log_preallocate_bytes: SEGMENT_SIZE_BYTES,
        fsync_delay,
        replication_delay: Duration::from_millis(17),
        s3_replication_delay: Duration::from_millis(500),
        replication_rollback_cooldown: Duration::from_millis(500),
        heartbeat_starve_threshold: Duration::ZERO,
        recent_write_cache_bytes,
        shard_dir,
        max_response_size: 16 * 1024 * 1024,
        max_request_size: 16 * 1024 * 1024,
        internode_max_request_size: 64 * 1024 * 1024,
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
        max_catchup_gap_bytes: Some(104_857_600),
        max_promotion_batch_bytes: None,
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
// BENCHMARKS
// =============================================================================

/// Benchmark write performance with different fsync amortization delays.
///
/// Writes are submitted in waves over time to simulate realistic arrival patterns.
/// This allows fsync amortization to batch multiple writes into single fsyncs.
///
/// Measurement: Only the cumulative time spent in write().await calls is measured.
/// Sleep time between waves is excluded from the benchmark results.
///
/// Expected behavior:
/// - 0ms fsync: Each write triggers immediate fsync (~TOTAL_WRITES fsyncs)
/// - 10ms fsync: Writes within 10ms windows batch together (~spread_time/10ms fsyncs)
/// - 100ms fsync: Aggressive batching, fewer fsyncs but higher per-write latency
fn bench_write_fsync_delays(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_fsync_delay");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(3));
    group.warm_up_time(Duration::from_secs(1));

    let bytes_per_iteration = EVENT_SIZE_BYTES * EVENTS_PER_WRITE * TOTAL_WRITES;
    group.throughput(Throughput::Bytes(bytes_per_iteration as u64));

    for (delay_name, fsync_delay) in fsync_delay_configs() {
        eprintln!(
            "\n=== Benchmarking fsync_delay: {} ({} total writes in waves of {}) ===",
            delay_name, TOTAL_WRITES, WRITES_PER_WAVE
        );

        // Multi-aggregate: each write goes to a different aggregate
        group.bench_with_input(
            BenchmarkId::new("multi_aggregate", delay_name),
            &fsync_delay,
            |b, &fsync_delay| {
                b.iter_custom(|iters| {
                    let mut total_duration = Duration::ZERO;

                    for _ in 0..iters {
                        let tempdir = tempdir().unwrap();
                        let shard_dir = tempdir.path().to_path_buf();

                        let iteration_duration = LocalExecutorBuilder::new(Placement::Fixed(0))
                            .spawn(move || async move {
                                let config =
                                    create_config(shard_dir, fsync_delay, 64 * 1024 * 1024);
                                let shard_wal = Rc::new(ShardWal::open(config, ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader).await.unwrap());

                                let mut all_handles = Vec::with_capacity(TOTAL_WRITES);
                                let num_waves = TOTAL_WRITES / WRITES_PER_WAVE;

                                for wave in 0..num_waves {
                                    // Submit a wave of writes
                                    for i in 0..WRITES_PER_WAVE {
                                        let write_id = (wave * WRITES_PER_WAVE + i) % NBR_AGGREGATE_MULTI;
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

                                            // Measure only the write time
                                            let start = Instant::now();
                                            let result = shard_wal.write(write_request).await;
                                            let elapsed = start.elapsed();

                                            black_box(result.unwrap());
                                            elapsed
                                        });
                                        all_handles.push(handle);
                                    }

                                    // Wait before next wave (simulates time between client requests)
                                    // This sleep time is NOT included in the measurement
                                    if wave < num_waves - 1 {
                                        sleep(INTER_WAVE_DELAY).await;
                                    }
                                }

                                // Wait for all writes to complete and sum their durations
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

        // Single-aggregate: all writes go to same aggregate (tests lock contention + fsync batching)
        group.bench_with_input(
            BenchmarkId::new("single_aggregate", delay_name),
            &fsync_delay,
            |b, &fsync_delay| {
                b.iter_custom(|iters| {
                    let mut total_duration = Duration::ZERO;

                    for _ in 0..iters {
                        let tempdir = tempdir().unwrap();
                        let shard_dir = tempdir.path().to_path_buf();

                        let iteration_duration = LocalExecutorBuilder::new(Placement::Fixed(0))
                            .spawn(move || async move {
                                let config =
                                    create_config(shard_dir, fsync_delay, 64 * 1024 * 1024);
                                let shard_wal = Rc::new(ShardWal::open(config, ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader).await.unwrap());

                                let aggregate_key = AggregateKey::new(1, 1, 1);
                                let mut all_handles = Vec::with_capacity(TOTAL_WRITES);
                                let num_waves = TOTAL_WRITES / WRITES_PER_WAVE;

                                for wave in 0..num_waves {
                                    for i in 0..WRITES_PER_WAVE {
                                        let write_id = wave * WRITES_PER_WAVE + i;
                                        let shard_wal = shard_wal.clone();
                                        let aggregate_key = aggregate_key.clone();

                                        let handle = glommio::spawn_local(async move {
                                            let base_index = (write_id * EVENTS_PER_WRITE) as u64;
                                            let events = create_events(
                                                EVENTS_PER_WRITE,
                                                EVENT_SIZE_BYTES,
                                                base_index,
                                            );
                                            let write_request = create_write_request(
                                                aggregate_key,
                                                events,
                                                write_id as u128,
                                            );

                                            // Measure only the write time
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

                                // Wait for all writes to complete and sum their durations
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

/// Benchmark write latency at low load (sequential writes, one at a time).
///
/// Each write completes fully (including fsync) before the next starts.
/// The sync_gate is always free, so the coordinator fast path fires every time.
///
/// Expected behavior with fast path:
/// - All fsync delays show identical performance (delay is never used)
/// Without fast path:
/// - Higher delays = proportionally higher latency (delay added to every write)
fn bench_write_idle_latency(c: &mut Criterion) {
    const SEQUENTIAL_WRITES: usize = 100;

    let mut group = c.benchmark_group("write_idle_latency");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(3));
    group.warm_up_time(Duration::from_secs(1));

    let bytes_per_iteration = EVENT_SIZE_BYTES * EVENTS_PER_WRITE * SEQUENTIAL_WRITES;
    group.throughput(Throughput::Bytes(bytes_per_iteration as u64));

    for (delay_name, fsync_delay) in fsync_delay_configs() {
        group.bench_with_input(
            BenchmarkId::new("sequential", delay_name),
            &fsync_delay,
            |b, &fsync_delay| {
                b.iter_custom(|iters| {
                    let mut total_duration = Duration::ZERO;

                    for _ in 0..iters {
                        let tempdir = tempdir().unwrap();
                        let shard_dir = tempdir.path().to_path_buf();

                        let iteration_duration = LocalExecutorBuilder::new(Placement::Fixed(0))
                            .spawn(move || async move {
                                let config =
                                    create_config(shard_dir, fsync_delay, 64 * 1024 * 1024);
                                let shard_wal = Rc::new(ShardWal::open(config, ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader).await.unwrap());

                                let aggregate_key = AggregateKey::new(1, 1, 1);
                                let mut cumulative_write_time = Duration::ZERO;

                                for write_id in 0..SEQUENTIAL_WRITES {
                                    let base_index = (write_id * EVENTS_PER_WRITE) as u64;
                                    let events = create_events(
                                        EVENTS_PER_WRITE,
                                        EVENT_SIZE_BYTES,
                                        base_index,
                                    );
                                    let write_request = create_write_request(
                                        aggregate_key.clone(),
                                        events,
                                        write_id as u128,
                                    );

                                    let start = Instant::now();
                                    let result = shard_wal.write(write_request).await;
                                    cumulative_write_time += start.elapsed();

                                    black_box(result.unwrap());
                                }

                                shard_wal.close().await;
                                cumulative_write_time / SEQUENTIAL_WRITES as u32
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

/// Benchmark write performance with recent write cache enabled vs disabled
fn bench_write_cache_impact(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_cache_impact");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(3));
    group.warm_up_time(Duration::from_secs(1));

    let bytes_per_iteration = EVENT_SIZE_BYTES * EVENTS_PER_WRITE * TOTAL_WRITES;
    group.throughput(Throughput::Bytes(bytes_per_iteration as u64));

    let fsync_delay = Duration::from_millis(10);

    for (cache_name, cache_bytes) in cache_configs() {
        eprintln!("\n=== Benchmarking cache config: {} ===", cache_name);

        group.bench_with_input(
            BenchmarkId::new("multi_aggregate", cache_name),
            &cache_bytes,
            |b, &cache_bytes| {
                b.iter_custom(|iters| {
                    let mut total_duration = Duration::ZERO;

                    for _ in 0..iters {
                        let tempdir = tempdir().unwrap();
                        let shard_dir = tempdir.path().to_path_buf();

                        let iteration_duration = LocalExecutorBuilder::new(Placement::Fixed(0))
                            .spawn(move || async move {
                                let config = create_config(shard_dir, fsync_delay, cache_bytes);
                                let shard_wal = Rc::new(ShardWal::open(config, ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader).await.unwrap());

                                let mut all_handles = Vec::with_capacity(TOTAL_WRITES);
                                let num_waves = TOTAL_WRITES / WRITES_PER_WAVE;

                                for wave in 0..num_waves {
                                    for i in 0..WRITES_PER_WAVE {
                                        let write_id = (wave * WRITES_PER_WAVE + i) % NBR_AGGREGATE_MULTI;
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

                                            // Measure only the write time
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

                                // Wait for all writes to complete and sum their durations
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

        group.bench_with_input(
            BenchmarkId::new("single_aggregate", cache_name),
            &cache_bytes,
            |b, &cache_bytes| {
                b.iter_custom(|iters| {
                    let mut total_duration = Duration::ZERO;

                    for _ in 0..iters {
                        let tempdir = tempdir().unwrap();
                        let shard_dir = tempdir.path().to_path_buf();

                        let iteration_duration = LocalExecutorBuilder::new(Placement::Fixed(0))
                            .spawn(move || async move {
                                let config = create_config(shard_dir, fsync_delay, cache_bytes);
                                let shard_wal = Rc::new(ShardWal::open(config, ValidatedNodeStatus::create_standalone(), StubReplicationClient, StubS3Downloader).await.unwrap());

                                let aggregate_key = AggregateKey::new(1, 1, 1);
                                let mut all_handles = Vec::with_capacity(TOTAL_WRITES);
                                let num_waves = TOTAL_WRITES / WRITES_PER_WAVE;

                                for wave in 0..num_waves {
                                    for i in 0..WRITES_PER_WAVE {
                                        let write_id = wave * WRITES_PER_WAVE + i;
                                        let shard_wal = shard_wal.clone();
                                        let aggregate_key = aggregate_key.clone();

                                        let handle = glommio::spawn_local(async move {
                                            let base_index = (write_id * EVENTS_PER_WRITE) as u64;
                                            let events = create_events(
                                                EVENTS_PER_WRITE,
                                                EVENT_SIZE_BYTES,
                                                base_index,
                                            );
                                            let write_request = create_write_request(
                                                aggregate_key,
                                                events,
                                                write_id as u128,
                                            );

                                            // Measure only the write time
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

                                // Wait for all writes to complete and sum their durations
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