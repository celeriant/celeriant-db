use std::collections::HashMap;
use std::hint::black_box;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use celeriant_shard::internal_shard_config::InternalShardConfig;
use celeriant_shard::replication_client::StubReplicationClient;
use celeriant_shard::timestamp_config::TimestampConfig;
use celeriant_msg::request::requests::{ExistsRequest, SingleAggregateWrite, WriteRequest};
use celeriant_shard::shard_wal::ShardWal;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::cluster_role::ClusterRole;
use celeriant_wal::compression_type::CompressionType;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use glommio::timer::sleep;
use glommio::{LocalExecutorBuilder, Placement};
use tempfile::tempdir;

criterion_group!(benches, bench_bloom_effectiveness);
criterion_main!(benches);

// =============================================================================
// CONFIGURATION
// =============================================================================

const EVENT_SIZE_BYTES: usize = 256;
const EVENTS_PER_WRITE: usize = 5;
const WRITES_PER_WAVE: usize = 40;
const INTER_WAVE_DELAY: Duration = Duration::from_micros(500);
const SEGMENT_SIZE_BYTES: u64 = 256 * 1024 * 1024;
const FSYNC_DELAY: Duration = Duration::from_millis(3);

/// Aggregate count configurations to test bloom effectiveness
/// More aggregates = higher bloom saturation = more false positives
fn aggregate_count_configs() -> Vec<(&'static str, usize, usize)> {
    // (name, num_unique_aggregates, total_writes)
    vec![
        ("100_aggs", 100, 2000),
        ("500_aggs", 500, 6000),
        ("3400_aggs", 3400, 35000),
        ("14400_aggs", 14400, 145000),
        ("61000_aggs", 61000, 615000),
        ("131700_aggs", 131700, 1000000),
    ]
}

/// Number of exists checks to perform per benchmark iteration
const EXISTS_CHECKS_PER_ITER: usize = 5;

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
        non_durable_writes: false,
        shard_dir,
        max_response_size: 16 * 1024 * 1024,
        max_request_size: 16 * 1024 * 1024,
        aggregate_snapshots_cache_bytes: 64 * 1024 * 1024,
        aggregate_client_snapshots_cache_bytes: 32 * 1024 * 1024,
        read_max_chunk_size: 32 * 1024,
        timestamp_config: TimestampConfig::default(),
        list_max_duration: Duration::from_millis(2000),
        list_page_size: 20000,
        list_wal_index_cache_bytes: 12 * 1024 * 1024,
        pending_replication_high_water_bytes: 67_108_864, // 64MB
        max_cluster_time_drift_ms: 5000,
        max_catchup_gap_bytes: 104_857_600,
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

/// Populate WAL with specified number of unique aggregates
async fn populate_wal(shard_wal: Rc<ShardWal<StubReplicationClient>>, num_aggregates: usize, total_writes: usize) {
    let num_waves = total_writes / WRITES_PER_WAVE;
    let mut all_handles = Vec::with_capacity(total_writes);

    for wave in 0..num_waves {
        for i in 0..WRITES_PER_WAVE {
            let write_id = wave * WRITES_PER_WAVE + i;
            let aggregate_id = (write_id % num_aggregates) as u128;
            let shard_wal = shard_wal.clone();

            let handle = glommio::spawn_local(async move {
                let aggregate_key = AggregateKey::new(1, 1, aggregate_id);
                let events = create_events(EVENTS_PER_WRITE, EVENT_SIZE_BYTES, 0);
                let write_request = create_write_request(aggregate_key, events, write_id as u128);
                let _ = shard_wal.write(0, write_request).await;
            });
            all_handles.push(handle);
        }

        if wave < num_waves - 1 {
            sleep(INTER_WAVE_DELAY).await;
        }
    }

    for h in all_handles {
        h.await;
    }
}

// =============================================================================
// BENCHMARK
// =============================================================================

/// Benchmark bloom filter effectiveness with different aggregate counts.
/// 
/// This demonstrates how the 128-byte bloom filter becomes useless as
/// the number of unique aggregates increases. The false positive rate
/// formula: (1 - e^(-k*n/m))^k where k=7 hashes, m=1024 bits, n=aggregates
///
/// Expected results:
/// - 100 aggregates: ~0.8% FPR - bloom very effective, most segments skipped
/// - 500 aggregates: ~40% FPR - bloom somewhat useful
/// - 1000 aggregates: ~85% FPR - bloom barely helps
/// - 2000+ aggregates: ~100% FPR - bloom completely saturated, no benefit
fn bench_bloom_effectiveness(c: &mut Criterion) {
    let mut group = c.benchmark_group("bloom_effectiveness");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));
    group.warm_up_time(Duration::from_secs(2));

    for (config_name, num_aggregates, total_writes) in aggregate_count_configs() {
        // Setup: Create and populate WAL
        let tempdir = tempdir().unwrap();
        let shard_dir = tempdir.path().to_path_buf();

        eprintln!("\n=== Setting up: {} ({} unique aggregates, {} writes) ===", 
                  config_name, num_aggregates, total_writes);

        // Populate in setup phase
        {
            let shard_dir = shard_dir.clone();
            LocalExecutorBuilder::new(Placement::Fixed(0))
                .spawn(move || async move {
                    let config = create_config(shard_dir);
                    let shard_wal = Rc::new(ShardWal::open(config, ClusterRole::Standalone, StubReplicationClient).await.unwrap());
                    populate_wal(shard_wal.clone(), num_aggregates, total_writes).await;
                    shard_wal.close().await.unwrap();
                })
                .unwrap()
                .join()
                .unwrap();
        }

        eprintln!("=== Setup complete ===\n");

        // Calculate theoretical false positive rate for reporting
        let m = 1024.0_f64; // bits (128 bytes)
        let k = 7.0_f64;    // hash functions
        let n = num_aggregates as f64;
        let fpr = (1.0 - (-k * n / m).exp()).powf(k);
        eprintln!("Theoretical FPR for {} aggregates: {:.1}%", num_aggregates, fpr * 100.0);

        group.throughput(Throughput::Elements(EXISTS_CHECKS_PER_ITER as u64));

        // Benchmark exists() for KNOWN aggregates (should find quickly via cache after first)
        group.bench_with_input(
            BenchmarkId::new("exists_known", config_name),
            &shard_dir.clone(),
            |b, shard_dir| {
                b.iter_custom(|iters| {
                    let shard_dir = shard_dir.clone();
                    let num_aggregates = num_aggregates;

                    let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
                        .spawn(move || async move {
                            let config = create_config(shard_dir);
                            let shard_wal = ShardWal::open(config, ClusterRole::Standalone, StubReplicationClient).await.unwrap();

                            let mut total_duration = Duration::ZERO;

                            for _ in 0..iters {
                                for i in 0..EXISTS_CHECKS_PER_ITER {
                                    // Cycle through known aggregate IDs
                                    let aggregate_id = (i % num_aggregates) as u128;
                                    let aggregate_key = AggregateKey::new(1, 1, aggregate_id);

                                    let exists_request = ExistsRequest {
                                        correlation_id: None,
                                        aggregate_key,
                                    };

                                    let start = Instant::now();
                                    let result = shard_wal.exists(&exists_request).await.unwrap();
                                    total_duration += start.elapsed();

                                    black_box(result);
                                }
                            }

                            total_duration
                        })
                        .unwrap();

                    handle.join().unwrap()
                });
            },
        );

        // Benchmark exists() for UNKNOWN aggregates - this is where bloom filter matters most!
        // Each check uses a unique key to avoid cache hits from put_aggregate_into_cache_as_not_found
        group.bench_with_input(
            BenchmarkId::new("exists_unknown", config_name),
            &shard_dir.clone(),
            |b, shard_dir| {
                b.iter_custom(|iters| {
                    let shard_dir = shard_dir.clone();

                    let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
                        .spawn(move || async move {
                            let config = create_config(shard_dir);
                            let shard_wal = ShardWal::open(config, ClusterRole::Standalone, StubReplicationClient).await.unwrap();

                            let mut total_duration = Duration::ZERO;
                            // Use IDs way outside the written range
                            let base_unknown_id = 1_000_000_u128;

                            for iter in 0..iters {
                                for i in 0..EXISTS_CHECKS_PER_ITER {
                                    // Each check uses unique ID to avoid cache
                                    let aggregate_id = base_unknown_id + (iter * EXISTS_CHECKS_PER_ITER as u64 + i as u64) as u128;
                                    let aggregate_key = AggregateKey::new(1, 1, aggregate_id);

                                    let exists_request = ExistsRequest {
                                        correlation_id: None,
                                        aggregate_key,
                                    };

                                    let start = Instant::now();
                                    let result = shard_wal.exists(&exists_request).await.unwrap();
                                    total_duration += start.elapsed();

                                    // Verify it's not found
                                    debug_assert_eq!(result.min_event_batch_index, 0);
                                    black_box(result);
                                }
                            }

                            total_duration
                        })
                        .unwrap();

                    handle.join().unwrap()
                });
            },
        );

        // Keep tempdir alive until benchmarks complete
        drop(tempdir);
    }

    group.finish();
}