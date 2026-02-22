use std::{collections::HashMap, rc::Rc};
use std::hint::black_box;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use celeriant_distributed::validated_node_status::ValidatedNodeStatus;
use celeriant_shard::internal_shard_config::InternalShardConfig;
use celeriant_shard::replication_client::StubReplicationClient;
use celeriant_shard::s3_downloader::StubS3Downloader;
use celeriant_shard::timestamp_config::TimestampConfig;
use celeriant_msg::request::requests::{AggregateDetailsRequest, SingleAggregateWrite, WriteRequest};
use celeriant_shard::shard_wal::ShardWal;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::compression_type::CompressionType;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use glommio::{LocalExecutorBuilder, Placement};
use tempfile::tempdir;

criterion_group!(benches, bench_exists_wal_sizes);
criterion_main!(benches);

// =============================================================================
// CONFIGURATION
// =============================================================================

/// WAL size configurations: (name, target_bytes, expected_segments)
/// Each segment is 128MB based on shard_log_preallocate_bytes
fn wal_size_configs() -> Vec<(&'static str, usize)> {
    vec![
        ("1seg_128mb", 128 * 1024 * 1024),
        ("2seg_256mb", 256 * 1024 * 1024),
        ("4seg_512mb", 512 * 1024 * 1024),
        ("8seg_1gb", 1024 * 1024 * 1024),
    ]
}

const NUM_AGGREGATES: usize = 2000;
const EVENT_SIZE_BYTES: usize = 1024; // 1KB per event
const EVENTS_PER_BATCH: usize = 10;
const SEGMENT_SIZE_BYTES: u64 = 128 * 1024 * 1024; // 128MB segments

// =============================================================================
// HELPERS
// =============================================================================

fn create_config(shard_dir: PathBuf) -> InternalShardConfig {
    InternalShardConfig {
        node_id: 1,
        max_open_files: 256,
        shard_log_preallocate_bytes: SEGMENT_SIZE_BYTES,
        fsync_delay: Duration::from_millis(10),
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
        list_wal_index_cache_bytes: 12 * 1024 * 1024,
        pending_replication_high_water_bytes: 67_108_864, // 64MB
        max_cluster_time_drift_ms: 5000,
        max_catchup_gap_bytes: 104_857_600,
        s3_download_max_rounds: 3,
        shard_id: 1,
        max_s3_fallback_batch_bytes: 100 * 1024 * 1024,
    }
}

fn create_events(count: usize, size: usize, base_index: u64) -> Vec<DatablockAggregateEvent> {
    (0..count)
        .map(|i| DatablockAggregateEvent {
            client_event_index: base_index + i as u64,
            event_index: 0, // Server assigns this
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

/// Populates a WAL with the target number of bytes spread across many aggregates.
/// Returns the actual bytes written (approximate).
fn setup_populated_wal(shard_dir: PathBuf, target_bytes: usize) -> usize {
    let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
        .spawn(move || async move {
            let config = create_config(shard_dir);
            let shard_wal = Rc::new(ShardWal::open(config, ValidatedNodeStatus::standalone(), StubReplicationClient, StubS3Downloader).await.unwrap());

            // Estimate bytes per write (events + metadata overhead ~512 bytes)
            let bytes_per_write_estimate = (EVENT_SIZE_BYTES * EVENTS_PER_BATCH) + 512;
            let total_writes = target_bytes / bytes_per_write_estimate;

            // Pre-compute all write requests to avoid mutable borrow issues with concurrent tasks
            let mut write_requests = Vec::with_capacity(total_writes);
            let mut aggregate_event_indices: HashMap<u128, u64> = HashMap::new();

            for i in 0..total_writes {
                let aggregate_id = (i % NUM_AGGREGATES) as u128;
                let aggregate_key = AggregateKey::new(1, 1, aggregate_id);

                let base_index = aggregate_event_indices.entry(aggregate_id).or_insert(0);

                let events = create_events(EVENTS_PER_BATCH, EVENT_SIZE_BYTES, *base_index);
                *base_index += EVENTS_PER_BATCH as u64;

                let write_request = create_write_request(aggregate_key, events, i as u128);
                write_requests.push(write_request);
            }

            // Spawn all writes concurrently
            let mut handles = Vec::with_capacity(total_writes);
            for (i, write_request) in write_requests.into_iter().enumerate() {

                //Required to prevent a thundering herd of writes larger than the log segment file size
                glommio::timer::sleep(Duration::from_micros(1)).await;

                let shard_wal = shard_wal.clone();
                handles.push(glommio::spawn_local(async move {
                    shard_wal.write(write_request).await.unwrap();

                    // Progress indicator for large WALs
                    if i % 1000 == 0 && i > 0 {
                        let progress = (i as f64 / total_writes as f64) * 100.0;
                        eprintln!("  Setup progress: {:.1}% ({}/{})", progress, i, total_writes);
                    }
                }));
            }

            // Await all write tasks
            for h in handles {
                h.await;
            }


            total_writes * bytes_per_write_estimate
        })
        .unwrap();

    handle.join().unwrap()
}

// =============================================================================
// BENCHMARK
// =============================================================================

fn bench_exists_wal_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("exists_wal_scan");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(30));
    group.warm_up_time(Duration::from_secs(5));

    for (size_name, target_bytes) in wal_size_configs() {
        // Setup: Create temp dir and populate WAL
        let tempdir = tempdir().unwrap();
        let shard_dir = tempdir.path().to_path_buf();

        eprintln!("\n=== Setting up WAL: {} ({} bytes) ===", size_name, target_bytes);
        let actual_bytes = setup_populated_wal(shard_dir.clone(), target_bytes);
        eprintln!("=== WAL setup complete: ~{} bytes written ===\n", actual_bytes);

        // Report throughput as bytes scanned - should show O(n) relationship
        group.throughput(Throughput::Bytes(target_bytes as u64));

        group.bench_with_input(
            BenchmarkId::new("known_aggregate", size_name),
            &shard_dir.clone(),
            |b, shard_dir| {
                b.iter_custom(|iters| {
                    let shard_dir = shard_dir.clone();

                    let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
                        .spawn(move || async move {
                            let config = create_config(shard_dir);
                            let shard_wal = ShardWal::open(config, ValidatedNodeStatus::standalone(), StubReplicationClient, StubS3Downloader).await.unwrap();

                            // Timed iterations - cycle through all known aggregates
                            let mut total_duration = Duration::ZERO;

                            for i in 0..iters {
                                // Cycle through all known aggregate IDs (0 to NUM_AGGREGATES-1)
                                let aggregate_id = (i as usize % NUM_AGGREGATES) as u128;
                                let test_aggregate = AggregateKey::new(1, 1, aggregate_id);

                                let exists_request = AggregateDetailsRequest {
                                    correlation_id: None,
                                    aggregate_key: test_aggregate,
                                };

                                let start = std::time::Instant::now();
                                let result = shard_wal.exists(&exists_request).await.unwrap();
                                total_duration += start.elapsed();

                                // Verify it's actually found
                                debug_assert!(result.min_event_batch_index > 0 || result.min_event_batch_index > 0);
                                black_box(result);
                            }

                            total_duration
                        })
                        .unwrap();

                    handle.join().unwrap()
                });
            },
        );

        // Test with aggregates that DON'T exist - must use unique keys per iteration
        // to avoid cache hits from put_aggregate_into_cache_as_not_found
        group.bench_with_input(
            BenchmarkId::new("unknown_aggregate", size_name),
            &shard_dir.clone(),
            |b, shard_dir| {
                b.iter_custom(|iters| {
                    let shard_dir = shard_dir.clone();

                    let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
                        .spawn(move || async move {
                            let config = create_config(shard_dir);
                            let shard_wal = ShardWal::open(config, ValidatedNodeStatus::standalone(), StubReplicationClient, StubS3Downloader).await.unwrap();

                            // Timed iterations - each uses a unique aggregate key
                            // Start from NUM_AGGREGATES to ensure they don't exist in WAL
                            let base_unknown_id = (NUM_AGGREGATES as u128) + 1_000_000;
                            let mut total_duration = Duration::ZERO;

                            for i in 0..iters {
                                let unknown_aggregate = AggregateKey::new(1, 1, base_unknown_id + i as u128);
                                let exists_request = AggregateDetailsRequest {
                                    correlation_id: None,
                                    aggregate_key: unknown_aggregate,
                                };

                                let start = std::time::Instant::now();
                                let result = shard_wal.exists(&exists_request).await.unwrap();
                                total_duration += start.elapsed();

                                // Verify it's actually not found
                                debug_assert_eq!(result.min_event_batch_index, 0);
                                black_box(result);
                            }

                            total_duration
                        })
                        .unwrap();

                    handle.join().unwrap()
                });
            },
        );
    }

    group.finish();
}