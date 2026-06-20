use std::collections::HashMap;
use std::hint::black_box;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use celeriant_distributed::validated_node_status::ValidatedNodeStatus;
use celeriant_msg::request::requests::{
    ListAggregateTypesRequest, ListAggregatesRequest, ListOrgsRequest, SingleAggregateWrite,
    WriteRequest,
};
use celeriant_shard::internal_shard_config::InternalShardConfig;
use celeriant_shard::replication_client::StubReplicationClient;
use celeriant_shard::s3_downloader::StubS3Downloader;
use celeriant_shard::shard_wal::ShardWal;
use celeriant_shard::timestamp_config::TimestampConfig;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use glommio::{LocalExecutorBuilder, Placement};
use tempfile::tempdir;

criterion_group!(benches, bench_list_aggregates);
criterion_main!(benches);

// =============================================================================
// CONFIGURATION
// =============================================================================

const ORG_ID: u128 = 1;
const AGG_TYPE_ID: u128 = 1;
const EVENT_SIZE_BYTES: usize = 64;
const SHARD_ID: u32 = 1;

/// Cardinality sweep: number of distinct aggregates populated into the WAL.
/// Override with LIST_BENCH_CARDINALITIES="1000,10000" for a short smoke run.
fn cardinality_configs() -> Vec<(String, usize)> {
    if let Ok(raw) = std::env::var("LIST_BENCH_CARDINALITIES") {
        return raw
            .split(',')
            .filter_map(|s| s.trim().parse::<usize>().ok())
            .map(|m| (format!("m_{}", m), m))
            .collect();
    }
    vec![
        ("m_1000".to_string(), 1_000),
        ("m_10000".to_string(), 10_000),
        ("m_100000".to_string(), 100_000),
    ]
}

/// Page sizes (config `list_page_size`) swept per cardinality. Because the
/// list path checks the page limit *between* segments and serves the active
/// segment summary in one shot, small page sizes do not bound the first-page
/// result when all aggregates live in the active segment summary.
fn page_size_configs() -> Vec<(&'static str, usize)> {
    vec![("page_100", 100), ("page_20000", 20_000)]
}

// =============================================================================
// HELPERS
// =============================================================================

fn create_config(shard_dir: PathBuf, list_page_size: usize) -> InternalShardConfig {
    InternalShardConfig {
        node_id: 1,
        max_open_files: 256,
        shard_log_preallocate_bytes: 128 * 1024 * 1024,
        fsync_delay: Duration::from_millis(4),
        replication_delay: Duration::from_millis(17),
        s3_replication_delay: Duration::from_millis(500),
        replication_rollback_cooldown: Duration::from_millis(500),
        heartbeat_starve_threshold: Duration::ZERO,
        recent_write_cache_bytes: 64 * 1024 * 1024,
        shard_dir,
        max_response_size: 16 * 1024 * 1024,
        max_request_size: 16 * 1024 * 1024,
        internode_max_request_size: 64 * 1024 * 1024,
        aggregate_snapshots_cache_bytes: 64 * 1024 * 1024,
        aggregate_client_snapshots_cache_bytes: 32 * 1024 * 1024,
        read_max_chunk_size: 32 * 1024,
        chain_read_window_bytes: 1024,
        timestamp_config: TimestampConfig::default(),
        // Generous duration so the page-size effect (not the time budget) is what
        // bounds a page during the sweep.
        list_max_duration: Duration::from_millis(60_000),
        list_page_size,
        list_max_concurrent: 16,
        read_max_concurrent: 64,
        schema_cache_bytes: 4 * 1024 * 1024,
        max_schema_size_bytes: 16384,
        max_catchup_gap_bytes: Some(104_857_600),
        max_promotion_batch_bytes: None,
        max_clock_drift_ms: 500,
        shard_id: SHARD_ID,
        compaction_check_interval: Duration::from_secs(600),
        compaction_min_reclaimable_ratio: 0.20,
        compaction_temp_dir: std::path::PathBuf::from("/tmp/test_compaction"),
        cache_warmup_max_duration: Duration::MAX,
        wal_compression_level: 3,
        dict_bytes: std::sync::Arc::from(celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES),
        s3_lease_duration_ms: 0,
    }
}

fn create_write_request(aggregate_id: u128) -> WriteRequest {
    let aggregate_key = AggregateKey::new(ORG_ID, AGG_TYPE_ID, aggregate_id);
    let events = vec![DatablockAggregateEvent {
        client_seq: 0,
        event_seq: 0,
        event_id: None,
        event_timestamp: 1_700_000_000_000,
        event_type_major: 1,
        event_type_minor: 0,
        event_value: Arc::new(vec![0xABu8; EVENT_SIZE_BYTES]),
        iv: None,
    }];

    let mut writes = HashMap::new();
    writes.insert(
        aggregate_key,
        SingleAggregateWrite {
            events,
            allow_create: true,
            expected_version: None,
            enforce_client_idempotency: false,
        },
    );

    WriteRequest {
        correlation_id: None,
        client_id: aggregate_id,
        user_id: None,
        writes,
    }
}

/// Populate a fresh WAL with `num_aggregates` distinct aggregates (one tiny
/// event each) and leave it on disk in `shard_dir`. Writes happen on a glommio
/// executor; the WAL is closed so a subsequent open replays it into the cache.
fn populate_wal(shard_dir: PathBuf, num_aggregates: usize) {
    LocalExecutorBuilder::new(Placement::Fixed(0))
        .spawn(move || async move {
            let config = create_config(shard_dir, 20_000);
            let shard_wal = Rc::new(
                ShardWal::open(
                    config,
                    ValidatedNodeStatus::create_standalone(),
                    StubReplicationClient,
                    StubS3Downloader,
                )
                .await
                .unwrap(),
            );

            // Concurrent writes, drip-fed to avoid overrunning the segment file.
            let mut handles = Vec::with_capacity(num_aggregates);
            for aggregate_id in 0..num_aggregates as u128 {
                glommio::timer::sleep(Duration::from_micros(1)).await;
                let shard_wal = shard_wal.clone();
                handles.push(glommio::spawn_local(async move {
                    shard_wal.write(create_write_request(aggregate_id)).await.unwrap();
                }));
            }
            for h in handles {
                h.await;
            }

            shard_wal.close().await;
        })
        .unwrap()
        .join()
        .unwrap();
}

// =============================================================================
// BENCHMARK
// =============================================================================

fn bench_list_aggregates(c: &mut Criterion) {
    let mut group = c.benchmark_group("list_aggregates");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));
    group.warm_up_time(Duration::from_secs(3));

    for (card_name, num_aggregates) in cardinality_configs() {
        // One populated WAL per cardinality, reused across page-size arms.
        let tempdir = tempdir().unwrap();
        let shard_dir = tempdir.path().to_path_buf();

        eprintln!(
            "\n=== Populating WAL: {} ({} aggregates) ===",
            card_name, num_aggregates
        );
        populate_wal(shard_dir.clone(), num_aggregates);
        eprintln!("=== Populated {} aggregates ===\n", num_aggregates);

        // Throughput = aggregates listed per op. If listing is O(M) full-scan
        // regardless of page size, this stays flat across page sizes.
        group.throughput(Throughput::Elements(num_aggregates as u64));

        for (page_name, page_size) in page_size_configs() {
            let bench_id =
                BenchmarkId::new("list_aggregates", format!("{}/{}", card_name, page_name));
            let shard_dir = shard_dir.clone();

            group.bench_function(bench_id, move |b| {
                b.iter_custom(|iters| {
                    let shard_dir = shard_dir.clone();
                    LocalExecutorBuilder::new(Placement::Fixed(0))
                        .spawn(move || async move {
                            let config = create_config(shard_dir, page_size);
                            let shard_wal = ShardWal::open(
                                config,
                                ValidatedNodeStatus::create_standalone(),
                                StubReplicationClient,
                                StubS3Downloader,
                            )
                            .await
                            .unwrap();

                            let mut total = Duration::ZERO;
                            for _ in 0..iters {
                                let request = ListAggregatesRequest {
                                    correlation_id: None,
                                    shard_id: SHARD_ID as u64,
                                    org_id: None,
                                    aggregate_type_id: None,
                                    cursor: None,
                                };
                                let start = std::time::Instant::now();
                                let resp = shard_wal.list_aggregates(request).await.unwrap();
                                total += start.elapsed();
                                black_box(&resp);
                            }
                            total
                        })
                        .unwrap()
                        .join()
                        .unwrap()
                });
            });
        }

        // Largest cardinality only: cheap extra arms for the sibling list paths.
        if num_aggregates == cardinality_configs().last().map(|(_, m)| *m).unwrap_or(0) {
            let shard_dir_orgs = shard_dir.clone();
            group.bench_function(BenchmarkId::new("list_orgs", &card_name), move |b| {
                b.iter_custom(|iters| {
                    let shard_dir = shard_dir_orgs.clone();
                    LocalExecutorBuilder::new(Placement::Fixed(0))
                        .spawn(move || async move {
                            let config = create_config(shard_dir, 20_000);
                            let shard_wal = ShardWal::open(
                                config,
                                ValidatedNodeStatus::create_standalone(),
                                StubReplicationClient,
                                StubS3Downloader,
                            )
                            .await
                            .unwrap();
                            let mut total = Duration::ZERO;
                            for _ in 0..iters {
                                let request = ListOrgsRequest {
                                    correlation_id: None,
                                    shard_id: SHARD_ID as u64,
                                    cursor: None,
                                };
                                let start = std::time::Instant::now();
                                let resp = shard_wal.list_orgs(request).await.unwrap();
                                total += start.elapsed();
                                black_box(&resp);
                            }
                            total
                        })
                        .unwrap()
                        .join()
                        .unwrap()
                });
            });

            let shard_dir_types = shard_dir.clone();
            group.bench_function(BenchmarkId::new("list_aggregate_types", &card_name), move |b| {
                b.iter_custom(|iters| {
                    let shard_dir = shard_dir_types.clone();
                    LocalExecutorBuilder::new(Placement::Fixed(0))
                        .spawn(move || async move {
                            let config = create_config(shard_dir, 20_000);
                            let shard_wal = ShardWal::open(
                                config,
                                ValidatedNodeStatus::create_standalone(),
                                StubReplicationClient,
                                StubS3Downloader,
                            )
                            .await
                            .unwrap();
                            let mut total = Duration::ZERO;
                            for _ in 0..iters {
                                let request = ListAggregateTypesRequest {
                                    correlation_id: None,
                                    shard_id: SHARD_ID as u64,
                                    org_id: None,
                                    cursor: None,
                                };
                                let start = std::time::Instant::now();
                                let resp = shard_wal.list_aggregate_types(request).await.unwrap();
                                total += start.elapsed();
                                black_box(&resp);
                            }
                            total
                        })
                        .unwrap()
                        .join()
                        .unwrap()
                });
            });
        }
    }

    group.finish();
}
