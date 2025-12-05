use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use celeriant_aggregate::{
    local_aggregate::LocalAggregate,
    node_config::NodeConfig,
    read_operations::read_structures::AggregateReadConfig,
    write_operations::aggregate_write_config::AggregateWriteConfig,
};
use celeriant_msg::request::{
    read_filters::ReadFilters,
    requests::{ReadRequest, WriteRequest},
};
use celeriant_wal::{
    aggregate_key::AggregateKey,
    compression_type::CompressionType,
    wal::event_item::EventItem,
};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use glommio::{LocalExecutorBuilder, Placement};
use tempfile::tempdir;

criterion_group!(
    benches,
    benchmark_write_fsync_modes,
    benchmark_write_then_read_with_filters,
    benchmark_read_cache_hit_vs_miss,
);
criterion_main!(benches);

// ============================================================================
// Helper Functions
// ============================================================================

fn create_event(index: u64, event_type: u64, payload_size: usize) -> EventItem {
    EventItem {
        client_event_index: index,
        event_index: 0, // Server assigns this
        event_id: Some(index as u128),
        event_timestamp: 1700000000000 + index * 1000,
        event_type_major: event_type,
        event_type_minor: 0,
        event_value: Arc::new(vec![0xAB; payload_size]),
        iv: None,
    }
}

/// Creates events with varied event types (cycling through 8 types)
fn create_events_batch(start_index: u64, event_count: usize, payload_size: usize) -> Vec<EventItem> {
    (0..event_count)
        .map(|i| {
            let idx = start_index + i as u64;
            let event_type = (i % 8) as u64 + 1; // Types 1-8
            create_event(idx, event_type, payload_size)
        })
        .collect()
}

fn create_aggregate_key(suffix: u64) -> AggregateKey {
    AggregateKey::new(1, 1, suffix as u128)
}

fn create_write_request(
    aggregate_key: AggregateKey,
    client_id: u128,
    events: Vec<EventItem>,
    durable_write_delay_us: Option<u64>,
) -> WriteRequest {
    WriteRequest {
        correlation_id: None,
        aggregate_key,
        client_id,
        user_id: Some(42),
        events,
        allow_create: true,
        expected_event_batch_index: None,
        enforce_client_idempotency: false,
        durable_write_with_delay_us: durable_write_delay_us,
        compression_type: CompressionType::Snappy,
    }
}

fn create_read_request(aggregate_key: AggregateKey, filters: ReadFilters) -> ReadRequest {
    ReadRequest {
        correlation_id: None,
        aggregate_key,
        filters,
    }
}

fn create_local_aggregate(
    data_folder: &str,
    max_data_cache_size_bytes: usize,
    async_flush_ms: u64,
) -> LocalAggregate {
    let node_config = NodeConfig {
        data_root_folder: data_folder.to_string(),
        node_id: 1,
        margin_ms: 500,
        lease_expiry_ms: 10000,
        async_flush_ms,
        max_open_aggregates: 1000,
        max_request_size: Some(64 * 1024 * 1024),
        listen_address: "127.0.0.1:0".to_string(),
        max_event_batches_response_size: Some(64 * 1024 * 1024),
        s3_enabled: false,
    };

    let read_config = AggregateReadConfig {
        max_chunk_size: 64 * 1024,
    };

    let write_config = AggregateWriteConfig {
        max_data_cache_size_bytes,
        cache_trim_factor: 2,
        max_chunk_size: 64 * 1024,
    };

    LocalAggregate::new(read_config, write_config, node_config)
}

// ============================================================================
// Benchmark: Write with different fsync modes
// ============================================================================

fn benchmark_write_fsync_modes(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_fsync_modes");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(10));
    group.warm_up_time(Duration::from_secs(2));

    let batch_count = 50;
    let events_per_batch = 20;
    let payload_size = 256;
    let total_bytes = batch_count * events_per_batch * payload_size;

    group.throughput(Throughput::Bytes(total_bytes as u64));

    let fsync_modes: Vec<(&str, Option<u64>)> = vec![
        ("immediate", Some(0)),
        ("delay_100us", Some(100)),
        ("delay_1ms", Some(1000)),
        ("delay_10ms", Some(10000)),
        ("background_async", None),
    ];

    for (mode_name, delay_us) in fsync_modes {
        group.bench_with_input(
            BenchmarkId::new("mode", mode_name),
            &delay_us,
            |b, &delay_us| {
                b.iter(|| {
                    let tempdir = tempdir().unwrap();
                    let data_folder = tempdir.path().to_str().unwrap().to_string();

                    let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
                        .spawn(move || async move {
                            let local_aggregate = create_local_aggregate(&data_folder, 64 * 1024 * 1024, 50);
                            let aggregate_key = create_aggregate_key(1);
                            let client_id = 100u128;

                            for i in 0..batch_count {
                                let start_idx = i as u64 * events_per_batch as u64;
                                let events = create_events_batch(start_idx, events_per_batch, payload_size);
                                let request = create_write_request(
                                    aggregate_key.clone(),
                                    client_id,
                                    events,
                                    delay_us,
                                );

                                let result = local_aggregate.write(1, request).await;
                                black_box(result.unwrap());
                            }
                        })
                        .unwrap();

                    handle.join().unwrap();
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Benchmark: Read with different filter types
// ============================================================================

fn benchmark_write_then_read_with_filters(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_filters");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(10));
    group.warm_up_time(Duration::from_secs(2));

    let batch_count = 100;
    let events_per_batch = 25;
    let payload_size = 512;

    // Filter configurations: (name, filter_builder_fn)
    let filter_configs: Vec<(&str, Box<dyn Fn() -> ReadFilters + Send + Sync>)> = vec![
        ("no_filter", Box::new(|| ReadFilters::new(1))),
        (
            "batch_range_first_half",
            Box::new(|| ReadFilters::new(1).to_event_batch_index(50)),
        ),
        (
            "batch_range_last_quarter",
            Box::new(|| ReadFilters::new(75)),
        ),
        (
            "event_type_single",
            Box::new(|| ReadFilters::new(1).include_event_types(vec![3])),
        ),
        (
            "event_type_multiple",
            Box::new(|| ReadFilters::new(1).include_event_types(vec![1, 3, 5, 7])),
        ),
        (
            "timestamp_range",
            Box::new(|| {
                ReadFilters::new(1)
                    .min_event_timestamp(1700000000000 + 500 * 1000)
                    .max_event_timestamp(1700000000000 + 1500 * 1000)
            }),
        ),
        (
            "client_id_filter",
            Box::new(|| ReadFilters::new(1).include_client_id(100)),
        ),
        (
            "combined_batch_and_type",
            Box::new(|| {
                ReadFilters::new(25)
                    .to_event_batch_index(75)
                    .include_event_types(vec![2, 4, 6])
            }),
        ),
    ];

    for (filter_name, filter_fn) in filter_configs.iter() {
        group.bench_with_input(
            BenchmarkId::new("filter", *filter_name),
            filter_name,
            |b, _| {
                b.iter_custom(|iters| {
                    let mut total_duration = Duration::ZERO;

                    for _ in 0..iters {
                        let tempdir = tempdir().unwrap();
                        let data_folder = tempdir.path().to_str().unwrap().to_string();
                        let filters = filter_fn();

                        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
                            .spawn(move || async move {
                                //Don't use the cache - force disk reads
                                let local_aggregate =
                                    create_local_aggregate(&data_folder, 0, 50);
                                let aggregate_key = create_aggregate_key(1);
                                let client_id = 100u128;

                                // Write phase (not timed)
                                for i in 0..batch_count {
                                    let start_idx = i as u64 * events_per_batch as u64;
                                    let events =
                                        create_events_batch(start_idx, events_per_batch, payload_size);
                                    let request = create_write_request(
                                        aggregate_key.clone(),
                                        client_id,
                                        events,
                                        Some(0),
                                    );

                                    local_aggregate.write(1, request).await.unwrap();
                                }

                                // Only time the read
                                let start = std::time::Instant::now();

                                let read_request =
                                    create_read_request(aggregate_key.clone(), filters);

                                let result = local_aggregate.read(&read_request).await;
                                black_box(result.unwrap());

                                start.elapsed()
                            })
                            .unwrap();

                        total_duration += handle.join().unwrap();
                    }

                    total_duration
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Benchmark: Read cache hit vs miss (writer cache enabled/disabled)
// ============================================================================

fn benchmark_read_cache_hit_vs_miss(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_cache");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(10));
    group.warm_up_time(Duration::from_secs(2));

    let batch_count = 50;
    let events_per_batch = 20;
    let payload_size = 256;

    let cache_configs: Vec<(&str, usize)> = vec![
        ("cache_disabled", 0),
        ("cache_64mb", 64 * 1024 * 1024),
    ];

    for (cache_name, cache_size) in cache_configs {
        group.bench_with_input(
            BenchmarkId::new("config", cache_name),
            &cache_size,
            |b, &cache_size| {
                b.iter_custom(|iters| {
                    let mut total_duration = Duration::ZERO;

                    for _ in 0..iters {
                        let tempdir = tempdir().unwrap();
                        let data_folder = tempdir.path().to_str().unwrap().to_string();

                        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
                            .spawn(move || async move {
                                let local_aggregate =
                                    create_local_aggregate(&data_folder, cache_size, 50);
                                let aggregate_key = create_aggregate_key(1);
                                let client_id = 100u128;

                                // Write phase (not timed)
                                for i in 0..batch_count {
                                    let start_idx = i as u64 * events_per_batch as u64;
                                    let events =
                                        create_events_batch(start_idx, events_per_batch, payload_size);
                                    let request = create_write_request(
                                        aggregate_key.clone(),
                                        client_id,
                                        events,
                                        Some(0),
                                    );

                                    local_aggregate.write(1, request).await.unwrap();
                                }

                                // Only time the reads
                                let start = std::time::Instant::now();

                                for _ in 0..5 {
                                    let filters = ReadFilters::new(1);
                                    let read_request =
                                        create_read_request(aggregate_key.clone(), filters);

                                    let result = local_aggregate.read(&read_request).await;
                                    black_box(result.unwrap());
                                }

                                start.elapsed()
                            })
                            .unwrap();

                        total_duration += handle.join().unwrap();
                    }

                    total_duration
                });
            },
        );
    }

    group.finish();
}