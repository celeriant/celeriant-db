use std::{sync::Arc, time::Duration};
use std::hint::black_box;

use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::{
    compression_type::CompressionType,
    wal::{
        event_batch_item::EventBatchItem,
        event_batch_metadata::{EventBatchMetadata, EventTypesData},
        event_item::EventItem,
    },
};
use celeriant_wire::wire_format::{
    from_wire_format_variable, from_wire_format_variable_msgpack, to_wire_format_variable,
    to_wire_format_variable_msgpack,
};
use criterion::{
    criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};

criterion_group!(
    benches,
    bench_event_batch_serialization,
    bench_metadata_serialization
);
criterion_main!(benches);

fn create_event(index: u64, payload_size: usize) -> EventItem {
    EventItem {
        client_event_index: index,
        event_index: index * 2,
        event_id: Some(index as u128),
        event_timestamp: 1700000000000 + index,
        event_type_major: 1,
        event_type_minor: 0,
        event_value: Arc::new(vec![0xAB; payload_size]),
        iv: None,
    }
}

fn create_event_batch(event_count: usize, payload_size: usize) -> EventBatchItem {
    let events: Vec<EventItem> = (0..event_count)
        .map(|i| create_event(i as u64, payload_size))
        .collect();

    EventBatchItem {
        event_batch_index: 42,
        server_timestamp: 1700000000000,
        client_id: 0x123456789ABCDEF0,
        user_id: Some(0xFEDCBA9876543210),
        node_id: 0x1111222233334444,
        lease_index: 100,
        events,
    }
}

fn create_metadata() -> EventBatchMetadata {
    EventBatchMetadata {
        aggregate_key: AggregateKey::new(1, 2, 3),
        uncompressed_size: 4096,
        event_types_data: EventTypesData::Direct([1, 2, 3, 4]),
        event_batch_index: 42,
        client_id: 0x123456789ABCDEF0,
        user_id: 0xFEDCBA9876543210,
        node_id: 0x1111222233334444,
        lease_index: 100,
        server_timestamp: 1700000000000,
        compressed_size: 2048,
        compression_type: 1,
        events_crc: 0xDEADBEEF,
        min_client_event_index: 0,
        max_client_event_index: 99,
        min_event_timestamp: 1700000000000,
        max_event_timestamp: 1700000099000,
        min_event_index: 0,
        max_event_index: 198,
    }
}

fn all_compression_types() -> Vec<(&'static str, CompressionType)> {
    vec![
        ("none", CompressionType::None),
        ("zstd_3", CompressionType::Zstd { level: 3 }),
        ("snappy", CompressionType::Snappy),
        ("brotli_4", CompressionType::Brotli { level: 4 }),
        ("gzip_6", CompressionType::Gzip { level: 6 }),
    ]
}

/// Batch configurations: (name, event_count, payload_size_per_event)
fn batch_configs() -> Vec<(&'static str, usize, usize)> {
    vec![
        ("small_1x64", 1, 64),
        ("medium_10x256", 10, 256),
        ("large_100x1024", 100, 1024),
        ("xlarge_500x2048", 500, 2048),
    ]
}

fn bench_event_batch_serialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("event_batch");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(2));
    group.warm_up_time(Duration::from_millis(500));

    for (batch_name, event_count, payload_size) in batch_configs() {
        let batch = create_event_batch(event_count, payload_size);
        let total_bytes = event_count * payload_size;

        for (comp_name, compression) in all_compression_types() {
            // Bincode serialization
            group.throughput(Throughput::Bytes(total_bytes as u64));
            group.bench_with_input(
                BenchmarkId::new(
                    format!("bincode/{}", comp_name),
                    batch_name,
                ),
                &(&batch, compression),
                |b, (batch, comp)| {
                    b.iter(|| {
                        let (uncompressed_size, encoded) =
                            to_wire_format_variable(black_box(*batch), *comp).unwrap();
                        let _decoded: EventBatchItem =
                            from_wire_format_variable(&encoded, *comp, uncompressed_size).unwrap();
                    });
                },
            );

            // MessagePack serialization
            group.throughput(Throughput::Bytes(total_bytes as u64));
            group.bench_with_input(
                BenchmarkId::new(
                    format!("msgpack/{}", comp_name),
                    batch_name,
                ),
                &(&batch, compression),
                |b, (batch, comp)| {
                    b.iter(|| {
                        let (uncompressed_size, encoded) =
                            to_wire_format_variable_msgpack(black_box(*batch), *comp).unwrap();
                        let _decoded: EventBatchItem =
                            from_wire_format_variable_msgpack(&encoded, *comp, uncompressed_size)
                                .unwrap();
                    });
                },
            );
        }
    }

    group.finish();
}

fn bench_metadata_serialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("metadata");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(2));
    group.warm_up_time(Duration::from_millis(500));
    
    let metadata = create_metadata();

    for (comp_name, compression) in all_compression_types() {
        // Bincode serialization
        group.bench_with_input(
            BenchmarkId::new("bincode", comp_name),
            &(&metadata, compression),
            |b, (meta, comp)| {
                b.iter(|| {
                    let (uncompressed_size, encoded) =
                        to_wire_format_variable(black_box(*meta), *comp).unwrap();
                    let _decoded: EventBatchMetadata =
                        from_wire_format_variable(&encoded, *comp, uncompressed_size).unwrap();
                });
            },
        );

        // MessagePack serialization
        group.bench_with_input(
            BenchmarkId::new("msgpack", comp_name),
            &(&metadata, compression),
            |b, (meta, comp)| {
                b.iter(|| {
                    let (uncompressed_size, encoded) =
                        to_wire_format_variable_msgpack(black_box(*meta), *comp).unwrap();
                    let _decoded: EventBatchMetadata =
                        from_wire_format_variable_msgpack(&encoded, *comp, uncompressed_size)
                            .unwrap();
                });
            },
        );
    }

    group.finish();
}