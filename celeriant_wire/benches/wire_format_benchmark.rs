use std::{sync::Arc, time::Duration};
use std::hint::black_box;

use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::datablocks::datablock_aggregate_event_batch::DatablockAggregateEventBatch;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use celeriant_wal::metablocks::metablock_event_batch::{MetablockEventBatch, EventTypesKind};
use celeriant_wal::compression_type::CompressionType;
use celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES;
use celeriant_wire::codec::{
    bincode::{fixed_serialise_heap as bincode_serialise, fixed_deserialise as bincode_deserialise},
    msgpack::{serialise_heap as msgpack_serialise, deserialise as msgpack_deserialise},
    compression::{compress_with_dict, decompress_with_dict},
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

fn create_event(index: u64, payload_size: usize) -> DatablockAggregateEvent {
    DatablockAggregateEvent {
        client_seq: index,
        event_seq: index * 2,
        event_id: Some(index as u128),
        event_timestamp: 1700000000000 + index,
        event_type_major: 1,
        event_type_minor: 0,
        event_value: Arc::new(vec![0xAB; payload_size]),
        iv: None,
    }
}

fn create_event_batch(event_count: usize, payload_size: usize) -> DatablockAggregateEventBatch {
    let events: Vec<DatablockAggregateEvent> = (0..event_count)
        .map(|i| create_event(i as u64, payload_size))
        .collect();

    DatablockAggregateEventBatch {
        aggregate_version: 42,
        events,
    }
}

fn create_metadata() -> MetablockEventBatch {
    MetablockEventBatch {
        aggregate_key: AggregateKey::new(1, 2, 3),
        event_types_data: EventTypesKind::Direct([1, 2, 3, 4]),
        aggregate_version: 42,
        trimmed_below_version: 1,
        client_id: 0x123456789ABCDEF0,
        user_id: Some(0xFEDCBA9876543210),
        min_client_seq: 0,
        max_client_seq: 99,
        min_event_timestamp: 1700000000000,
        max_event_timestamp: 1700000099000,
        min_event_seq: 0,
        max_event_seq: 198,
    }
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

    let dict = BUILTIN_DICT_BYTES;

    for (batch_name, event_count, payload_size) in batch_configs() {
        let batch = create_event_batch(event_count, payload_size);
        let total_bytes = event_count * payload_size;

        for (comp_name, compression) in [("none", CompressionType::None), ("zstd_dict_3", CompressionType::ZstdDict)] {
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
                        let serialised = bincode_serialise(black_box(*batch)).unwrap();
                        let uncompressed_size = serialised.len();
                        let (compressed, decompressed) = match comp {
                            CompressionType::None => {
                                let c = serialised.clone();
                                let d = c.clone();
                                (c, d)
                            }
                            CompressionType::ZstdDict => {
                                let c = compress_with_dict(&serialised, 3, dict).unwrap();
                                let d = decompress_with_dict(&c, uncompressed_size, dict).unwrap();
                                (c, d)
                            }
                        };
                        let _decoded: DatablockAggregateEventBatch = bincode_deserialise(&decompressed).unwrap();
                        black_box(compressed);
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
                        let serialised = msgpack_serialise(black_box(*batch)).unwrap();
                        let uncompressed_size = serialised.len();
                        let (compressed, decompressed) = match comp {
                            CompressionType::None => {
                                let c = serialised.clone();
                                let d = c.clone();
                                (c, d)
                            }
                            CompressionType::ZstdDict => {
                                let c = compress_with_dict(&serialised, 3, dict).unwrap();
                                let d = decompress_with_dict(&c, uncompressed_size, dict).unwrap();
                                (c, d)
                            }
                        };
                        let _decoded: DatablockAggregateEventBatch = msgpack_deserialise(&decompressed).unwrap();
                        black_box(compressed);
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

    for (comp_name, compression) in [("none", CompressionType::None), ("zstd_dict_3", CompressionType::ZstdDict)] {
        let dict = BUILTIN_DICT_BYTES;
        group.bench_with_input(
            BenchmarkId::new("bincode", comp_name),
            &(&metadata, compression),
            |b, (meta, comp)| {
                b.iter(|| {
                    let serialised = bincode_serialise(black_box(*meta)).unwrap();
                    let uncompressed_size = serialised.len();
                    let (compressed, decompressed) = match comp {
                        CompressionType::None => {
                            let c = serialised.clone();
                            let d = c.clone();
                            (c, d)
                        }
                        CompressionType::ZstdDict => {
                            let c = compress_with_dict(&serialised, 3, dict).unwrap();
                            let d = decompress_with_dict(&c, uncompressed_size, dict).unwrap();
                            (c, d)
                        }
                    };
                    let _decoded: MetablockEventBatch = bincode_deserialise(&decompressed).unwrap();
                    black_box(compressed);
                });
            },
        );
    }

    group.finish();
}
