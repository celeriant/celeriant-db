use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use event_storage::event_batch_item::{EventBatchItem};
use event_storage::event_item::EventItem;
use event_storage::file_cache::create_append_writer;
use event_storage::wire_format::{compress_data, decompress_data, deserialize_event_batch_item, serialize_event_batch_item};
use std::fs;
use std::io::Write;
use tempfile::TempDir;

fn random_event_item(array_size: i32) -> EventItem {
    use rand::Rng;

    // Create dummy storage data with configurable array sizes
    let mut dummy_storage = EventItem::new();
    let mut rng = rand::rng();

    dummy_storage.ed = rng.random::<u64>();
    dummy_storage.iv = Some("kjldsfsdfasdfasdfasfawserfwfdsafdasdfas".to_string());
    dummy_storage.tp = rng.random::<u16>() as u64;

    dummy_storage.int_values = Some((0..array_size).map(|_| rng.random::<i64>()).collect());

    dummy_storage.f32_values = Some((0..array_size).map(|_| rng.random::<f32>()).collect());

    dummy_storage.f64_values = Some((0..array_size).map(|_| rng.random::<f64>()).collect());

    dummy_storage.bool_values = Some(
        (0..array_size)
            .map(|_| rng.random::<bool>())
            .collect(),
    );

    dummy_storage.uint_values = Some((0..array_size).map(|_| rng.random::<u64>()).collect());
    
    // Generate random strings using nanoid
    dummy_storage.string_values = Some((0..array_size).map(|_| Some(nanoid::nanoid!())).collect());
    // Null out positions 10, 11, 13
    if let Some(ref mut vec) = dummy_storage.string_values {
        if vec.len() > 10 {
            vec[10] = None;
        }
        if vec.len() > 11 {
            vec[11] = None;
        }
        if vec.len() > 13 {
            vec[13] = None;
        }
    }

    // Generate byte arrays with random sizes and content
    dummy_storage.byte_arrays = Some(
        (0..array_size)
            .map(|_| {
                let size = rng.random_range(1..=320);
                Some((0..size).map(|_| rng.random::<u8>()).collect())
            })
            .collect(),
    );
    // Null out positions 10, 11, 13
    if let Some(ref mut vec) = dummy_storage.byte_arrays {
        if vec.len() > 10 {
            vec[10] = None;
        }
        if vec.len() > 11 {
            vec[11] = None;
        }
        if vec.len() > 13 {
            vec[13] = None;
        }
    }

    dummy_storage
}

fn benchmark_event_generation(c: &mut Criterion) {
    let mut group = c.benchmark_group("event_generation");

    let array_sizes = vec![100, 500, 1000, 2000];

    for array_size in array_sizes {
        group.bench_with_input(
            BenchmarkId::new("generate_single_event", array_size),
            &array_size,
            |b, &array_size| {
                b.iter(|| {
                    let event = random_event_item(std::hint::black_box(array_size));
                    std::hint::black_box(event);
                });
            },
        );
    }

    group.finish();
}

fn create_event_batch_item(
    si: u64,
    cb: Option<String>,
    sd: u64,
    events: Vec<EventItem>,
) -> EventBatchItem {
    EventBatchItem {
        si,
        cb,
        sd,
        events,
    }
}

fn benchmark_event_item_full_cycle(c: &mut Criterion) {
    let mut group = c.benchmark_group("event_item_full_cycle");

    let test_cases = vec![
        (10, 100),   // 10 events, 100 array size
        (50, 500),   // 50 events, 500 array size
        (100, 1000), // 100 events, 1000 array size
    ];

    for (event_count, array_size) in test_cases {
        // Pre-generate events for consistent size measurements
        let events: Vec<_> = (0..event_count)
            .map(|_| random_event_item(array_size))
            .collect();
        let event_batch_item = create_event_batch_item(32423, Some("lksdflkjdslk".to_string()), 32423423423, events);
        let encoded_events = serialize_event_batch_item(&event_batch_item).expect("Serialize events");
        
        // Set throughput to show bytes processed per second
        group.throughput(Throughput::Bytes(encoded_events.len() as u64));
        
        group.bench_with_input(
            BenchmarkId::new(
                "full_cycle",
                format!("{}events_{}array", 
                       event_count, 
                       array_size,
                ),
            ),
            &(event_count, array_size),
            |b, &(event_count, array_size)| {
                b.iter(|| {
                    let temp_dir = TempDir::new().expect("Failed to create temp directory");
                    let temp_path = temp_dir.path();
                    let events_bin = temp_path.join("benchmark_events.bin");

                    // Generate events
                    let events: Vec<_> = (0..event_count)
                        .map(|_| random_event_item(array_size))
                        .collect();
                    let event_batch_item = create_event_batch_item(32423, Some("lksdflkjdslk".to_string()), 32423423423, events);

                    // Serialize
                    let encoded_events = serialize_event_batch_item(&event_batch_item).expect("Serialize events");

                    // Compress
                    let compressed_events = compress_data(&encoded_events).expect("Compress events");

                    // Write to disk
                    let mut writer = create_append_writer(events_bin.to_str().unwrap())
                        .expect("Open writer to events.bin");
                    writer
                        .write_all(&compressed_events)
                        .expect("write events to bin");
                    writer.flush().expect("flush events bin");

                    // Read from disk
                    let compressed_data = fs::read(&events_bin).expect("Failed to read events.bin");

                    // Decompress
                    let decompressed_data = decompress_data(&compressed_data, encoded_events.len())
                        .expect("Failed to decompress data");

                    // Deserialize
                    let deserialized_events = deserialize_event_batch_item(&decompressed_data)
                        .expect("Failed to deserialize events");

                    std::hint::black_box(deserialized_events);
                });
            },
        );
    }

    group.finish();
}

fn benchmark_serialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("serialization");

    // Pre-generate events for consistent benchmarking and size tracking
    let events_10_100: Vec<_> = (0..10).map(|_| random_event_item(100)).collect();
    let events_50_500: Vec<_> = (0..50).map(|_| random_event_item(500)).collect();
    let events_100_1000: Vec<_> = (0..100).map(|_| random_event_item(1000)).collect();

    // Pre-serialize to get sizes for throughput
    let event_batch_item_events_10_100 = create_event_batch_item(32423, Some("lksdflkjdslk".to_string()), 32423423423, events_10_100);
    let encoded_10_100 = serialize_event_batch_item(&event_batch_item_events_10_100).expect("Serialize events");

    let event_batch_item_events_50_500 = create_event_batch_item(32423, Some("lksdflkjdslk".to_string()), 32423423423, events_50_500);
    let encoded_50_500 = serialize_event_batch_item(&event_batch_item_events_50_500).expect("Serialize events");

    let event_batch_item_events_100_1000 = create_event_batch_item(32423, Some("lksdflkjdslk".to_string()), 32423423423, events_100_1000);
    let encoded_100_1000 = serialize_event_batch_item(&event_batch_item_events_100_1000).expect("Serialize events");

    group.throughput(Throughput::Bytes(encoded_10_100.len() as u64));
    group.bench_function(
        "serialize_10events_100", 
        |b| {
            b.iter(|| {
                let encoded = serialize_event_batch_item(std::hint::black_box(&event_batch_item_events_10_100))
                    .expect("Serialize events");
                std::hint::black_box(encoded);
            });
        }
    );

    group.throughput(Throughput::Bytes(encoded_50_500.len() as u64));
    group.bench_function(
        "serialize_50events_500", 
        |b| {
            b.iter(|| {
                let encoded = serialize_event_batch_item(std::hint::black_box(&event_batch_item_events_50_500))
                    .expect("Serialize events");
                std::hint::black_box(encoded);
            });
        }
    );

    group.throughput(Throughput::Bytes(encoded_100_1000.len() as u64));
    group.bench_function(
        "serialize_100events_1000", 
        |b| {
            b.iter(|| {
                let encoded = serialize_event_batch_item(std::hint::black_box(&event_batch_item_events_100_1000))
                    .expect("Serialize events");
                std::hint::black_box(encoded);
            });
        }
    );

    group.finish();
}

fn benchmark_compression(c: &mut Criterion) {
    let mut group = c.benchmark_group("compression");

    // Pre-generate and serialize events
    let events_10_100: Vec<_> = (0..10).map(|_| random_event_item(100)).collect();
    let events_50_500: Vec<_> = (0..50).map(|_| random_event_item(500)).collect();
    let events_100_1000: Vec<_> = (0..100).map(|_| random_event_item(1000)).collect();

    let event_batch_item_events_10_100 = create_event_batch_item(32423, Some("lksdflkjdslk".to_string()), 32423423423, events_10_100);
    let encoded_10_100 = serialize_event_batch_item(&event_batch_item_events_10_100).expect("Serialize events");

    let event_batch_item_events_50_500 = create_event_batch_item(32423, Some("lksdflkjdslk".to_string()), 32423423423, events_50_500);
    let encoded_50_500 = serialize_event_batch_item(&event_batch_item_events_50_500).expect("Serialize events");

    let event_batch_item_events_100_1000 = create_event_batch_item(32423, Some("lksdflkjdslk".to_string()), 32423423423, events_100_1000);
    let encoded_100_1000 = serialize_event_batch_item(&event_batch_item_events_100_1000).expect("Serialize events");

    group.throughput(Throughput::Bytes(encoded_10_100.len() as u64));
    group.bench_function(
        "compress_10events_100", 
        |b| {
            b.iter(|| {
                let compressed = compress_data(std::hint::black_box(&encoded_10_100))
                    .expect("Compress events");
                std::hint::black_box(compressed);
            });
        }
    );

    group.throughput(Throughput::Bytes(encoded_50_500.len() as u64));
    group.bench_function(
        "compress_50events_500", 
        |b| {
            b.iter(|| {
                let compressed = compress_data(std::hint::black_box(&encoded_50_500))
                    .expect("Compress events");
                std::hint::black_box(compressed);
            });
        }
    );

    group.throughput(Throughput::Bytes(encoded_100_1000.len() as u64));
    group.bench_function(
        "compress_100events_1000", 
        |b| {
            b.iter(|| {
                let compressed = compress_data(std::hint::black_box(&encoded_100_1000))
                    .expect("Compress events");
                std::hint::black_box(compressed);
            });
        }
    );

    group.finish();
}

fn benchmark_decompression(c: &mut Criterion) {
    let mut group = c.benchmark_group("decompression");

    // Pre-generate, serialize, and compress events
    let events_10_100: Vec<_> = (0..10).map(|_| random_event_item(100)).collect();
    let events_50_500: Vec<_> = (0..50).map(|_| random_event_item(500)).collect();
    let events_100_1000: Vec<_> = (0..100).map(|_| random_event_item(1000)).collect();

    let event_batch_item_events_10_100 = create_event_batch_item(32423, Some("lksdflkjdslk".to_string()), 32423423423, events_10_100);
    let encoded_10_100 = serialize_event_batch_item(&event_batch_item_events_10_100).expect("Serialize events");

    let event_batch_item_events_50_500 = create_event_batch_item(32423, Some("lksdflkjdslk".to_string()), 32423423423, events_50_500);
    let encoded_50_500 = serialize_event_batch_item(&event_batch_item_events_50_500).expect("Serialize events");

    let event_batch_item_events_100_1000 = create_event_batch_item(32423, Some("lksdflkjdslk".to_string()), 32423423423, events_100_1000);
    let encoded_100_1000 = serialize_event_batch_item(&event_batch_item_events_100_1000).expect("Serialize events");

    let compressed_10_100 = compress_data(&encoded_10_100).expect("Compress events");
    let compressed_50_500 = compress_data(&encoded_50_500).expect("Compress events");
    let compressed_100_1000 = compress_data(&encoded_100_1000).expect("Compress events");

    group.throughput(Throughput::Bytes(compressed_10_100.len() as u64));
    group.bench_function(
        "decompress_10events_100", 
        |b| {
            b.iter(|| {
                let decompressed = decompress_data(
                    std::hint::black_box(&compressed_10_100),
                    encoded_10_100.len(),
                ).expect("Decompress events");
                std::hint::black_box(decompressed);
            });
        }
    );

    group.throughput(Throughput::Bytes(compressed_50_500.len() as u64));
    group.bench_function(
        "decompress_50events_500array", 
        |b| {
            b.iter(|| {
                let decompressed = decompress_data(
                    std::hint::black_box(&compressed_50_500),
                    encoded_50_500.len(),
                ).expect("Decompress events");
                std::hint::black_box(decompressed);
            });
        }
    );

    group.throughput(Throughput::Bytes(compressed_100_1000.len() as u64));
    group.bench_function(
        "decompress_100events_1000", 
        |b| {
            b.iter(|| {
                let decompressed = decompress_data(
                    std::hint::black_box(&compressed_100_1000),
                    encoded_100_1000.len(),
                ).expect("Decompress events");
                std::hint::black_box(decompressed);
            });
        }
    );

    group.finish();
}

// Add a dedicated size tracking benchmark
fn benchmark_size_metrics(c: &mut Criterion) {
    let mut group = c.benchmark_group("size_metrics");
    
    let test_cases = vec![
        (10, 100),
        (50, 500), 
        (100, 1000),
        (200, 2000), // Add more test cases
    ];

    for (event_count, array_size) in test_cases {       
        group.throughput(Throughput::Elements(event_count as u64));
        group.bench_function(
            &format!("size_{}events_{}",
                    event_count, array_size),
            |b| {
                b.iter(|| {
                    // Just do a minimal operation to measure the "cost" of these sizes
                    let events: Vec<_> = (0..event_count).map(|_| random_event_item(array_size)).collect();
                    let event_batch_item = create_event_batch_item(23423432, None, 3232423432, events);
                    let encoded = serialize_event_batch_item(std::hint::black_box(&event_batch_item)).expect("Serialize");
                    let compressed = compress_data(std::hint::black_box(&encoded)).expect("Compress");
                    std::hint::black_box((encoded.len(), compressed.len()));
                });
            }
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    benchmark_event_item_full_cycle,
    benchmark_event_generation,
    benchmark_serialization,
    benchmark_compression,
    benchmark_decompression,
    benchmark_size_metrics
);
criterion_main!(benches);
