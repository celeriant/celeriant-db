use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use event_storage::event_batch_item::{EventBatchItem};
use event_storage::event_item::EventItem;
use event_storage::event_storage_cache::EventStorageCache;
use std::fs;
use tempfile::TempDir;
use rand::prelude::*;

// Include the random_event_item function from the other benchmark file
fn random_event_item(array_size: i32) -> EventItem {

    // Create dummy storage data with configurable array sizes
    let mut dummy_storage = EventItem::new();
    let mut rng = rand::thread_rng();

    dummy_storage.ed = rng.r#gen::<u64>();
    dummy_storage.iv = Some("kjldsfsdfasdfasdfasfawserfwfdsafdasdfas".to_string());
    dummy_storage.tp = rng.r#gen::<u16>() as u64;

    dummy_storage.int_values = Some((0..array_size).map(|_| rng.r#gen::<i64>()).collect());

    dummy_storage.f32_values = Some((0..array_size).map(|_| rng.r#gen::<f32>()).collect());

    dummy_storage.f64_values = Some((0..array_size).map(|_| rng.r#gen::<f64>()).collect());

    dummy_storage.bool_values = Some(
        (0..array_size)
            .map(|_| rng.r#gen::<bool>())
            .collect(),
    );

    dummy_storage.uint_values = Some((0..array_size).map(|_| rng.r#gen::<u64>()).collect());
    
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
                let size = rng.gen_range(1..=320);
                Some((0..size).map(|_| rng.r#gen::<u8>()).collect())
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
// Utility function to create EventBatchItem
fn create_event_batch_item(
    si: u64,
    cb: Option<String>,
    sd: u64,
    events: Vec<EventItem>,
) -> EventBatchItem {
    EventBatchItem { si, cb, sd, events }
}

fn benchmark_storage_cache_ops(c: &mut Criterion) {
    let mut group = c.benchmark_group("event_storage_cache_ops");
    let array_size = 100;
    let event_counts = vec![100, 500, 1000];

    for event_count in &event_counts {
        group.throughput(Throughput::Elements(*event_count as u64));
        group.bench_with_input(
            BenchmarkId::new("write_read_delete", format!("{}events", event_count)),
            event_count,
            |b, &event_count| {
                b.iter(|| {
                    let temp_dir = TempDir::new().expect("Failed to create temp directory");
                    let temp_path = temp_dir.path();
                    let events_bin = temp_path.join("events_cache.bin");
                    let file_path = events_bin.to_str().unwrap();

                    let mut storage = EventStorageCache::new(30, 1000000, 10000);

                    // Generate events
                    let events_batch: Vec<EventItem> = (0..event_count)
                        .map(|_| random_event_item(array_size))
                        .collect();
                    let event_batch_item = create_event_batch_item(0, None, 0, events_batch);

                    // Write events
                    storage.write(file_path, true, event_batch_item).expect("Write events");

                    // Read events
                    storage.read(file_path, 0, usize::MAX, None).expect("Read events");

                    // Delete file
                    storage.delete(file_path).expect("Delete file");
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("write_read_combined", format!("{}events", event_count)),
            event_count,
            |b, &event_count| {
                b.iter(|| {
                    let temp_dir = TempDir::new().expect("Failed to create temp directory");
                    let temp_path = temp_dir.path();
                    let events_bin = temp_path.join("events_cache_combined.bin");
                    let file_path = events_bin.to_str().unwrap();

                    let mut storage = EventStorageCache::new(30, 1000000, 10000);

                    // Generate events
                    let events_batch: Vec<EventItem> = (0..event_count)
                        .map(|_| random_event_item(array_size))
                        .collect();
                    let event_batch_item = create_event_batch_item(0, None, 0, events_batch);

                    // Write events
                    storage.write(file_path, true, event_batch_item).expect("Write events");

                    // Read events immediately after writing
                    storage.read(file_path, 0, usize::MAX, None).expect("Read events");
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, benchmark_storage_cache_ops);
criterion_main!(benches);