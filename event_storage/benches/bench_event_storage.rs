use std::usize;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use event_storage::event_batch_item::{EventBatchItem};
use event_storage::event_item::EventItem;
use event_storage::event_storage::{append_event_batch, read_from_si};
use event_storage::file_cache::{create_append_writer, create_reader};
use tempfile::TempDir;
use rand::prelude::*;

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

// Include the random_event_item function from the other benchmark file
fn random_event_item(array_size: i32) -> EventItem {

    // Create dummy storage data with configurable array sizes
    let mut dummy_storage = EventItem::new();
    let mut rng = rand::thread_rng();

    dummy_storage.ed = rng.r#gen::<u64>();
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

fn benchmark_event_storage_ops(c: &mut Criterion) {
    let mut group = c.benchmark_group("event_storage_ops");

    let array_size = 100;
    let event_counts = vec![100, 500, 1000]; // Different numbers of events

    for event_count in &event_counts {
        group.throughput(Throughput::Elements(*event_count as u64));
        group.bench_with_input(
            BenchmarkId::new(
                "write_read_catchup",
                format!("{}events", event_count),
            ),
            event_count,
            |b, &event_count| {
                b.iter(|| {
                    let temp_dir = TempDir::new().expect("Failed to create temp directory");
                    let temp_path = temp_dir.path();
                    let events_bin = temp_path.join("events.bin");

                    // Generate events in batches
                    let mut batches: Vec<EventBatchItem> = Vec::new();
                    let batch_size = event_count / 50; // 50 batches
                    for i in 0..50 {
                        let start_index = i * batch_size;
                        let end_index = if i == 49 {
                            event_count
                        } else {
                            (i + 1) * batch_size
                        };

                        let events_batch: Vec<EventItem> = (start_index..end_index)
                            .map(|_| {
                                random_event_item(array_size)
                            })
                            .collect();

                        let event_batch_item = create_event_batch_item(324234234, None, 32423423432, events_batch);
                        batches.push(event_batch_item);
                    }

                    // Write events to file
                    let mut writer = create_append_writer(events_bin.to_str().unwrap())
                        .expect("Open writer to events.bin");
                    
                    // Break down events into batches and append
                    for batch in batches {
                        append_event_batch(&mut writer, &batch).expect("Append event batch");
                    }

                    // Read all events
                    let mut reader = create_reader(events_bin.to_str().unwrap())
                        .expect("Open reader to events.bin");
                    let _all_read_events = read_from_si(&mut reader, 0, usize::MAX, None).expect("Read all events");

                    // Catchup from a specific si
                    let mut reader = create_reader(events_bin.to_str().unwrap())
                        .expect("Open reader to events.bin");
                    let target_si = (event_count / 2) as u64; // Catchup from middle si
                    let _catchup_result = read_from_si(&mut reader, target_si, usize::max_value(), None)
                        .expect("Read from si");
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, benchmark_event_storage_ops);
criterion_main!(benches);