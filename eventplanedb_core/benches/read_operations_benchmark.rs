use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use eventplanedb_core::files::read_operations::{
    read_fixed_records_visit_const, read_objects, read_objects_absolute, AbsoluteObjectPosition,
};
use glommio::{GlommioError, LocalExecutorBuilder, Placement};
use std::fs::File;
use std::hint::black_box;
use std::io::{Seek, SeekFrom, Write};
use tempfile::tempdir;

// Helper function to create a test file with variable-sized objects.
fn create_test_file(path: &str, object_sizes: &[usize]) -> (String, Vec<u64>, Vec<u64>) {
    let file_path = format!("{}/testfile.bin", path);
    let mut file = File::create(&file_path).unwrap();
    let mut start_positions = Vec::with_capacity(object_sizes.len());
    let mut end_positions = Vec::with_capacity(object_sizes.len());
    
    let mut pos = 0u64;

    for (i, &size) in object_sizes.iter().enumerate() {
        start_positions.push(pos);
        end_positions.push(pos + size as u64);
        let byte = (i % 256) as u8;
        let buf = vec![byte; size];
        file.write_all(&buf).unwrap();
        pos += size as u64;
    }
    file.flush().unwrap();
    (file_path, start_positions, end_positions)
}

// Helper function to create a file with fixed-sized records.
fn create_fixed_record_file(
    path: &str,
    record_size: usize,
    record_count: usize,
) -> (String, u64) {
    let file_path = format!("{}/fixed_records.bin", path);
    let mut file = File::create(&file_path).unwrap();
    for i in 0..record_count {
        let byte = (i % 256) as u8;
        let buf = vec![byte; record_size];
        file.write_all(&buf).unwrap();
    }
    file.flush().unwrap();
    let file_size = (record_size as u64) * (record_count as u64);
    (file_path, file_size)
}

fn bench_read_objects(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_objects");
    let tempdir = tempdir().unwrap();
    let folder = tempdir.path().to_str().unwrap();

    let object_count = 10_000;
    let object_size = 1024; // 1KB
    let object_sizes = vec![object_size; object_count];
    let total_bytes = (object_count * object_size) as u64;

    let (file_path, start_positions, _end_positions) = create_test_file(folder, &object_sizes);

    group.throughput(Throughput::Bytes(total_bytes));

    let chunk_size = 1024 * 1024; // 1MB

    group.bench_function("10k_1KB_objects", |b| {
        b.iter(|| {
            let file_path = file_path.clone();
            let start_positions = start_positions.clone();
            let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(move || async move {
                let objects =
                    read_objects(&file_path, black_box(&start_positions), chunk_size).await?;
                black_box(objects);
                Ok::<(), GlommioError<()>>(())
            }).unwrap();
            let _ = handle.join().unwrap();
        });
    });

    group.finish();
}

fn bench_read_objects_absolute(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_objects_absolute");
    let tempdir = tempdir().unwrap();
    let folder = tempdir.path().to_str().unwrap();
    let chunk_size = 1024 * 1024; // 1MB

    // Scenario 1: Dense (contiguous) objects
    let dense_object_count = 10_000;
    let dense_object_size = 1024; // 1KB
    let dense_object_sizes = vec![dense_object_size; dense_object_count];
    let dense_total_bytes = (dense_object_count * dense_object_size) as u64;

    let (dense_file_path, dense_starts, dense_ends) =
        create_test_file(folder, &dense_object_sizes);
    let dense_positions: Vec<_> = dense_starts
        .iter()
        .zip(dense_ends.iter())
        .map(|(&start_pos, &end_pos)| AbsoluteObjectPosition { start_pos, end_pos })
        .collect();

    group.throughput(Throughput::Bytes(dense_total_bytes));

    group.bench_function("dense_10k_1KB_objects", |b| {
        b.iter(|| {
            let dense_file_path = dense_file_path.clone();
            let dense_positions = dense_positions.clone();
            let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(move || async move {
                let objects = read_objects_absolute(
                    &dense_file_path,
                    black_box(&dense_positions),
                    chunk_size,
                )
                .await?;
                black_box(objects);
                Ok::<(), GlommioError<()>>(())
            }).unwrap();
            let _ = handle.join().unwrap();
        });
    });

    // Scenario 2: Sparse objects (with gaps)
    let sparse_object_count = 1_000;
    let sparse_object_size = 4 * 1024; // 4KB
    let gap_size = 12 * 1024; // 12KB gap

    let sparse_file_path = format!("{}/sparse_file.bin", folder);
    let mut file = File::create(&sparse_file_path).unwrap();
    let mut sparse_positions = Vec::new();
    let mut pos = 0u64;
    for i in 0..sparse_object_count {
        let start_pos = pos;
        let end_pos = start_pos + sparse_object_size as u64;
        sparse_positions.push(AbsoluteObjectPosition { start_pos, end_pos });

        let byte = (i % 256) as u8;
        let buf = vec![byte; sparse_object_size];
        file.write_all(&buf).unwrap();
        pos = end_pos + gap_size as u64;
        file.seek(SeekFrom::Start(pos)).unwrap();
    }
    file.flush().unwrap();
    let sparse_total_bytes = (sparse_object_count * sparse_object_size) as u64;

    group.throughput(Throughput::Bytes(sparse_total_bytes));
    group.bench_function("sparse_1k_4KB_objects", |b| {
        b.iter(|| {
            let sparse_file_path = sparse_file_path.clone();
            let sparse_positions = sparse_positions.clone();
            let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(move || async move {
                let objects = read_objects_absolute(
                    &sparse_file_path,
                    black_box(&sparse_positions),
                    chunk_size,
                )
                .await?;
                black_box(objects);
                Ok::<(), GlommioError<()>>(())
            }).unwrap();
            let _ = handle.join().unwrap();
        });
    });

    group.finish();
}

fn bench_read_fixed_records(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_fixed_records_visit_const");
    let tempdir = tempdir().unwrap();
    let folder = tempdir.path().to_str().unwrap();

    const RECORD_SIZE: usize = 256;
    let record_count = 400_000; // ~100MB
    let total_bytes = (record_count * RECORD_SIZE) as u64;

    let (file_path, _file_size) = create_fixed_record_file(folder, RECORD_SIZE, record_count);

    group.throughput(Throughput::Bytes(total_bytes));
    let chunk_size = 1024 * 1024; // 1MB

    group.bench_function("100MB_of_256B_records", |b| {
        b.iter(|| {
            let file_path = file_path.clone();
            let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(move || async move {
                let count = read_fixed_records_visit_const::<_, RECORD_SIZE>(
                    &file_path,
                    0,
                    None,
                    chunk_size,
                    |rec| {
                        black_box(rec);
                    },
                )
                .await?;
                black_box(count);
                Ok::<(), GlommioError<()>>(())
            }).unwrap();
            let _ = handle.join().unwrap();
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_read_objects,
    bench_read_objects_absolute,
    bench_read_fixed_records
);
criterion_main!(benches);