use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use eventplanedb_core::files::read_objects::{
    read_fixed_records_visit_const, read_objects, read_objects_absolute, AbsoluteObjectPosition,
};
use glommio::io::DmaFile;
use glommio::{
    channels::channel_mesh::{Full, MeshBuilder},
    enclose, CpuSet, GlommioError, LocalExecutorBuilder, LocalExecutorPoolBuilder, Placement,
    PoolPlacement,
};
use std::fs::File;
use std::hint::black_box;
use std::io::{Seek, SeekFrom, Write};
use std::sync::Arc;
use std::time::Duration;
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

// Run N executors, each with its own independent file.
// This measures total throughput across all executors.
fn bench_read_objects_multi_executors(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_objects_multi_executors");
    group
        .sample_size(10)
        .measurement_time(Duration::from_secs(15))
        .warm_up_time(Duration::from_secs(5));

    let nbr_shards = num_cpus::get();
    let online_cpus = CpuSet::online().ok();
    
    let tempdir = tempdir().unwrap();
    let folder = tempdir.path().to_str().unwrap();

    // Match TCP server style: one executor per shard using MaxSpread
    let executors = nbr_shards;
    let glommio_tasks_per_executor: usize = 8;

    let object_count = 10_000;
    let object_size = 1024; // 1KB
    let object_sizes = vec![object_size; object_count];
    let total_bytes_per_file = (object_count * object_size) as u64;
    let chunk_size = 1024 * 1024; // 1MB

    // Pre-create one file per executor/shard
    let mut file_paths: Vec<String> = Vec::with_capacity(executors);
    let mut positions_list = Vec::with_capacity(executors);
    for _ in 0..executors {
        let (file_path, starts, _ends) = create_test_file(folder, &object_sizes);
        file_paths.push(file_path);
        positions_list.push(Arc::<[u64]>::from(starts.into_boxed_slice()));
    }

    let file_paths = Arc::new(file_paths);
    let positions_list = Arc::new(positions_list);

    group.throughput(Throughput::Bytes(
        total_bytes_per_file * (glommio_tasks_per_executor as u64) * (executors as u64),
    ));

    group.bench_function(
        format!("{}exec_x_{}tasks_each_10k_1KB", executors, glommio_tasks_per_executor),
        |b| {
            b.iter(|| {
                // Build a pool spread across shards and run the work on all of them
                LocalExecutorPoolBuilder::new(PoolPlacement::MaxSpread(
                    executors,
                    online_cpus.clone(),
                ))
                .on_all_shards({
                    let file_paths = file_paths.clone();
                    let positions_list = positions_list.clone();
                    move || {
                        let file_paths = file_paths.clone();
                        let positions_list = positions_list.clone();
                        async move {
                            let executor_id = glommio::executor().id() % file_paths.len();;
                            let file_path = file_paths[executor_id].clone();
                            let positions = positions_list[executor_id].clone();

                            let file: DmaFile = DmaFile::open(&file_path).await.unwrap();

                            let mut handles = Vec::with_capacity(glommio_tasks_per_executor);
                            for _ in 0..glommio_tasks_per_executor {
                                let file = file.dup().unwrap();
                                let positions = positions.clone();
                                handles.push(glommio::spawn_local(async move {
                                    let objects = read_objects(
                                        &file,
                                        black_box(positions.as_ref()),
                                        chunk_size,
                                    )
                                    .await
                                    .unwrap();
                                    black_box(objects);
                                }));
                            }

                            for h in handles {
                                h.await;
                            }
                        }
                    }
                })
                .unwrap()
                .join_all();
            });
        },
    );

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
                let file: DmaFile = DmaFile::open(&dense_file_path).await.unwrap();
                let objects = read_objects_absolute(
                    &file,
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
                let file: DmaFile = DmaFile::open(&sparse_file_path).await.unwrap();
                let objects = read_objects_absolute(
                    &file,
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
                let file: DmaFile = DmaFile::open(&file_path).await.unwrap();
                let count = read_fixed_records_visit_const::<RECORD_SIZE, ()>(
                    &file,
                    0,
                    None,
                    chunk_size,
                    |rec| {
                        black_box(rec);
                        Ok(())
                    },
                )
                .await.unwrap();
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
    bench_read_objects_absolute,
    bench_read_fixed_records,
    bench_read_objects_multi_executors
);
criterion_main!(benches);