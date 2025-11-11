
use std::hint::black_box;
use std::time::Duration;

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use criterion::{criterion_group, criterion_main};
use eventplanedb_core::files::read_objects::AbsoluteObjectPosition;
use eventplanedb_core::files::read_objects::read_objects_absolute;
use eventplanedb_core::files::read_objects::test::create_event_batch_file;
use eventplanedb_core::files::read_objects::test::object_sizes;
use glommio::CpuSet;
use glommio::LocalExecutorPoolBuilder;
use glommio::PoolPlacement;
use glommio::io::DmaFile;
use tempfile::tempdir;

use eventplanedb_core::files::read_objects::{
    read_objects
};

criterion_group!(benches, benchmark_read_objects_chunk_sizes, benchmark_read_objects_different_lengths);

criterion_main!(benches);

fn benchmark_read_objects_chunk_sizes(c: &mut Criterion) {

    let mut group = c.benchmark_group("read_objects_chunk_sizes");
    
    group
        .sample_size(10)
        .measurement_time(Duration::from_secs(15))
        .warm_up_time(Duration::from_secs(5));

    let nbr_shards = num_cpus::get();
    let online_cpus = CpuSet::online().ok();
    
    let tempdir = tempdir().unwrap();
    let folder = tempdir.path().to_str().unwrap();

    let total_bytes_per_file = 1024 * 1024 * 13; //13MB
    let glommio_tasks_per_executor: usize = 4;

    // Use kb_sizes only
    let label = "kb_sizes";
    let size_vec = object_sizes(9 * 1024, 9, total_bytes_per_file);

    // Test different chunk sizes
    let chunk_sizes: Vec<u64> = vec![
        // 512,                // 512 bytes (minimum)
        // 4 * 1024,          // 4 KB
        8 * 1024,          // 8 KB
        16 * 1024,         // 16 KB
        32 * 1024,         // 32 KB
        64 * 1024,         // 64 KB
        // 128 * 1024,        // 128 KB
        // 256 * 1024,        // 256 KB
        // 512 * 1024,        // 512 KB
        // 1024 * 1024,       // 1 MB
        // 2 * 1024 * 1024,   // 2 MB
        // 8 * 1024 * 1024,   // 8 MB
        // 15 * 1024 * 1024,   // 15 MB
    ];

    group.throughput(Throughput::Bytes(
        (total_bytes_per_file as u64) * (glommio_tasks_per_executor as u64) * (nbr_shards as u64),
    ));

    // Create the files before benchmarks start
    let file_path = format!("{}/{}_event_batches.bin", folder, label);
    let (start_positions, end_positions) = create_event_batch_file(&file_path, &size_vec);

    let object_positions: Vec<AbsoluteObjectPosition> = start_positions
        .into_iter()
        .zip(end_positions)
        .map(|(start_pos, end_pos)| AbsoluteObjectPosition {
            start_pos,
            end_pos,
        })
        .collect();

    // Create independent files for each task/executor combo
    for shard_nbr in 0..nbr_shards {
        for task_nbr in 0..glommio_tasks_per_executor {
            let copy_path = format!("{}/{}_{}_{}_event_batches.bin", folder, label, shard_nbr, task_nbr);
            std::fs::copy(&file_path, &copy_path).expect("Failed to copy file");
        }
    }

    for &chunk_size in chunk_sizes.iter() {
        let chunk_label = if chunk_size < 1024 {
            format!("{}b", chunk_size)
        } else if chunk_size < 1024 * 1024 {
            format!("{}kb", chunk_size / 1024)
        } else {
            format!("{}mb", chunk_size / (1024 * 1024))
        };

        group.bench_with_input(
            BenchmarkId::new("chunk_size", chunk_label), 
            &object_positions, 
            |b, object_positions| {
                b.iter(|| execute_read_objects_different_lengths(
                    black_box(chunk_size),
                    black_box(folder),
                    black_box(label),
                    black_box(&object_positions), 
                    black_box(glommio_tasks_per_executor),
                    black_box(nbr_shards),
                    black_box(&online_cpus),
                ));
            });
    }

    group.finish();
}

fn benchmark_read_objects_different_lengths(c: &mut Criterion) {

    let mut group = c.benchmark_group("read_objects");
    
    group.save_baseline = Some("main".to_string());

    group
        .sample_size(10)
        .measurement_time(Duration::from_secs(15))
        .warm_up_time(Duration::from_secs(5));

    let nbr_shards = num_cpus::get();
    let online_cpus = CpuSet::online().ok();
    
    let tempdir = tempdir().unwrap();
    let folder = tempdir.path().to_str().unwrap();

    let total_bytes_per_file = 1024 * 1024 * 13; //13MB
    let glommio_tasks_per_executor: usize = 4;

    let sizes: Vec<(String, Vec<usize>)> =
        vec![
            ("by_sizes".to_string(), object_sizes(173, 0, total_bytes_per_file)),           //Byte size
            ("kb_sizes".to_string(), object_sizes(1024, 1, total_bytes_per_file)),          //KB size
            ("mb_sizes".to_string(), object_sizes(1024*1024, 1, total_bytes_per_file)),     //MB size
            // ("512_size_fixed".to_string(), object_sizes(512, 0, total_bytes_per_file)),     //512 bytes - nvme alignment size
            // ("mb_size_fixed".to_string(), object_sizes(1024*1024, 0, total_bytes_per_file)),//1024*1024 bytes - nvme alignment size multiple   
        ];

    group.throughput(Throughput::Bytes(
        (total_bytes_per_file as u64) * (glommio_tasks_per_executor as u64) * (nbr_shards as u64),
    ));

    for (label, size_vec) in sizes.iter() {

        // Create the files before benchmarks start to avoid including their costs
        let file_path = format!("{}/{}_event_batches.bin", folder, label);
        let (start_positions, end_positions) = create_event_batch_file(&file_path, &size_vec);

        let object_positions: Vec<AbsoluteObjectPosition> = start_positions
            .into_iter()
            .zip(end_positions)
            .map(|(start_pos, end_pos)| AbsoluteObjectPosition {
                start_pos,
                end_pos,
            })
            .collect();

        // Create independant files for each task/executor combo
        for shard_nbr in 0..nbr_shards {
            for task_nbr in 0..glommio_tasks_per_executor {
                let copy_path = format!("{}/{}_{}_{}_event_batches.bin", folder, label, shard_nbr, task_nbr);
                std::fs::copy(&file_path, &copy_path).expect("Failed to copy file");
            }
        }

        group.bench_with_input(
            BenchmarkId::new("size", label), 
            &object_positions, 
            |b, object_positions| {
                b.iter(|| execute_read_objects_different_lengths(
                    black_box(32 * 1024), //32KB is optimal based on testing with benchmark_read_objects_chunk_sizes
                    black_box(folder),
                    black_box(label),
                    black_box(&object_positions), 
                    black_box(glommio_tasks_per_executor),
                    black_box(nbr_shards),
                    black_box(&online_cpus),
                ));
            });
    }

    group.finish();
}

fn execute_read_objects_different_lengths(max_chunk_size: u64, folder: &str, label: &str, object_positions: &Vec<AbsoluteObjectPosition>, glommio_tasks_per_executor: usize, nbr_shards: usize, online_cpus: &Option<CpuSet>) {
    
    // Convert borrowed references to owned values before moving into closures
    let folder = folder.to_string();
    let label = label.to_string();
    let object_positions = object_positions.clone();

    LocalExecutorPoolBuilder::new(PoolPlacement::MaxSpread(
        nbr_shards,
        online_cpus.clone(),
    ))
    .on_all_shards({
        move || {
            async move {
                let shard_nbr = glommio::executor().id() % nbr_shards;

                let mut handles = Vec::with_capacity(glommio_tasks_per_executor);
                for task_nbr in 0..glommio_tasks_per_executor {

                    let folder = folder.clone();
                    let label = label.clone();
                    let object_positions = object_positions.clone();

                    handles.push(glommio::spawn_local(async move {

                        let file_path = format!("{}/{}_{}_{}_event_batches.bin", folder, label, shard_nbr, task_nbr);
                        let file: DmaFile = DmaFile::open(&file_path).await.unwrap();

                        let objects = read_objects_absolute(
                            &file,
                            black_box(object_positions.as_ref()),
                            max_chunk_size,
                        )
                        .await
                        .unwrap();
                    
                        black_box(objects);

                        file.close().await.unwrap();
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
    
}