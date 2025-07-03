use std::pin::Pin;
use std::{usize};
use std::time::Instant;
use core_affinity;
use event_storage::event_batch_item::EventBatchItem;
use event_storage::event_item::EventItem;
use event_storage_threads::{create_thread_pool, read_async, write_async};
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

fn create_random_event_batch_item() -> EventBatchItem {
    create_event_batch_item(0, None, 5464564645, vec![
        random_event_item(300),
        random_event_item(300),
        random_event_item(300),
    ])
}

#[tokio::main]
async fn main() {
    println!("Starting event storage thread pool test...");
    
    // Create temporary directory for test files
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let temp_path = temp_dir.path();
    println!("Using temp directory: {}", temp_path.display());
    
    // Create thread pool
    let cores = core_affinity::get_core_ids().unwrap();
    let workers = create_thread_pool(cores.len());
    println!("Created thread pool with {} workers", workers.len());

    // Test concurrent writes - execute all tasks concurrently
    let start = Instant::now();
    let mut write_tasks = Vec::new();
    
    for i in 0..100 {
        let file_path = temp_path.join(format!("test_file_{}.dat", i % 10)).to_string_lossy().to_string();
        let event_batch = create_random_event_batch_item();
        
        let task = write_async(
            &workers,
            file_path,
            true, // allow_create
            event_batch,
        );
        
        write_tasks.push(task);
    }

    // Wait for all writes to complete concurrently
    let write_results = futures::future::join_all(write_tasks).await;
    let successful_writes = write_results.iter().filter(|r| r.is_ok()).count();
    let write_duration = start.elapsed();
    println!("Completed {} writes in {:?}", successful_writes, write_duration);

    // Test concurrent reads - wait a bit to ensure files are written
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    
    let start = Instant::now();
    let mut read_tasks = Vec::new();
    
    for i in 0..50 {
        let file_path = temp_path.join(format!("test_file_{}.dat", i % 10)).to_string_lossy().to_string();
        
        let task = read_async(
            &workers,
            file_path,
            0,    // from_si
            1024, // max_bytes
        );
        
        read_tasks.push(task);
    }

    // Wait for all reads to complete concurrently
    let read_results = futures::future::join_all(read_tasks).await;
    let successful_reads = read_results.iter().filter(|r| r.is_ok()).count();
    let read_duration = start.elapsed();
    println!("Completed {} reads in {:?}", successful_reads, read_duration);

    // Test mixed workload - first do writes, then mixed operations
    println!("\nTesting mixed workload...");
    
    // First, ensure some files exist
    let prep_start = Instant::now();
    let mut prep_tasks = Vec::new();
    
    for i in 0..5 {
        let file_path = temp_path.join(format!("mixed_test_{}.dat", i)).to_string_lossy().to_string();
        let event_batch = create_random_event_batch_item();
        
        let task = write_async(&workers, file_path.clone(), true, event_batch);
        prep_tasks.push(task);
        let task = write_async(&workers, file_path, true, create_random_event_batch_item());
        prep_tasks.push(task);
    }
    
    let _prep_results = futures::future::join_all(prep_tasks).await;
    println!("Prepared {} files in {:?}", 5, prep_start.elapsed());
    
    // Now do mixed operations
    let start = Instant::now();
    let mut mixed_tasks = Vec::new();
    
    for i in 0..1000 {
        let file_path = temp_path.join(format!("mixed_test_{}.dat", i % 5)).to_string_lossy().to_string();
        let workers_clone = workers.clone(); // Clone for each iteration
        
        if i % 3 == 0 {
            // Write operation
            let event_batch = create_random_event_batch_item();
            let task = async move {
                write_async(&workers_clone, file_path, true, event_batch).await
                    .map(|si| format!("Write: SI {}", si))
                    .map_err(|e| format!("Write error: {}", e))
            };
            mixed_tasks.push(Box::pin(task) as Pin<Box<dyn std::future::Future<Output = Result<String, String>> + Send>>);
        } else {
            // Read operation
            let task = async move {
                read_async(&workers_clone, file_path, 0, usize::MAX).await
                    .map(|result| format!("Read: {} event batches", result.event_batches.len()))
                    .map_err(|e| format!("Read error: {}", e))
            };
            mixed_tasks.push(Box::pin(task) as Pin<Box<dyn std::future::Future<Output = Result<String, String>> + Send>>);
        }
    }

    // Execute mixed workload concurrently
    let mixed_results = futures::future::join_all(mixed_tasks).await;
    let successful_mixed = mixed_results.iter().filter(|r| r.is_ok()).count();
    let failed_mixed = mixed_results.iter().filter(|r| r.is_err()).count();
    
    let mixed_duration = start.elapsed();
    println!("Completed {} mixed operations ({} successful, {} failed) in {:?}", 
             mixed_results.len(), successful_mixed, failed_mixed, mixed_duration);

    // Print some sample results
    println!("\nSample results:");
    for (i, result) in mixed_results.iter().take(50).enumerate() {
        match result {
            Ok(msg) => println!("  {}: {}", i + 1, msg),
            Err(e) => println!("  {}: ERROR - {}", i + 1, e),
        }
    }

    println!("\nTest completed successfully!");
    println!("Temp directory will be cleaned up automatically");
}