#[cfg(test)]
mod test_concurrency_and_idempotency {
    use std::{num::NonZeroUsize, sync::Arc, time::Duration};

    use eventplanedb_structures::{
        aggregate_key::AggregateKey, compression_type::CompressionType, event_item::EventItem,
    };
    use glommio::{LocalExecutorBuilder, Placement};

    use crate::{
        cache::aggregate_cache::AggregateCache, node_config::test_node_config::test_config, read_operations::read_structures::AggregateReadConfig, write_operations::{
            write_error::WriteError,
            write_operations::WriteOperations,
            write_structures::{AggregateWriteConfig, WriteOptions},
        }
    };

    /// Helper to create test events
    fn create_test_events(
        starting_client_event_index: u64,
        count: usize,
        base_timestamp: u64,
    ) -> Vec<EventItem> {
        let mut events = Vec::with_capacity(count);
        for i in 0..count {
            events.push(EventItem::new(
                starting_client_event_index + i as u64,
                0,
                None,
                base_timestamp + i as u64,
                1,
                0,
                vec![i as u8; 50],
            ));
        }
        events
    }

    #[test]
    fn test_no_concurrency_check_allows_any_write() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let aggregate_read_config = AggregateReadConfig {
                    max_chunk_size: 1 << 20,
                };

                let aggregate_write_config = AggregateWriteConfig {
                    max_data_cache_size_bytes: 1 << 25,
                    cache_trim_factor: 25,
                    max_chunk_size: 1 << 20,
                };

                let tempdir = tempfile::tempdir().unwrap();
                let data_root_folder = tempdir.path().to_str().unwrap();

                let aggregates_cache = AggregateCache::new(
                    NonZeroUsize::new(1000).unwrap(),
                    test_config(data_root_folder),
                    aggregate_read_config,
                    aggregate_write_config,
                );
                let aggregate_key = AggregateKey::new(1, 1, 1);

                let aggregate_resources = aggregates_cache.get_aggregate_resources(&aggregate_key);

                // Write first batch with concurrency check
                let events1 = create_test_events(1, 2, 1000);
                let write_options1 = WriteOptions {
                    client_id: 100,
                    compression_type: CompressionType::None,
                    enforce_client_idempotency: true,
                    expected_event_batch_index: Some(1),
                    server_timestamp_millis: 1000,
                    user_id: None,
                };

                {
                    let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();
                    let result = writer
                        .as_mut()
                        .unwrap()
                        .queue_events_in_memory(0, 0,  events1, &write_options1)
                        .unwrap();
                    assert_eq!(result.next_event_batch_index, 2);
                    writer.as_mut().unwrap().sync_with_rollback().await.unwrap();
                }

                // Write second batch with NO concurrency check (expected_event_batch_index = None)
                // This should succeed even though current batch index is 2
                let events2 = create_test_events(3, 2, 2000);
                let write_options2 = WriteOptions {
                    client_id: 100,
                    compression_type: CompressionType::None,
                    enforce_client_idempotency: true,
                    expected_event_batch_index: None, // No concurrency check
                    server_timestamp_millis: 2000,
                    user_id: None,
                };

                {
                    let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();
                    let result = writer
                        .as_mut()
                        .unwrap()
                        .queue_events_in_memory(0, 0,  events2, &write_options2);
                    
                    // Should succeed since no concurrency check is performed
                    assert!(result.is_ok());
                    assert_eq!(result.unwrap().next_event_batch_index, 3);
                }
            })
            .unwrap();
        handle.join().unwrap();
    }
    
    #[test]
    fn test_optimistic_concurrency_violation() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let aggregate_read_config = AggregateReadConfig {
                    max_chunk_size: 1 << 20,
                };

                let aggregate_write_config = AggregateWriteConfig {
                    max_data_cache_size_bytes: 1 << 25,
                    cache_trim_factor: 25,
                    max_chunk_size: 1 << 20,
                };

                let tempdir = tempfile::tempdir().unwrap();
                let data_root_folder = tempdir.path().to_str().unwrap();

                let aggregates_cache = AggregateCache::new(
                    NonZeroUsize::new(1000).unwrap(),
                    test_config(data_root_folder),
                    aggregate_read_config,
                    aggregate_write_config,
                );
                let aggregate_key = AggregateKey::new(1, 1, 1);

                // Write first batch successfully
                let events = create_test_events(1, 2, 1000);
                let write_options = WriteOptions {
                    client_id: 100,
                    compression_type: CompressionType::None,
                    enforce_client_idempotency: true,
                    expected_event_batch_index: Some(1),
                    server_timestamp_millis: 1000,
                    user_id: None,
                };

                let aggregate_resources = aggregates_cache.get_aggregate_resources(&aggregate_key);
                
                // First write succeeds
                {
                    let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();
                    let result = writer
                        .as_mut()
                        .unwrap()
                        .queue_events_in_memory(0, 0,  events.clone(), &write_options)
                        .unwrap();
                    assert_eq!(result.next_event_batch_index, 2);
                    writer.as_mut().unwrap().sync_with_rollback().await.unwrap();
                }

                // Second write with same expected_event_batch_index should fail
                {
                    let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();
                    let events2 = create_test_events(3, 2, 2000);
                    let result = writer
                        .as_mut()
                        .unwrap()
                        .queue_events_in_memory(0, 0,  events2, &write_options);

                    match result {
                        Err(WriteError::OptimisticConcurrencyViolation {
                            client_id,
                            expected_event_batch_index,
                            current_event_batch_index,
                        }) => {
                            assert_eq!(client_id, 100);
                            assert_eq!(expected_event_batch_index, 1);
                            assert_eq!(current_event_batch_index, 2);
                        }
                        _ => panic!("Expected OptimisticConcurrencyViolation error"),
                    }
                }
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_client_idempotency_violation() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let aggregate_read_config = AggregateReadConfig {
                    max_chunk_size: 1 << 20,
                };

                let aggregate_write_config = AggregateWriteConfig {
                    max_data_cache_size_bytes: 1 << 25,
                    cache_trim_factor: 25,
                    max_chunk_size: 1 << 20,
                };

                let tempdir = tempfile::tempdir().unwrap();
                let data_root_folder = tempdir.path().to_str().unwrap();

                let aggregates_cache = AggregateCache::new(
                    NonZeroUsize::new(1000).unwrap(),
                    test_config(data_root_folder),
                    aggregate_read_config,
                    aggregate_write_config,
                );
                let aggregate_key = AggregateKey::new(1, 1, 1);

                let aggregate_resources = aggregates_cache.get_aggregate_resources(&aggregate_key);

                // Write first batch with client_event_index 1-3
                let events = create_test_events(1, 3, 1000);
                let write_options = WriteOptions {
                    client_id: 100,
                    compression_type: CompressionType::None,
                    enforce_client_idempotency: true,
                    expected_event_batch_index: Some(1),
                    server_timestamp_millis: 1000,
                    user_id: None,
                };

                {
                    let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();
                    writer
                        .as_mut()
                        .unwrap()
                        .queue_events_in_memory(0, 0,  events, &write_options)
                        .unwrap();
                    writer.as_mut().unwrap().sync_with_rollback().await.unwrap();
                }

                // Attempt to write overlapping client_event_index (2-4)
                let events2 = create_test_events(2, 3, 2000);
                let write_options2 = WriteOptions {
                    client_id: 100,
                    compression_type: CompressionType::None,
                    enforce_client_idempotency: true,
                    expected_event_batch_index: Some(2),
                    server_timestamp_millis: 2000,
                    user_id: None,
                };

                {
                    let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();
                    let result = writer
                        .as_mut()
                        .unwrap()
                        .queue_events_in_memory(0, 0,  events2, &write_options2);

                    match result {
                        Err(WriteError::ClientIdempotencyViolation {
                            client_id,
                            last_client_event_index,
                            attempted_client_event_index,
                        }) => {
                            assert_eq!(client_id, 100);
                            assert_eq!(last_client_event_index, 3);
                            assert_eq!(attempted_client_event_index, 2);
                        }
                        _ => panic!("Expected ClientIdempotencyViolation error"),
                    }
                }
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_concurrent_get_reader_initializes_once() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let aggregate_read_config = AggregateReadConfig {
                    max_chunk_size: 1 << 20,
                };

                let aggregate_write_config = AggregateWriteConfig {
                    max_data_cache_size_bytes: 1 << 25,
                    cache_trim_factor: 25,
                    max_chunk_size: 1 << 20,
                };

                let tempdir = tempfile::tempdir().unwrap();
                let data_root_folder = tempdir.path().to_str().unwrap();

                let aggregates_cache = Arc::new(AggregateCache::new(
                    NonZeroUsize::new(1000).unwrap(),
                    test_config(data_root_folder),
                    aggregate_read_config,
                    aggregate_write_config,
                ));
                let aggregate_key = AggregateKey::new(1, 1, 1);

                // Spawn multiple concurrent tasks that try to get reader
                let mut tasks = Vec::new();
                for _ in 0..5 {
                    let cache = aggregates_cache.clone();
                    let key = aggregate_key.clone();
                    
                    let task = glommio::spawn_local(async move {
                        let resources = cache.get_aggregate_resources(&key);
                        let reader = resources.get_reader(true).await.unwrap();
                        // Verify reader was initialized
                        assert!(reader.is_some());
                    });
                    
                    tasks.push(task);
                }

                // Wait for all tasks to complete
                for task in tasks {
                    task.await;
                }

                // Verify that the files exist and were only created once
                let metadata_path = format!(
                    "{}/{}/{}/{}/metadata.bin",
                    data_root_folder, 1, 1, 1
                );
                let event_batches_path = format!(
                    "{}/{}/{}/{}/event_batches.bin",
                    data_root_folder, 1, 1, 1
                );
                
                assert!(std::path::Path::new(&metadata_path).exists());
                assert!(std::path::Path::new(&event_batches_path).exists());
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_concurrent_get_writer_initializes_once() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let aggregate_read_config = AggregateReadConfig {
                    max_chunk_size: 1 << 20,
                };

                let aggregate_write_config = AggregateWriteConfig {
                    max_data_cache_size_bytes: 1 << 25,
                    cache_trim_factor: 25,
                    max_chunk_size: 1 << 20,
                };

                let tempdir = tempfile::tempdir().unwrap();
                let data_root_folder = tempdir.path().to_str().unwrap();

                let aggregates_cache = Arc::new(AggregateCache::new(
                    NonZeroUsize::new(1000).unwrap(),
                    test_config(data_root_folder),
                    aggregate_read_config,
                    aggregate_write_config,
                ));
                let aggregate_key = AggregateKey::new(1, 1, 1);

                // Spawn multiple concurrent tasks that try to get writer
                let mut tasks = Vec::new();
                for _ in 0..5 {
                    let cache = aggregates_cache.clone();
                    let key = aggregate_key.clone();
                    
                    let task = glommio::spawn_local(async move {
                        let resources = cache.get_aggregate_resources(&key);
                        let writer = resources.get_writer(true).await.unwrap();
                        // Verify writer was initialized
                        assert!(writer.is_some());
                    });
                    
                    tasks.push(task);
                }

                // Wait for all tasks to complete
                for task in tasks {
                    task.await;
                }

                // Verify that the files exist and were only created once
                let metadata_path = format!(
                    "{}/{}/{}/{}/metadata.bin",
                    data_root_folder, 1, 1, 1
                );
                let event_batches_path = format!(
                    "{}/{}/{}/{}/event_batches.bin",
                    data_root_folder, 1, 1, 1
                );
                
                assert!(std::path::Path::new(&metadata_path).exists());
                assert!(std::path::Path::new(&event_batches_path).exists());
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_sync_with_delay_coalesces_requests() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let aggregate_read_config = AggregateReadConfig {
                    max_chunk_size: 1 << 20,
                };

                let aggregate_write_config = AggregateWriteConfig {
                    max_data_cache_size_bytes: 1 << 25,
                    cache_trim_factor: 25,
                    max_chunk_size: 1 << 20,
                };

                let tempdir = tempfile::tempdir().unwrap();
                let data_root_folder = tempdir.path().to_str().unwrap();

                let aggregates_cache = Arc::new(AggregateCache::new(
                    NonZeroUsize::new(1000).unwrap(),
                    test_config(data_root_folder),
                    aggregate_read_config,
                    aggregate_write_config,
                ));
                let aggregate_key = AggregateKey::new(1, 1, 1);

                let aggregate_resources = aggregates_cache.get_aggregate_resources(&aggregate_key);

                // Queue some events first
                let events = create_test_events(1, 2, 1000);
                let write_options = WriteOptions {
                    client_id: 100,
                    compression_type: CompressionType::None,
                    enforce_client_idempotency: true,
                    expected_event_batch_index: Some(1),
                    server_timestamp_millis: 1000,
                    user_id: None,
                };

                {
                    let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();
                    writer
                        .as_mut()
                        .unwrap()
                        .queue_events_in_memory(0, 0,  events, &write_options)
                        .unwrap();
                }

                // Spawn multiple concurrent sync_with_delay tasks
                let mut tasks = Vec::new();
                let sync_delay = Duration::from_millis(50);
                
                for _ in 0..5 {
                    let resources = aggregate_resources.clone();
                    
                    let task = glommio::spawn_local(async move {
                        let result = resources.sync_with_delay(Some(sync_delay)).await;
                        // All tasks should complete successfully
                        assert!(result.is_ok());
                    });
                    
                    tasks.push(task);
                }

                // Wait for all tasks to complete
                for task in tasks {
                    task.await;
                }

                // Verify that data was synced (check that next_event_batch_index advanced)
                {
                    let writer = aggregate_resources.get_writer(true).await.unwrap();
                    let writer_ref = writer.as_ref().unwrap();
                    assert_eq!(writer_ref.next_event_batch_index, 2);
                }
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_client_idempotency_with_different_clients() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let aggregate_read_config = AggregateReadConfig {
                    max_chunk_size: 1 << 20,
                };

                let aggregate_write_config = AggregateWriteConfig {
                    max_data_cache_size_bytes: 1 << 25,
                    cache_trim_factor: 25,
                    max_chunk_size: 1 << 20,
                };

                let tempdir = tempfile::tempdir().unwrap();
                let data_root_folder = tempdir.path().to_str().unwrap();

                let aggregates_cache = AggregateCache::new(
                    NonZeroUsize::new(1000).unwrap(),
                    test_config(data_root_folder),
                    aggregate_read_config,
                    aggregate_write_config,
                );
                let aggregate_key = AggregateKey::new(1, 1, 1);

                let aggregate_resources = aggregates_cache.get_aggregate_resources(&aggregate_key);

                // Client 100 writes events with client_event_index 1-3
                let events1 = create_test_events(1, 3, 1000);
                let write_options1 = WriteOptions {
                    client_id: 100,
                    compression_type: CompressionType::None,
                    enforce_client_idempotency: true,
                    expected_event_batch_index: Some(1),
                    server_timestamp_millis: 1000,
                    user_id: None,
                };

                {
                    let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();
                    writer
                        .as_mut()
                        .unwrap()
                        .queue_events_in_memory(0, 0,  events1, &write_options1)
                        .unwrap();
                    writer.as_mut().unwrap().sync_with_rollback().await.unwrap();
                }

                // Client 200 can use the same client_event_index range (1-3) without conflict
                let events2 = create_test_events(1, 3, 2000);
                let write_options2 = WriteOptions {
                    client_id: 200,
                    compression_type: CompressionType::None,
                    enforce_client_idempotency: true,
                    expected_event_batch_index: Some(2),
                    server_timestamp_millis: 2000,
                    user_id: None,
                };

                {
                    let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();
                    let result = writer
                        .as_mut()
                        .unwrap()
                        .queue_events_in_memory(0, 0,  events2, &write_options2);
                    
                    // Should succeed since it's a different client
                    assert!(result.is_ok());
                    assert_eq!(result.unwrap().next_event_batch_index, 3);
                }
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_idempotency_disabled_allows_duplicate_indexes() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let aggregate_read_config = AggregateReadConfig {
                    max_chunk_size: 1 << 20,
                };

                let aggregate_write_config = AggregateWriteConfig {
                    max_data_cache_size_bytes: 1 << 25,
                    cache_trim_factor: 25,
                    max_chunk_size: 1 << 20,
                };

                let tempdir = tempfile::tempdir().unwrap();
                let data_root_folder = tempdir.path().to_str().unwrap();

                let aggregates_cache = AggregateCache::new(
                    NonZeroUsize::new(1000).unwrap(),
                    test_config(data_root_folder),
                    aggregate_read_config,
                    aggregate_write_config,
                );
                let aggregate_key = AggregateKey::new(1, 1, 1);

                let aggregate_resources = aggregates_cache.get_aggregate_resources(&aggregate_key);

                // Write first batch with client_event_index 1-3
                let events1 = create_test_events(1, 3, 1000);
                let write_options1 = WriteOptions {
                    client_id: 100,
                    compression_type: CompressionType::None,
                    enforce_client_idempotency: false, // Disabled
                    expected_event_batch_index: Some(1),
                    server_timestamp_millis: 1000,
                    user_id: None,
                };

                {
                    let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();
                    writer
                        .as_mut()
                        .unwrap()
                        .queue_events_in_memory(0, 0,  events1, &write_options1)
                        .unwrap();
                    writer.as_mut().unwrap().sync_with_rollback().await.unwrap();
                }

                // Write second batch with overlapping client_event_index (2-4)
                let events2 = create_test_events(2, 3, 2000);
                let write_options2 = WriteOptions {
                    client_id: 100,
                    compression_type: CompressionType::None,
                    enforce_client_idempotency: false, // Disabled
                    expected_event_batch_index: Some(2),
                    server_timestamp_millis: 2000,
                    user_id: None,
                };

                {
                    let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();
                    let result = writer
                        .as_mut()
                        .unwrap()
                        .queue_events_in_memory(0, 0,  events2, &write_options2);
                    
                    // Should succeed since idempotency checking is disabled
                    assert!(result.is_ok());
                    assert_eq!(result.unwrap().next_event_batch_index, 3);
                }
            })
            .unwrap();
        handle.join().unwrap();
    }
}