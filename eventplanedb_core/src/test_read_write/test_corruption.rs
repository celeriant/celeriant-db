#[cfg(test)]
pub mod test_corruption {
    use std::num::NonZeroUsize;

    use eventplanedb_structures::{
        aggregate_key::AggregateKey, constants::METADATA_BATCH_SIZE_BYTES,
        event_item::EventItem,
    };
    use glommio::{io::OpenOptions, LocalExecutorBuilder, Placement};

    use crate::{
        cache::aggregate_cache::AggregateCache,
        read_operations::{
            read_error::ReadError,
            read_operations::ReadOperations,
            read_structures::AggregateReadConfig,
        },
        write_operations::{
            write_operations::WriteOperations,
            write_structures::{AggregateWriteConfig, WriteOptions},
        },
    };

    pub async fn write_batch(
        aggregates_cache: &AggregateCache,
        aggregate_key: &AggregateKey,
        client_id: u128,
        client_event_index: u64,
        expected_batch_index: u64,
    ) {
        let events = vec![EventItem::new(
            client_event_index,
            0,
            None,
            333,
            2,
            3,
            vec![1, 2, 3, 4, 5],
        )];

        let append_options = WriteOptions {
            client_id,
            compression_type: eventplanedb_structures::compression_type::CompressionType::None,
            enforce_client_idempotency: true,
            expected_event_batch_index: Some(expected_batch_index),
            server_timestamp_millis: 998,
            user_id: None,
        };

        let aggregate_resources = aggregates_cache.get(aggregate_key);
        let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();

        writer
            .as_mut()
            .unwrap()
            .queue_events_in_memory(events, &append_options)
            .unwrap();

        writer.as_mut().unwrap().sync_with_rollback().await.unwrap();
    }

    async fn corrupt_event_batches_file(data_folder: &str, trim_bytes: u64) {
        let path = format!("{}/1/1/1/event_batches.bin", data_folder);
        let file = OpenOptions::new()
            .write(true)
            .dma_open(&path)
            .await
            .unwrap();

        let current_size = file.file_size().await.unwrap();
        file.truncate(current_size - trim_bytes).await.unwrap();
        file.close().await.unwrap();
    }

    async fn corrupt_metadata_file_partial(data_folder: &str, trim_bytes: u64) {
        let path = format!("{}/1/1/1/metadata.bin", data_folder);
        let file = OpenOptions::new()
            .write(true)
            .dma_open(&path)
            .await
            .unwrap();

        let current_size = file.file_size().await.unwrap();
        file.truncate(current_size - trim_bytes).await.unwrap();
        file.close().await.unwrap();
    }

    async fn corrupt_metadata_file(data_folder: &str, remove_entries: u64) {
        let path = format!("{}/1/1/1/metadata.bin", data_folder);
        let file = OpenOptions::new()
            .write(true)
            .dma_open(&path)
            .await
            .unwrap();

        let current_size = file.file_size().await.unwrap();
        let trim_bytes = remove_entries * METADATA_BATCH_SIZE_BYTES as u64;
        file.truncate(current_size - trim_bytes).await.unwrap();
        file.close().await.unwrap();
    }

    #[test]
    fn test_1_single_batch_no_corruption() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let aggregate_read_config = AggregateReadConfig {
                    max_chunk_size: 1 << 20,
                    max_data_cache_size_bytes: 1 << 20,
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
                    data_root_folder.to_string(),
                    aggregate_read_config,
                    aggregate_write_config,
                );
                let aggregate_key = AggregateKey::new(1, 1, 1);

                // Write a single batch
                write_batch(&aggregates_cache, &aggregate_key, 123, 45, 1).await;

                // Verify no corruption
                let aggregate_resources = aggregates_cache.get(&aggregate_key);
                let reader = aggregate_resources.get_reader(true).await.unwrap();
                let result = reader
                    .as_ref()
                    .unwrap()
                    .get_write_operations_data_requirements()
                    .await
                    .unwrap();

                assert_eq!(result.write_operations_data_requirements.next_event_batch_index, 2);
                assert_eq!(result.write_operations_data_requirements.next_event_index, 2);
                assert_eq!(result.write_operations_data_requirements.minimum_available_event_batch_index, 1);
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_2_single_batch_corrupt_no_repair() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let aggregate_read_config = AggregateReadConfig {
                    max_chunk_size: 1 << 20,
                    max_data_cache_size_bytes: 1 << 20,
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
                    data_root_folder.to_string(),
                    aggregate_read_config,
                    aggregate_write_config,
                );
                let aggregate_key = AggregateKey::new(1, 1, 1);

                // Write a single batch
                write_batch(&aggregates_cache, &aggregate_key, 123, 45, 1).await;

                // Corrupt the event batches file
                corrupt_event_batches_file(data_root_folder, 5).await;

                // Should fail with CorruptEventBatch error (no auto-repair possible)
                let aggregate_resources = aggregates_cache.get(&aggregate_key);
                let reader = aggregate_resources.get_reader(true).await.unwrap();
                let result = reader
                    .as_ref()
                    .unwrap()
                    .get_write_operations_data_requirements()
                    .await;

                match result {
                    Err(ReadError::CorruptEventBatch { event_batch_index, .. }) => {
                        assert_eq!(event_batch_index, 1);
                    }
                    _ => panic!("Expected CorruptEventBatch error"),
                }
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_3_two_batches_last_corrupt_auto_repair() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let aggregate_read_config = AggregateReadConfig {
                    max_chunk_size: 1 << 20,
                    max_data_cache_size_bytes: 1 << 20,
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
                    data_root_folder.to_string(),
                    aggregate_read_config,
                    aggregate_write_config,
                );
                let aggregate_key = AggregateKey::new(1, 1, 1);

                // Write two batches
                write_batch(&aggregates_cache, &aggregate_key, 123, 45, 1).await;
                write_batch(&aggregates_cache, &aggregate_key, 123, 46, 2).await;

                // Corrupt the last event batch
                corrupt_event_batches_file(data_root_folder, 5).await;

                // Should auto-repair by truncating the last batch
                let aggregate_resources = aggregates_cache.get(&aggregate_key);
                let reader = aggregate_resources.get_reader(true).await.unwrap();
                let result = reader
                    .as_ref()
                    .unwrap()
                    .get_write_operations_data_requirements()
                    .await
                    .unwrap();

                // Should have truncated to batch 1, so next batch index should be 2
                assert_eq!(result.write_operations_data_requirements.next_event_batch_index, 2);
                assert_eq!(result.write_operations_data_requirements.next_event_index, 2);
                assert_eq!(result.write_operations_data_requirements.client_event_indexes.get(&123), Some(&45u64));
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_4_two_batches_remove_last_metadata_corrupt() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let aggregate_read_config = AggregateReadConfig {
                    max_chunk_size: 1 << 20,
                    max_data_cache_size_bytes: 1 << 20,
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
                    data_root_folder.to_string(),
                    aggregate_read_config.clone(),
                    aggregate_write_config.clone(),
                );
                let aggregate_key = AggregateKey::new(1, 1, 1);

                // Write two batches
                write_batch(&aggregates_cache, &aggregate_key, 123, 45, 1).await;
                write_batch(&aggregates_cache, &aggregate_key, 123, 46, 2).await;

                // Remove last metadata entry
                corrupt_metadata_file(data_root_folder, 1).await;

                // Should auto-repair by recognizing only 1 complete metadata entry
                let aggregate_resources = aggregates_cache.get(&aggregate_key);
                let reader = aggregate_resources.get_reader(true).await.unwrap();
                let result = reader
                    .as_ref()
                    .unwrap()
                    .get_write_operations_data_requirements()
                    .await;

                match result {
                    Err(ReadError::CorruptEventBatch { file_pos_metadata, event_batch_index, .. }) => {
                        assert_eq!(file_pos_metadata, 0);
                        assert_eq!(event_batch_index, 1);
                    }
                    _ => panic!("Expected CorruptMetadata error"),
                }

            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_4a_two_batches_metadata_partial_corrupt() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let aggregate_read_config = AggregateReadConfig {
                    max_chunk_size: 1 << 20,
                    max_data_cache_size_bytes: 1 << 20,
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
                    data_root_folder.to_string(),
                    aggregate_read_config.clone(),
                    aggregate_write_config.clone(),
                );
                let aggregate_key = AggregateKey::new(1, 1, 1);

                // Write two batches
                write_batch(&aggregates_cache, &aggregate_key, 123, 45, 1).await;
                write_batch(&aggregates_cache, &aggregate_key, 123, 46, 2).await;

                // Remove last metadata entry
                corrupt_metadata_file_partial(data_root_folder, 10).await;

                // Should auto-repair by recognizing only 1 complete metadata entry
                let aggregate_resources = aggregates_cache.get(&aggregate_key);
                let reader = aggregate_resources.get_reader(true).await.unwrap();
                let result = reader
                    .as_ref()
                    .unwrap()
                    .get_write_operations_data_requirements()
                    .await;

                match result {
                    Err(ReadError::CorruptMetadata { file_pos_metadata, .. }) => {
                        assert_eq!(file_pos_metadata, METADATA_BATCH_SIZE_BYTES as u64);
                    }
                    _ => panic!("Expected CorruptMetadata error"),
                }

            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_5_three_batches_corrupt_last_two_no_repair() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let aggregate_read_config = AggregateReadConfig {
                    max_chunk_size: 1 << 20,
                    max_data_cache_size_bytes: 1 << 20,
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
                    data_root_folder.to_string(),
                    aggregate_read_config,
                    aggregate_write_config,
                );
                let aggregate_key = AggregateKey::new(1, 1, 1);

                // Write three batches
                write_batch(&aggregates_cache, &aggregate_key, 123, 45, 1).await;
                write_batch(&aggregates_cache, &aggregate_key, 123, 46, 2).await;
                write_batch(&aggregates_cache, &aggregate_key, 123, 47, 3).await;

                // Get the size of one batch to corrupt two batches
                let aggregate_resources = aggregates_cache.get(&aggregate_key);
                let reader = aggregate_resources.get_reader(true).await.unwrap();
                let initial_result = reader
                    .as_ref()
                    .unwrap()
                    .get_write_operations_data_requirements()
                    .await
                    .unwrap();
                
                let batch_size = initial_result.write_operations_data_requirements.file_len_event_batch / 3;

                // Corrupt by removing approximately 2 batches worth of data
                corrupt_event_batches_file(data_root_folder, batch_size * 2 + 5).await;

                // Should fail - cannot auto-repair when 2+ entries are corrupt
                let reader2 = aggregate_resources.get_reader(true).await.unwrap();
                let result = reader2
                    .as_ref()
                    .unwrap()
                    .get_write_operations_data_requirements()
                    .await;

                match result {
                    Err(ReadError::CorruptEventBatch { event_batch_index, .. }) => {
                        assert_eq!(event_batch_index, 2); // Second-to-last is also corrupt
                    }
                    _ => panic!("Expected CorruptEventBatch error"),
                }
            })
            .unwrap();
        handle.join().unwrap();
    }

}