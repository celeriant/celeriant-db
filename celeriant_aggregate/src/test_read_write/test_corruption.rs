#[cfg(test)]
pub mod test_corruption {
    use std::num::NonZeroUsize;

    use celeriant_msg::request::requests::WriteRequest;
    use celeriant_wal::{
        aggregate_key::AggregateKey, compression_type, wal::event_item::EventItem,
    };
    use glommio::{LocalExecutorBuilder, Placement, io::OpenOptions};

    use crate::{
        cache::aggregate_cache::AggregateCache,
        node_config::test_node_config::test_config,
        read_operations::{read_operations::ReadOperations, read_structures::AggregateReadConfig},
        write_operations::{
            aggregate_write_config::AggregateWriteConfig, write_operations::WriteOperations,
        },
    };

    pub async fn write_batch(
        aggregates_cache: &AggregateCache,
        aggregate_key: &AggregateKey,
        client_id: u128,
        client_event_index: u64,
        expected_batch_index: u64,
        skip_close: bool,
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

        let mut write_request = WriteRequest {
            client_id,
            compression_type: compression_type::CompressionType::None,
            enforce_client_idempotency: true,
            expected_event_batch_index: Some(expected_batch_index),
            user_id: None,
            correlation_id: None,
            aggregate_key: aggregate_key.clone(),
            events,
            allow_create: false,
            durable_write_with_delay_us: None,
        };

        let aggregate_resources = aggregates_cache.get_aggregate_resources(aggregate_key);
        let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();

        writer
            .queue_events_in_memory(0, 0, 998, &mut write_request)
            .unwrap();

        writer.sync_with_rollback().await.unwrap();

        if !skip_close {
            writer.close().await.unwrap();
        }
    }

    fn corrupt_event_batches_file(data_folder: &str, append_bytes: u64) {
        use std::fs::OpenOptions;
        use std::io::Write;

        let path = format!("{}/1/1/1/event_batches.bin", data_folder);
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(&path)
            .unwrap();

        // Append garbage bytes to corrupt the file
        let garbage_data = vec![0xFF; append_bytes as usize];
        file.write_all(&garbage_data).unwrap();
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

    #[test]
    fn test_1_single_batch_no_corruption() {
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

                // Write a single batch
                write_batch(&aggregates_cache, &aggregate_key, 123, 45, 1, false).await;

                // Verify no corruption
                let aggregate_resources = aggregates_cache.get_aggregate_resources(&aggregate_key);
                let reader = aggregate_resources.get_reader(true).await.unwrap();
                let result = reader
                    .get_write_operations_data_requirements()
                    .await
                    .unwrap();

                assert_eq!(result.next_event_batch_index, 2);
                assert_eq!(result.next_event_index, 2);
                assert_eq!(result.minimum_available_event_batch_index, 1);
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_1_single_batch_no_close_auto_trim() {
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

                // Write a single batch
                write_batch(&aggregates_cache, &aggregate_key, 123, 45, 1, true).await;

                // Verify no corruption
                let aggregate_resources = aggregates_cache.get_aggregate_resources(&aggregate_key);
                let reader = aggregate_resources.get_reader(true).await.unwrap();
                let result = reader
                    .get_write_operations_data_requirements()
                    .await
                    .unwrap();

                assert_eq!(result.file_len_event_batch, 24);
                assert_eq!(result.file_len_metadata, 256);
                assert_eq!(result.next_event_batch_index, 2);
                assert_eq!(result.next_event_index, 2);
                assert_eq!(result.minimum_available_event_batch_index, 1);
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_2_single_batch_corrupt_allow_repair() {
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

                // Write a single batch
                write_batch(&aggregates_cache, &aggregate_key, 123, 45, 1, false).await;

                // Corrupt the event batches file
                corrupt_event_batches_file(data_root_folder, 5);

                aggregates_cache.pop(&aggregate_key).await.unwrap();

                // Should fail with CorruptEventBatch error (no auto-repair possible)
                let aggregate_resources = aggregates_cache.get_aggregate_resources(&aggregate_key);
                let reader = aggregate_resources.get_reader(true).await.unwrap();
                let result = reader.get_write_operations_data_requirements().await;

                assert!(result.is_ok());
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

                // Write two batches
                write_batch(&aggregates_cache, &aggregate_key, 123, 45, 1, true).await;
                write_batch(&aggregates_cache, &aggregate_key, 123, 46, 2, false).await;

                // Corrupt the last event batch
                corrupt_event_batches_file(data_root_folder, 5);

                aggregates_cache.pop(&aggregate_key).await.unwrap();

                // Should auto-repair by truncating the last batch
                let aggregate_resources = aggregates_cache.get_aggregate_resources(&aggregate_key);
                let reader = aggregate_resources.get_reader(true).await.unwrap();
                let result = reader
                    .get_write_operations_data_requirements()
                    .await
                    .unwrap();

                // Should have truncated to batch 1, so next batch index should be 2
                assert_eq!(result.next_event_batch_index, 3);
                assert_eq!(result.next_event_index, 3);
                assert_eq!(result.client_event_indexes.get(&123), Some(&46u64));
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_4_single_batch_partial_metadata_corrupt_auto_repair() {
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

                // Write two batches
                write_batch(&aggregates_cache, &aggregate_key, 123, 45, 1, true).await;
                write_batch(&aggregates_cache, &aggregate_key, 123, 46, 2, false).await;

                // Corrupt metadata file by trimming partial bytes (simulating incomplete write)
                corrupt_metadata_file_partial(data_root_folder, 64).await;

                aggregates_cache.pop(&aggregate_key).await.unwrap();

                // Should auto-repair by truncating the partial metadata entry
                let aggregate_resources = aggregates_cache.get_aggregate_resources(&aggregate_key);
                let reader = aggregate_resources.get_reader(true).await.unwrap();
                let result = reader
                    .get_write_operations_data_requirements()
                    .await
                    .unwrap();

                // Should have truncated the incomplete metadata, rolling back to batch 1
                assert_eq!(result.next_event_batch_index, 2);
                assert_eq!(result.next_event_index, 2);
                assert_eq!(result.client_event_indexes.get(&123), Some(&45u64));
            })
            .unwrap();
        handle.join().unwrap();
    }
}
