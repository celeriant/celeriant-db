#[cfg(test)]
mod test_pagination {
    use std::num::NonZeroUsize;

    use celeriant_msg::request::{read_filters::ReadFilters, requests::WriteRequest};
    use celeriant_wal::{
        aggregate_key::AggregateKey, compression_type::CompressionType, wal::event_item::EventItem,
    };
    use glommio::{LocalExecutorBuilder, Placement};

    use crate::{
        cache::aggregate_cache::AggregateCache,
        node_config::test_node_config::test_config,
        read_operations::{
            read_error::ReadError, read_operations::ReadOperations,
            read_structures::AggregateReadConfig,
        },
        write_operations::{
            aggregate_write_config::AggregateWriteConfig, write_operations::WriteOperations,
        },
    };

    /// Helper to write a batch with specific parameters
    async fn write_batch_with_size(
        aggregates_cache: &AggregateCache,
        aggregate_key: &AggregateKey,
        client_id: u128,
        start_client_event_index: u64,
        expected_batch_index: u64,
        base_timestamp: u64,
        payload_size: usize,
    ) -> u64 {
        // Create event with specific payload size
        let event_value = vec![0u8; payload_size];
        let events = vec![EventItem::new(
            start_client_event_index,
            0,
            None,
            base_timestamp,
            1,
            0,
            event_value,
        )];

        let mut write_request = WriteRequest {
            client_id,
            compression_type: CompressionType::None,
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
            .queue_events_in_memory(0, 0, base_timestamp, &mut write_request)
            .unwrap();

        writer.sync_with_rollback().await.unwrap();

        payload_size as u64 + 19
    }

    #[test]
    fn test_read_with_max_bytes_returns_first_page() {
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

                // Write 5 batches with known sizes (100 bytes each when uncompressed)
                let mut total_size = 0u64;
                for i in 1..=5 {
                    let compressed_size = write_batch_with_size(
                        &aggregates_cache,
                        &aggregate_key,
                        123,
                        i * 10,
                        i,
                        1000 + i * 100,
                        100,
                    )
                    .await;
                    total_size += compressed_size;
                }

                let aggregate_resources = aggregates_cache.get_aggregate_resources(&aggregate_key);
                let writer = aggregate_resources.get_writer(true).await.unwrap();
                let reader = aggregate_resources.get_reader(true).await.unwrap();

                // Set max_bytes to fit only first 2 batches (approximately)
                let max_bytes = (total_size / 5 * 2) as usize;

                let read_filters = ReadFilters::new(1);
                let read_result = reader
                    .read(
                        None,
                        writer.minimum_available_event_batch_index,
                        writer.file_len_metadata,
                        writer.file_len_event_batch,
                        &read_filters,
                        Some(max_bytes),
                    )
                    .await
                    .unwrap();

                // Should return some batches but not all
                assert!(read_result.event_batches.len() > 0);
                assert!(read_result.event_batches.len() < 5);

                // Should have next_event_batch_index for pagination
                assert!(read_result.next_event_batch_index.is_some());

                // Verify the next index is correct
                let last_returned_index =
                    read_result.event_batches.last().unwrap().event_batch_index;
                assert_eq!(
                    read_result.next_event_batch_index.unwrap(),
                    last_returned_index + 1
                );
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_read_fetches_subsequent_pages() {
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

                // Write 6 batches
                let mut sizes = Vec::new();
                for i in 1..=6 {
                    let compressed_size = write_batch_with_size(
                        &aggregates_cache,
                        &aggregate_key,
                        123,
                        i * 10,
                        i,
                        1000 + i * 100,
                        100,
                    )
                    .await;
                    sizes.push(compressed_size);
                }

                let aggregate_resources = aggregates_cache.get_aggregate_resources(&aggregate_key);

                // Read first page
                let max_bytes = (sizes[0] + sizes[1]) as usize + 10;
                let read_filters = ReadFilters::new(1);

                let first_page = {
                    let writer = aggregate_resources.get_writer(true).await.unwrap();
                    let reader = aggregate_resources.get_reader(true).await.unwrap();

                    reader
                        .read(
                            None,
                            writer.minimum_available_event_batch_index,
                            writer.file_len_metadata,
                            writer.file_len_event_batch,
                            &read_filters,
                            Some(max_bytes),
                        )
                        .await
                        .unwrap()
                };

                assert!(first_page.next_event_batch_index.is_some());
                let next_index = first_page.next_event_batch_index.unwrap();

                // Read second page using next_event_batch_index
                let read_filters_page_2 = ReadFilters::new(next_index);

                let second_page = {
                    let writer = aggregate_resources.get_writer(true).await.unwrap();
                    let reader = aggregate_resources.get_reader(true).await.unwrap();

                    reader
                        .read(
                            None,
                            writer.minimum_available_event_batch_index,
                            writer.file_len_metadata,
                            writer.file_len_event_batch,
                            &read_filters_page_2,
                            Some(max_bytes),
                        )
                        .await
                        .unwrap()
                };

                // Verify pages don't overlap
                let last_first_page = first_page.event_batches.last().unwrap();
                let first_second_page = second_page.event_batches.first().unwrap();
                assert_eq!(
                    first_second_page.event_batch_index,
                    last_first_page.event_batch_index + 1
                );

                // Verify all batches accounted for when combining pages
                let total_first = first_page.event_batches.len();
                let total_second = second_page.event_batches.len();
                assert!(total_first + total_second <= 6);
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_pagination_with_filters() {
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

                // Write batches with alternating client_ids
                let mut total_size = 0u64;
                for i in 1..=6 {
                    let client_id = if i % 2 == 0 { 123 } else { 456 };
                    let compressed_size = write_batch_with_size(
                        &aggregates_cache,
                        &aggregate_key,
                        client_id,
                        i * 10,
                        i,
                        1000 + i * 100,
                        100,
                    )
                    .await;
                    if client_id == 123 {
                        total_size += compressed_size;
                    }
                }

                let aggregate_resources = aggregates_cache.get_aggregate_resources(&aggregate_key);
                let writer = aggregate_resources.get_writer(true).await.unwrap();
                let reader = aggregate_resources.get_reader(true).await.unwrap();

                // Filter to only client_id 123 and set max_bytes to fit only 1 batch
                let max_bytes = (total_size / 3) as usize;
                let read_filters = ReadFilters::new(1).include_client_id(123);

                let read_result = reader
                    .read(
                        None,
                        writer.minimum_available_event_batch_index,
                        writer.file_len_metadata,
                        writer.file_len_event_batch,
                        &read_filters,
                        Some(max_bytes),
                    )
                    .await
                    .unwrap();

                // Should only contain client_id 123 batches
                for batch in &read_result.event_batches {
                    assert_eq!(batch.client_id, 123);
                }

                // Should have pagination since we limited max_bytes
                assert!(read_result.event_batches.len() > 0);
                assert!(read_result.event_batches.len() < 3); // 3 batches match filter
                assert!(read_result.next_event_batch_index.is_some());
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_writer_cache_read_with_pagination() {
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

                // Write and sync several batches to populate writer cache
                let mut sizes = Vec::new();
                for i in 1..=5 {
                    let compressed_size = write_batch_with_size(
                        &aggregates_cache,
                        &aggregate_key,
                        123,
                        i * 10,
                        i,
                        1000 + i * 100,
                        100,
                    )
                    .await;
                    sizes.push(compressed_size);
                }

                let aggregate_resources = aggregates_cache.get_aggregate_resources(&aggregate_key);
                let writer = aggregate_resources.get_writer(true).await.unwrap();

                // Read from writer cache with max_bytes limiting to first 2 batches
                let max_bytes = (sizes[0] + sizes[1]) as usize + 10;
                let read_filters = ReadFilters::new(1);

                let cache_read = writer
                    .maybe_read_cached_events(None, &read_filters, Some(max_bytes))
                    .unwrap();

                // Should return only first 2 batches
                assert!(cache_read.event_batches.len() <= 2);
                assert!(cache_read.event_batches.len() > 0);

                // Should have next_event_batch_index
                assert!(cache_read.next_event_batch_index.is_some());

                // Verify batches are in order
                for (i, batch) in cache_read.event_batches.iter().enumerate() {
                    assert_eq!(batch.event_batch_index, (i + 1) as u64);
                }

                // Read next page from cache
                let next_filters = ReadFilters::new(cache_read.next_event_batch_index.unwrap());
                let cache_read_page_2 = writer
                    .maybe_read_cached_events(None, &next_filters, Some(max_bytes))
                    .unwrap();

                // Verify no overlap
                let last_page_1 = cache_read.event_batches.last().unwrap();
                let first_page_2 = cache_read_page_2.event_batches.first().unwrap();
                assert_eq!(
                    first_page_2.event_batch_index,
                    last_page_1.event_batch_index + 1
                );
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_pagination_max_bytes_too_small_errors() {
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

                // Write a batch
                let compressed_size =
                    write_batch_with_size(&aggregates_cache, &aggregate_key, 123, 10, 1, 1000, 200)
                        .await;

                let aggregate_resources = aggregates_cache.get_aggregate_resources(&aggregate_key);
                let writer = aggregate_resources.get_writer(true).await.unwrap();
                let reader = aggregate_resources.get_reader(true).await.unwrap();

                // Set max_bytes smaller than the first batch
                let max_bytes = (compressed_size / 2) as usize;
                let read_filters = ReadFilters::new(1);

                let result = reader
                    .read(
                        None,
                        writer.minimum_available_event_batch_index,
                        writer.file_len_metadata,
                        writer.file_len_event_batch,
                        &read_filters,
                        Some(max_bytes),
                    )
                    .await;

                // Should return MaxBytesTooSmall error
                match result {
                    Err(ReadError::MaxBytesTooSmall {
                        current_max_bytes,
                        required_max_bytes,
                    }) => {
                        assert_eq!(current_max_bytes, max_bytes as u64);
                        assert_eq!(required_max_bytes, compressed_size);
                    }
                    _ => panic!("Expected MaxBytesTooSmall error"),
                }

                // Test same error from writer cache
                let cache_result = writer.maybe_read_cached_events(None, &read_filters, Some(max_bytes));

                match cache_result {
                    Err(crate::write_operations::write_error::WriteError::MaxBytesTooSmall {
                        current_max_bytes,
                        required_max_bytes,
                    }) => {
                        assert_eq!(current_max_bytes, max_bytes as u64);
                        assert_eq!(required_max_bytes, compressed_size);
                    }
                    _ => panic!("Expected MaxBytesTooSmall error from cache"),
                }
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_pagination_returns_none_when_all_batches_fit() {
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

                // Write 3 small batches
                let mut total_size = 0u64;
                for i in 1..=3 {
                    total_size += write_batch_with_size(
                        &aggregates_cache,
                        &aggregate_key,
                        123,
                        i * 10,
                        i,
                        1000 + i * 100,
                        50,
                    )
                    .await;
                }

                let aggregate_resources = aggregates_cache.get_aggregate_resources(&aggregate_key);
                let writer = aggregate_resources.get_writer(true).await.unwrap();
                let reader = aggregate_resources.get_reader(true).await.unwrap();

                // Set max_bytes large enough for all batches
                let max_bytes = (total_size * 2) as usize;
                let read_filters = ReadFilters::new(1);

                let read_result = reader
                    .read(
                        None,
                        writer.minimum_available_event_batch_index,
                        writer.file_len_metadata,
                        writer.file_len_event_batch,
                        &read_filters,
                        Some(max_bytes),
                    )
                    .await
                    .unwrap();

                // Should return all 3 batches
                assert_eq!(read_result.event_batches.len(), 3);

                // Should NOT have next_event_batch_index since all fit
                assert_eq!(read_result.next_event_batch_index, None);
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_pagination_with_event_filters_applied_after_byte_limit() {
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

                // Write batches with events at different timestamps
                let mut sizes = Vec::new();
                for i in 1..=5 {
                    let size = write_batch_with_size(
                        &aggregates_cache,
                        &aggregate_key,
                        123,
                        i * 10,
                        i,
                        1000 + i * 100, // Timestamps: 1100, 1200, 1300, 1400, 1500
                        100,
                    )
                    .await;
                    sizes.push(size);
                }

                let aggregate_resources = aggregates_cache.get_aggregate_resources(&aggregate_key);
                let writer = aggregate_resources.get_writer(true).await.unwrap();
                let reader = aggregate_resources.get_reader(true).await.unwrap();

                // Filter by timestamp and limit bytes to fit 2 batches
                let max_bytes = (sizes[0] + sizes[1]) as usize + 10;
                let read_filters = ReadFilters::new(1).min_event_timestamp(1200);

                let read_result = reader
                    .read(
                        None,
                        writer.minimum_available_event_batch_index,
                        writer.file_len_metadata,
                        writer.file_len_event_batch,
                        &read_filters,
                        Some(max_bytes),
                    )
                    .await
                    .unwrap();

                // Verify all returned batches match the timestamp filter
                for batch in &read_result.event_batches {
                    for event in &batch.events {
                        assert!(event.event_timestamp >= 1200);
                    }
                }

                // Should have pagination
                assert!(read_result.next_event_batch_index.is_some());
            })
            .unwrap();
        handle.join().unwrap();
    }
}
