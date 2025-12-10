#[cfg(test)]
mod test_writer_cache {
    use std::{num::NonZeroUsize, rc::Rc};

    use celeriant_msg::request::{read_filters::ReadFilters, requests::WriteRequest};
    use celeriant_wal::{
        aggregate_key::AggregateKey, compression_type::CompressionType, wal::event_item::EventItem,
    };
    use glommio::{LocalExecutorBuilder, Placement};

    use crate::{
        cache::aggregate_cache::AggregateCache, node_config::test_node_config::test_config, read_operations::read_structures::AggregateReadConfig, watch::watched_aggregates::WatchedAggregates, write_operations::{
            aggregate_write_config::AggregateWriteConfig, write_error::WriteError,
            write_operations::WriteOperations,
        }
    };

    /// Helper to write a batch with specific parameters
    async fn write_batch_with_params(
        aggregates_cache: &AggregateCache,
        aggregate_key: &AggregateKey,
        client_id: u128,
        start_client_event_index: u64,
        event_count: usize,
        expected_batch_index: u64,
        base_timestamp: u64,
        event_type: u64,
    ) {
        let mut events = Vec::with_capacity(event_count);
        for i in 0..event_count {
            events.push(EventItem::new(
                start_client_event_index + i as u64,
                0,
                None,
                base_timestamp + i as u64,
                event_type,
                0,
                vec![i as u8; 50],
            ));
        }

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

        writer.sync_with_rollback(Rc::new(WatchedAggregates::new())).await.unwrap();
    }

    #[test]
    fn test_cache_hit_with_various_filters() {
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

                // Write 5 batches with different characteristics
                write_batch_with_params(&aggregates_cache, &aggregate_key, 100, 1, 3, 1, 1000, 1)
                    .await;
                write_batch_with_params(&aggregates_cache, &aggregate_key, 100, 4, 3, 2, 2000, 2)
                    .await;
                write_batch_with_params(&aggregates_cache, &aggregate_key, 200, 1, 3, 3, 3000, 1)
                    .await;
                write_batch_with_params(&aggregates_cache, &aggregate_key, 200, 4, 3, 4, 4000, 3)
                    .await;
                write_batch_with_params(&aggregates_cache, &aggregate_key, 100, 7, 3, 5, 5000, 2)
                    .await;

                let aggregate_resources = aggregates_cache.get_aggregate_resources(&aggregate_key);
                let writer = aggregate_resources.get_writer(true).await.unwrap();

                // Test 1: Filter by time range
                let filters = ReadFilters::new(1)
                    .min_event_timestamp(2000)
                    .max_event_timestamp(4000);
                let result = writer.maybe_read_cached_events(None, &filters, None).unwrap();
                assert!(result.event_batches.len() >= 2);
                for batch in &result.event_batches {
                    for event in &batch.events {
                        assert!(event.event_timestamp >= 2000 && event.event_timestamp <= 4000);
                    }
                }

                // Test 2: Filter by user (client_id)
                let filters = ReadFilters::new(1).include_client_id(200);
                let result = writer.maybe_read_cached_events(None, &filters, None).unwrap();
                assert_eq!(result.event_batches.len(), 2);
                for batch in &result.event_batches {
                    assert_eq!(batch.client_id, 200);
                }

                // Test 3: Filter by event type
                let filters = ReadFilters::new(1).include_event_types(vec![1]);
                let result = writer.maybe_read_cached_events(None, &filters, None).unwrap();
                assert!(result.event_batches.len() >= 2);
                for batch in &result.event_batches {
                    for event in &batch.events {
                        assert_eq!(event.event_type_major, 1);
                    }
                }

                // Test 4: Combine filters
                let filters = ReadFilters::new(1)
                    .include_client_id(100)
                    .include_event_types(vec![2]);
                let result = writer.maybe_read_cached_events(None, &filters, None).unwrap();
                assert_eq!(result.event_batches.len(), 2);
                for batch in &result.event_batches {
                    assert_eq!(batch.client_id, 100);
                    for event in &batch.events {
                        assert_eq!(event.event_type_major, 2);
                    }
                }
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_cache_miss_for_older_data() {
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

                // Write batches 1-10
                for i in 1..=10 {
                    write_batch_with_params(
                        &aggregates_cache,
                        &aggregate_key,
                        100,
                        i * 10,
                        2,
                        i,
                        1000 * i,
                        1,
                    )
                    .await;
                }

                // Dispose of aggregate_resources to clear cache
                aggregates_cache.pop(&aggregate_key).await.unwrap();

                // Write batches 11-20 with a new writer instance
                for i in 11..=20 {
                    write_batch_with_params(
                        &aggregates_cache,
                        &aggregate_key,
                        100,
                        i * 10,
                        2,
                        i,
                        1000 * i,
                        1,
                    )
                    .await;
                }

                // Attempt to read from batch 5 - should get cache miss
                let aggregate_resources = aggregates_cache.get_aggregate_resources(&aggregate_key);
                let writer = aggregate_resources.get_writer(true).await.unwrap();

                let filters = ReadFilters::new(5);
                let result = writer.maybe_read_cached_events(None, &filters, None);

                match result {
                    Err(WriteError::CacheMiss {
                        missing_from_event_batch_index,
                        missing_to_event_batch_index,
                    }) => {
                        assert_eq!(missing_from_event_batch_index, 5);
                        // Cache should have batches 11-20, so missing range is 5-10
                        assert!(missing_to_event_batch_index.is_some());
                        assert_eq!(missing_to_event_batch_index.unwrap(), 10);
                    }
                    _ => panic!("Expected CacheMiss error for missing batches 5-10"),
                }
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_cache_miss_after_cache_is_cleared() {
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

                // Write some batches
                for i in 1..=5 {
                    write_batch_with_params(
                        &aggregates_cache,
                        &aggregate_key,
                        100,
                        i * 10,
                        2,
                        i,
                        1000 * i,
                        1,
                    )
                    .await;
                }

                let aggregate_resources = aggregates_cache.get_aggregate_resources(&aggregate_key);

                // Verify cache hit before clearing
                {
                    let writer = aggregate_resources.get_writer(true).await.unwrap();

                    let filters = ReadFilters::new(1);
                    let result = writer.maybe_read_cached_events(None, &filters, None);
                    assert!(result.is_ok());
                }

                // Manually clear the cache
                {
                    let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();

                    writer.data_cache.clear();
                    writer.total_cache_size_bytes = 0; // Reset cache size tracking

                    // Verify cache is cleared
                    assert_eq!(writer.data_cache.len(), 0);
                }

                // Attempt to read - should get cache miss
                {
                    let writer = aggregate_resources.get_writer(true).await.unwrap();

                    let filters = ReadFilters::new(1);
                    let result = writer.maybe_read_cached_events(None, &filters, None);

                    match result {
                        Err(WriteError::CacheMiss {
                            missing_from_event_batch_index,
                            missing_to_event_batch_index,
                        }) => {
                            assert_eq!(missing_from_event_batch_index, 1);
                            assert_eq!(missing_to_event_batch_index, None);
                        }
                        _ => panic!("Expected CacheMiss error after cache clear"),
                    }
                }
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_cache_trims_when_oversize() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let aggregate_read_config = AggregateReadConfig {
                    max_chunk_size: 1 << 20,
                };

                // Set a small cache size to trigger trimming
                let aggregate_write_config = AggregateWriteConfig {
                    max_data_cache_size_bytes: 5000, // Small cache
                    cache_trim_factor: 10,           // Aggressive trimming
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

                // Write many batches with large payloads to exceed cache size
                for i in 1..=20 {
                    let mut events = Vec::new();
                    for j in 0..5 {
                        events.push(EventItem::new(
                            (i - 1) * 5 + j + 1,
                            0,
                            None,
                            1000 * i + j,
                            1,
                            0,
                            vec![j as u8; 500], // 500 bytes each
                        ));
                    }

                    let mut write_request = WriteRequest {
                        client_id: 100,
                        compression_type: CompressionType::None,
                        enforce_client_idempotency: true,
                        expected_event_batch_index: Some(i),
                        user_id: None,
                        correlation_id: None,
                        aggregate_key: aggregate_key.clone(),
                        events,
                        allow_create: false,
                        durable_write_with_delay_us: None,
                    };

                    let aggregate_resources =
                        aggregates_cache.get_aggregate_resources(&aggregate_key);
                    let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();

                    writer
                        .queue_events_in_memory(0, 0, 1000 * i, &mut write_request)
                        .unwrap();

                    writer.sync_with_rollback(Rc::new(WatchedAggregates::new())).await.unwrap();
                }

                // Verify that cache was trimmed
                let aggregate_resources = aggregates_cache.get_aggregate_resources(&aggregate_key);
                let writer = aggregate_resources.get_writer(true).await.unwrap();

                // Cache should not have all 20 batches
                assert!(writer.data_cache.len() < 20);

                // Oldest batches should be evicted - reading from batch 1 should miss
                let filters = ReadFilters::new(1);
                let result = writer.maybe_read_cached_events(None, &filters, None);

                match result {
                    Err(WriteError::CacheMiss {
                        missing_from_event_batch_index,
                        missing_to_event_batch_index,
                    }) => {
                        assert_eq!(missing_from_event_batch_index, 1);
                        assert!(missing_to_event_batch_index.is_some());
                    }
                    _ => {
                        // If we got a result, verify it doesn't include batch 1
                        panic!("Expected cache miss for oldest batches");
                    }
                }

                // Recent batches should still be in cache
                let first_cached_index = writer
                    .data_cache
                    .front()
                    .unwrap()
                    .event_batch_item
                    .event_batch_index;
                let filters = ReadFilters::new(first_cached_index);
                let result = writer.maybe_read_cached_events(None, &filters, None);
                assert!(result.is_ok());
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_sync_rollback_on_failure() {
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
                write_batch_with_params(&aggregates_cache, &aggregate_key, 100, 1, 2, 1, 1000, 1)
                    .await;

                let aggregate_resources = aggregates_cache.get_aggregate_resources(&aggregate_key);

                // Queue a second batch but don't sync yet
                let events = vec![
                    EventItem::new(3, 0, None, 2000, 1, 0, vec![1; 50]),
                    EventItem::new(4, 0, None, 2001, 1, 0, vec![2; 50]),
                ];

                let mut write_request = WriteRequest {
                    client_id: 100,
                    compression_type: CompressionType::None,
                    enforce_client_idempotency: true,
                    expected_event_batch_index: Some(2),
                    user_id: None,
                    correlation_id: None,
                    aggregate_key,
                    events,
                    allow_create: false,
                    durable_write_with_delay_us: None,
                };

                {
                    let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();

                    // Record state before queueing
                    let next_event_index_before = writer.next_event_index;
                    let next_event_batch_index_before = writer.next_event_batch_index;
                    let client_event_index_before =
                        writer.client_event_indexes.get(&100).copied().unwrap_or(0);

                    writer
                        .queue_events_in_memory(0, 0, 2000, &mut write_request)
                        .unwrap();

                    // State should be updated after queuing
                    assert_ne!(writer.next_event_index, next_event_index_before);
                    assert_ne!(writer.next_event_batch_index, next_event_batch_index_before);

                    // Close the metadata file to simulate IO error on sync
                    let mut metadata_file = std::mem::replace(
                        &mut writer.metadata_dma_file,
                        Some(glommio::io::DmaFile::open("/dev/null").await.unwrap()),
                    );
                    metadata_file.take().unwrap().close().await.unwrap();

                    // Attempt sync - should fail and rollback
                    let sync_result = writer.sync_with_rollback(Rc::new(WatchedAggregates::new())).await;
                    assert!(sync_result.is_err());

                    // Verify state was rolled back
                    assert_eq!(writer.next_event_index, next_event_index_before);
                    assert_eq!(writer.next_event_batch_index, next_event_batch_index_before);
                    assert_eq!(
                        writer.client_event_indexes.get(&100).copied().unwrap(),
                        client_event_index_before
                    );
                }
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_update_max_data_cache_size_bytes() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let aggregate_read_config = AggregateReadConfig {
                    max_chunk_size: 1 << 20,
                };

                let aggregate_write_config = AggregateWriteConfig {
                    max_data_cache_size_bytes: 50000, // Start with larger cache
                    cache_trim_factor: 10,
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

                // Write several batches
                for i in 1..=10 {
                    write_batch_with_params(
                        &aggregates_cache,
                        &aggregate_key,
                        100,
                        i * 10,
                        5,
                        i,
                        1000 * i,
                        1,
                    )
                    .await;
                }

                let aggregate_resources = aggregates_cache.get_aggregate_resources(&aggregate_key);

                // Verify all batches are cached
                {
                    let writer = aggregate_resources.get_writer(true).await.unwrap();

                    assert_eq!(writer.data_cache.len(), 10);

                    let filters = ReadFilters::new(1);
                    let result = writer.maybe_read_cached_events(None, &filters, None);
                    assert!(result.is_ok());
                    assert_eq!(result.unwrap().event_batches.len(), 10);
                }

                // Dynamically reduce cache size
                {
                    let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();

                    writer.update_max_data_cache_size_bytes(3000); // Much smaller
                }

                // Verify cache was trimmed
                {
                    let writer = aggregate_resources.get_writer(true).await.unwrap();

                    // Cache should have fewer items now
                    assert!(writer.data_cache.len() < 10);

                    // Oldest batches should be evicted
                    let filters = ReadFilters::new(1);
                    let result = writer.maybe_read_cached_events(None, &filters, None);
                    match result {
                        Err(WriteError::CacheMiss { .. }) => {
                            // Expected - oldest batches were trimmed
                        }
                        Ok(res) => {
                            // If we got results, they shouldn't start from batch 1
                            assert!(res.event_batches[0].event_batch_index > 1);
                        }
                        _ => panic!("wrong branch"),
                    }

                    // Recent batches should still be cached
                    let first_cached = writer
                        .data_cache
                        .front()
                        .unwrap()
                        .event_batch_item
                        .event_batch_index;
                    let filters = ReadFilters::new(first_cached);
                    let result = writer.maybe_read_cached_events(None, &filters, None);
                    assert!(result.is_ok());
                }
            })
            .unwrap();
        handle.join().unwrap();
    }
}
