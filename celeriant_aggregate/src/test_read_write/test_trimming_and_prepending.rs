#[cfg(test)]
mod test_trimming_and_prepending {
    use std::{num::NonZeroUsize, sync::Arc};

    use celeriant_disk::files::open_dma_files::existing_file_read_only_dma;
    use celeriant_msg::request::{read_filters::ReadFilters, requests::WriteRequest};
    use celeriant_wal::{aggregate_key::AggregateKey, compression_type::CompressionType, wal::event_item::EventItem};
    use glommio::{LocalExecutorBuilder, Placement};

    use crate::{
        cache::aggregate_cache::AggregateCache, node_config::test_node_config::test_config, read_operations::{
            read_error::ReadError, read_operations::ReadOperations,
            read_structures::AggregateReadConfig,
        }, write_operations::{
            write_error::WriteError, write_operations::WriteOperations,
            aggregate_write_config::{AggregateWriteConfig},
        }
    };

    /// Creates a test aggregate key with predictable values
    fn create_test_aggregate_key() -> AggregateKey {
        AggregateKey::new(1, 1, 1)
    }

    /// Creates test events with sequential indexes
    /// 
    /// # Parameters
    /// * `starting_client_event_index` - Base index for client event indexes
    /// * `count` - Number of events to create
    /// * `base_timestamp` - Base timestamp for events
    fn create_test_events(
        starting_client_event_index: u64,
        count: usize,
        base_timestamp: u64,
    ) -> Vec<EventItem> {
        let mut events = Vec::with_capacity(count);
        
        for i in 0..count {
            let client_event_index = starting_client_event_index + i as u64;
            
            events.push(EventItem {
                client_event_index,
                event_index: 0, // Will be set by writer
                event_id: Some((client_event_index as u128) << 64 | i as u128),
                event_timestamp: base_timestamp + i as u64,
                event_type_major: 1 + (i % 3) as u64, // Vary event types 1, 2, 3
                event_type_minor: 0,
                event_value: Arc::new(format!("test_event_{}", client_event_index).into_bytes()),
                iv: None,
            });
        }
        
        events
    }

    /// Helper to write a batch with specific parameters
    async fn write_batch_with_params(
        aggregates_cache: &AggregateCache,
        aggregate_key: &AggregateKey,
        client_id: u128,
        start_client_event_index: u64,
        event_count: usize,
        expected_batch_index: u64,
        base_timestamp: u64,
    ) {
        let events = create_test_events(start_client_event_index, event_count, base_timestamp);

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
            .as_mut()
            .unwrap()
            .queue_events_in_memory(0, 0,  base_timestamp, &mut write_request)
            .unwrap();

        writer.as_mut().unwrap().sync_with_rollback().await.unwrap();
    }

    #[test]
    fn test_trim_start_successfully_removes_data() {
        // Integration test: Verify that trim_start removes old batches and updates minimum_available_event_batch_index
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
                let aggregate_key = create_test_aggregate_key();

                // Write 5 batches
                for i in 1..=5 {
                    write_batch_with_params(
                        &aggregates_cache,
                        &aggregate_key,
                        123,
                        i * 10,
                        3,
                        i,
                        1000 + i * 100,
                    )
                    .await;
                }
                
                let aggregate_resources = aggregates_cache.get_aggregate_resources(&aggregate_key);

                let first_batch = {
                    let writer = aggregate_resources.get_writer(true).await.unwrap();
                    let writer_ref = writer.as_ref().unwrap();
                    // Let's get the first two batches from the writer cache to add back later
                    let filters = ReadFilters::new(1).to_event_batch_index(1);
                    writer_ref.maybe_read_cached_events(&filters, None).unwrap()
                };

                let first_three_batches = {
                    let writer = aggregate_resources.get_writer(true).await.unwrap();
                    let writer_ref = writer.as_ref().unwrap();
                    // Let's get the first two batches from the writer cache to add back later
                    let filters = ReadFilters::new(1).to_event_batch_index(3);
                    writer_ref.maybe_read_cached_events(&filters, None).unwrap()
                };

                let first_two_batches = {
                    let writer = aggregate_resources.get_writer(true).await.unwrap();
                    let writer_ref = writer.as_ref().unwrap();
                    // Let's get the first two batches from the writer cache to add back later
                    let filters = ReadFilters::new(1).to_event_batch_index(2);
                    writer_ref.maybe_read_cached_events(&filters, None).unwrap()
                };

                {
                    //Everything is cached
                    let writer = aggregate_resources.get_writer(true).await.unwrap();
                    let writer_ref = writer.as_ref().unwrap();
                    // Let's get the first two batches from the writer cache to add back later
                    let filters = ReadFilters::new(1);
                    writer_ref.maybe_read_cached_events(&filters, None).unwrap()
                };

                // Check we can do a read of all batches
                {
                    let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();
                    let reader = aggregate_resources.get_reader(true).await.unwrap();
                    let reader_ref = reader.as_ref().unwrap();
                    let writer_ref = writer.as_mut().unwrap();

                    assert_eq!(writer_ref.minimum_available_event_batch_index, 1);

                    // Should be able to read from batch 3
                    let read_filters = ReadFilters::new(1);
                    let read_result = reader_ref
                        .read(
                            writer_ref.minimum_available_event_batch_index,
                            writer_ref.file_len_metadata,
                            writer_ref.file_len_event_batch,
                            &read_filters,
                            None,
                        )
                        .await
                        .unwrap();

                    assert_eq!(read_result.event_batches.len(), 5);
                }

                let file_positions = {
                    // Get file positions to trim first 2 batches
                    let reader = aggregate_resources.get_reader(true).await.unwrap();
                    let writer = aggregate_resources.get_writer(true).await.unwrap();
                    let reader_ref = reader.as_ref().unwrap();
                    let writer_ref = writer.as_ref().unwrap();

                    reader_ref
                        .get_file_positions(
                            writer_ref.minimum_available_event_batch_index,
                            3, // Keep from batch 3 onwards
                            writer_ref.file_len_metadata,
                            writer_ref.file_len_event_batch,
                        )
                        .await
                        .unwrap()
                };

                {
                    // Perform trim_start
                    let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();
                    let writer_ref = writer.as_mut().unwrap();
                    let mut reader = aggregate_resources.get_reader_mut(true).await.unwrap();
                    let reader_ref = reader.as_mut().unwrap();

                    writer_ref.trim_start(
                            3, 
                            reader_ref.metadata_dma_file.as_ref().unwrap(), 
                            reader_ref.event_batches_dma_file.as_ref().unwrap(),
                            file_positions.metadata_position,
                            file_positions.event_batch_position,
                        )
                        .await
                        .unwrap();

                    // Verify writer cache now only has batches 3,4,5
                    assert_eq!(writer_ref.data_cache.len(), 3);
                    assert_eq!(writer_ref.data_cache.iter().last().unwrap().event_batch_item.event_batch_index, 5);
                    
                    let data_requirements = reader_ref.replace_dma_files(
                        existing_file_read_only_dma(&aggregate_resources.path_metadata).await.unwrap(),
                        existing_file_read_only_dma(&aggregate_resources.path_event_batches).await.unwrap(),
                    ).await.unwrap();

                    writer_ref.update_write_operations_data_requirements(data_requirements);
                }

                // Verify batches 1 and 2 are gone, 3-5 remain
                {
                    let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();
                    let reader = aggregate_resources.get_reader(true).await.unwrap();
                    let reader_ref = reader.as_ref().unwrap();
                    let writer_ref = writer.as_mut().unwrap();

                    assert_eq!(writer_ref.minimum_available_event_batch_index, 3);

                    // Should be able to read from batch 3
                    let read_filters = ReadFilters::new(3);
                    let read_result = reader_ref
                        .read(
                            writer_ref.minimum_available_event_batch_index,
                            writer_ref.file_len_metadata,
                            writer_ref.file_len_event_batch,
                            &read_filters,
                            None,
                        )
                        .await
                        .unwrap();

                    assert_eq!(read_result.event_batches.len(), 3);
                    assert_eq!(
                        read_result.event_batches[0].event_batch_index,
                        3
                    );
                    assert_eq!(
                        read_result.event_batches[1].event_batch_index,
                        4
                    );
                    assert_eq!(
                        read_result.event_batches[2].event_batch_index,
                        5
                    );

                    let read_filters = ReadFilters::new(2);
                    let result = reader_ref
                        .read(
                            writer_ref.minimum_available_event_batch_index,
                            writer_ref.file_len_metadata,
                            writer_ref.file_len_event_batch,
                            &read_filters,
                            None,
                        )
                        .await;

                    match result {
                        Err(ReadError::UnavailableBatchIndex {
                            minimum_available_event_batch_index,
                            requested_event_batch_index,
                        }) => {
                            assert_eq!(minimum_available_event_batch_index, 3);
                            assert_eq!(requested_event_batch_index, 2);
                        }
                        _ => panic!("Expected UnavailableBatchIndex error"),
                    }
                }

                {
                    let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();
                    let writer_ref = writer.as_mut().unwrap();

                    //Write some more events at the end
                    let events = create_test_events(99, 3, 8778);

                    let mut write_options = WriteRequest {
                        client_id: 76765,
                        compression_type: CompressionType::Snappy,
                        enforce_client_idempotency: true,
                        expected_event_batch_index: Some(6),
                        user_id: None,
                        correlation_id: None,
                        aggregate_key,
                        events,
                        allow_create: false,
                        durable_write_with_delay_us: None,
                    };
                    writer_ref.queue_events_in_memory(0, 0,  8779, &mut write_options).unwrap();

                    writer_ref.sync_with_rollback().await.unwrap();
                }

                {
                    let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();
                    let mut reader = aggregate_resources.get_reader_mut(true).await.unwrap();
                    let reader_ref = reader.as_mut().unwrap();
                    let writer_ref = writer.as_mut().unwrap();


                    //Now lets try and prepend back the trimmed batches
                    //First error condition - creating a gap
                    let err = writer_ref.prepend_batches(CompressionType::None, &first_batch.event_batches, reader_ref.metadata_dma_file.as_ref().unwrap(), reader_ref.event_batches_dma_file.as_ref().unwrap()).await.unwrap_err();
                    match err {
                        WriteError::PrependCreatesEventBatchIndexGap { 
                            provided_last_batch_index,
                            current_first_event_batch_index 
                        } => {
                            assert_eq!(provided_last_batch_index, 1);
                            assert_eq!(current_first_event_batch_index, 3);
                        }
                        _ => panic!("Expected PrependCreatesEventBatchIndexGap error, got {:?}", err),
                    }

                    //Second error condition - overlap
                    let err = writer_ref.prepend_batches(CompressionType::None, &first_three_batches.event_batches, reader_ref.metadata_dma_file.as_ref().unwrap(), reader_ref.event_batches_dma_file.as_ref().unwrap()).await.unwrap_err();
                    match err {
                        WriteError::PrependCreatesEventBatchIndexGap { 
                            provided_last_batch_index,
                            current_first_event_batch_index 
                        } => {
                            assert_eq!(provided_last_batch_index, 3);
                            assert_eq!(current_first_event_batch_index, 3);
                        }
                        _ => panic!("Expected PrependCreatesEventBatchIndexGap error, got {:?}", err),
                    }

                    writer_ref.prepend_batches(CompressionType::Snappy, &first_two_batches.event_batches, reader_ref.metadata_dma_file.as_ref().unwrap(), reader_ref.event_batches_dma_file.as_ref().unwrap()).await.unwrap();
                    let data_requirements = reader_ref.replace_dma_files(
                        existing_file_read_only_dma(&aggregate_resources.path_metadata).await.unwrap(),
                        existing_file_read_only_dma(&aggregate_resources.path_event_batches).await.unwrap(),
                    ).await.unwrap();

                    writer_ref.update_write_operations_data_requirements(data_requirements);
                }

                {
                    let writer = aggregate_resources.get_writer(true).await.unwrap();
                    let reader = aggregate_resources.get_reader(true).await.unwrap();
                    let reader_ref = reader.as_ref().unwrap();
                    let writer_ref = writer.as_ref().unwrap();

                    let read_filters = ReadFilters::new(1);
                    let read_result = reader_ref
                        .read(
                            writer_ref.minimum_available_event_batch_index,
                            writer_ref.file_len_metadata,
                            writer_ref.file_len_event_batch,
                            &read_filters,
                            None,
                        )
                        .await.unwrap();

                    assert_eq!(read_result.event_batches.len(), 6);
                    assert_eq!(
                        read_result.event_batches[0].event_batch_index,
                        1
                    );
                    assert_eq!(
                        read_result.event_batches[5].event_batch_index,
                        6
                    );
                }
            })
            .unwrap();
        handle.join().unwrap();
    }

}