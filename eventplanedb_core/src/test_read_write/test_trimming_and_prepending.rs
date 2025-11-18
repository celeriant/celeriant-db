#[cfg(test)]
mod test_trimming_and_prepending {
    use std::num::NonZeroUsize;

    use eventplanedb_structures::{
        aggregate_key::AggregateKey, compression_type::CompressionType,
        event_batch_item::EventBatchItem, event_item::EventItem, read_filters::ReadFilters,
    };
    use glommio::{LocalExecutorBuilder, Placement};

    use crate::{
        cache::aggregate_cache::AggregateCache,
        read_operations::{
            read_error::ReadError, read_operations::ReadOperations,
            read_structures::AggregateReadConfig,
        },
        test_read_write::test_helpers::{create_test_aggregate_key, create_test_events},
        write_operations::{
            write_error::WriteError, write_operations::WriteOperations,
            write_structures::{AggregateWriteConfig, WriteOptions},
        },
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
    ) {
        let events = create_test_events(start_client_event_index, event_count, base_timestamp);

        let append_options = WriteOptions {
            client_id,
            compression_type: CompressionType::None,
            enforce_client_idempotency: true,
            expected_event_batch_index: Some(expected_batch_index),
            server_timestamp_millis: base_timestamp,
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

    /// Helper to create event batches for prepending (without writing to disk)
    fn create_prepend_batches(
        start_batch_index: u64,
        batch_count: usize,
        client_id: u128,
        events_per_batch: usize,
    ) -> Vec<EventBatchItem> {
        let mut batches = Vec::new();

        for i in 0..batch_count {
            let batch_index = start_batch_index + i as u64;
            let client_event_index_start = batch_index * 10; // Space them out
            let server_timestamp = 1000 + (batch_index * 100);

            let events = create_test_events(
                client_event_index_start,
                events_per_batch,
                server_timestamp,
            );

            // Set event indexes manually for prepended batches
            let mut events_with_indexes = events;
            for (j, event) in events_with_indexes.iter_mut().enumerate() {
                event.event_index = (batch_index - 1) * events_per_batch as u64 + j as u64 + 1;
            }

            batches.push(EventBatchItem::new(
                batch_index,
                server_timestamp,
                client_id,
                None,
                events_with_indexes,
            ));
        }

        batches
    }

    #[test]
    fn test_trim_start_successfully_removes_data() {
        // Integration test: Verify that trim_start removes old batches and updates minimum_available_event_batch_index
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
                
                let aggregate_resources = aggregates_cache.get(&aggregate_key);

                let first_two_batches = {
                    let writer = aggregate_resources.get_writer(true).await.unwrap();
                    let writer_ref = writer.as_ref().unwrap();
                    // Let's get the first two batches from the writer cache to add back later
                    let filters = ReadFilters::new(1).to_event_batch_index(2);
                    writer_ref.maybe_read_cached_events(&filters, None).unwrap()
                };

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
                    writer_ref.trim_start(
                            3,
                            file_positions.metadata_position,
                            file_positions.event_batch_position,
                        )
                        .await
                        .unwrap();

                    // Verify writer cache is now gone
                    assert_eq!(writer_ref.data_cache.len(), 0);

                    // Update reader with new file handles
                    let mut reader = aggregate_resources.get_reader_mut(true).await.unwrap();
                    
                    reader.as_mut().unwrap().trim_start(
                        writer_ref.metadata_dma_file.dup().unwrap(),
                        writer_ref.event_batches_dma_file.dup().unwrap(),
                    );
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

                    assert_eq!(read_result.filtered_event_batches.len(), 3);
                    assert_eq!(
                        read_result.filtered_event_batches[0].event_batch_index,
                        3
                    );
                    assert_eq!(
                        read_result.filtered_event_batches[1].event_batch_index,
                        4
                    );
                    assert_eq!(
                        read_result.filtered_event_batches[2].event_batch_index,
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

                    let write_options = WriteOptions {
                        client_id: 76765,
                        compression_type: CompressionType::Snappy,
                        enforce_client_idempotency: true,
                        expected_event_batch_index: Some(6),
                        server_timestamp_millis: 8779,
                        user_id: None,
                    };
                    writer_ref.queue_events_in_memory(events, &write_options).unwrap();

                    writer_ref.sync_with_rollback().await.unwrap();
                }

                {
                    let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();
                    let mut reader = aggregate_resources.get_reader_mut(true).await.unwrap();
                    let reader_ref = reader.as_mut().unwrap();
                    let writer_ref = writer.as_mut().unwrap();
                    //Now lets try and prepend back the trimmed batches
                    writer_ref.prepend_batches(CompressionType::Snappy, &first_two_batches.filtered_event_batches).await.unwrap();
                    reader.as_mut().unwrap().trim_start(
                        writer_ref.metadata_dma_file.dup().unwrap(),
                        writer_ref.event_batches_dma_file.dup().unwrap(),
                    );
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

                    assert_eq!(read_result.filtered_event_batches.len(), 6);
                    assert_eq!(
                        read_result.filtered_event_batches[0].event_batch_index,
                        1
                    );
                    assert_eq!(
                        read_result.filtered_event_batches[5].event_batch_index,
                        6
                    );
                }
            })
            .unwrap();
        handle.join().unwrap();
    }


    // #[test]
    // fn test_trim_end_removes_recent_data() {
    //     // Integration test: Verify that trim_end removes recent batches from the end
    //     let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
    //         .spawn(|| async move {
    //             let aggregate_read_config = AggregateReadConfig {
    //                 max_chunk_size: 1 << 20,
    //                 max_data_cache_size_bytes: 1 << 20,
    //             };

    //             let aggregate_write_config = AggregateWriteConfig {
    //                 max_data_cache_size_bytes: 1 << 25,
    //                 cache_trim_factor: 25,
    //                 max_chunk_size: 1 << 20,
    //             };

    //             let tempdir = tempfile::tempdir().unwrap();
    //             let data_root_folder = tempdir.path().to_str().unwrap();

    //             let aggregates_cache = AggregateCache::new(
    //                 NonZeroUsize::new(1000).unwrap(),
    //                 data_root_folder.to_string(),
    //                 aggregate_read_config,
    //                 aggregate_write_config,
    //             );
    //             let aggregate_key = create_test_aggregate_key();

    //             // Write 4 batches
    //             for i in 1..=4 {
    //                 write_batch_with_params(
    //                     &aggregates_cache,
    //                     &aggregate_key,
    //                     123,
    //                     i * 10,
    //                     2,
    //                     i,
    //                     1000 + i * 100,
    //                 )
    //                 .await;
    //             }

    //             let aggregate_resources = aggregates_cache.get(&aggregate_key);

    //             // Calculate positions to keep only first 2 batches
    //             let reader = aggregate_resources.get_reader(true).await.unwrap();
    //             let writer = aggregate_resources.get_writer(true).await.unwrap();
    //             let reader_ref = reader.as_ref().unwrap();
    //             let writer_ref = writer.as_ref().unwrap();

    //             let file_positions = reader_ref
    //                 .get_file_positions(
    //                     writer_ref.minimum_available_event_batch_index,
    //                     3, // Keep up to (but not including) batch 3
    //                     writer_ref.file_len_metadata,
    //                     writer_ref.file_len_event_batch,
    //                 )
    //                 .await
    //                 .unwrap();

    //             drop(reader);
    //             drop(writer);

    //             // Perform trim_end
    //             let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();
    //             writer
    //                 .as_mut()
    //                 .unwrap()
    //                 .trim_end(
    //                     file_positions.metadata_position,
    //                     file_positions.event_batch_position,
    //                 )
    //                 .await
    //                 .unwrap();

    //             drop(writer);

    //             // Verify only first 2 batches remain
    //             let writer = aggregate_resources.get_writer(true).await.unwrap();
    //             let reader = aggregate_resources.get_reader(true).await.unwrap();
    //             let reader_ref = reader.as_ref().unwrap();
    //             let writer_ref = writer.as_ref().unwrap();

    //             let read_filters = ReadFilters::new(1);
    //             let read_result = reader_ref
    //                 .read(
    //                     writer_ref.minimum_available_event_batch_index,
    //                     writer_ref.file_len_metadata,
    //                     writer_ref.file_len_event_batch,
    //                     &read_filters,
    //                     None,
    //                 )
    //                 .await
    //                 .unwrap();

    //             assert_eq!(read_result.filtered_event_batches.len(), 2);
    //             assert_eq!(
    //                 read_result.filtered_event_batches[0].event_batch_index,
    //                 1
    //             );
    //             assert_eq!(
    //                 read_result.filtered_event_batches[1].event_batch_index,
    //                 2
    //             );
    //         })
    //         .unwrap();
    //     handle.join().unwrap();
    // }

    // #[test]
    // fn test_prepend_successfully_adds_older_data() {
    //     // Integration test: Prepending contiguous older batches should make them readable
    //     let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
    //         .spawn(|| async move {
    //             let aggregate_read_config = AggregateReadConfig {
    //                 max_chunk_size: 1 << 20,
    //                 max_data_cache_size_bytes: 1 << 20,
    //             };

    //             let aggregate_write_config = AggregateWriteConfig {
    //                 max_data_cache_size_bytes: 1 << 25,
    //                 cache_trim_factor: 25,
    //                 max_chunk_size: 1 << 20,
    //             };

    //             let tempdir = tempfile::tempdir().unwrap();
    //             let data_root_folder = tempdir.path().to_str().unwrap();

    //             let aggregates_cache = AggregateCache::new(
    //                 NonZeroUsize::new(1000).unwrap(),
    //                 data_root_folder.to_string(),
    //                 aggregate_read_config,
    //                 aggregate_write_config,
    //             );
    //             let aggregate_key = create_test_aggregate_key();

    //             // Write batches 10-12
    //             for i in 10..=12 {
    //                 write_batch_with_params(
    //                     &aggregates_cache,
    //                     &aggregate_key,
    //                     123,
    //                     i * 10,
    //                     2,
    //                     i,
    //                     1000 + i * 100,
    //                 )
    //                 .await;
    //             }

    //             let aggregate_resources = aggregates_cache.get(&aggregate_key);

    //             // Create batches 7-9 for prepending
    //             let prepend_batches = create_prepend_batches(7, 3, 123, 2);

    //             // Prepend batches
    //             let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();
    //             writer
    //                 .as_mut()
    //                 .unwrap()
    //                 .prepend_batches(CompressionType::None, &prepend_batches)
    //                 .await
    //                 .unwrap();

    //             drop(writer);

    //             // Update reader with new file handles
    //             {
    //                 let writer = aggregate_resources.get_writer(true).await.unwrap();
    //                 let mut reader = aggregate_resources.get_reader_mut(true).await.unwrap();
    //                 let writer_ref = writer.as_ref().unwrap();
    //                 reader.as_mut().unwrap().trim_start(
    //                     writer_ref.metadata_dma_file.dup().unwrap(),
    //                     writer_ref.event_batches_dma_file.dup().unwrap(),
    //                 );
    //             }

    //             // Verify we can now read batches 7-12
    //             let writer = aggregate_resources.get_writer(true).await.unwrap();
    //             let reader = aggregate_resources.get_reader(true).await.unwrap();
    //             let reader_ref = reader.as_ref().unwrap();
    //             let writer_ref = writer.as_ref().unwrap();

    //             let read_filters = ReadFilters::new(7);
    //                             let read_result = reader_ref
    //                 .read(
    //                     writer_ref.minimum_available_event_batch_index,
    //                     writer_ref.file_len_metadata,
    //                     writer_ref.file_len_event_batch,
    //                     &read_filters,
    //                     None,
    //                 )
    //                 .await
    //                 .unwrap();

    //             assert_eq!(read_result.filtered_event_batches.len(), 6);
    //             assert_eq!(
    //                 read_result.filtered_event_batches[0].event_batch_index,
    //                 7
    //             );
    //             assert_eq!(
    //                 read_result.filtered_event_batches[5].event_batch_index,
    //                 12
    //             );
    //             assert_eq!(writer_ref.minimum_available_event_batch_index, 7);
    //         })
    //         .unwrap();
    //     handle.join().unwrap();
    // }

    // #[test]
    // fn test_prepend_with_index_gap_fails() {
    //     // Unit test: Attempting to prepend batches that create a gap should return PrependCreatesEventBatchIndexGap error
    //     let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
    //         .spawn(|| async move {
    //             let aggregate_read_config = AggregateReadConfig {
    //                 max_chunk_size: 1 << 20,
    //                 max_data_cache_size_bytes: 1 << 20,
    //             };

    //             let aggregate_write_config = AggregateWriteConfig {
    //                 max_data_cache_size_bytes: 1 << 25,
    //                 cache_trim_factor: 25,
    //                 max_chunk_size: 1 << 20,
    //             };

    //             let tempdir = tempfile::tempdir().unwrap();
    //             let data_root_folder = tempdir.path().to_str().unwrap();

    //             let aggregates_cache = AggregateCache::new(
    //                 NonZeroUsize::new(1000).unwrap(),
    //                 data_root_folder.to_string(),
    //                 aggregate_read_config,
    //                 aggregate_write_config,
    //             );
    //             let aggregate_key = create_test_aggregate_key();

    //             // Write batches 10-12
    //             for i in 10..=12 {
    //                 write_batch_with_params(
    //                     &aggregates_cache,
    //                     &aggregate_key,
    //                     123,
    //                     i * 10,
    //                     2,
    //                     i,
    //                     1000 + i * 100,
    //                 )
    //                 .await;
    //             }

    //             let aggregate_resources = aggregates_cache.get(&aggregate_key);

    //             // Try to prepend batches 5-7 (creates a gap: 5,6,7 -> gap -> 10,11,12)
    //             let prepend_batches = create_prepend_batches(5, 3, 123, 2);

    //             let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();
    //             let result = writer
    //                 .as_mut()
    //                 .unwrap()
    //                 .prepend_batches(CompressionType::None, &prepend_batches)
    //                 .await;

    //             match result {
    //                 Err(WriteError::PrependCreatesEventBatchIndexGap {
    //                     provided_last_batch_index,
    //                     current_first_event_batch_index,
    //                 }) => {
    //                     assert_eq!(provided_last_batch_index, 7);
    //                     assert_eq!(current_first_event_batch_index, 10);
    //                 }
    //                 _ => panic!("Expected PrependCreatesEventBatchIndexGap error"),
    //             }
    //         })
    //         .unwrap();
    //     handle.join().unwrap();
    // }

    // #[test]
    // fn test_prepend_with_non_contiguous_data_fails() {
    //     // Unit test: Attempting to prepend non-contiguous batches should return PrependNonContiguousBatches error
    //     let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
    //         .spawn(|| async move {
    //             let aggregate_read_config = AggregateReadConfig {
    //                 max_chunk_size: 1 << 20,
    //                 max_data_cache_size_bytes: 1 << 20,
    //             };

    //             let aggregate_write_config = AggregateWriteConfig {
    //                 max_data_cache_size_bytes: 1 << 25,
    //                 cache_trim_factor: 25,
    //                 max_chunk_size: 1 << 20,
    //             };

    //             let tempdir = tempfile::tempdir().unwrap();
    //             let data_root_folder = tempdir.path().to_str().unwrap();

    //             let aggregates_cache = AggregateCache::new(
    //                 NonZeroUsize::new(1000).unwrap(),
    //                 data_root_folder.to_string(),
    //                 aggregate_read_config,
    //                 aggregate_write_config,
    //             );
    //             let aggregate_key = create_test_aggregate_key();

    //             // Write batches 10-12
    //             for i in 10..=12 {
    //                 write_batch_with_params(
    //                     &aggregates_cache,
    //                     &aggregate_key,
    //                     123,
    //                     i * 10,
    //                     2,
    //                     i,
    //                     1000 + i * 100,
    //                 )
    //                 .await;
    //             }

    //             let aggregate_resources = aggregates_cache.get(&aggregate_key);

    //             // Create non-contiguous batches: 7, 8, then skip to 10 (missing 9)
    //             let mut prepend_batches = create_prepend_batches(7, 2, 123, 2);
    //             let batch_10 = create_prepend_batches(10, 1, 123, 2);
    //             prepend_batches.extend(batch_10);

    //             let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();
    //             let result = writer
    //                 .as_mut()
    //                 .unwrap()
    //                 .prepend_batches(CompressionType::None, &prepend_batches)
    //                 .await;

    //             match result {
    //                 Err(WriteError::PrependNonContiguousBatches {
    //                     from_event_batch_index,
    //                     to_event_batch_index,
    //                 }) => {
    //                     assert_eq!(from_event_batch_index, 8);
    //                     assert_eq!(to_event_batch_index, 10);
    //                 }
    //                 _ => panic!("Expected PrependNonContiguousBatches error"),
    //             }
    //         })
    //         .unwrap();
    //     handle.join().unwrap();
    // }

    // #[test]
    // fn test_get_file_positions_accurate() {
    //     // Integration test: Verify that get_file_positions correctly calculates byte offsets for trim operation
    //     let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
    //         .spawn(|| async move {
    //             let aggregate_read_config = AggregateReadConfig {
    //                 max_chunk_size: 1 << 20,
    //                 max_data_cache_size_bytes: 1 << 20,
    //             };

    //             let aggregate_write_config = AggregateWriteConfig {
    //                 max_data_cache_size_bytes: 1 << 25,
    //                 cache_trim_factor: 25,
    //                 max_chunk_size: 1 << 20,
    //             };

    //             let tempdir = tempfile::tempdir().unwrap();
    //             let data_root_folder = tempdir.path().to_str().unwrap();

    //             let aggregates_cache = AggregateCache::new(
    //                 NonZeroUsize::new(1000).unwrap(),
    //                 data_root_folder.to_string(),
    //                 aggregate_read_config,
    //                 aggregate_write_config,
    //             );
    //             let aggregate_key = create_test_aggregate_key();

    //             // Write 5 batches
    //             for i in 1..=5 {
    //                 write_batch_with_params(
    //                     &aggregates_cache,
    //                     &aggregate_key,
    //                     123,
    //                     i * 10,
    //                     3,
    //                     i,
    //                     1000 + i * 100,
    //                 )
    //                 .await;
    //             }

    //             let aggregate_resources = aggregates_cache.get(&aggregate_key);

    //             // Get file positions to keep from batch 3 onwards
    //             let reader = aggregate_resources.get_reader(true).await.unwrap();
    //             let writer = aggregate_resources.get_writer(true).await.unwrap();
    //             let reader_ref = reader.as_ref().unwrap();
    //             let writer_ref = writer.as_ref().unwrap();

    //             let file_positions = reader_ref
    //                 .get_file_positions(
    //                     writer_ref.minimum_available_event_batch_index,
    //                     3,
    //                     writer_ref.file_len_metadata,
    //                     writer_ref.file_len_event_batch,
    //                 )
    //                 .await
    //                 .unwrap();

    //             // Verify metadata position is correct (2 batches * METADATA_BATCH_SIZE_BYTES)
    //             use eventplanedb_structures::constants::METADATA_BATCH_SIZE_BYTES;
    //             assert_eq!(
    //                 file_positions.metadata_position,
    //                 2 * METADATA_BATCH_SIZE_BYTES as u64
    //             );

    //             // Verify event_batch_position by reading metadata for first 2 batches
    //             let read_filters = ReadFilters::new(1).to_event_batch_index(2);
    //             let read_result = reader_ref
    //                 .read(
    //                     writer_ref.minimum_available_event_batch_index,
    //                     writer_ref.file_len_metadata,
    //                     writer_ref.file_len_event_batch,
    //                     &read_filters,
    //                     None,
    //                 )
    //                 .await
    //                 .unwrap();

    //             // Calculate expected event batch position
    //             let mut expected_event_batch_pos = 0u64;
    //             for metadata in read_result.uncached_metadata_set.iter() {
    //                 expected_event_batch_pos += metadata.event_batch_metadata.compressed_size;
    //             }

    //             assert_eq!(
    //                 file_positions.event_batch_position,
    //                 expected_event_batch_pos
    //             );

    //             // Perform the trim and verify it works correctly
    //             drop(reader);
    //             drop(writer);

    //             let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();
    //             writer
    //                 .as_mut()
    //                 .unwrap()
    //                 .trim_start(
    //                     file_positions.metadata_position,
    //                     file_positions.event_batch_position,
    //                 )
    //                 .await
    //                 .unwrap();

    //             let mut reader = aggregate_resources.get_reader_mut(true).await.unwrap();
    //             let writer_ref = writer.as_ref().unwrap();
    //             reader.as_mut().unwrap().trim_start(
    //                 writer_ref.metadata_dma_file.dup().unwrap(),
    //                 writer_ref.event_batches_dma_file.dup().unwrap(),
    //             );

    //             writer.as_mut().unwrap().minimum_available_event_batch_index = 3;

    //             drop(writer);
    //             drop(reader);

    //             // Verify we can now only read batches 3-5
    //             let writer = aggregate_resources.get_writer(true).await.unwrap();
    //             let reader = aggregate_resources.get_reader(true).await.unwrap();
    //             let reader_ref = reader.as_ref().unwrap();
    //             let writer_ref = writer.as_ref().unwrap();

    //             let read_filters = ReadFilters::new(3);
    //             let read_result = reader_ref
    //                 .read(
    //                     writer_ref.minimum_available_event_batch_index,
    //                     writer_ref.file_len_metadata,
    //                     writer_ref.file_len_event_batch,
    //                     &read_filters,
    //                     None,
    //                 )
    //                 .await
    //                 .unwrap();

    //             assert_eq!(read_result.filtered_event_batches.len(), 3);
    //             assert_eq!(
    //                 read_result.filtered_event_batches[0].event_batch_index,
    //                 3
    //             );
    //         })
    //         .unwrap();
    //     handle.join().unwrap();
    // }

    // #[test]
    // fn test_trim_start_with_zero_bytes_is_noop() {
    //     // Unit test: Calling trim_start with 0 bytes should be a no-op
    //     let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
    //         .spawn(|| async move {
    //             let aggregate_read_config = AggregateReadConfig {
    //                 max_chunk_size: 1 << 20,
    //                 max_data_cache_size_bytes: 1 << 20,
    //             };

    //             let aggregate_write_config = AggregateWriteConfig {
    //                 max_data_cache_size_bytes: 1 << 25,
    //                 cache_trim_factor: 25,
    //                 max_chunk_size: 1 << 20,
    //             };

    //             let tempdir = tempfile::tempdir().unwrap();
    //             let data_root_folder = tempdir.path().to_str().unwrap();

    //             let aggregates_cache = AggregateCache::new(
    //                 NonZeroUsize::new(1000).unwrap(),
    //                 data_root_folder.to_string(),
    //                 aggregate_read_config,
    //                 aggregate_write_config,
    //             );
    //             let aggregate_key = create_test_aggregate_key();

    //             // Write 2 batches
    //             for i in 1..=2 {
    //                 write_batch_with_params(
    //                     &aggregates_cache,
    //                     &aggregate_key,
    //                     123,
    //                     i * 10,
    //                     2,
    //                     i,
    //                     1000 + i * 100,
    //                 )
    //                 .await;
    //             }

    //             let aggregate_resources = aggregates_cache.get(&aggregate_key);

    //             // Get initial file lengths
    //             let writer = aggregate_resources.get_writer(true).await.unwrap();
    //             let initial_metadata_len = writer.as_ref().unwrap().file_len_metadata;
    //             let initial_event_batch_len = writer.as_ref().unwrap().file_len_event_batch;
    //             drop(writer);

    //             // Call trim_start with 0 bytes
    //             let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();
    //             writer.as_mut().unwrap().trim_start(0, 0).await.unwrap();

    //             // Verify file lengths are unchanged
    //             assert_eq!(
    //                 writer.as_ref().unwrap().file_len_metadata,
    //                 initial_metadata_len
    //             );
    //             assert_eq!(
    //                 writer.as_ref().unwrap().file_len_event_batch,
    //                 initial_event_batch_len
    //             );

    //             drop(writer);

    //             // Verify we can still read both batches
    //             let writer = aggregate_resources.get_writer(true).await.unwrap();
    //             let reader = aggregate_resources.get_reader(true).await.unwrap();
    //             let reader_ref = reader.as_ref().unwrap();
    //             let writer_ref = writer.as_ref().unwrap();

    //             let read_filters = ReadFilters::new(1);
    //             let read_result = reader_ref
    //                 .read(
    //                     writer_ref.minimum_available_event_batch_index,
    //                     writer_ref.file_len_metadata,
    //                     writer_ref.file_len_event_batch,
    //                     &read_filters,
    //                     None,
    //                 )
    //                 .await
    //                 .unwrap();

    //             assert_eq!(read_result.filtered_event_batches.len(), 2);
    //         })
    //         .unwrap();
    //     handle.join().unwrap();
    // }

    // #[test]
    // fn test_prepend_empty_list_fails() {
    //     // Unit test: Attempting to prepend an empty list should return EmptyEventsList error
    //     let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
    //         .spawn(|| async move {
    //             let aggregate_read_config = AggregateReadConfig {
    //                 max_chunk_size: 1 << 20,
    //                 max_data_cache_size_bytes: 1 << 20,
    //             };

    //             let aggregate_write_config = AggregateWriteConfig {
    //                 max_data_cache_size_bytes: 1 << 25,
    //                 cache_trim_factor: 25,
    //                 max_chunk_size: 1 << 20,
    //             };

    //             let tempdir = tempfile::tempdir().unwrap();
    //             let data_root_folder = tempdir.path().to_str().unwrap();

    //             let aggregates_cache = AggregateCache::new(
    //                 NonZeroUsize::new(1000).unwrap(),
    //                 data_root_folder.to_string(),
    //                 aggregate_read_config,
    //                 aggregate_write_config,
    //             );
    //             let aggregate_key = create_test_aggregate_key();

    //             // Write a batch
    //             write_batch_with_params(&aggregates_cache, &aggregate_key, 123, 10, 2, 1, 1000)
    //                 .await;

    //             let aggregate_resources = aggregates_cache.get(&aggregate_key);

    //             // Try to prepend empty list
    //             let empty_batches: Vec<EventBatchItem> = vec![];

    //             let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();
    //             let result = writer
    //                 .as_mut()
    //                 .unwrap()
    //                 .prepend_batches(CompressionType::None, &empty_batches)
    //                 .await;

    //             match result {
    //                 Err(WriteError::EmptyEventsList) => {
    //                     // Expected
    //                 }
    //                 _ => panic!("Expected EmptyEventsList error"),
    //             }
    //         })
    //         .unwrap();
    //     handle.join().unwrap();
    // }

}