#[cfg(test)]
mod test_basic_read_write {
    use std::num::NonZeroUsize;

    use uuid::Uuid;

    use eventplanedb_structures::{
        aggregate_key::AggregateKey, event_item::EventItem, read_filters::ReadFilters,
    };
    use glommio::{LocalExecutorBuilder, Placement};

    use crate::{
        cache::aggregate_cache::AggregateCache,
        read_operations::{
            read_operations::ReadOperations,
            read_structures::{AggregateReadConfig, CacheableReadResult},
        },
        write_operations::{
            write_error::WriteError,
            write_operations::WriteOperations,
            write_structures::{AggregateWriteConfig, WriteOptions},
        },
    };

    fn check_read_1(read_result: &CacheableReadResult, event_id: u128, expected_cache_len: usize) {
        assert_eq!(read_result.filtered_event_batches.len(), 2);

        assert_eq!(read_result.filtered_event_batches[0].client_id, 123);
        assert_eq!(read_result.filtered_event_batches[0].event_batch_index, 1);
        assert_eq!(read_result.filtered_event_batches[0].server_timestamp, 998);
        assert_eq!(read_result.filtered_event_batches[0].user_id, None);
        assert_eq!(read_result.filtered_event_batches[0].events.len(), 2);

        assert_eq!(
            read_result.filtered_event_batches[0].events[0].client_event_index,
            45
        );
        assert_eq!(
            read_result.filtered_event_batches[0].events[0].event_id,
            None
        );
        assert_eq!(
            read_result.filtered_event_batches[0].events[0].event_index,
            1
        );
        assert_eq!(
            read_result.filtered_event_batches[0].events[0].event_timestamp,
            333
        );
        assert_eq!(
            read_result.filtered_event_batches[0].events[0].event_type_major,
            2
        );
        assert_eq!(
            read_result.filtered_event_batches[0].events[0].event_type_minor,
            3
        );
        assert_eq!(
            read_result.filtered_event_batches[0].events[0].event_value,
            vec![1, 2, 3, 4, 5].into()
        );

        assert_eq!(
            read_result.filtered_event_batches[0].events[1].client_event_index,
            46
        );
        assert_eq!(
            read_result.filtered_event_batches[0].events[1].event_id,
            None
        );
        assert_eq!(
            read_result.filtered_event_batches[0].events[1].event_index,
            2
        );
        assert_eq!(
            read_result.filtered_event_batches[0].events[1].event_timestamp,
            334
        );
        assert_eq!(
            read_result.filtered_event_batches[0].events[1].event_type_major,
            4
        );
        assert_eq!(
            read_result.filtered_event_batches[0].events[1].event_type_minor,
            0
        );
        assert_eq!(
            read_result.filtered_event_batches[0].events[1].event_value,
            vec![6, 7, 8, 9, 10].into()
        );

        assert_eq!(read_result.filtered_event_batches[1].client_id, 123);
        assert_eq!(read_result.filtered_event_batches[1].event_batch_index, 2);
        assert_eq!(read_result.filtered_event_batches[1].server_timestamp, 999);
        assert_eq!(read_result.filtered_event_batches[1].user_id, Some(34343));
        assert_eq!(read_result.filtered_event_batches[1].events.len(), 1);

        assert_eq!(
            read_result.filtered_event_batches[1].events[0].client_event_index,
            47
        );
        assert_eq!(
            read_result.filtered_event_batches[1].events[0].event_id,
            Some(event_id)
        );
        assert_eq!(
            read_result.filtered_event_batches[1].events[0].event_index,
            3
        );
        assert_eq!(
            read_result.filtered_event_batches[1].events[0].event_timestamp,
            339
        );
        assert_eq!(
            read_result.filtered_event_batches[1].events[0].event_type_major,
            2
        );
        assert_eq!(
            read_result.filtered_event_batches[1].events[0].event_type_minor,
            3
        );
        assert_eq!(
            read_result.filtered_event_batches[1].events[0].event_value,
            vec![11, 12, 13].into()
        );

        assert_eq!(read_result.next_event_batch_index, None);
        assert_eq!(read_result.uncached_metadata_set.len(), expected_cache_len);
    }

    fn check_read_2(read_result: &CacheableReadResult, expected_cache_len: usize) {
        assert_eq!(read_result.filtered_event_batches.len(), 0);
        assert_eq!(read_result.next_event_batch_index, None);
        assert_eq!(read_result.uncached_metadata_set.len(), expected_cache_len); //Not affected by filters
    }

    fn check_read_3(read_result: &CacheableReadResult, expected_cache_len: usize) {
        assert_eq!(read_result.filtered_event_batches.len(), 2);

        assert_eq!(read_result.filtered_event_batches[0].event_batch_index, 1);
        assert_eq!(read_result.filtered_event_batches[0].events.len(), 1);

        assert_eq!(
            read_result.filtered_event_batches[0].events[0].event_value,
            vec![6, 7, 8, 9, 10].into()
        );

        assert_eq!(read_result.filtered_event_batches[1].event_batch_index, 2);
        assert_eq!(read_result.filtered_event_batches[1].events.len(), 1);
        assert_eq!(
            read_result.filtered_event_batches[1].events[0].event_value,
            vec![11, 12, 13].into()
        );

        assert_eq!(read_result.next_event_batch_index, None);
        assert_eq!(read_result.uncached_metadata_set.len(), expected_cache_len);
    }

    #[test]
    fn basic_read_write_two_writers() {
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

                // Create the files and a writer
                let tempdir = tempfile::tempdir().unwrap();
                let data_root_folder = tempdir.path().to_str().unwrap();

                let aggregates_cache = AggregateCache::new(
                    NonZeroUsize::new(1000).unwrap(),
                    data_root_folder.to_string(),
                    aggregate_read_config,
                    aggregate_write_config,
                );
                let aggregate_key = AggregateKey::new(1, 1, 1);
                let aggregate_resources = aggregates_cache.get(&aggregate_key);

                {
                    // Write some event batches
                    let events = vec![
                        EventItem::new(45, 0, None, 333, 2, 3, vec![1, 2, 3, 4, 5]),
                        EventItem::new(46, 0, None, 334, 4, 0, vec![6, 7, 8, 9, 10]),
                    ];
                    let append_options = WriteOptions {
                        client_id: 123,
                        compression_type:
                            eventplanedb_structures::compression_type::CompressionType::None,
                        enforce_client_idempotency: true,
                        expected_event_batch_index: Some(1),
                        server_timestamp_millis: 998,
                        user_id: None,
                    };

                    let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();

                    let append_result = writer
                        .as_mut()
                        .unwrap()
                        .queue_events_in_memory(events, &append_options)
                        .unwrap();
                    assert_eq!(append_result.next_event_batch_index, 2);

                    //Write to disk
                    writer.as_mut().unwrap().sync_with_rollback().await.unwrap();
                }

                // Generate a guid and map it to u128
                let id = Uuid::new_v4();
                let event_id = id.as_u128();

                {
                    let events = vec![EventItem::new(
                        47,
                        0,
                        Some(event_id),
                        339,
                        2,
                        3,
                        vec![11, 12, 13],
                    )];
                    let append_options = WriteOptions {
                        client_id: 123,
                        compression_type:
                            eventplanedb_structures::compression_type::CompressionType::None,
                        enforce_client_idempotency: true,
                        expected_event_batch_index: Some(2),
                        server_timestamp_millis: 999,
                        user_id: Some(34343),
                    };

                    let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();
                    let append_result = writer
                        .as_mut()
                        .unwrap()
                        .queue_events_in_memory(events, &append_options)
                        .unwrap();
                    assert_eq!(append_result.next_event_batch_index, 3);

                    //Write to disk
                    writer.as_mut().unwrap().sync_with_rollback().await.unwrap();
                }

                let mut read_filters = ReadFilters::new(1);
                read_filters = read_filters.min_event_timestamp(334);

                let writer = aggregate_resources.get_writer(true).await.unwrap();
                let reader = aggregate_resources.get_reader(true).await.unwrap();
                let writer_ref = writer.as_ref().unwrap();
                let reader_ref = reader.as_ref().unwrap();

                let read_result = reader_ref
                    .read(
                        writer_ref.minimum_available_event_batch_index,
                        writer_ref.file_len_metadata,
                        writer_ref.file_len_event_batch,
                        &read_filters,
                        None
                    )
                    .await
                    .unwrap();
                check_read_3(&read_result, 2);
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn basic_read_write() {
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

                // Create the files and a writer
                let tempdir = tempfile::tempdir().unwrap();
                let data_root_folder = tempdir.path().to_str().unwrap();

                let aggregates_cache = AggregateCache::new(
                    NonZeroUsize::new(1000).unwrap(),
                    data_root_folder.to_string(),
                    aggregate_read_config,
                    aggregate_write_config,
                );
                let aggregate_key = AggregateKey::new(1, 1, 1);
                let aggregate_resources = aggregates_cache.get(&aggregate_key);

                // Write some event batches
                let events = vec![
                    EventItem::new(45, 0, None, 333, 2, 3, vec![1, 2, 3, 4, 5]),
                    EventItem::new(46, 0, None, 334, 4, 0, vec![6, 7, 8, 9, 10]),
                ];
                let append_options = WriteOptions {
                    client_id: 123,
                    compression_type:
                        eventplanedb_structures::compression_type::CompressionType::None,
                    enforce_client_idempotency: true,
                    expected_event_batch_index: Some(1),
                    server_timestamp_millis: 998,
                    user_id: None,
                };

                // Generate a guid and map it to u128
                let id = Uuid::new_v4();
                let event_id = id.as_u128();

                {
                    let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();
                    let append_result = writer
                        .as_mut()
                        .unwrap()
                        .queue_events_in_memory(events, &append_options)
                        .unwrap();
                    assert_eq!(append_result.next_event_batch_index, 2);

                    let events = vec![EventItem::new(
                        47,
                        0,
                        Some(event_id),
                        339,
                        2,
                        3,
                        vec![11, 12, 13],
                    )];
                    let append_options = WriteOptions {
                        client_id: 123,
                        compression_type:
                            eventplanedb_structures::compression_type::CompressionType::None,
                        enforce_client_idempotency: true,
                        expected_event_batch_index: Some(2),
                        server_timestamp_millis: 999,
                        user_id: Some(34343),
                    };
                    let append_result = writer
                        .as_mut()
                        .unwrap()
                        .queue_events_in_memory(events, &append_options)
                        .unwrap();
                    assert_eq!(append_result.next_event_batch_index, 3);
                }

                let mut read_filters = ReadFilters::new(1);
                {
                    let writer = aggregate_resources.get_writer(true).await.unwrap();
                    let cache_read_attempt = writer
                        .as_ref()
                        .unwrap()
                        .maybe_read_cached_events(&read_filters, None)
                        .expect_err("Cache did not miss");

                    match cache_read_attempt {
                        WriteError::CacheMiss {
                            missing_from_event_batch_index,
                            missing_to_event_batch_index,
                        } => {
                            assert_eq!(missing_from_event_batch_index, 1);
                            assert_eq!(missing_to_event_batch_index, None);
                        }
                        _ => unreachable!(),
                    }
                }
                {
                    //Write to disk
                    let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();
                    writer.as_mut().unwrap().sync_with_rollback().await.unwrap();
                }

                {
                    //Now that we have sync success, it should be in the writer cache
                    let writer = aggregate_resources.get_writer(true).await.unwrap();
                    let cache_read = writer
                        .as_ref()
                        .unwrap()
                        .maybe_read_cached_events(&read_filters, None)
                        .unwrap();
                    check_read_1(&cache_read, event_id, 0);
                }

                let writer = aggregate_resources.get_writer(true).await.unwrap();
                let writer_ref = writer.as_ref().unwrap();

                let read_result = {
                    let reader = aggregate_resources.get_reader(true).await.unwrap();
                    let reader_ref = reader.as_ref().unwrap();

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

                    check_read_1(&read_result, event_id, 2);

                    //Basic filter on metadata
                    read_filters = read_filters.exclude_client_id(123);
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
                    check_read_2(&read_result, 2);

                    let cache_read = writer_ref.maybe_read_cached_events(&read_filters, None).unwrap();
                    check_read_2(&cache_read, 0);

                    //Basic filter on event batches
                    let mut read_filters = ReadFilters::new(1);
                    read_filters = read_filters.min_event_timestamp(334);
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

                    check_read_3(&read_result, 2);

                    let cache_result = writer_ref.maybe_read_cached_events(&read_filters, None).unwrap();
                    check_read_3(&cache_result, 0);

                    read_result
                };

                {
                    //Let's update the cache now and check cache is used for metadata
                    let mut reader2 = aggregate_resources.get_reader_mut(true).await.unwrap();
                    let reader2_ref = reader2.as_mut().unwrap();
                    reader2_ref.update_metadata_cache(read_result.uncached_metadata_set.clone());
                }

                {
                    //Cache should be idempotent
                    let mut reader = aggregate_resources.get_reader_mut(true).await.unwrap();
                    let reader_ref = reader.as_mut().unwrap();
                    reader_ref.update_metadata_cache(read_result.uncached_metadata_set);
                    let mut read_filters = ReadFilters::new(1);
                    read_filters = read_filters.min_event_timestamp(334);
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
                    check_read_3(&read_result, 0);
                }
            })
            .unwrap();
        handle.join().unwrap();
    }
}
