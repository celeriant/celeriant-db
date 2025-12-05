#[cfg(test)]
mod test_basic_read_write {
    use std::num::NonZeroUsize;

    use celeriant_disk::files::open_dma_files::{create_and_write_only_dma, existing_file_read_only_dma};
    use celeriant_msg::{request::{read_filters::ReadFilters, requests::WriteRequest}, response::responses::ReadResponse};
    use celeriant_wal::{aggregate_key::AggregateKey, compression_type, wal::event_item::EventItem};
    use tempfile::tempdir_in;
    use uuid::Uuid;

    use glommio::{LocalExecutorBuilder, Placement};

    use crate::{
        cache::aggregate_cache::AggregateCache, node_config::{test_node_config::test_config}, read_operations::{
            read_operations::{ReadOperations, ReadOperationsWithDmaFiles},
            read_structures::AggregateReadConfig,
        }, write_operations::{
            write_error::WriteError,
            write_operations::{WriteOperations, WriteOperationsWithDmaFile},
            aggregate_write_config::{AggregateWriteConfig},
        }
    };

    fn check_read_1(read_result: &ReadResponse, event_id: u128) {
        assert_eq!(read_result.event_batches.len(), 2);

        assert_eq!(read_result.event_batches[0].client_id, 123);
        assert_eq!(read_result.event_batches[0].event_batch_index, 1);
        assert_eq!(read_result.event_batches[0].server_timestamp, 998);
        assert_eq!(read_result.event_batches[0].user_id, None);
        assert_eq!(read_result.event_batches[0].events.len(), 2);

        assert_eq!(
            read_result.event_batches[0].events[0].client_event_index,
            45
        );
        assert_eq!(
            read_result.event_batches[0].events[0].event_id,
            None
        );
        assert_eq!(
            read_result.event_batches[0].events[0].event_index,
            1
        );
        assert_eq!(
            read_result.event_batches[0].events[0].event_timestamp,
            333
        );
        assert_eq!(
            read_result.event_batches[0].events[0].event_type_major,
            2
        );
        assert_eq!(
            read_result.event_batches[0].events[0].event_type_minor,
            3
        );
        assert_eq!(
            read_result.event_batches[0].events[0].event_value,
            vec![1, 2, 3, 4, 5].into()
        );

        assert_eq!(
            read_result.event_batches[0].events[1].client_event_index,
            46
        );
        assert_eq!(
            read_result.event_batches[0].events[1].event_id,
            None
        );
        assert_eq!(
            read_result.event_batches[0].events[1].event_index,
            2
        );
        assert_eq!(
            read_result.event_batches[0].events[1].event_timestamp,
            334
        );
        assert_eq!(
            read_result.event_batches[0].events[1].event_type_major,
            4
        );
        assert_eq!(
            read_result.event_batches[0].events[1].event_type_minor,
            0
        );
        assert_eq!(
            read_result.event_batches[0].events[1].event_value,
            vec![6, 7, 8, 9, 10].into()
        );

        assert_eq!(read_result.event_batches[1].client_id, 123);
        assert_eq!(read_result.event_batches[1].event_batch_index, 2);
        assert_eq!(read_result.event_batches[1].server_timestamp, 999);
        assert_eq!(read_result.event_batches[1].user_id, Some(34343));
        assert_eq!(read_result.event_batches[1].events.len(), 1);

        assert_eq!(
            read_result.event_batches[1].events[0].client_event_index,
            47
        );
        assert_eq!(
            read_result.event_batches[1].events[0].event_id,
            Some(event_id)
        );
        assert_eq!(
            read_result.event_batches[1].events[0].event_index,
            3
        );
        assert_eq!(
            read_result.event_batches[1].events[0].event_timestamp,
            339
        );
        assert_eq!(
            read_result.event_batches[1].events[0].event_type_major,
            2
        );
        assert_eq!(
            read_result.event_batches[1].events[0].event_type_minor,
            3
        );
        assert_eq!(
            read_result.event_batches[1].events[0].event_value,
            vec![11, 12, 13].into()
        );

        assert_eq!(read_result.next_event_batch_index, None);
    }

    fn check_read_2(read_result: &ReadResponse) {
        assert_eq!(read_result.event_batches.len(), 0);
        assert_eq!(read_result.next_event_batch_index, None);
    }

    fn check_read_3(read_result: &ReadResponse) {
        assert_eq!(read_result.event_batches.len(), 2);

        assert_eq!(read_result.event_batches[0].event_batch_index, 1);
        assert_eq!(read_result.event_batches[0].events.len(), 1);

        assert_eq!(
            read_result.event_batches[0].events[0].event_value,
            vec![6, 7, 8, 9, 10].into()
        );

        assert_eq!(read_result.event_batches[1].event_batch_index, 2);
        assert_eq!(read_result.event_batches[1].events.len(), 1);
        assert_eq!(
            read_result.event_batches[1].events[0].event_value,
            vec![11, 12, 13].into()
        );

        assert_eq!(read_result.next_event_batch_index, None);
    }

    #[test]
    fn basic_read_write_two_writers() {
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

                // Create the files and a writer
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

                {
                    // Write some event batches
                    let events = vec![
                        EventItem::new(45, 0, None, 333, 2, 3, vec![1, 2, 3, 4, 5]),
                        EventItem::new(46, 0, None, 334, 4, 0, vec![6, 7, 8, 9, 10]),
                    ];
                    let mut write_request = WriteRequest {
                        client_id: 123,
                        compression_type:
                            compression_type::CompressionType::None,
                        enforce_client_idempotency: true,
                        expected_event_batch_index: Some(1),
                        user_id: None,
                        correlation_id: None,
                        aggregate_key: aggregate_key.clone(),
                        events,
                        allow_create: false,
                        durable_write_with_delay_us: Some(0),
                    };

                    let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();

                    let append_result = writer
                        .as_mut()
                        .unwrap()
                        .queue_events_in_memory(0, 0,  998, &mut write_request)
                        .unwrap();
                    assert_eq!(append_result.event_batch_index, 1);

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
                    let mut write_request = WriteRequest {
                        client_id: 123,
                        compression_type:
                            compression_type::CompressionType::None,
                        enforce_client_idempotency: true,
                        expected_event_batch_index: Some(2),
                        user_id: Some(34343),
                        correlation_id: None,
                        aggregate_key: aggregate_key.clone(),
                        events,
                        allow_create: false,
                        durable_write_with_delay_us: None,
                    };

                    let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();
                    let append_result = writer
                        .as_mut()
                        .unwrap()
                        .queue_events_in_memory(0, 0,  999, &mut write_request)
                        .unwrap();
                    assert_eq!(append_result.event_batch_index, 2);

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
                check_read_3(&read_result);
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn super_basic_dma_direct() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let aggregate_key = AggregateKey::new(1, 1, 1);
                let tempdir = tempdir_in(".").unwrap();
                let data_root_folder = tempdir.path().to_str().unwrap();
                let base_folder = format!(
                    "{}/{}/{}/{}",
                    data_root_folder,
                    aggregate_key.org_id,
                    aggregate_key.aggregate_type_id,
                    aggregate_key.aggregate_id
                );
                let path_metadata = format!("{}/metadata.bin", base_folder);

                std::fs::create_dir_all(&base_folder).unwrap();
                // std::fs::File::create(&path_metadata).unwrap();

                let writer_metadata_dma_file = create_and_write_only_dma(&path_metadata).await.unwrap();
                writer_metadata_dma_file.pre_allocate(512, false).await.unwrap();
                let reader_metadata_dma_file = existing_file_read_only_dma(&path_metadata).await.unwrap();

                let buffer_size = writer_metadata_dma_file.alignment();
                let mut buf = writer_metadata_dma_file.alloc_dma_buffer(buffer_size as usize);
                buf.as_bytes_mut()[0..5].copy_from_slice(b"hello");

                let _written = writer_metadata_dma_file.write_at(buf, 0).await.unwrap();
                writer_metadata_dma_file.fdatasync().await.unwrap();

                let read = reader_metadata_dma_file.read_at_aligned(0, 512).await.unwrap();
                assert_eq!(read.len(), 512);
                assert_eq!(
                    &read[0..5],
                    b"hello",
                    "{}",
                    String::from_utf8_lossy(&read[0..6])
                );

            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn super_basic_operators_direct() {
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
                let aggregate_key = AggregateKey::new(1, 1, 1);
                let tempdir = tempdir_in(".").unwrap();
                let data_root_folder = tempdir.path().to_str().unwrap();
                let base_folder = format!(
                    "{}/{}/{}/{}",
                    data_root_folder,
                    aggregate_key.org_id,
                    aggregate_key.aggregate_type_id,
                    aggregate_key.aggregate_id
                );
                let path_metadata = format!("{}/metadata.bin", base_folder);
                let path_event_batches = format!("{}/event_batches.bin", base_folder);

                // Create base folder if needed
                std::fs::create_dir_all(&base_folder).unwrap();

                // Open DMA files - must be done in this order due to direct I/O fs constraints
                let writer_metadata_dma_file = create_and_write_only_dma(&path_metadata).await.unwrap();
                let reader_metadata_dma_file = existing_file_read_only_dma(&path_metadata).await.unwrap();
                let writer_event_batch_dma_file = create_and_write_only_dma(&path_event_batches).await.unwrap();
                let reader_event_batch_dma_file = existing_file_read_only_dma(&path_event_batches).await.unwrap();

                let read_operations = ReadOperationsWithDmaFiles::new(
                    reader_metadata_dma_file, reader_event_batch_dma_file, aggregate_read_config.clone());
                let data_requirements = read_operations.get_write_operations_data_requirements().await.unwrap();
                let mut write_operations = WriteOperationsWithDmaFile::new(
                    writer_metadata_dma_file, writer_event_batch_dma_file, data_requirements,
                    aggregate_write_config.clone(),
                );

                // Write some event batches
                let events = vec![
                    EventItem::new(45, 0, None, 333, 2, 3, vec![1, 2, 3, 4, 5]),
                ];
                let mut write_request = WriteRequest {
                    client_id: 123,
                    compression_type:
                        compression_type::CompressionType::None,
                    enforce_client_idempotency: true,
                    expected_event_batch_index: Some(1),
                    user_id: None,
                    correlation_id: None,
                    aggregate_key,
                    events,
                    allow_create: false,
                    durable_write_with_delay_us: Some(0),
                };

                let append_result = write_operations
                    .queue_events_in_memory(0, 0,  998, &mut write_request)
                    .unwrap();
                assert_eq!(append_result.event_batch_index, 1);
                write_operations.sync_with_rollback().await.unwrap();

                let _read_result = read_operations
                    .read(
                        write_operations.minimum_available_event_batch_index,
                        write_operations.file_len_metadata,
                        write_operations.file_len_event_batch,
                        &ReadFilters::new(1),
                        None,
                    )
                    .await
                    .unwrap();

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
                    test_config(data_root_folder),
                    aggregate_read_config,
                    aggregate_write_config,
                );
                let aggregate_key = AggregateKey::new(1, 1, 1);
                let aggregate_resources = aggregates_cache.get_aggregate_resources(&aggregate_key);

                // Write some event batches
                let events = vec![
                    EventItem::new(45, 0, None, 333, 2, 3, vec![1, 2, 3, 4, 5]),
                    EventItem::new(46, 0, None, 334, 4, 0, vec![6, 7, 8, 9, 10]),
                ];
                let mut write_request = WriteRequest {
                    client_id: 123,
                    compression_type:
                        compression_type::CompressionType::None,
                    enforce_client_idempotency: true,
                    expected_event_batch_index: Some(1),
                    user_id: None,
                    correlation_id: None,
                        aggregate_key: aggregate_key.clone(),
                    events,
                    allow_create: false,
                    durable_write_with_delay_us: Some(0),
                };

                // Generate a guid and map it to u128
                let id = Uuid::new_v4();
                let event_id = id.as_u128();

                {
                    let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();
                    let append_result = writer
                        .as_mut()
                        .unwrap()
                        .queue_events_in_memory(0, 0,  998, &mut write_request)
                        .unwrap();
                    assert_eq!(append_result.event_batch_index, 1);

                    let events = vec![EventItem::new(
                        47,
                        0,
                        Some(event_id),
                        339,
                        2,
                        3,
                        vec![11, 12, 13],
                    )];
                    let mut write_request = WriteRequest {
                        client_id: 123,
                        compression_type:
                            compression_type::CompressionType::None,
                        enforce_client_idempotency: true,
                        expected_event_batch_index: Some(2),
                        user_id: Some(34343),
                        correlation_id: None,
                        aggregate_key: aggregate_key.clone(),
                        events,
                        allow_create: false,
                        durable_write_with_delay_us: Some(0),
                    };
                    let append_result = writer
                        .as_mut()
                        .unwrap()
                        .queue_events_in_memory(0, 0,  999, &mut write_request)
                        .unwrap();
                    assert_eq!(append_result.event_batch_index, 2);
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
                    check_read_1(&cache_read, event_id);
                }

                let writer = aggregate_resources.get_writer(true).await.unwrap();
                let writer_ref = writer.as_ref().unwrap();

                {
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

                    check_read_1(&read_result, event_id);

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
                    check_read_2(&read_result);

                    let cache_read = writer_ref.maybe_read_cached_events(&read_filters, None).unwrap();
                    check_read_2(&cache_read);

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

                    check_read_3(&read_result);

                    let cache_result = writer_ref.maybe_read_cached_events(&read_filters, None).unwrap();
                    check_read_3(&cache_result);
                }

                {
                    let mut reader = aggregate_resources.get_reader_mut(true).await.unwrap();
                    let reader_ref = reader.as_mut().unwrap();
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
                    check_read_3(&read_result);
                }
            })
            .unwrap();
        handle.join().unwrap();
    }
}
