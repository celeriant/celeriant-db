
#[cfg(test)]
mod test_basic_read_write {
    use std::{cell::RefCell, collections::HashMap, rc::Rc};

    use uuid::Uuid;

    use eventplanedb_structures::{aggregate_key::{AggregateKey}, event_item::EventItem, read_filters::ReadFilters};
    use glommio::{LocalExecutorBuilder, Placement};

    use crate::files::{helper::{get_or_create_reader, get_or_create_writer}, read_operations::{AggregateReadConfig, CacheableReadResult, ReadOperations}, write_operations::{AggregateWriteConfig, AppendOptions, CacheReadError, WriteOperations}};

    fn check_read_1(read_result: &CacheableReadResult, event_id: u128, expected_cache_len: usize) {
        assert_eq!(read_result.filtered_event_batches.len(), 2);

        assert_eq!(read_result.filtered_event_batches[0].client_id, 123);
        assert_eq!(read_result.filtered_event_batches[0].event_batch_index, 1);
        assert_eq!(read_result.filtered_event_batches[0].server_timestamp, 998);
        assert_eq!(read_result.filtered_event_batches[0].user_id, None);
        assert_eq!(read_result.filtered_event_batches[0].events.len(), 2);

        assert_eq!(read_result.filtered_event_batches[0].events[0].client_event_index, 45);
        assert_eq!(read_result.filtered_event_batches[0].events[0].event_id, None);
        assert_eq!(read_result.filtered_event_batches[0].events[0].event_index, 1);
        assert_eq!(read_result.filtered_event_batches[0].events[0].event_timestamp, 333);
        assert_eq!(read_result.filtered_event_batches[0].events[0].event_type_major, 2);
        assert_eq!(read_result.filtered_event_batches[0].events[0].event_type_minor, 3);
        assert_eq!(read_result.filtered_event_batches[0].events[0].event_value, vec![1,2,3,4,5].into());

        assert_eq!(read_result.filtered_event_batches[0].events[1].client_event_index, 46);
        assert_eq!(read_result.filtered_event_batches[0].events[1].event_id, None);
        assert_eq!(read_result.filtered_event_batches[0].events[1].event_index, 2);
        assert_eq!(read_result.filtered_event_batches[0].events[1].event_timestamp, 334);
        assert_eq!(read_result.filtered_event_batches[0].events[1].event_type_major, 4);
        assert_eq!(read_result.filtered_event_batches[0].events[1].event_type_minor, 0);
        assert_eq!(read_result.filtered_event_batches[0].events[1].event_value, vec![6,7,8,9,10].into());

        assert_eq!(read_result.filtered_event_batches[1].client_id, 123);
        assert_eq!(read_result.filtered_event_batches[1].event_batch_index, 2);
        assert_eq!(read_result.filtered_event_batches[1].server_timestamp, 999);
        assert_eq!(read_result.filtered_event_batches[1].user_id, Some(34343));
        assert_eq!(read_result.filtered_event_batches[1].events.len(), 1);

        assert_eq!(read_result.filtered_event_batches[1].events[0].client_event_index, 47);
        assert_eq!(read_result.filtered_event_batches[1].events[0].event_id, Some(event_id));
        assert_eq!(read_result.filtered_event_batches[1].events[0].event_index, 3);
        assert_eq!(read_result.filtered_event_batches[1].events[0].event_timestamp, 339);
        assert_eq!(read_result.filtered_event_batches[1].events[0].event_type_major, 2);
        assert_eq!(read_result.filtered_event_batches[1].events[0].event_type_minor, 3);
        assert_eq!(read_result.filtered_event_batches[1].events[0].event_value, vec![11,12,13].into());

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

        assert_eq!(read_result.filtered_event_batches[0].events[0].event_value, vec![6,7,8,9,10].into());

        assert_eq!(read_result.filtered_event_batches[1].event_batch_index, 2);
        assert_eq!(read_result.filtered_event_batches[1].events.len(), 1);
        assert_eq!(read_result.filtered_event_batches[1].events[0].event_value, vec![11,12,13].into());

        assert_eq!(read_result.next_event_batch_index, None);
        assert_eq!(read_result.uncached_metadata_set.len(), expected_cache_len);
    }

    #[test]
    fn basic_read_write_two_writers() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            
            let read_operations_cache = Rc::new(RefCell::new(HashMap::<AggregateKey, Rc<RefCell<ReadOperations>>>::new()));
            let write_operations_cache = Rc::new(RefCell::new(HashMap::<AggregateKey, Rc<RefCell<WriteOperations>>>::new()));

            let aggregate_key = AggregateKey::new(1, 1, 1);

            let aggregate_read_config = AggregateReadConfig {
                max_chunk_size: 1 << 20,
                max_data_cache_size_bytes: 1 << 20,
            };

            let aggregate_write_config = AggregateWriteConfig {
                max_data_cache_size_bytes: 1 << 25
            };

            // Create the files and a writer
            let tempdir = tempfile::tempdir().unwrap();
            let data_root_folder = tempdir.path().to_str().unwrap();

            {            
                // Write some event batches
                let events = vec![
                    EventItem::new(45, 0, None, 333, 2, 3, vec![1,2,3,4,5]),
                    EventItem::new(46, 0, None, 334, 4, 0, vec![6,7,8,9,10]),
                ];
                let append_options = AppendOptions {
                    client_id: 123,
                    compression_type: eventplanedb_structures::compression_type::CompressionType::None,
                    enforce_client_idempotency: true,
                    expected_event_batch_index: Some(1),
                    server_timestamp_millis: 998,
                    user_id: None
                };

                let writer = get_or_create_writer(&aggregate_key, data_root_folder, true, &read_operations_cache, &aggregate_read_config, &write_operations_cache, &aggregate_write_config).await.unwrap();
                
                let append_result = writer.borrow_mut().queue_events_in_memory(events, &append_options).unwrap();
                assert_eq!(append_result.next_event_batch_index, 2);

                //Write to disk
                writer.borrow_mut().sync_with_rollback().await.unwrap();
            }

            // Generate a guid and map it to u128
            let id = Uuid::new_v4();
            let event_id = id.as_u128();
            
            {
                let events = vec![
                    EventItem::new(47, 0, Some(event_id), 339, 2, 3, vec![11,12,13]),
                ];
                let append_options = AppendOptions {
                    client_id: 123,
                    compression_type: eventplanedb_structures::compression_type::CompressionType::None,
                    enforce_client_idempotency: true,
                    expected_event_batch_index: Some(2),
                    server_timestamp_millis: 999,
                    user_id: Some(34343)
                };
                
                let writer = get_or_create_writer(&aggregate_key, data_root_folder, false, &read_operations_cache, &aggregate_read_config, &write_operations_cache, &aggregate_write_config).await.unwrap();
                let append_result = writer.borrow_mut().queue_events_in_memory(events, &append_options).unwrap();
                assert_eq!(append_result.next_event_batch_index, 3);

                //Write to disk
                writer.borrow_mut().sync_with_rollback().await.unwrap();
            }

            let mut read_filters = ReadFilters::new(1);
            read_filters = read_filters.min_event_timestamp(334);

            let writer = get_or_create_writer(&aggregate_key, data_root_folder, false, &read_operations_cache, &aggregate_read_config, &write_operations_cache, &aggregate_write_config).await.unwrap();
            let reader = get_or_create_reader(&aggregate_key, data_root_folder, false, &read_operations_cache, &aggregate_read_config).await.unwrap();
            let read_result = reader.borrow().read(1, writer.borrow().file_len_metadata(), writer.borrow().file_len_event_batch(), &read_filters).await.unwrap();
            check_read_3(&read_result, 2);


        }).unwrap();
        handle.join().unwrap();
    }
    
    #[test]
    fn basic_read_write() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            
            let read_operations_cache = Rc::new(RefCell::new(HashMap::<AggregateKey, Rc<RefCell<ReadOperations>>>::new()));
            let write_operations_cache = Rc::new(RefCell::new(HashMap::<AggregateKey, Rc<RefCell<WriteOperations>>>::new()));

            let aggregate_key = AggregateKey::new(1, 1, 1);

            let aggregate_read_config = AggregateReadConfig {
                max_chunk_size: 1 << 20,
                max_data_cache_size_bytes: 1 << 20,
            };

            let aggregate_write_config = AggregateWriteConfig {
                max_data_cache_size_bytes: 1 << 25
            };

            // Create the files and a writer
            let tempdir = tempfile::tempdir().unwrap();
            let data_root_folder = tempdir.path().to_str().unwrap();

            let base_folder = format!("{}/{}/{}/{}", data_root_folder, aggregate_key.org_id, aggregate_key.aggregate_type_id, aggregate_key.aggregate_id);
            std::fs::create_dir_all(&base_folder).unwrap();
            std::fs::File::create(format!("{}/metadata.bin", base_folder)).unwrap();
            std::fs::File::create(format!("{}/event_batches.bin", base_folder)).unwrap();
            
            // Write some event batches
            let events = vec![
                EventItem::new(45, 0, None, 333, 2, 3, vec![1,2,3,4,5]),
                EventItem::new(46, 0, None, 334, 4, 0, vec![6,7,8,9,10]),
            ];
            let append_options = AppendOptions {
                client_id: 123,
                compression_type: eventplanedb_structures::compression_type::CompressionType::None,
                enforce_client_idempotency: true,
                expected_event_batch_index: Some(1),
                server_timestamp_millis: 998,
                user_id: None
            };

            let writer = get_or_create_writer(&aggregate_key, data_root_folder, false, &read_operations_cache, &aggregate_read_config, &write_operations_cache, &aggregate_write_config).await.unwrap();
            let append_result = writer.borrow_mut().queue_events_in_memory(events, &append_options).unwrap();
            assert_eq!(append_result.next_event_batch_index, 2);

            // Generate a guid and map it to u128
            let id = Uuid::new_v4();
            let event_id = id.as_u128();

            let events = vec![
                EventItem::new(47, 0, Some(event_id), 339, 2, 3, vec![11,12,13]),
            ];
            let append_options = AppendOptions {
                client_id: 123,
                compression_type: eventplanedb_structures::compression_type::CompressionType::None,
                enforce_client_idempotency: true,
                expected_event_batch_index: Some(2),
                server_timestamp_millis: 999,
                user_id: Some(34343)
            };
            let append_result = writer.borrow_mut().queue_events_in_memory(events, &append_options).unwrap();
            assert_eq!(append_result.next_event_batch_index, 3);

            let mut read_filters = ReadFilters::new(1);
            let cache_read_attempt = writer.borrow().maybe_read_cached_events(&read_filters).expect_err("Cache did not miss");
            
            match cache_read_attempt {
                CacheReadError::CacheMiss { missing_from_event_batch_index, missing_to_event_batch_index } => {
                    assert_eq!(missing_from_event_batch_index, 1);
                    assert_eq!(missing_to_event_batch_index, None);
                }
            }

            //Write to disk
            writer.borrow_mut().sync_with_rollback().await.unwrap();

            //Now that we have sync success, it should be in the writer cache
            let cache_read = writer.borrow().maybe_read_cached_events(&read_filters).unwrap();
            check_read_1(&cache_read, event_id, 0);

            let reader = get_or_create_reader(&aggregate_key, data_root_folder, false, &read_operations_cache, &aggregate_read_config).await.unwrap();
            
            let read_result = reader.borrow().read(1, writer.borrow().file_len_metadata(), writer.borrow().file_len_event_batch(), &read_filters).await.unwrap();

            check_read_1(&read_result, event_id, 2);

            //Basic filter on metadata
            read_filters = read_filters.exclude_client_id(123);
            let read_result = reader.borrow().read(1, writer.borrow().file_len_metadata(), writer.borrow().file_len_event_batch(), &read_filters).await.unwrap();
            check_read_2(&read_result, 2);

            let cache_read = writer.borrow().maybe_read_cached_events(&read_filters).unwrap();
            check_read_2(&cache_read, 0);

            //Basic filter on event batches
            let mut read_filters = ReadFilters::new(1);
            read_filters = read_filters.min_event_timestamp(334);
            let read_result = reader.borrow().read(1, writer.borrow().file_len_metadata(), writer.borrow().file_len_event_batch(), &read_filters).await.unwrap();

            check_read_3(&read_result, 2);

            let cache_result = writer.borrow().maybe_read_cached_events(&read_filters).unwrap();
            check_read_3(&cache_result, 0);

            //Let's update the cache now and check cache is used for metadata
            let reader2 = get_or_create_reader(&aggregate_key, data_root_folder, false, &read_operations_cache, &aggregate_read_config).await.unwrap();
            reader2.borrow_mut().update_metadata_cache(read_result.uncached_metadata_set);       

            //Cache should be idempotent
            let read_result = reader.borrow().read(1, writer.borrow().file_len_metadata(), writer.borrow().file_len_event_batch(), &read_filters).await.unwrap();     
            reader2.borrow_mut().update_metadata_cache(read_result.uncached_metadata_set);

            let read_result = reader2.borrow().read(1, writer.borrow().file_len_metadata(), writer.borrow().file_len_event_batch(), &read_filters).await.unwrap();
            check_read_3(&read_result, 0);

        }).unwrap();
        handle.join().unwrap();
    }

}