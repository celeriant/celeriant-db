#[cfg(test)]
mod test_write_updates_metadata_cache {
    use std::{num::NonZeroUsize, sync::Arc};

    use eventplanedb_structures::{
        aggregate_key::AggregateKey, compression_type::CompressionType, event_item::EventItem, read_filters::ReadFilters
    };
    use glommio::{LocalExecutorBuilder, Placement};

    use crate::{
        cache::{aggregate_cache::AggregateCache, aggregate_resources::AggregateResources}, node_config::test_node_config::test_config, read_operations::{
            read_operations::ReadOperations,
            read_structures::AggregateReadConfig,
        }, write_operations::{
            write_operations::WriteOperations,
            write_structures::{AggregateWriteConfig, WriteOptions},
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
                event_value: Arc::new(format!("test_event_{}", i).into_bytes()),
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

        let append_options = WriteOptions {
            client_id,
            compression_type: CompressionType::None,
            enforce_client_idempotency: true,
            expected_event_batch_index: Some(expected_batch_index),
            server_timestamp_millis: base_timestamp,
            user_id: None,
        };

        let aggregate_resources = aggregates_cache.get_aggregate_resources(aggregate_key);
        let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();
        let writer_ref = writer.as_mut().unwrap();

        writer_ref.queue_events_in_memory(0, 0,  events, &append_options)
            .unwrap();

        writer_ref.sync_with_rollback().await.unwrap();
    }

    async fn read_with_expected_len(aggregate_resources: &AggregateResources, expected_len: usize) {
        let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();
        let mut reader = aggregate_resources.get_reader_mut(true).await.unwrap();
        let reader_ref = reader.as_mut().unwrap();
        let writer_ref = writer.as_mut().unwrap();
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
        assert_eq!(read_result.event_batches.len(), expected_len);
    }

    #[test]
    pub fn test_check_update() {
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

                write_batch_with_params(
                    &aggregates_cache,
                    &aggregate_key,
                    123,
                    1,
                    3,
                    1,
                    1000,
                )
                .await;
                
                let aggregate_resources = aggregates_cache.get_aggregate_resources(&aggregate_key);

                read_with_expected_len(&aggregate_resources, 1).await;

                write_batch_with_params(
                    &aggregates_cache,
                    &aggregate_key,
                    123,
                    4,
                    3,
                    2,
                    1000,
                )
                .await;

                read_with_expected_len(&aggregate_resources, 2).await;

            })
            .unwrap();
        handle.join().unwrap();
    }
}