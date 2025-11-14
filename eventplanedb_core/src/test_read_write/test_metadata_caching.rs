#[cfg(test)]
mod test_metadata_caching {
    use std::num::NonZeroUsize;

    use eventplanedb_structures::
        aggregate_key::AggregateKey
    ;
    use glommio::{LocalExecutorBuilder, Placement};

    use crate::{
        cache::aggregate_cache::AggregateCache, read_operations::{
            read_operations::ReadOperations,
            read_structures::AggregateReadConfig,
        }, test_read_write::test_corruption::test_corruption::write_batch, write_operations::
            write_structures::AggregateWriteConfig
        
    };

    #[test]
    fn test_get_write_operations_data_requirements_caching() {
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
                write_batch(&aggregates_cache, &aggregate_key, 124, 22, 2).await;
                write_batch(&aggregates_cache, &aggregate_key, 123, 47, 3).await;

                // Verify no corruption
                let aggregate_resources = aggregates_cache.get(&aggregate_key);
                let mut reader = aggregate_resources.get_reader_mut(false).await.unwrap();
                let w_reader = reader.as_mut().unwrap();

                assert_eq!(w_reader.cache_metadata.len(), 0);

                let result = w_reader.get_write_operations_data_requirements().await.unwrap();

                assert_eq!(result.write_operations_data_requirements.next_event_batch_index, 4);
                assert_eq!(result.write_operations_data_requirements.next_event_index, 4);
                assert_eq!(result.write_operations_data_requirements.client_event_indexes.get(&123), Some(&47));
                assert_eq!(result.write_operations_data_requirements.client_event_indexes.get(&124), Some(&22));
                assert_eq!(result.write_operations_data_requirements.minimum_available_event_batch_index, 1);

                assert_eq!(result.uncached_metadata_set.len(), 3);

                // Only add last metadata entry to cache and try again
                let uncached_metadata_set = vec![result.uncached_metadata_set.last().unwrap().clone()];
                w_reader.update_metadata_cache(uncached_metadata_set);

                assert_eq!(w_reader.cache_metadata.len(), 1);

                let result = w_reader.get_write_operations_data_requirements().await.unwrap();

                assert_eq!(result.write_operations_data_requirements.next_event_batch_index, 4);
                assert_eq!(result.write_operations_data_requirements.next_event_index, 4);
                assert_eq!(result.write_operations_data_requirements.client_event_indexes.get(&123), Some(&47));
                assert_eq!(result.write_operations_data_requirements.client_event_indexes.get(&124), Some(&22));
                assert_eq!(result.write_operations_data_requirements.minimum_available_event_batch_index, 1);

                assert_eq!(result.uncached_metadata_set.len(), 2);

                //Update the full cache now
                w_reader.update_metadata_cache(result.uncached_metadata_set);

                assert_eq!(w_reader.cache_metadata.len(), 3);

                let result = w_reader.get_write_operations_data_requirements().await.unwrap();

                assert_eq!(result.write_operations_data_requirements.next_event_batch_index, 4);
                assert_eq!(result.write_operations_data_requirements.next_event_index, 4);
                assert_eq!(result.write_operations_data_requirements.client_event_indexes.get(&123), Some(&47));
                assert_eq!(result.write_operations_data_requirements.client_event_indexes.get(&124), Some(&22));
                assert_eq!(result.write_operations_data_requirements.minimum_available_event_batch_index, 1);

                assert_eq!(result.uncached_metadata_set.len(), 0);                

            })
            .unwrap();
        handle.join().unwrap();
    }
}