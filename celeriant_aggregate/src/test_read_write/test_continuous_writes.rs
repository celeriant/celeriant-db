#[cfg(test)]
pub mod test_continuous_writes {
    use std::{num::NonZeroUsize, rc::Rc};

    use celeriant_msg::request::{read_filters::ReadFilters, requests::WriteRequest};
    use celeriant_wal::{
        aggregate_key::AggregateKey, compression_type::CompressionType, wal::event_item::EventItem,
    };
    use glommio::{LocalExecutorBuilder, Placement};

    use crate::{
        cache::aggregate_cache::AggregateCache,
        node_config::test_node_config::test_config,
        read_operations::{read_operations::ReadOperations, read_structures::AggregateReadConfig},
        watch::watched_aggregates::WatchedAggregates,
        write_operations::{
            aggregate_write_config::AggregateWriteConfig, write_operations::WriteOperations,
        },
    };

    const NUM_BATCHES: u64 = 100;
    const EVENTS_PER_BATCH: usize = 10;

    async fn write_continuous_batches(
        aggregates_cache: &AggregateCache,
        aggregate_key: &AggregateKey,
        client_id: u128,
        durable_write_with_delay_us: Option<u64>,
    ) {
        let aggregate_resources = aggregates_cache.get_aggregate_resources(aggregate_key);
        let watched_aggregates = Rc::new(WatchedAggregates::new());

        for batch_index in 1..=NUM_BATCHES {
            let events: Vec<EventItem> = (0..EVENTS_PER_BATCH)
                .map(|i| {
                    let client_event_index = (batch_index - 1) * EVENTS_PER_BATCH as u64 + i as u64 + 1;
                    EventItem::new(
                        client_event_index,
                        0,
                        None,
                        1000 + client_event_index,
                        (i % 5 + 1) as u64, // event_type_major 1-5
                        i as u64,
                        vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10], // 10 bytes payload
                    )
                })
                .collect();

            let mut write_request = WriteRequest {
                client_id,
                compression_type: CompressionType::None,
                enforce_client_idempotency: true,
                expected_event_batch_index: Some(batch_index),
                user_id: None,
                correlation_id: None,
                aggregate_key: aggregate_key.clone(),
                events,
                allow_create: false,
                durable_write_with_delay_us,
            };

            let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();

            writer
                .queue_events_in_memory(0, 0, 1000 + batch_index, &mut write_request)
                .unwrap();

            writer
                .sync_with_rollback(watched_aggregates.clone())
                .await
                .unwrap();
        }
    }

    async fn verify_no_corruption(
        aggregates_cache: &AggregateCache,
        aggregate_key: &AggregateKey,
    ) -> Result<(), String> {
        let aggregate_resources = aggregates_cache.get_aggregate_resources(aggregate_key);
        let writer = aggregate_resources.get_writer(true).await.unwrap();
        let reader = aggregate_resources.get_reader(true).await.unwrap();

        let read_filters = ReadFilters::new(1);

        let read_result = reader
            .read(
                None,
                writer.minimum_available_event_batch_index,
                writer.file_len_metadata,
                writer.file_len_event_batch,
                &read_filters,
                None,
            )
            .await
            .map_err(|e| format!("Read error: {:?}", e))?;

        // Verify we got all batches
        if read_result.event_batches.len() != NUM_BATCHES as usize {
            return Err(format!(
                "Expected {} batches, got {}",
                NUM_BATCHES,
                read_result.event_batches.len()
            ));
        }

        // Verify each batch has correct number of events
        for (i, batch) in read_result.event_batches.iter().enumerate() {
            if batch.events.len() != EVENTS_PER_BATCH {
                return Err(format!(
                    "Batch {} expected {} events, got {}",
                    i + 1,
                    EVENTS_PER_BATCH,
                    batch.events.len()
                ));
            }

            // Verify event_batch_index is correct
            if batch.event_batch_index != (i + 1) as u64 {
                return Err(format!(
                    "Batch {} has wrong event_batch_index: {}",
                    i + 1,
                    batch.event_batch_index
                ));
            }
        }

        Ok(())
    }

    #[test]
    fn test_continuous_writes_no_durable_delay() {
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

                // Write continuous batches with durable_write_with_delay_us = None
                write_continuous_batches(
                    &aggregates_cache,
                    &aggregate_key,
                    123,
                    None,
                )
                .await;

                // Verify no corruption
                verify_no_corruption(&aggregates_cache, &aggregate_key)
                    .await
                    .expect("Data corruption detected with durable_write_with_delay_us = None");
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_continuous_writes_immediate_sync() {
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
                let aggregate_key = AggregateKey::new(1, 1, 2);

                // Write continuous batches with durable_write_with_delay_us = Some(0)
                write_continuous_batches(
                    &aggregates_cache,
                    &aggregate_key,
                    124,
                    Some(0),
                )
                .await;

                // Verify no corruption
                verify_no_corruption(&aggregates_cache, &aggregate_key)
                    .await
                    .expect("Data corruption detected with durable_write_with_delay_us = Some(0)");
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_continuous_writes_delayed_sync() {
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
                let aggregate_key = AggregateKey::new(1, 1, 3);

                // Write continuous batches with durable_write_with_delay_us = Some(20)
                write_continuous_batches(
                    &aggregates_cache,
                    &aggregate_key,
                    125,
                    Some(20),
                )
                .await;

                // Verify no corruption
                verify_no_corruption(&aggregates_cache, &aggregate_key)
                    .await
                    .expect("Data corruption detected with durable_write_with_delay_us = Some(20)");
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_continuous_writes_reopen_and_verify() {
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
                    aggregate_read_config.clone(),
                    aggregate_write_config.clone(),
                );
                let aggregate_key = AggregateKey::new(1, 1, 4);

                // Write continuous batches
                write_continuous_batches(
                    &aggregates_cache,
                    &aggregate_key,
                    126,
                    Some(0),
                )
                .await;

                // Close writer explicitly
                {
                    let resources = aggregates_cache.get_aggregate_resources(&aggregate_key);
                    let mut writer = resources
                        .get_writer_mut(true)
                        .await
                        .unwrap();
                    writer.close().await.unwrap();
                }

                // Pop from cache to force reopen
                aggregates_cache.pop(&aggregate_key).await.unwrap();

                // Create new cache instance to simulate restart
                let aggregates_cache_2 = AggregateCache::new(
                    NonZeroUsize::new(1000).unwrap(),
                    test_config(data_root_folder),
                    aggregate_read_config,
                    aggregate_write_config,
                );

                // Verify no corruption after reopen
                verify_no_corruption(&aggregates_cache_2, &aggregate_key)
                    .await
                    .expect("Data corruption detected after reopen");
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_continuous_writes_with_compression() {
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
                let aggregate_key = AggregateKey::new(1, 1, 5);
                let aggregate_resources = aggregates_cache.get_aggregate_resources(&aggregate_key);
                let watched_aggregates = Rc::new(WatchedAggregates::new());

                // Write with LZ4 compression
                for batch_index in 1..=NUM_BATCHES {
                    let events: Vec<EventItem> = (0..EVENTS_PER_BATCH)
                        .map(|i| {
                            let client_event_index =
                                (batch_index - 1) * EVENTS_PER_BATCH as u64 + i as u64 + 1;
                            EventItem::new(
                                client_event_index,
                                0,
                                None,
                                1000 + client_event_index,
                                (i % 5 + 1) as u64,
                                i as u64,
                                vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                            )
                        })
                        .collect();

                    let mut write_request = WriteRequest {
                        client_id: 127,
                        compression_type: CompressionType::Zstd { level: 6 },
                        enforce_client_idempotency: true,
                        expected_event_batch_index: Some(batch_index),
                        user_id: None,
                        correlation_id: None,
                        aggregate_key: aggregate_key.clone(),
                        events,
                        allow_create: false,
                        durable_write_with_delay_us: Some(0),
                    };

                    let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();
                    writer
                        .queue_events_in_memory(0, 0, 1000 + batch_index, &mut write_request)
                        .unwrap();
                    writer
                        .sync_with_rollback(watched_aggregates.clone())
                        .await
                        .unwrap();
                }

                // Verify no corruption
                verify_no_corruption(&aggregates_cache, &aggregate_key)
                    .await
                    .expect("Data corruption detected with LZ4 compression");
            })
            .unwrap();
        handle.join().unwrap();
    }
}