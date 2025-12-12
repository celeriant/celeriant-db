#[cfg(test)]
pub mod test_unclean_shutdown {
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

    /// Simulates unclean shutdown by NOT calling close() on the writer
    /// This leaves trailing zeros in the file due to DMA alignment
    async fn write_batches_without_close(
        aggregates_cache: &AggregateCache,
        aggregate_key: &AggregateKey,
        client_id: u128,
        num_batches: u64,
        events_per_batch: usize,
    ) {
        let aggregate_resources = aggregates_cache.get_aggregate_resources(aggregate_key);
        let watched_aggregates = Rc::new(WatchedAggregates::new());

        for batch_index in 1..=num_batches {
            let events: Vec<EventItem> = (0..events_per_batch)
                .map(|i| {
                    let client_event_index =
                        (batch_index - 1) * events_per_batch as u64 + i as u64 + 1;
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
                client_id,
                compression_type: CompressionType::None,
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

        // Intentionally NOT calling close() to simulate unclean shutdown
        // This leaves trailing zeros in the file from DMA alignment padding
        // let mut writer = aggregate_resources.get_writer_mut(true).await.unwrap();
        // let _ = writer.close().await;
    }

    /// Write batches with varying payload sizes to create different alignment scenarios
    async fn write_batches_varying_sizes_without_close(
        aggregates_cache: &AggregateCache,
        aggregate_key: &AggregateKey,
        client_id: u128,
        num_batches: u64,
    ) {
        let aggregate_resources = aggregates_cache.get_aggregate_resources(aggregate_key);
        let watched_aggregates = Rc::new(WatchedAggregates::new());

        for batch_index in 1..=num_batches {
            // Vary number of events and payload size per batch
            let events_in_batch = ((batch_index % 5) + 1) as usize;
            let payload_size = ((batch_index * 17) % 100 + 10) as usize;

            let events: Vec<EventItem> = (0..events_in_batch)
                .map(|i| {
                    let client_event_index =
                        (batch_index - 1) * 10 + i as u64 + 1; // Use fixed multiplier for simplicity
                    EventItem::new(
                        client_event_index,
                        0,
                        None,
                        1000 + client_event_index,
                        (i % 5 + 1) as u64,
                        i as u64,
                        vec![0xAB; payload_size],
                    )
                })
                .collect();

            let mut write_request = WriteRequest {
                client_id,
                compression_type: CompressionType::None,
                enforce_client_idempotency: false, // Disable for varying event counts
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
    }

    async fn verify_all_batches_readable(
        aggregates_cache: &AggregateCache,
        aggregate_key: &AggregateKey,
        expected_batches: u64,
        expected_events_per_batch: Option<usize>,
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

        if read_result.event_batches.len() != expected_batches as usize {
            return Err(format!(
                "Expected {} batches, got {}",
                expected_batches,
                read_result.event_batches.len()
            ));
        }

        for (i, batch) in read_result.event_batches.iter().enumerate() {
            if let Some(expected) = expected_events_per_batch {
                if batch.events.len() != expected {
                    return Err(format!(
                        "Batch {} expected {} events, got {}",
                        i + 1,
                        expected,
                        batch.events.len()
                    ));
                }
            }

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

    /// Read each batch individually to identify which specific batch is corrupted
    async fn verify_each_batch_individually(
        aggregates_cache: &AggregateCache,
        aggregate_key: &AggregateKey,
        expected_batches: u64,
    ) -> Result<(), String> {
        let aggregate_resources = aggregates_cache.get_aggregate_resources(aggregate_key);
        let writer = aggregate_resources.get_writer(true).await.unwrap();
        let reader = aggregate_resources.get_reader(true).await.unwrap();

        for batch_index in 1..=expected_batches {
            let mut read_filters = ReadFilters::new(batch_index);
            read_filters = read_filters.to_event_batch_index(batch_index);

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
                .map_err(|e| format!("Batch {} read error: {:?}", batch_index, e))?;

            if read_result.event_batches.len() != 1 {
                return Err(format!(
                    "Batch {} expected 1 batch in result, got {}",
                    batch_index,
                    read_result.event_batches.len()
                ));
            }

            if read_result.event_batches[0].event_batch_index != batch_index {
                return Err(format!(
                    "Batch {} returned wrong index: {}",
                    batch_index, read_result.event_batches[0].event_batch_index
                ));
            }
        }

        Ok(())
    }

    #[test]
    fn test_unclean_shutdown_5_batches_reopen() {
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
                let aggregate_key = AggregateKey::new(1, 1, 1);

                // Write 5 batches WITHOUT calling close (simulate crash)
                write_batches_without_close(&aggregates_cache, &aggregate_key, 123, 5, 3).await;

                // Pop from cache to force reopen (simulates restart)
                aggregates_cache.pop(&aggregate_key).await.unwrap();

                // Create new cache to simulate fresh start
                let aggregates_cache_2 = AggregateCache::new(
                    NonZeroUsize::new(1000).unwrap(),
                    test_config(data_root_folder),
                    aggregate_read_config,
                    aggregate_write_config,
                );

                // Verify all batches are readable
                verify_all_batches_readable(&aggregates_cache_2, &aggregate_key, 5, Some(3))
                    .await
                    .expect("Corruption detected after unclean shutdown with 5 batches");

                // Also verify each batch individually
                verify_each_batch_individually(&aggregates_cache_2, &aggregate_key, 5)
                    .await
                    .expect("Individual batch read failed after unclean shutdown");
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_unclean_shutdown_10_batches_reopen() {
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
                let aggregate_key = AggregateKey::new(1, 1, 2);

                write_batches_without_close(&aggregates_cache, &aggregate_key, 124, 10, 5).await;

                aggregates_cache.pop(&aggregate_key).await.unwrap();

                let aggregates_cache_2 = AggregateCache::new(
                    NonZeroUsize::new(1000).unwrap(),
                    test_config(data_root_folder),
                    aggregate_read_config,
                    aggregate_write_config,
                );

                verify_all_batches_readable(&aggregates_cache_2, &aggregate_key, 10, Some(5))
                    .await
                    .expect("Corruption detected after unclean shutdown with 10 batches");

                verify_each_batch_individually(&aggregates_cache_2, &aggregate_key, 10)
                    .await
                    .expect("Individual batch read failed after unclean shutdown");
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_unclean_shutdown_varying_batch_sizes() {
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
                let aggregate_key = AggregateKey::new(1, 1, 3);

                // Write batches with varying sizes to stress alignment edge cases
                write_batches_varying_sizes_without_close(&aggregates_cache, &aggregate_key, 125, 20)
                    .await;

                aggregates_cache.pop(&aggregate_key).await.unwrap();

                let aggregates_cache_2 = AggregateCache::new(
                    NonZeroUsize::new(1000).unwrap(),
                    test_config(data_root_folder),
                    aggregate_read_config,
                    aggregate_write_config,
                );

                verify_all_batches_readable(&aggregates_cache_2, &aggregate_key, 20, None)
                    .await
                    .expect("Corruption detected with varying batch sizes");

                verify_each_batch_individually(&aggregates_cache_2, &aggregate_key, 20)
                    .await
                    .expect("Individual batch read failed with varying sizes");
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_unclean_shutdown_write_more_after_reopen() {
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

                // Write initial batches without close
                write_batches_without_close(&aggregates_cache, &aggregate_key, 123, 5, 3).await;

                // Simulate restart
                aggregates_cache.pop(&aggregate_key).await.unwrap();

                let aggregates_cache_2 = AggregateCache::new(
                    NonZeroUsize::new(1000).unwrap(),
                    test_config(data_root_folder),
                    aggregate_read_config.clone(),
                    aggregate_write_config.clone(),
                );

                // Write MORE batches after reopen (continuing from batch 6)
                {
                    let aggregate_resources =
                        aggregates_cache_2.get_aggregate_resources(&aggregate_key);
                    let watched_aggregates = Rc::new(WatchedAggregates::new());

                    for batch_index in 6..=10 {
                        let events: Vec<EventItem> = (0..3)
                            .map(|i| {
                                let client_event_index =
                                    (batch_index - 1) * 3 + i as u64 + 1;
                                EventItem::new(
                                    client_event_index,
                                    0,
                                    None,
                                    2000 + client_event_index,
                                    (i % 5 + 1) as u64,
                                    i as u64,
                                    vec![0xCD; 20],
                                )
                            })
                            .collect();

                        let mut write_request = WriteRequest {
                            client_id: 123,
                            compression_type: CompressionType::None,
                            enforce_client_idempotency: true,
                            expected_event_batch_index: Some(batch_index),
                            user_id: None,
                            correlation_id: None,
                            aggregate_key: aggregate_key.clone(),
                            events,
                            allow_create: false,
                            durable_write_with_delay_us: Some(0),
                        };

                        let mut writer =
                            aggregate_resources.get_writer_mut(true).await.unwrap();
                        writer
                            .queue_events_in_memory(0, 0, 2000 + batch_index, &mut write_request)
                            .unwrap();
                        writer
                            .sync_with_rollback(watched_aggregates.clone())
                            .await
                            .unwrap();
                    }
                }

                // Verify all 10 batches
                verify_all_batches_readable(&aggregates_cache_2, &aggregate_key, 10, Some(3))
                    .await
                    .expect("Corruption after writing more batches post-restart");

                verify_each_batch_individually(&aggregates_cache_2, &aggregate_key, 10)
                    .await
                    .expect("Individual batch read failed after post-restart writes");
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_unclean_shutdown_multiple_restarts() {
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
                let aggregate_key = AggregateKey::new(1, 1, 5);

                // First session: write batches 1-3 without close
                {
                    let cache = AggregateCache::new(
                        NonZeroUsize::new(1000).unwrap(),
                        test_config(data_root_folder),
                        aggregate_read_config.clone(),
                        aggregate_write_config.clone(),
                    );
                    write_batches_without_close(&cache, &aggregate_key, 126, 3, 2).await;
                }

                // Second session: write batches 4-6 without close
                {
                    let cache = AggregateCache::new(
                        NonZeroUsize::new(1000).unwrap(),
                        test_config(data_root_folder),
                        aggregate_read_config.clone(),
                        aggregate_write_config.clone(),
                    );

                    let aggregate_resources = cache.get_aggregate_resources(&aggregate_key);
                    let watched_aggregates = Rc::new(WatchedAggregates::new());

                    for batch_index in 4..=6 {
                        let events: Vec<EventItem> = (0..2)
                            .map(|i| {
                                let client_event_index = (batch_index - 1) * 2 + i as u64 + 1;
                                EventItem::new(
                                    client_event_index,
                                    0,
                                    None,
                                    3000 + client_event_index,
                                    (i % 5 + 1) as u64,
                                    i as u64,
                                    vec![0xEF; 15],
                                )
                            })
                            .collect();

                        let mut write_request = WriteRequest {
                            client_id: 126,
                            compression_type: CompressionType::None,
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
                            .queue_events_in_memory(0, 0, 3000 + batch_index, &mut write_request)
                            .unwrap();
                        writer
                            .sync_with_rollback(watched_aggregates.clone())
                            .await
                            .unwrap();
                    }
                    // No close - simulate another crash
                }

                // Third session: verify all data
                {
                    let cache = AggregateCache::new(
                        NonZeroUsize::new(1000).unwrap(),
                        test_config(data_root_folder),
                        aggregate_read_config,
                        aggregate_write_config,
                    );

                    verify_all_batches_readable(&cache, &aggregate_key, 6, Some(2))
                        .await
                        .expect("Corruption after multiple unclean restarts");

                    verify_each_batch_individually(&cache, &aggregate_key, 6)
                        .await
                        .expect("Individual batch read failed after multiple restarts");
                }
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_unclean_shutdown_with_compression() {
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
                let aggregate_key = AggregateKey::new(1, 1, 6);
                let aggregate_resources = aggregates_cache.get_aggregate_resources(&aggregate_key);
                let watched_aggregates = Rc::new(WatchedAggregates::new());

                // Write with LZ4 compression without close
                for batch_index in 1..=8 {
                    let events: Vec<EventItem> = (0..4)
                        .map(|i| {
                            let client_event_index = (batch_index - 1) * 4 + i as u64 + 1;
                            EventItem::new(
                                client_event_index,
                                0,
                                None,
                                4000 + client_event_index,
                                (i % 5 + 1) as u64,
                                i as u64,
                                vec![0x42; 50], // Compressible data
                            )
                        })
                        .collect();

                    let mut write_request = WriteRequest {
                        client_id: 127,
                        compression_type: CompressionType::Snappy,
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
                        .queue_events_in_memory(0, 0, 4000 + batch_index, &mut write_request)
                        .unwrap();
                    writer
                        .sync_with_rollback(watched_aggregates.clone())
                        .await
                        .unwrap();
                }
                // No close - simulate crash

                aggregates_cache.pop(&aggregate_key).await.unwrap();

                let aggregates_cache_2 = AggregateCache::new(
                    NonZeroUsize::new(1000).unwrap(),
                    test_config(data_root_folder),
                    aggregate_read_config,
                    aggregate_write_config,
                );

                verify_all_batches_readable(&aggregates_cache_2, &aggregate_key, 8, Some(4))
                    .await
                    .expect("Corruption with LZ4 compression after unclean shutdown");

                verify_each_batch_individually(&aggregates_cache_2, &aggregate_key, 8)
                    .await
                    .expect("Individual batch read failed with compression");
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_unclean_shutdown_small_batches_alignment_edge_cases() {
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
                let aggregate_key = AggregateKey::new(1, 1, 7);
                let aggregate_resources = aggregates_cache.get_aggregate_resources(&aggregate_key);
                let watched_aggregates = Rc::new(WatchedAggregates::new());

                // Write very small batches that will have lots of alignment padding
                for batch_index in 1..=15 {
                    let events = vec![EventItem::new(
                        batch_index,
                        0,
                        None,
                        5000 + batch_index,
                        1,
                        0,
                        vec![0x11; 5], // Very small payload
                    )];

                    let mut write_request = WriteRequest {
                        client_id: 128,
                        compression_type: CompressionType::None,
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
                        .queue_events_in_memory(0, 0, 5000 + batch_index, &mut write_request)
                        .unwrap();
                    writer
                        .sync_with_rollback(watched_aggregates.clone())
                        .await
                        .unwrap();
                }

                aggregates_cache.pop(&aggregate_key).await.unwrap();

                let aggregates_cache_2 = AggregateCache::new(
                    NonZeroUsize::new(1000).unwrap(),
                    test_config(data_root_folder),
                    aggregate_read_config,
                    aggregate_write_config,
                );

                verify_all_batches_readable(&aggregates_cache_2, &aggregate_key, 15, Some(1))
                    .await
                    .expect("Corruption with small batches after unclean shutdown");

                verify_each_batch_individually(&aggregates_cache_2, &aggregate_key, 15)
                    .await
                    .expect("Individual batch read failed with small batches");
            })
            .unwrap();
        handle.join().unwrap();
    }
}