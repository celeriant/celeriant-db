#[cfg(test)]
mod test {
    use std::hint::black_box;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;
    use std::{collections::HashMap, rc::Rc};

    use crate::internal_shard_config::InternalShardConfig;
    use crate::shard_wal::ShardWal;
    use crate::timestamp_config::TimestampConfig;
    use celeriant_msg::request::requests::{ExistsRequest, SingleAggregateWrite, WriteRequest};
    use celeriant_wal::aggregate_key::AggregateKey;
    use celeriant_wal::compression_type::CompressionType;
    use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
    use glommio::{LocalExecutorBuilder, Placement};
    use tempfile::tempdir;

    #[test]
    fn test1() {
        for (size_name, target_bytes) in wal_size_configs() {
            // Setup: Create temp dir and populate WAL
            let tempdir = tempdir().unwrap();
            let shard_dir = tempdir.path().to_path_buf();

            eprintln!("\n=== Setting up WAL: {} ({} bytes) ===", size_name, target_bytes);
            let actual_bytes = setup_populated_wal(shard_dir.clone(), target_bytes);
            eprintln!("=== WAL setup complete: ~{} bytes written ===\n", actual_bytes);

            // Test with an aggregate that exists (aggregate_id = 0)
            let existing_aggregate = AggregateKey::new(1, 1, 0);

            let shard_dir = shard_dir.clone();

            let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
                .spawn(move || async move {
                    let config = create_config(shard_dir);
                    let shard_wal = ShardWal::open(config).await.unwrap();

                    // Warm-up: populate caches with one exists call (not timed)
                    let warmup_request = ExistsRequest {
                        correlation_id: None,
                        aggregate_key: existing_aggregate.clone(),
                    };
                    let _ = shard_wal.exists(&warmup_request).await;

                    // Timed iterations
                    let mut total_duration = Duration::ZERO;

                    let exists_request = ExistsRequest {
                        correlation_id: None,
                        aggregate_key: existing_aggregate.clone(),
                    };

                    let start = std::time::Instant::now();
                    let result = shard_wal.exists(&exists_request).await.unwrap();
                    total_duration += start.elapsed();

                    black_box(result);

                    total_duration
                })
                .unwrap();

            handle.join().unwrap();
        }
    }

    // =============================================================================
    // CONFIGURATION
    // =============================================================================

    /// WAL size configurations: (name, target_bytes, expected_segments)
    /// Each segment is 128MB based on shard_log_preallocate_bytes
    fn wal_size_configs() -> Vec<(&'static str, usize)> {
        vec![
            ("1seg_128mb", 128 * 1024 * 1024),
            ("2seg_256mb", 256 * 1024 * 1024),
            ("4seg_512mb", 512 * 1024 * 1024),
            ("8seg_1gb", 1024 * 1024 * 1024),
        ]
    }

    const NUM_AGGREGATES: usize = 2000;
    const EVENT_SIZE_BYTES: usize = 1024; // 1KB per event
    const EVENTS_PER_BATCH: usize = 10;
    const SEGMENT_SIZE_BYTES: u64 = 128 * 1024 * 1024; // 128MB segments

    // =============================================================================
    // HELPERS
    // =============================================================================

    fn create_config(shard_dir: PathBuf) -> InternalShardConfig {
        InternalShardConfig {
            node_id: 1,
            max_open_files: 256,
            shard_log_preallocate_bytes: SEGMENT_SIZE_BYTES,
            fsync_delay: Duration::from_millis(10),
            recent_write_cache_bytes: 64 * 1024 * 1024,
            non_durable_writes: false,
            shard_dir,
            max_response_size: 16 * 1024 * 1024,
            aggregate_snapshots_cache_bytes: 64 * 1024 * 1024,
            aggregate_client_snapshots_cache_bytes: 32 * 1024 * 1024,
            read_max_chunk_size: 32 * 1024,
            timestamp_config: TimestampConfig::default(),
        }
    }

    fn create_events(count: usize, size: usize, base_index: u64) -> Vec<DatablockAggregateEvent> {
        (0..count)
            .map(|i| DatablockAggregateEvent {
                client_event_index: base_index + i as u64,
                event_index: 0, // Server assigns this
                event_id: None,
                event_timestamp: 1_700_000_000_000 + i as u64,
                event_type_major: 1,
                event_type_minor: 0,
                event_value: Arc::new(vec![0xABu8; size]),
                iv: None,
            })
            .collect()
    }

    fn create_write_request(aggregate_key: AggregateKey, events: Vec<DatablockAggregateEvent>, client_id: u128) -> WriteRequest {
        let mut writes = HashMap::new();
        writes.insert(
            aggregate_key,
            SingleAggregateWrite {
                events,
                allow_create: true,
                expected_event_batch_index: None,
                enforce_client_idempotency: false,
                compression_type: CompressionType::None,
            },
        );

        WriteRequest {
            correlation_id: None,
            client_id,
            user_id: None,
            writes,
        }
    }

    /// Populates a WAL with the target number of bytes spread across many aggregates.
    /// Returns the actual bytes written (approximate).
    fn setup_populated_wal(shard_dir: PathBuf, target_bytes: usize) -> usize {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(move || async move {
                let config = create_config(shard_dir);
                let shard_wal = Rc::new(ShardWal::open(config).await.unwrap());

                // Estimate bytes per write (events + metadata overhead ~512 bytes)
                let bytes_per_write_estimate = (EVENT_SIZE_BYTES * EVENTS_PER_BATCH) + 512;
                let total_writes = target_bytes / bytes_per_write_estimate;

                // Pre-compute all write requests to avoid mutable borrow issues with concurrent tasks
                let mut write_requests = Vec::with_capacity(total_writes);
                let mut aggregate_event_indices: HashMap<u128, u64> = HashMap::new();

                for i in 0..total_writes {
                    let aggregate_id = (i % NUM_AGGREGATES) as u128;
                    let aggregate_key = AggregateKey::new(1, 1, aggregate_id);

                    let base_index = aggregate_event_indices.entry(aggregate_id).or_insert(0);

                    let events = create_events(EVENTS_PER_BATCH, EVENT_SIZE_BYTES, *base_index);
                    *base_index += EVENTS_PER_BATCH as u64;

                    let write_request = create_write_request(aggregate_key, events, i as u128);
                    write_requests.push(write_request);
                }

                // Spawn all writes concurrently
                let mut handles = Vec::with_capacity(total_writes);
                for (i, write_request) in write_requests.into_iter().enumerate() {
                    //TODO: Required to prevent a thundering herd of writes larger than the log segment file size
                    glommio::timer::sleep(Duration::from_micros(1)).await;

                    let shard_wal = shard_wal.clone();
                    handles.push(glommio::spawn_local(async move {
                        shard_wal.write(0, write_request).await.unwrap();

                        // Progress indicator for large WALs
                        if i % 1000 == 0 && i > 0 {
                            let progress = (i as f64 / total_writes as f64) * 100.0;
                            eprintln!("  Setup progress: {:.1}% ({}/{})", progress, i, total_writes);
                        }
                    }));
                }

                // Await all write tasks
                for h in handles {
                    h.await;
                }

                total_writes * bytes_per_write_estimate
            })
            .unwrap();

        handle.join().unwrap()
    }
}
