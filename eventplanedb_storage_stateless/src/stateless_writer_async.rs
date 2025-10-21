use eventplanedb_storage_structures::compression_type::CompressionType;
use eventplanedb_storage_structures::constants::{BINCODE_CONFIG_FIXED, BLOOM_BYTES};
use eventplanedb_storage_structures::event_batch_item::EventBatchItem;
use eventplanedb_storage_structures::event_batch_metadata::EventBatchMetadata;
use eventplanedb_storage_structures::event_item::EventItem;
use fastbloom::BloomFilter;
use glommio::io::{DmaFile, DmaStreamWriterBuilder};

use std::collections::HashSet;
use std::io;

use crate::stateless_engine::StatelessEngine;
use crate::wire_format::to_wire_format_variable;

pub trait StatelessWriterAsync {
    async fn append_event_batch_async(
        &self,
        event_batch_writer: &DmaFile,
        metadata_writer: &DmaFile,
        bloom_filter: &mut BloomFilter,
        event_type_dedup: &mut HashSet<u64>,
        compression_type: CompressionType,
        event_batch_item: &EventBatchItem,
    ) -> io::Result<EventBatchMetadata>;
}

impl StatelessWriterAsync for StatelessEngine {
    async fn append_event_batch_async(
        &self,
        event_batch_writer: &DmaFile,
        metadata_writer: &DmaFile,
        bloom_filter: &mut BloomFilter,
        event_type_dedup: &mut HashSet<u64>,
        compression_type: CompressionType,
        event_batch_item: &EventBatchItem,
    ) -> io::Result<EventBatchMetadata> {
        if event_batch_item.events.is_empty() {
            return Err(io::Error::other("Cannot write empty event batch"));
        }

        // Serialize and compress the event data
        let (uncompressed_size, compressed_event_batch_item) =
            to_wire_format_variable(&event_batch_item, compression_type)?;
        let events_crc = crc32fast::hash(&compressed_event_batch_item);

        // Determine event types data (bloom filter or direct array)
        let (event_types, use_bloom) = extract_unique_event_types(&event_batch_item.events);
        let event_types_data = if use_bloom {
            let bloom_bytes =
                create_bloom_filter_bytes(bloom_filter, event_type_dedup, &event_batch_item.events);
            eventplanedb_storage_structures::event_batch_metadata::EventTypesData::Bloom(
                bloom_bytes,
            )
        } else {
            eventplanedb_storage_structures::event_batch_metadata::EventTypesData::Direct(
                event_types,
            )
        };

        // Create and serialize metadata
        let metadata = eventplanedb_storage_structures::event_batch_metadata::EventBatchMetadata::from_batch_item(
            event_batch_item,
            uncompressed_size as u64,
            compressed_event_batch_item.len() as u64,
            compression_type,
            event_types_data,
            events_crc,
        );

        let metadata_bytes = bincode::encode_to_vec(&metadata, BINCODE_CONFIG_FIXED)
            .map_err(|e| io::Error::other(e.to_string()))?;

        // Use DmaStreamWriter to handle alignment automatically and write only actual data
        use futures_lite::AsyncWriteExt;

        // Write event batch data
        {
            let writer_file = event_batch_writer.dup()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Failed to duplicate event batch file: {}", e)))?;
            
            let mut writer = DmaStreamWriterBuilder::new(writer_file)
                .with_buffer_size(64 * 1024)  // 64KB buffer
                .with_write_behind(1)
                .build();
            
            writer.write_all(&compressed_event_batch_item).await
                .map_err(|e| io::Error::new(io::ErrorKind::WriteZero, format!("Failed to write event batch: {}", e)))?;
            
            writer.close().await
                .map_err(|e| io::Error::new(io::ErrorKind::WriteZero, format!("Failed to close event batch writer: {}", e)))?;
        }

        // Write metadata
        {
            let writer_file = metadata_writer.dup()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Failed to duplicate metadata file: {}", e)))?;
            
            let mut writer = DmaStreamWriterBuilder::new(writer_file)
                .with_buffer_size(64 * 1024)  // 64KB buffer  
                .with_write_behind(1)
                .build();
            
            writer.write_all(&metadata_bytes).await
                .map_err(|e| io::Error::new(io::ErrorKind::WriteZero, format!("Failed to write metadata: {}", e)))?;
            
            writer.close().await
                .map_err(|e| io::Error::new(io::ErrorKind::WriteZero, format!("Failed to close metadata writer: {}", e)))?;
        }

        Ok(metadata)
    }
}

fn extract_unique_event_types(events: &[EventItem]) -> ([u64; 4], bool) {
    let mut bloom_or_event_types = [u64::MAX, u64::MAX, u64::MAX, u64::MAX];
    let mut use_bloom = false;
    let mut unique_count = 0;

    for event in events {
        let event_type = event.event_type_major;

        // Check if we already have this event type
        if unique_count > 0 && bloom_or_event_types[0] == event_type {
            continue;
        }
        if unique_count > 1 && bloom_or_event_types[1] == event_type {
            continue;
        }
        if unique_count > 2 && bloom_or_event_types[2] == event_type {
            continue;
        }
        if unique_count > 3 && bloom_or_event_types[3] == event_type {
            continue;
        }

        // New unique event type
        if unique_count < 4 {
            bloom_or_event_types[unique_count] = event_type;
            unique_count += 1;
        } else {
            use_bloom = true;
            break;
        }
    }

    (bloom_or_event_types, use_bloom)
}

fn create_bloom_filter_bytes(
    filter: &mut BloomFilter,
    event_type_dedup: &mut HashSet<u64>,
    events: &[EventItem],
) -> [u64; BLOOM_BYTES / 8] {
    // Populate bloom filter with multiple event types
    filter.clear();
    event_type_dedup.clear();

    for event in events {
        event_type_dedup.insert(event.event_type_major);
    }

    for &event_type in event_type_dedup.iter() {
        filter.insert(&event_type.to_le_bytes());
    }

    filter.as_slice().try_into().expect("Conversion failed")
}

#[cfg(test)]
mod tests {
    use super::*;
    use eventplanedb_storage_structures::constants::{BLOOM_HASH_COUNT, BLOOM_HASH_SEED};
    use glommio::{LocalExecutor, LocalExecutorBuilder, Placement};
    use tempfile::TempDir;

    #[test]
    fn test_async_write_basic() {
        let ex = LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(|| async {
                let temp_dir = TempDir::new().unwrap();
                let event_batch_path = temp_dir.path().join("events.bin");
                let metadata_path = temp_dir.path().join("metadata.bin");

                let event_batch_file = DmaFile::create(&event_batch_path).await.unwrap();
                let metadata_file = DmaFile::create(&metadata_path).await.unwrap();

                let engine = StatelessEngine::builder().build();
                let mut bloom_filter = BloomFilter::with_num_bits(BLOOM_BYTES * 8)
                    .seed(&BLOOM_HASH_SEED)
                    .hashes(BLOOM_HASH_COUNT);
                let mut event_type_dedup = HashSet::new();

                let events = vec![
                    EventItem::new(1, 1, 1000, 42, 1, b"test event 1".to_vec()),
                    EventItem::new(2, 2, 1001, 43, 1, b"test event 2".to_vec()),
                ];

                let event_batch = EventBatchItem::new(1, 12345, 100, Some(200), events);

                let metadata = engine
                    .append_event_batch_async(
                        &event_batch_file,
                        &metadata_file,
                        &mut bloom_filter,
                        &mut event_type_dedup,
                        CompressionType::None,
                        &event_batch,
                    )
                    .await
                    .unwrap();

                assert_eq!(metadata.event_batch_index, 1);
                assert_eq!(metadata.server_timestamp, 12345);
                assert_eq!(metadata.client_id, 100);
                assert_eq!(metadata.user_id, 200);
                assert!(metadata.compressed_size > 0);
                assert!(metadata.uncompressed_size > 0);

                // Verify files were written
                let event_batch_size = event_batch_file.file_size().await.unwrap();
                let metadata_size = metadata_file.file_size().await.unwrap();

                assert!(event_batch_size > 0);
                assert!(metadata_size > 0);

                event_batch_file.close().await.unwrap();
                metadata_file.close().await.unwrap();
            })
            .unwrap();

        ex.join().unwrap();
    }

    #[test]
    fn test_async_write_with_compression() {
        let ex = LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(|| async {
                let temp_dir = TempDir::new().unwrap();
                let event_batch_path = temp_dir.path().join("events.bin");
                let metadata_path = temp_dir.path().join("metadata.bin");

                let event_batch_file = DmaFile::create(&event_batch_path).await.unwrap();
                let metadata_file = DmaFile::create(&metadata_path).await.unwrap();

                let engine = StatelessEngine::builder().build();
                let mut bloom_filter = BloomFilter::with_num_bits(BLOOM_BYTES * 8)
                    .seed(&BLOOM_HASH_SEED)
                    .hashes(BLOOM_HASH_COUNT);
                let mut event_type_dedup = HashSet::new();

                // Create events with larger, more compressible data
                let large_data = vec![b'A'; 1000];
                let events = vec![
                    EventItem::new(1, 1, 1000, 42, 1, large_data.clone()),
                    EventItem::new(2, 2, 1001, 43, 1, large_data.clone()),
                ];

                let event_batch = EventBatchItem::new(1, 12345, 100, Some(200), events);

                let metadata = engine
                    .append_event_batch_async(
                        &event_batch_file,
                        &metadata_file,
                        &mut bloom_filter,
                        &mut event_type_dedup,
                        CompressionType::Zstd { level: 3 },
                        &event_batch,
                    )
                    .await
                    .unwrap();

                // With compression, compressed size should be less than uncompressed
                assert!(metadata.compressed_size < metadata.uncompressed_size);

                event_batch_file.close().await.unwrap();
                metadata_file.close().await.unwrap();
            })
            .unwrap();

        ex.join().unwrap();
    }

    #[test]
    fn test_async_write_empty_events_error() {
        let ex = LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(|| async {
                let temp_dir = TempDir::new().unwrap();
                let event_batch_path = temp_dir.path().join("events.bin");
                let metadata_path = temp_dir.path().join("metadata.bin");

                let event_batch_file = DmaFile::create(&event_batch_path).await.unwrap();
                let metadata_file = DmaFile::create(&metadata_path).await.unwrap();

                let engine = StatelessEngine::builder().build();
                let mut bloom_filter = BloomFilter::with_num_bits(BLOOM_BYTES * 8)
                    .seed(&BLOOM_HASH_SEED)
                    .hashes(BLOOM_HASH_COUNT);
                let mut event_type_dedup = HashSet::new();

                let event_batch = EventBatchItem::new(1, 12345, 100, Some(200), vec![]);

                let result = engine
                    .append_event_batch_async(
                        &event_batch_file,
                        &metadata_file,
                        &mut bloom_filter,
                        &mut event_type_dedup,
                        CompressionType::None,
                        &event_batch,
                    )
                    .await;

                assert!(result.is_err());
                assert!(result
                    .unwrap_err()
                    .to_string()
                    .contains("Cannot write empty event batch"));

                event_batch_file.close().await.unwrap();
                metadata_file.close().await.unwrap();
            })
            .unwrap();

        ex.join().unwrap();
    }

    #[test]
    fn test_async_write_bloom_filter_vs_direct() {
        let ex = LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(|| async {
                let temp_dir = TempDir::new().unwrap();
                let engine = StatelessEngine::builder().build();
                let mut bloom_filter = BloomFilter::with_num_bits(BLOOM_BYTES * 8)
                    .seed(&BLOOM_HASH_SEED)
                    .hashes(BLOOM_HASH_COUNT);
                let mut event_type_dedup = HashSet::new();

                // Test with few event types (should use direct array)
                {
                    let event_batch_path = temp_dir.path().join("events_direct.bin");
                    let metadata_path = temp_dir.path().join("metadata_direct.bin");

                    let event_batch_file = DmaFile::create(&event_batch_path).await.unwrap();
                    let metadata_file = DmaFile::create(&metadata_path).await.unwrap();

                    let events = vec![
                        EventItem::new(1, 1, 1000, 42, 1, b"event1".to_vec()),
                        EventItem::new(2, 2, 1001, 43, 1, b"event2".to_vec()),
                    ];
                    let event_batch = EventBatchItem::new(1, 12345, 100, None, events);

                    let metadata = engine
                        .append_event_batch_async(
                            &event_batch_file,
                            &metadata_file,
                            &mut bloom_filter,
                            &mut event_type_dedup,
                            CompressionType::None,
                            &event_batch,
                        )
                        .await
                        .unwrap();

                    // Should use direct array
                    match metadata.event_types_data {
                        eventplanedb_storage_structures::event_batch_metadata::EventTypesData::Direct(types) => {
                            assert!(types.contains(&42));
                            assert!(types.contains(&43));
                        }
                        _ => panic!("Expected direct event types"),
                    }

                    event_batch_file.close().await.unwrap();
                    metadata_file.close().await.unwrap();
                }

                // Test with many event types (should use bloom filter)
                {
                    let event_batch_path = temp_dir.path().join("events_bloom.bin");
                    let metadata_path = temp_dir.path().join("metadata_bloom.bin");

                    let event_batch_file = DmaFile::create(&event_batch_path).await.unwrap();
                    let metadata_file = DmaFile::create(&metadata_path).await.unwrap();

                    let events: Vec<EventItem> = (0..10)
                        .map(|i| EventItem::new(i + 1, i + 1, 1000 + i, 100 + i, 1, format!("event{}", i).into_bytes()))
                        .collect();
                    let event_batch = EventBatchItem::new(1, 12345, 100, None, events);

                    let metadata = engine
                        .append_event_batch_async(
                            &event_batch_file,
                            &metadata_file,
                            &mut bloom_filter,
                            &mut event_type_dedup,
                            CompressionType::None,
                            &event_batch,
                        )
                        .await
                        .unwrap();

                    // Should use bloom filter
                    match metadata.event_types_data {
                        eventplanedb_storage_structures::event_batch_metadata::EventTypesData::Bloom(_) => {
                            // Good, this is expected
                        }
                        _ => panic!("Expected bloom filter event types"),
                    }

                    event_batch_file.close().await.unwrap();
                    metadata_file.close().await.unwrap();
                }
            })
            .unwrap();

        ex.join().unwrap();
    }
}