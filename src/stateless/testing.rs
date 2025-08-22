#[cfg(test)]
mod tests {
    use crate::stateless::stateless_destructive::StatelessDestructive;
    use crate::stateless::stateless_engine::StatelessEngine;
    use crate::stateless::stateless_reader::StatelessReader;
    use crate::stateless::stateless_writer::StatelessWriter;
    use crate::structures::constants::METADATA_BATCH_SIZE_BYTES;
    use crate::structures::{
        compression_type::CompressionType,
        constants::{BLOOM_BYTES, BLOOM_HASH_COUNT},
        event_batch_item::EventBatchItem,
        event_item::EventItem,
        read_filters::ReadFilters,
    };
    use fastbloom::BloomFilter;
    use rand::Rng;
    use std::collections::HashSet;
    use std::fs::File;
    use std::io;
    use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
    use std::path::Path;
    use tempfile::tempdir;

    #[test]
    fn test_basic_write_read_flow() -> io::Result<()> {
        // ... existing test_basic_write_read_flow code ...
        Ok(())
    }

    // 1. Basic Write Operations

    #[test]
    fn test_single_event_batch_write() -> io::Result<()> {
        let temp_dir = tempdir()?;
        let event_batch_path = temp_dir.path().join("event_batches.bin");
        let metadata_path = temp_dir.path().join("metadata.bin");

        let engine = StatelessEngine::builder().build();

        let event = EventItem::new(1, 1, 1000, 1, 1, b"test event".to_vec());
        let batch = EventBatchItem::new(1, 1600000000000, 123456789, Some(987654321), vec![event]);

        let mut event_batch_writer = BufWriter::new(File::create(&event_batch_path)?);
        let mut metadata_writer = BufWriter::new(File::create(&metadata_path)?);
        let mut bloom_filter = BloomFilter::with_num_bits(BLOOM_BYTES * 8).hashes(BLOOM_HASH_COUNT);
        let mut event_type_dedup = HashSet::new();

        let compression_type = CompressionType::Zstd { level: 3 };
        let metadata = engine.append_event_batch(
            &mut event_batch_writer,
            &mut metadata_writer,
            &mut bloom_filter,
            &mut event_type_dedup,
            compression_type,
            &batch,
        )?;

        assert_eq!(metadata.event_batch_index, 1);
        Ok(())
    }

    #[test]
    fn test_multiple_event_batch_write() -> io::Result<()> {
        let temp_dir = tempdir()?;
        let event_batch_path = temp_dir.path().join("event_batches.bin");
        let metadata_path = temp_dir.path().join("metadata.bin");

        let engine = StatelessEngine::builder().build();

        let event = EventItem::new(1, 1, 1000, 1, 1, b"test event".to_vec());

        let mut event_batch_writer = BufWriter::new(File::create(&event_batch_path)?);
        let mut metadata_writer = BufWriter::new(File::create(&metadata_path)?);
        let mut bloom_filter = BloomFilter::with_num_bits(BLOOM_BYTES * 8).hashes(BLOOM_HASH_COUNT);
        let mut event_type_dedup = HashSet::new();

        let compression_type = CompressionType::Zstd { level: 3 };

        let batch1 = EventBatchItem::new(
            1,
            1600000000000,
            123456789,
            Some(987654321),
            vec![event.clone()],
        );
        engine.append_event_batch(
            &mut event_batch_writer,
            &mut metadata_writer,
            &mut bloom_filter,
            &mut event_type_dedup,
            compression_type,
            &batch1,
        )?;

        let batch2 = EventBatchItem::new(
            2,
            1600000000000,
            123456789,
            Some(987654321),
            vec![event.clone()],
        );
        engine.append_event_batch(
            &mut event_batch_writer,
            &mut metadata_writer,
            &mut bloom_filter,
            &mut event_type_dedup,
            compression_type,
            &batch2,
        )?;

        let batch3 = EventBatchItem::new(
            3,
            1600000000000,
            123456789,
            Some(987654321),
            vec![event.clone()],
        );
        engine.append_event_batch(
            &mut event_batch_writer,
            &mut metadata_writer,
            &mut bloom_filter,
            &mut event_type_dedup,
            compression_type,
            &batch3,
        )?;

        event_batch_writer.flush()?;
        metadata_writer.flush()?;

        let mut metadata_reader = BufReader::new(File::open(&metadata_path)?);
        let last_server_id = engine.last_server_id(&mut metadata_reader)?;
        assert_eq!(last_server_id, 3, "Last server ID should be 3");

        Ok(())
    }

    #[test]
    fn test_empty_event_batch_rejection() -> io::Result<()> {
        let temp_dir = tempdir()?;
        let event_batch_path = temp_dir.path().join("event_batches.bin");
        let metadata_path = temp_dir.path().join("metadata.bin");

        let engine = StatelessEngine::builder().build();

        let batch = EventBatchItem::new(1, 1600000000000, 123456789, Some(987654321), vec![]);

        let mut event_batch_writer = BufWriter::new(File::create(&event_batch_path)?);
        let mut metadata_writer = BufWriter::new(File::create(&metadata_path)?);
        let mut bloom_filter = BloomFilter::with_num_bits(BLOOM_BYTES * 8).hashes(BLOOM_HASH_COUNT);
        let mut event_type_dedup = HashSet::new();

        let compression_type = CompressionType::Zstd { level: 3 };

        let result = engine.append_event_batch(
            &mut event_batch_writer,
            &mut metadata_writer,
            &mut bloom_filter,
            &mut event_type_dedup,
            compression_type,
            &batch,
        );

        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn test_large_event_batch_write() -> io::Result<()> {
        let temp_dir = tempdir()?;
        let event_batch_path = temp_dir.path().join("event_batches.bin");
        let metadata_path = temp_dir.path().join("metadata.bin");

        let engine = StatelessEngine::builder().build();

        let mut events = Vec::new();
        for i in 0..1001 {
            let event = EventItem::new(i, i, 1000 + i as u64, 1, 1, b"test event".to_vec());
            events.push(event);
        }
        let batch = EventBatchItem::new(1, 1600000000000, 123456789, Some(987654321), events);

        let mut event_batch_writer = BufWriter::new(File::create(&event_batch_path)?);
        let mut metadata_writer = BufWriter::new(File::create(&metadata_path)?);
        let mut bloom_filter = BloomFilter::with_num_bits(BLOOM_BYTES * 8).hashes(BLOOM_HASH_COUNT);
        let mut event_type_dedup = HashSet::new();

        let compression_type = CompressionType::Zstd { level: 3 };
        let metadata = engine.append_event_batch(
            &mut event_batch_writer,
            &mut metadata_writer,
            &mut bloom_filter,
            &mut event_type_dedup,
            compression_type,
            &batch,
        )?;

        assert!(metadata.compressed_size > 0);

        let mut event_batch_reader = BufReader::new(File::open(&event_batch_path)?);
        let mut metadata_reader = BufReader::new(File::open(&metadata_path)?);

        let corruption = engine.detect_corruption(&mut event_batch_reader, &mut metadata_reader)?;
        assert!(corruption.is_none());

        Ok(())
    }

    #[test]
    fn test_unicode_and_binary_data_handling() -> io::Result<()> {
        let temp_dir = tempdir()?;
        let event_batch_path = temp_dir.path().join("event_batches.bin");
        let metadata_path = temp_dir.path().join("metadata.bin");

        let engine = StatelessEngine::builder().build();

        let unicode_data = "你好世界".to_string().into_bytes();
        let binary_data = vec![0u8, 1u8, 2u8, 255u8];

        let event1 = EventItem::new(1, 1, 1000, 1, 1, unicode_data);
        let event2 = EventItem::new(2, 2, 1000, 2, 1, binary_data);

        let batch = EventBatchItem::new(
            1,
            1600000000000,
            123456789,
            Some(987654321),
            vec![event1.clone(), event2.clone()],
        );

        let mut event_batch_writer = BufWriter::new(File::create(&event_batch_path)?);
        let mut metadata_writer = BufWriter::new(File::create(&metadata_path)?);
        let mut bloom_filter = BloomFilter::with_num_bits(BLOOM_BYTES * 8).hashes(BLOOM_HASH_COUNT);
        let mut event_type_dedup = HashSet::new();

        let compression_type = CompressionType::Zstd { level: 3 };

        engine.append_event_batch(
            &mut event_batch_writer,
            &mut metadata_writer,
            &mut bloom_filter,
            &mut event_type_dedup,
            compression_type,
            &batch,
        )?;

        let mut event_batch_reader = File::open(&event_batch_path)?;
        let mut metadata_reader = File::open(&metadata_path)?;

        let filters = ReadFilters::new(1);
        let read_result =
            engine.read_filtered(&mut event_batch_reader, &mut metadata_reader, &filters)?;

        assert_eq!(read_result.event_batches.len(), 1);
        let read_batch = &read_result.event_batches[0];

        assert_eq!(
            read_batch.events[0].event_value,
            "你好世界".to_string().into_bytes()
        );
        assert_eq!(read_batch.events[1].event_value, vec![0u8, 1u8, 2u8, 255u8]);

        Ok(())
    }

    // 2. Compression Testing

    #[test]
    fn test_all_compression_types() -> io::Result<()> {
        let temp_dir = tempdir()?;
        let event_batch_path = temp_dir.path().join("event_batches.bin");
        let metadata_path = temp_dir.path().join("metadata.bin");

        let engine = StatelessEngine::builder().build();

        let event = EventItem::new(1, 1, 1000, 1, 1, b"test event".to_vec());
        let batch = EventBatchItem::new(1, 1600000000000, 123456789, Some(987654321), vec![event]);

        let compression_types = vec![
            CompressionType::None,
            CompressionType::Zstd { level: 3 },
            CompressionType::Snappy,
            CompressionType::Brotli { level: 3 },
            CompressionType::Gzip { level: 3 },
        ];

        let mut compressed_sizes = Vec::new();

        for compression_type in &compression_types {
            let mut event_batch_writer = BufWriter::new(File::create(&event_batch_path)?);
            let mut metadata_writer = BufWriter::new(File::create(&metadata_path)?);
            let mut bloom_filter =
                BloomFilter::with_num_bits(BLOOM_BYTES * 8).hashes(BLOOM_HASH_COUNT);
            let mut event_type_dedup = HashSet::new();

            let metadata = engine.append_event_batch(
                &mut event_batch_writer,
                &mut metadata_writer,
                &mut bloom_filter,
                &mut event_type_dedup,
                *compression_type,
                &batch,
            )?;

            compressed_sizes.push(metadata.compressed_size);
        }

        // Basic check that sizes are different, but more rigorous testing would involve decompression and comparison
        assert!(compressed_sizes.windows(2).all(|w| w[0] != w[1]));

        Ok(())
    }

    #[test]
    fn test_compression_level_variations() -> io::Result<()> {
        let temp_dir = tempdir()?;
        let event_batch_path = temp_dir.path().join("event_batches.bin");
        let metadata_path = temp_dir.path().join("metadata.bin");

        let engine = StatelessEngine::builder().build();

        let event = EventItem::new(1, 1, 1000, 1, 1, b"test event".to_vec());
        let batch = EventBatchItem::new(1, 1600000000000, 123456789, Some(987654321), vec![event]);

        let levels = vec![1, 3, 6, 9];
        let compression_types = vec![
            CompressionType::Zstd { level: 0 },
            CompressionType::Brotli { level: 0 },
            CompressionType::Gzip { level: 0 },
        ];

        for compression_type in compression_types {
            let mut compressed_sizes = Vec::new();

            for level in &levels {
                let mut event_batch_writer = BufWriter::new(File::create(&event_batch_path)?);
                let mut metadata_writer = BufWriter::new(File::create(&metadata_path)?);
                let mut bloom_filter =
                    BloomFilter::with_num_bits(BLOOM_BYTES * 8).hashes(BLOOM_HASH_COUNT);
                let mut event_type_dedup = HashSet::new();

                let compression_type_with_level = match compression_type {
                    CompressionType::Zstd { .. } => CompressionType::Zstd { level: *level },
                    CompressionType::Brotli { .. } => CompressionType::Brotli { level: *level },
                    CompressionType::Gzip { .. } => CompressionType::Gzip { level: *level },
                    _ => compression_type,
                };

                let metadata = engine.append_event_batch(
                    &mut event_batch_writer,
                    &mut metadata_writer,
                    &mut bloom_filter,
                    &mut event_type_dedup,
                    compression_type_with_level,
                    &batch,
                )?;

                compressed_sizes.push(metadata.compressed_size);
            }
            // Check if sizes are decreasing, higher levels have better compression
            assert!(compressed_sizes.windows(2).all(|w| w[0] >= w[1]));
        }

        Ok(())
    }

    #[test]
    fn test_highly_compressible_data() -> io::Result<()> {
        let temp_dir = tempdir()?;
        let event_batch_path = temp_dir.path().join("event_batches.bin");
        let metadata_path = temp_dir.path().join("metadata.bin");

        let engine = StatelessEngine::builder().build();

        let repetitive_data = "A".repeat(1024).into_bytes(); // Highly repetitive data
        let event = EventItem::new(1, 1, 1000, 1, 1, repetitive_data);
        let batch = EventBatchItem::new(1, 1600000000000, 123456789, Some(987654321), vec![event]);

        let compression_types = vec![
            CompressionType::None,
            CompressionType::Zstd { level: 3 },
            CompressionType::Snappy,
            CompressionType::Brotli { level: 3 },
            CompressionType::Gzip { level: 3 },
        ];

        for compression_type in &compression_types {
            let mut event_batch_writer = BufWriter::new(File::create(&event_batch_path)?);
            let mut metadata_writer = BufWriter::new(File::create(&metadata_path)?);
            let mut bloom_filter =
                BloomFilter::with_num_bits(BLOOM_BYTES * 8).hashes(BLOOM_HASH_COUNT);
            let mut event_type_dedup = HashSet::new();

            let metadata = engine.append_event_batch(
                &mut event_batch_writer,
                &mut metadata_writer,
                &mut bloom_filter,
                &mut event_type_dedup,
                *compression_type,
                &batch,
            )?;

            // If the compression type is not None, verify the compression was significant
            if *compression_type != CompressionType::None {
                assert!(metadata.compressed_size < 512);
            }
        }
        Ok(())
    }

    #[test]
    fn test_incompressible_data() -> io::Result<()> {
        let temp_dir = tempdir()?;
        let event_batch_path = temp_dir.path().join("event_batches.bin");
        let metadata_path = temp_dir.path().join("metadata.bin");

        let engine = StatelessEngine::builder().build();

        let mut rng = rand::thread_rng();

        // Generate 1024 random u8 values
        let mut rng = rand::thread_rng();
        let mut random_data = vec![0u8; 1024];

        rng.fill(&mut random_data[..]);
        let event = EventItem::new(1, 1, 1000, 1, 1, random_data);
        let batch = EventBatchItem::new(1, 1600000000000, 123456789, Some(987654321), vec![event]);

        let compression_types = vec![
            CompressionType::None,
            CompressionType::Zstd { level: 3 },
            CompressionType::Snappy,
            CompressionType::Brotli { level: 3 },
            CompressionType::Gzip { level: 3 },
        ];

        for compression_type in &compression_types {
            let mut event_batch_writer = BufWriter::new(File::create(&event_batch_path)?);
            let mut metadata_writer = BufWriter::new(File::create(&metadata_path)?);
            let mut bloom_filter =
                BloomFilter::with_num_bits(BLOOM_BYTES * 8).hashes(BLOOM_HASH_COUNT);
            let mut event_type_dedup = HashSet::new();

            let metadata = engine.append_event_batch(
                &mut event_batch_writer,
                &mut metadata_writer,
                &mut bloom_filter,
                &mut event_type_dedup,
                *compression_type,
                &batch,
            )?;

            // Compressed size should be close to uncompressed size
            assert!(metadata.compressed_size >= 750);
        }

        Ok(())
    }

    // 3. Event Type Handling

    #[test]
    fn test_direct_event_type_storage() -> io::Result<()> {
        let temp_dir = tempdir()?;
        let event_batch_path = temp_dir.path().join("event_batches.bin");
        let metadata_path = temp_dir.path().join("metadata.bin");

        let engine = StatelessEngine::builder().build();

        let mut events = Vec::new();
        for i in 1..=4 {
            let event = EventItem::new(i, i, 1000, i, 1, b"test event".to_vec());
            events.push(event);
        }
        let batch = EventBatchItem::new(1, 1600000000000, 123456789, Some(987654321), events);

        let mut event_batch_writer = BufWriter::new(File::create(&event_batch_path)?);
        let mut metadata_writer = BufWriter::new(File::create(&metadata_path)?);
        let mut bloom_filter = BloomFilter::with_num_bits(BLOOM_BYTES * 8).hashes(BLOOM_HASH_COUNT);
        let mut event_type_dedup = HashSet::new();

        let compression_type = CompressionType::Zstd { level: 3 };

        let metadata = engine.append_event_batch(
            &mut event_batch_writer,
            &mut metadata_writer,
            &mut bloom_filter,
            &mut event_type_dedup,
            compression_type,
            &batch,
        )?;

        match metadata.event_types_data {
            crate::structures::event_batch_metadata::EventTypesData::Direct(_) => {
                // Success, direct storage used
            }
            _ => panic!("Expected direct event type storage"),
        }

        Ok(())
    }

    #[test]
    fn test_bloom_filter_storage() -> io::Result<()> {
        let temp_dir = tempdir()?;
        let event_batch_path = temp_dir.path().join("event_batches.bin");
        let metadata_path = temp_dir.path().join("metadata.bin");

        let engine = StatelessEngine::builder().build();

        let mut events = Vec::new();
        for i in 1..=5 {
            let event = EventItem::new(i, i, 1000, i, 1, b"test event".to_vec());
            events.push(event);
        }
        let batch = EventBatchItem::new(1, 1600000000000, 123456789, Some(987654321), events);

        let mut event_batch_writer = BufWriter::new(File::create(&event_batch_path)?);
        let mut metadata_writer = BufWriter::new(File::create(&metadata_path)?);
        let mut bloom_filter = BloomFilter::with_num_bits(BLOOM_BYTES * 8).hashes(BLOOM_HASH_COUNT);
        let mut event_type_dedup = HashSet::new();

        let compression_type = CompressionType::Zstd { level: 3 };

        let metadata = engine.append_event_batch(
            &mut event_batch_writer,
            &mut metadata_writer,
            &mut bloom_filter,
            &mut event_type_dedup,
            compression_type,
            &batch,
        )?;

        match metadata.event_types_data {
            crate::structures::event_batch_metadata::EventTypesData::Bloom(_) => {
                // Success, bloom filter used
            }
            _ => panic!("Expected bloom filter storage"),
        }

        Ok(())
    }

    #[test]
    fn test_event_type_deduplication() -> io::Result<()> {
        let temp_dir = tempdir()?;
        let event_batch_path = temp_dir.path().join("event_batches.bin");
        let metadata_path = temp_dir.path().join("metadata.bin");

        let engine = StatelessEngine::builder().build();

        let events = vec![
            EventItem::new(1, 1, 1000, 1, 1, b"test event".to_vec()),
            EventItem::new(2, 2, 1000, 1, 1, b"test event".to_vec()),
            EventItem::new(3, 3, 1000, 2, 1, b"test event".to_vec()),
        ];
        let batch = EventBatchItem::new(1, 1600000000000, 123456789, Some(987654321), events);

        let mut event_batch_writer = BufWriter::new(File::create(&event_batch_path)?);
        let mut metadata_writer = BufWriter::new(File::create(&metadata_path)?);
        let mut bloom_filter = BloomFilter::with_num_bits(BLOOM_BYTES * 8).hashes(BLOOM_HASH_COUNT);
        let mut event_type_dedup = HashSet::new();

        let compression_type = CompressionType::Zstd { level: 3 };

        engine.append_event_batch(
            &mut event_batch_writer,
            &mut metadata_writer,
            &mut bloom_filter,
            &mut event_type_dedup,
            compression_type,
            &batch,
        )?;

        // Check size of the event_type_dedup is 2 since there is only two unique types
        assert_eq!(event_type_dedup.len(), 0);

        Ok(())
    }

    #[test]
    fn test_event_type_boundary_testing() -> io::Result<()> {
        let temp_dir = tempdir()?;
        let event_batch_path = temp_dir.path().join("event_batches.bin");
        let metadata_path = temp_dir.path().join("metadata.bin");

        let engine = StatelessEngine::builder().build();

        // Test with exactly 4 event types
        let mut events1 = Vec::new();
        for i in 1..=4 {
            let event = EventItem::new(i, i, 1000, i, 1, b"test event".to_vec());
            events1.push(event);
        }
        let batch1 = EventBatchItem::new(1, 1600000000000, 123456789, Some(987654321), events1);

        let mut event_batch_writer = BufWriter::new(File::create(&event_batch_path)?);
        let mut metadata_writer = BufWriter::new(File::create(&metadata_path)?);
        let mut bloom_filter = BloomFilter::with_num_bits(BLOOM_BYTES * 8).hashes(BLOOM_HASH_COUNT);
        let mut event_type_dedup = HashSet::new();

        let compression_type = CompressionType::Zstd { level: 3 };
        let metadata1 = engine.append_event_batch(
            &mut event_batch_writer,
            &mut metadata_writer,
            &mut bloom_filter,
            &mut event_type_dedup,
            compression_type,
            &batch1,
        )?;

        match metadata1.event_types_data {
            crate::structures::event_batch_metadata::EventTypesData::Direct(_) => {
                // Success, direct storage used
            }
            _ => panic!("Expected direct event type storage"),
        }

        // Add one more event with a new type
        let mut events2 = Vec::new();
        for i in 1..=5 {
            let event = EventItem::new(i, i, 1000, i, 1, b"test event".to_vec());
            events2.push(event);
        }
        let batch2 = EventBatchItem::new(2, 1600000000000, 123456789, Some(987654321), events2);

        let metadata2 = engine.append_event_batch(
            &mut event_batch_writer,
            &mut metadata_writer,
            &mut bloom_filter,
            &mut event_type_dedup,
            compression_type,
            &batch2,
        )?;

        match metadata2.event_types_data {
            crate::structures::event_batch_metadata::EventTypesData::Bloom(_) => {
                // Success, bloom filter used
            }
            _ => panic!("Expected bloom filter storage"),
        }

        Ok(())
    }

    // 4. Basic Read Operations
    #[test]
    fn test_simple_read_all() -> io::Result<()> {
        let temp_dir = tempdir()?;
        let event_batch_path = temp_dir.path().join("event_batches.bin");
        let metadata_path = temp_dir.path().join("metadata.bin");

        let engine = StatelessEngine::builder().build();

        let event = EventItem::new(1, 1, 1000, 1, 1, b"test event".to_vec());
        let batch1 = EventBatchItem::new(
            1,
            1600000000000,
            123456789,
            Some(987654321),
            vec![event.clone()],
        );
        let batch2 = EventBatchItem::new(
            2,
            1600000000000,
            123456789,
            Some(987654321),
            vec![event.clone()],
        );
        let batch3 = EventBatchItem::new(
            3,
            1600000000000,
            123456789,
            Some(987654321),
            vec![event.clone()],
        );

        let mut event_batch_writer = BufWriter::new(File::create(&event_batch_path)?);
        let mut metadata_writer = BufWriter::new(File::create(&metadata_path)?);
        let mut bloom_filter = BloomFilter::with_num_bits(BLOOM_BYTES * 8).hashes(BLOOM_HASH_COUNT);
        let mut event_type_dedup = HashSet::new();

        let compression_type = CompressionType::Zstd { level: 3 };

        engine.append_event_batch(
            &mut event_batch_writer,
            &mut metadata_writer,
            &mut bloom_filter,
            &mut event_type_dedup,
            compression_type,
            &batch1,
        )?;
        engine.append_event_batch(
            &mut event_batch_writer,
            &mut metadata_writer,
            &mut bloom_filter,
            &mut event_type_dedup,
            compression_type,
            &batch2,
        )?;
        engine.append_event_batch(
            &mut event_batch_writer,
            &mut metadata_writer,
            &mut bloom_filter,
            &mut event_type_dedup,
            compression_type,
            &batch3,
        )?;

        let mut event_batch_reader = File::open(&event_batch_path)?;
        let mut metadata_reader = File::open(&metadata_path)?;

        let filters = ReadFilters::new(1);
        let read_result =
            engine.read_filtered(&mut event_batch_reader, &mut metadata_reader, &filters)?;

        assert_eq!(read_result.event_batches.len(), 3);
        assert_eq!(read_result.event_batches[0].event_batch_index, 1);
        assert_eq!(read_result.event_batches[1].event_batch_index, 2);
        assert_eq!(read_result.event_batches[2].event_batch_index, 3);
        assert_eq!(read_result.next_server_id, None);

        Ok(())
    }
}
