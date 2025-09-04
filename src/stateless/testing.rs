#[cfg(test)]
mod tests {
    // Platform-specific raw file descriptor traits
    #[cfg(unix)]
    use std::os::fd::AsRawFd;
    #[cfg(windows)]
    use std::os::windows::io::AsRawHandle;

    use crate::stateless::stateless_destructive::StatelessDestructive;
    use crate::stateless::stateless_engine::StatelessEngine;
    use crate::stateless::stateless_reader::{CorruptPositions, StatelessReader};
    use crate::stateless::stateless_writer::StatelessWriter;
    use crate::stateless::test_fixture::tests::TestFixture;
    use crate::structures::constants::{
        BINCODE_CONFIG_FIXED, BLOOM_HASH_SEED, METADATA_BATCH_SIZE_BYTES,
    };
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

    // 1. Basic Write Operations

    /// Test writing a single event in a single batch, checking all metadata and field values
    #[test]
    fn test_single_event_batch_write() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        let mut event = fixture.create_simple_event(0);
        event.client_event_index = 33;
        let batch = fixture.create_simple_batch(0, vec![event]);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        let metadata =
            fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch)?;

        assert_eq!(metadata.server_timestamp, 1600000000000);
        assert_eq!(metadata.event_batch_index, 0);
        assert!(metadata.uncompressed_size > 0);
        assert!(metadata.compressed_size > 0);
        assert!(metadata.events_crc > 0);
        assert_eq!(metadata.compression_type, 1);
        assert_eq!(metadata.client_id, 123456789);
        assert_eq!(metadata.user_id, 987654321);
        assert_eq!(metadata.min_event_timestamp, 1000);
        assert_eq!(metadata.max_event_timestamp, 1000);
        assert_eq!(metadata.min_event_index, 0);
        assert_eq!(metadata.max_event_index, 0);
        assert_eq!(metadata.min_client_event_index, 33);
        assert_eq!(metadata.max_client_event_index, 33);
        match metadata.event_types_data {
            crate::structures::event_batch_metadata::EventTypesData::Direct(ref types) => {
                assert_eq!(types.len(), 4);
                assert_eq!(types[0], 1);
                assert_eq!(types[1], u64::MAX);
                assert_eq!(types[2], u64::MAX);
                assert_eq!(types[3], u64::MAX);
            }
            _ => panic!("Expected direct event type storage"),
        }

        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;
        let read_result = fixture.read_filtered(
            &mut event_batch_reader,
            &mut metadata_reader,
            &crate::structures::read_filters::ReadFilters::default(),
        )?;

        assert_eq!(read_result.next_event_batch_index, None);
        assert_eq!(read_result.event_batches.len(), 1);
        let event_batch_0 = &read_result.event_batches[0];

        assert_eq!(event_batch_0.server_timestamp, 1600000000000);
        assert_eq!(event_batch_0.event_batch_index, 0);
        assert_eq!(event_batch_0.client_id, 123456789);
        assert_eq!(event_batch_0.user_id, Some(987654321));

        assert_eq!(event_batch_0.events.len(), 1);
        assert_eq!(event_batch_0.events[0].event_index, 0);
        assert_eq!(event_batch_0.events[0].event_timestamp, 1000);
        assert_eq!(event_batch_0.events[0].client_event_index, 33);
        assert_eq!(event_batch_0.events[0].event_type_major, 1);
        assert_eq!(event_batch_0.events[0].event_type_minor, 1);
        assert_eq!(event_batch_0.events[0].event_value, b"test event".to_vec());

        Ok(())
    }

    /// Test writing multiple events in a single batch, checking all metadata and field values
    #[test]
    fn test_dual_event_batch_write() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        let mut event1 = fixture.create_simple_event(0);
        event1.client_event_index = 33;

        let mut event2 = fixture.create_simple_event(1);
        event2.client_event_index = 34;
        event2.event_index = 1;
        event2.event_timestamp = 1550;
        event2.event_type_major = 20;
        event2.event_type_minor = 23;
        event2.event_value = b"test event 2".to_vec();

        let batch = fixture.create_simple_batch(0, vec![event1, event2]);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        let metadata =
            fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch)?;

        assert_eq!(metadata.server_timestamp, 1600000000000);
        assert_eq!(metadata.event_batch_index, 0);
        assert!(metadata.uncompressed_size > 0);
        assert!(metadata.compressed_size > 0);
        assert!(metadata.events_crc > 0);
        assert_eq!(metadata.compression_type, 1);
        assert_eq!(metadata.client_id, 123456789);
        assert_eq!(metadata.user_id, 987654321);
        assert_eq!(metadata.min_event_timestamp, 1000);
        assert_eq!(metadata.max_event_timestamp, 1550);
        assert_eq!(metadata.min_event_index, 0);
        assert_eq!(metadata.max_event_index, 1);
        assert_eq!(metadata.min_client_event_index, 33);
        assert_eq!(metadata.max_client_event_index, 34);
        match metadata.event_types_data {
            crate::structures::event_batch_metadata::EventTypesData::Direct(ref types) => {
                assert_eq!(types.len(), 4);
                assert_eq!(types[0], 1);
                assert_eq!(types[1], 20);
                assert_eq!(types[2], u64::MAX);
                assert_eq!(types[3], u64::MAX);
            }
            _ => panic!("Expected direct event type storage"),
        }

        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;
        let read_result = fixture.read_filtered(
            &mut event_batch_reader,
            &mut metadata_reader,
            &crate::structures::read_filters::ReadFilters::default(),
        )?;

        assert_eq!(read_result.next_event_batch_index, None);
        assert_eq!(read_result.event_batches.len(), 1);
        let event_batch_0 = &read_result.event_batches[0];

        assert_eq!(event_batch_0.server_timestamp, 1600000000000);
        assert_eq!(event_batch_0.event_batch_index, 0);
        assert_eq!(event_batch_0.client_id, 123456789);
        assert_eq!(event_batch_0.user_id, Some(987654321));

        assert_eq!(event_batch_0.events.len(), 2);
        assert_eq!(event_batch_0.events[0].event_index, 0);
        assert_eq!(event_batch_0.events[0].event_timestamp, 1000);
        assert_eq!(event_batch_0.events[0].client_event_index, 33);
        assert_eq!(event_batch_0.events[0].event_type_major, 1);
        assert_eq!(event_batch_0.events[0].event_type_minor, 1);
        assert_eq!(event_batch_0.events[0].event_value, b"test event".to_vec());
        assert_eq!(event_batch_0.events[1].event_index, 1);
        assert_eq!(event_batch_0.events[1].event_timestamp, 1550);
        assert_eq!(event_batch_0.events[1].client_event_index, 34);
        assert_eq!(event_batch_0.events[1].event_type_major, 20);
        assert_eq!(event_batch_0.events[1].event_type_minor, 23);
        assert_eq!(
            event_batch_0.events[1].event_value,
            b"test event 2".to_vec()
        );

        Ok(())
    }

    /// Test writing multiple event batches and reading them back, checking all metadata and field values
    #[test]
    fn test_multiple_event_batch_write() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        let mut event1 = fixture.create_simple_event(0);
        event1.client_event_index = 33;

        let mut event2 = fixture.create_simple_event(1);
        event2.client_event_index = 34;
        event2.event_index = 1;
        event2.event_timestamp = 1550;
        event2.event_type_major = 20;
        event2.event_type_minor = 23;
        event2.event_value = b"test event 2".to_vec();

        let batch = fixture.create_simple_batch(0, vec![event1, event2]);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        let metadata =
            fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch)?;

        assert_eq!(metadata.server_timestamp, 1600000000000);
        assert_eq!(metadata.event_batch_index, 0);
        assert!(metadata.uncompressed_size > 0);
        assert!(metadata.compressed_size > 0);
        assert!(metadata.events_crc > 0);
        assert_eq!(metadata.compression_type, 1);
        assert_eq!(metadata.client_id, 123456789);
        assert_eq!(metadata.user_id, 987654321);
        assert_eq!(metadata.min_event_timestamp, 1000);
        assert_eq!(metadata.max_event_timestamp, 1550);
        assert_eq!(metadata.min_event_index, 0);
        assert_eq!(metadata.max_event_index, 1);
        assert_eq!(metadata.min_client_event_index, 33);
        assert_eq!(metadata.max_client_event_index, 34);
        match metadata.event_types_data {
            crate::structures::event_batch_metadata::EventTypesData::Direct(ref types) => {
                assert_eq!(types.len(), 4);
                assert_eq!(types[0], 1);
                assert_eq!(types[1], 20);
                assert_eq!(types[2], u64::MAX);
                assert_eq!(types[3], u64::MAX);
            }
            _ => panic!("Expected direct event type storage"),
        }

        let mut event3 = fixture.create_simple_event(2);
        event3.client_event_index = 35;
        event3.event_timestamp = 1999;
        event3.event_value = b"test event 3".to_vec();

        let mut batch2 = fixture.create_simple_batch(1, vec![event3]);
        batch2.client_id = 123456790;
        batch2.user_id = Some(987654322);
        batch2.server_timestamp = 1600000000001;

        let metadata =
            fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch2)?;

        assert_eq!(metadata.server_timestamp, 1600000000001);
        assert_eq!(metadata.event_batch_index, 1);
        assert!(metadata.uncompressed_size > 0);
        assert!(metadata.compressed_size > 0);
        assert!(metadata.events_crc > 0);
        assert_eq!(metadata.compression_type, 1);
        assert_eq!(metadata.client_id, 123456790);
        assert_eq!(metadata.user_id, 987654322);
        assert_eq!(metadata.min_event_timestamp, 1999);
        assert_eq!(metadata.max_event_timestamp, 1999);
        assert_eq!(metadata.min_event_index, 2);
        assert_eq!(metadata.max_event_index, 2);
        assert_eq!(metadata.min_client_event_index, 35);
        assert_eq!(metadata.max_client_event_index, 35);
        match metadata.event_types_data {
            crate::structures::event_batch_metadata::EventTypesData::Direct(ref types) => {
                assert_eq!(types.len(), 4);
                assert_eq!(types[0], 1);
                assert_eq!(types[1], u64::MAX);
                assert_eq!(types[2], u64::MAX);
                assert_eq!(types[3], u64::MAX);
            }
            _ => panic!("Expected direct event type storage"),
        }

        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;
        let read_result = fixture.read_filtered(
            &mut event_batch_reader,
            &mut metadata_reader,
            &crate::structures::read_filters::ReadFilters::default(),
        )?;

        assert_eq!(read_result.next_event_batch_index, None);
        assert_eq!(read_result.event_batches.len(), 2);
        let event_batch_0 = &read_result.event_batches[0];

        assert_eq!(event_batch_0.server_timestamp, 1600000000000);
        assert_eq!(event_batch_0.event_batch_index, 0);
        assert_eq!(event_batch_0.client_id, 123456789);
        assert_eq!(event_batch_0.user_id, Some(987654321));

        assert_eq!(event_batch_0.events.len(), 2);
        assert_eq!(event_batch_0.events[0].event_index, 0);
        assert_eq!(event_batch_0.events[0].event_timestamp, 1000);
        assert_eq!(event_batch_0.events[0].client_event_index, 33);
        assert_eq!(event_batch_0.events[0].event_type_major, 1);
        assert_eq!(event_batch_0.events[0].event_type_minor, 1);
        assert_eq!(event_batch_0.events[0].event_value, b"test event".to_vec());
        assert_eq!(event_batch_0.events[1].event_index, 1);
        assert_eq!(event_batch_0.events[1].event_timestamp, 1550);
        assert_eq!(event_batch_0.events[1].client_event_index, 34);
        assert_eq!(event_batch_0.events[1].event_type_major, 20);
        assert_eq!(event_batch_0.events[1].event_type_minor, 23);
        assert_eq!(
            event_batch_0.events[1].event_value,
            b"test event 2".to_vec()
        );

        let event_batch_1 = &read_result.event_batches[1];
        assert_eq!(event_batch_1.server_timestamp, 1600000000001);
        assert_eq!(event_batch_1.event_batch_index, 1);
        assert_eq!(event_batch_1.client_id, 123456790);
        assert_eq!(event_batch_1.user_id, Some(987654322));

        assert_eq!(event_batch_1.events.len(), 1);
        assert_eq!(event_batch_1.events[0].event_index, 2);
        assert_eq!(event_batch_1.events[0].event_timestamp, 1999);
        assert_eq!(event_batch_1.events[0].client_event_index, 35);
        assert_eq!(event_batch_1.events[0].event_type_major, 1);
        assert_eq!(event_batch_1.events[0].event_type_minor, 1);
        assert_eq!(
            event_batch_1.events[0].event_value,
            b"test event 3".to_vec()
        );

        Ok(())
    }

    /// Test that writing an empty event batch is rejected and does not create the file - read also should fail
    #[test]
    fn test_empty_event_batch_rejection() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        let batch = fixture.create_simple_batch(0, vec![]);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        let write_result =
            fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch);

        assert!(write_result.is_err());

        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;
        let read_result = fixture.read_filtered(
            &mut event_batch_reader,
            &mut metadata_reader,
            &crate::structures::read_filters::ReadFilters::default(),
        );
        assert!(read_result.is_err());

        Ok(())
    }

    #[test]
    fn test_large_event_batch_write() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;

        let num_batches = 2001;
        let num_events_per_batch = 501;

        for batch_index in 0..num_batches {
            let mut events = Vec::new();
            for event_index_in_batch in 0..num_events_per_batch {
                let global_event_index =
                    (batch_index * num_events_per_batch + event_index_in_batch) as u64;
                let event = fixture.create_simple_event(global_event_index);
                events.push(event);
            }

            let batch = fixture.create_simple_batch(batch_index as u64, events);

            fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch)?;
        }

        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;
        let read_result = fixture.read_filtered(
            &mut event_batch_reader,
            &mut metadata_reader,
            &crate::structures::read_filters::ReadFilters::default(),
        )?;

        assert_eq!(read_result.event_batches.len(), num_batches);

        let number_events = read_result
            .event_batches
            .iter()
            .map(|b| b.events.len())
            .sum::<usize>();

        assert_eq!(number_events, num_batches * num_events_per_batch);

        Ok(())
    }

    #[test]
    fn test_unicode_and_binary_data_handling() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;
        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;

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

        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch)?;

        let filters = ReadFilters::new(1);
        let read_result =
            fixture.read_filtered(&mut event_batch_reader, &mut metadata_reader, &filters)?;

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
            let mut bloom_filter = BloomFilter::with_num_bits(BLOOM_BYTES * 8)
                .seed(&BLOOM_HASH_SEED)
                .hashes(BLOOM_HASH_COUNT);
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
                let mut bloom_filter = BloomFilter::with_num_bits(BLOOM_BYTES * 8)
                    .seed(&BLOOM_HASH_SEED)
                    .hashes(BLOOM_HASH_COUNT);
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
            let mut bloom_filter = BloomFilter::with_num_bits(BLOOM_BYTES * 8)
                .seed(&BLOOM_HASH_SEED)
                .hashes(BLOOM_HASH_COUNT);
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

    // 3. Event Type Handling

    #[test]
    fn test_direct_event_type_storage() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;
        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;

        let mut events = Vec::new();
        for i in 1..=4 {
            let event = EventItem::new(i, i, 1000, i, 1, b"test event".to_vec());
            events.push(event);
        }
        let batch = EventBatchItem::new(1, 1600000000000, 123456789, Some(987654321), events);

        let metadata =
            fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch)?;

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
        let mut fixture = TestFixture::new()?;
        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;

        let mut events = Vec::new();
        for i in 1..=5 {
            let event = EventItem::new(i, i, 1000, i, 1, b"test event".to_vec());
            events.push(event);
        }
        let batch = EventBatchItem::new(1, 1600000000000, 123456789, Some(987654321), events);

        let metadata =
            fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch)?;

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
        let mut fixture = TestFixture::new()?;
        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        let mut event_type_dedup = HashSet::new();

        let events = vec![
            EventItem::new(1, 1, 1000, 1, 1, b"test event".to_vec()),
            EventItem::new(2, 2, 1000, 1, 1, b"test event".to_vec()),
            EventItem::new(3, 3, 1000, 2, 1, b"test event".to_vec()),
            EventItem::new(4, 4, 1000, 2, 1, b"test event".to_vec()),
            EventItem::new(5, 5, 1000, 4, 1, b"test event".to_vec()),
            EventItem::new(6, 6, 1000, 5, 1, b"test event".to_vec()),
            EventItem::new(7, 7, 1000, 6, 1, b"test event".to_vec()),
        ];
        let batch = EventBatchItem::new(1, 1600000000000, 123456789, Some(987654321), events);

        fixture.write_batch_with_dedup(
            &mut event_batch_writer,
            &mut metadata_writer,
            &mut event_type_dedup,
            &batch,
        )?;

        // Check size of the event_type_dedup is 5 since there is only two unique types
        assert_eq!(event_type_dedup.len(), 5);

        Ok(())
    }

    // 4. Basic Read Operations
    #[test]
    fn test_simple_read_all() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

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

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;

        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch1)?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch2)?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch3)?;

        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;

        let filters = ReadFilters::new(1);
        let read_result =
            fixture.read_filtered(&mut event_batch_reader, &mut metadata_reader, &filters)?;

        assert_eq!(read_result.event_batches.len(), 3);
        assert_eq!(read_result.event_batches[0].event_batch_index, 1);
        assert_eq!(read_result.event_batches[1].event_batch_index, 2);
        assert_eq!(read_result.event_batches[2].event_batch_index, 3);
        assert_eq!(read_result.next_event_batch_index, None);

        Ok(())
    }

    #[test]
    fn test_read_from_specific_server_id() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;
        let batches = fixture.generate_test_batches();

        let filters = ReadFilters::new(3); // Read from server_id 3
        let read_result = fixture.write_and_read(&batches, &filters)?;

        assert_eq!(read_result.event_batches.len(), 6); // Batches 3 to 8
        assert_eq!(read_result.event_batches[0].event_batch_index, 3);
        Ok(())
    }

    #[test]
    fn test_read_non_existent_server_id_future() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;
        let batches = fixture.generate_test_batches();

        let filters = ReadFilters::new(10); // Read from server_id 10, which doesn't exist
        let read_result = fixture.write_and_read(&batches, &filters)?;

        assert_eq!(read_result.event_batches.len(), 0);
        assert_eq!(read_result.next_event_batch_index, None);
        Ok(())
    }

    #[test]
    fn test_server_id_range_filtering() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;
        let batches = fixture.generate_test_batches();

        let mut filters = ReadFilters::new(3);
        filters.to_event_batch_index = Some(7); // Read from 3 to 7
        let read_result = fixture.write_and_read(&batches, &filters)?;

        assert_eq!(read_result.event_batches.len(), 5); // Batches 3, 4, 5, 6, 7
        assert_eq!(read_result.event_batches[0].event_batch_index, 3);
        assert_eq!(
            read_result.event_batches.last().unwrap().event_batch_index,
            7
        );
        Ok(())
    }

    #[test]
    fn test_client_id_filtering_include() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;
        let batches = fixture.generate_test_batches();

        let mut filters = ReadFilters::new(1);
        filters.include_client_id = Some(12345); // Include client ID from batch 3
        let read_result = fixture.write_and_read(&batches, &filters)?;

        assert_eq!(read_result.event_batches.len(), 1); // Only batch 3 matches
        assert_eq!(read_result.event_batches[0].client_id, 12345);
        Ok(())
    }

    #[test]
    fn test_client_id_filtering_exclude() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;
        let batches = fixture.generate_test_batches();

        let mut filters = ReadFilters::new(1);
        filters.exclude_client_id = Some(12345); // Exclude client ID from batch 3
        let read_result = fixture.write_and_read(&batches, &filters)?;

        assert_eq!(read_result.event_batches.len(), 7); // All batches except 3
        Ok(())
    }

    #[test]
    fn test_user_id_filtering_include() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;
        let batches = fixture.generate_test_batches();

        let mut filters = ReadFilters::new(1);
        filters.include_user_id = Some(67890); // Include user ID from batch 4
        let read_result = fixture.write_and_read(&batches, &filters)?;

        assert_eq!(read_result.event_batches.len(), 1); // Only batch 4 matches
        assert_eq!(read_result.event_batches[0].user_id, Some(67890));
        Ok(())
    }

    #[test]
    fn test_user_id_filtering_exclude() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;
        let batches = fixture.generate_test_batches();

        let mut filters = ReadFilters::new(1);
        filters.exclude_user_id = Some(67890); // Exclude user ID from batch 4
        let read_result = fixture.write_and_read(&batches, &filters)?;

        assert_eq!(read_result.event_batches.len(), 7); // All batches except 4
        Ok(())
    }

    #[test]
    fn test_server_time_range_filtering() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;
        let batches = fixture.generate_test_batches();

        let mut filters = ReadFilters::new(1);
        filters.min_server_timestamp = Some(1640000000000);
        filters.max_server_timestamp = Some(1650000000000); // Batch 5 is 1650000000000
        let read_result = fixture.write_and_read(&batches, &filters)?;

        assert_eq!(read_result.event_batches.len(), 2); // Only batch 5 and batch 7 matches
        assert_eq!(read_result.event_batches[0].server_timestamp, 1650000000000);
        assert_eq!(read_result.event_batches[1].server_timestamp, 1650000000000);
        Ok(())
    }

    #[test]
    fn test_local_index_range_filtering() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;
        let batches = fixture.generate_test_batches();

        let mut filters = ReadFilters::new(1);
        filters.min_client_event_index = Some(1);
        filters.max_client_event_index = Some(2); // Events in batch 1 have index 1 and 2
        let read_result = fixture.write_and_read(&batches, &filters)?;

        assert_eq!(read_result.event_batches.len(), 8);
        assert_eq!(read_result.event_batches[0].events.len(), 2); // Only events 1 and 2 remain
        Ok(())
    }

    #[test]
    fn test_event_time_range_filtering() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;
        let batches = fixture.generate_test_batches();

        let mut filters = ReadFilters::new(1);
        filters.min_event_timestamp = Some(1640000000000);
        filters.max_event_timestamp = Some(1660000000000); // Event in batch 7 is in range
        let read_result = fixture.write_and_read(&batches, &filters)?;

        assert_eq!(read_result.event_batches.len(), 1);
        assert_eq!(read_result.event_batches[0].events.len(), 2); // Only 2 event remains
        Ok(())
    }

    #[test]
    fn test_event_type_filtering_bloom_filter() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;
        let batches = fixture.generate_test_batches();

        let mut filters = ReadFilters::new(1);
        let event_types = vec![10_u64, 80_u64];
        filters.include_event_types = Some(event_types.as_slice()); // Batch 6 has types 10, 20, 30, 40, 50, 60, 70, 80
        let read_result = fixture.write_and_read(&batches, &filters)?;

        assert_eq!(read_result.event_batches.len(), 2);
        assert_eq!(read_result.event_batches[0].events.len(), 1); // Only events of type 10
        assert_eq!(read_result.event_batches[0].events[0].event_type_major, 10);
        assert_eq!(read_result.event_batches[1].events.len(), 2); // Only events of type 10 and 80
        assert_eq!(read_result.event_batches[1].events[0].event_type_major, 10);
        assert_eq!(read_result.event_batches[1].events[1].event_type_major, 80);
        assert_eq!(read_result.event_batches[0].event_batch_index, 2);
        assert_eq!(read_result.event_batches[1].event_batch_index, 6);
        Ok(())
    }

    #[test]
    fn test_combined_filter_scenarios() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;
        let batches = fixture.generate_test_batches();

        let mut filters = ReadFilters::new(1);
        filters.include_client_id = Some(12345); // Batch 3 has this client ID
        filters.min_server_timestamp = Some(1600000000000);
        filters.max_server_timestamp = Some(1660000000000);
        let event_types = vec![1_u64, 2_u64, 3_u64];
        filters.include_event_types = Some(event_types.as_slice()); // Batch 3 has event type 1
        let read_result = fixture.write_and_read(&batches, &filters)?;

        assert_eq!(read_result.event_batches.len(), 1); // Only batch 3 matches all criteria
        assert_eq!(read_result.event_batches[0].client_id, 12345);
        Ok(())
    }

    #[test]
    fn test_client_id_filtering_include_and_exclude() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;
        let batches = fixture.generate_test_batches();

        let mut filters = ReadFilters::new(1);
        filters.include_client_id = Some(12345); // Client ID from batch 3
        filters.exclude_user_id = Some(67890); // User ID from batch 4
        let read_result = fixture.write_and_read(&batches, &filters)?;

        assert_eq!(read_result.event_batches.len(), 1); // Only batch 3 matches
        assert_eq!(read_result.event_batches[0].client_id, 12345);
        Ok(())
    }

    #[test]
    fn test_server_time_and_event_type_filtering() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;
        let batches = fixture.generate_test_batches();

        let mut filters = ReadFilters::new(1);
        filters.min_server_timestamp = Some(1600000000000);
        filters.max_server_timestamp = Some(1650000000000); // Batch 5 is 1650000000000
        let event_types = vec![10_u64, 20_u64, 30_u64];
        filters.include_event_types = Some(event_types.as_slice()); // Batch 2 has these types
        let read_result = fixture.write_and_read(&batches, &filters)?;

        //Batches 2, 5 match the filters criteria
        assert_eq!(read_result.event_batches.len(), 2);
        Ok(())
    }

    #[test]
    fn test_event_type_and_local_index_filtering() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;
        let batches = fixture.generate_test_batches();

        let mut filters = ReadFilters::new(1);
        let event_types = vec![10_u64, 20_u64, 30_u64, 40_u64, 50_u64];
        filters.include_event_types = Some(event_types.as_slice()); // Batch 2 has these types
        filters.min_client_event_index = Some(1);
        filters.max_client_event_index = Some(2);
        let read_result = fixture.write_and_read(&batches, &filters)?;

        //Batch 2 and 6 only match the filters criteria
        assert_eq!(read_result.event_batches.len(), 2);
        assert_eq!(read_result.event_batches[0].events.len(), 2); // Only events with index 1 and 2 remain
        assert_eq!(read_result.event_batches[1].events.len(), 2); // Only events with index 1 and 2 remain
        assert_eq!(read_result.event_batches[0].event_batch_index, 2);
        assert_eq!(read_result.event_batches[1].event_batch_index, 6);
        Ok(())
    }

    #[test]
    fn test_min_max_server_timestamp_filtering() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;
        let batches = fixture.generate_test_batches();

        let mut filters = ReadFilters::new(1);
        filters.min_server_timestamp = Some(1600000000000);
        filters.max_server_timestamp = Some(1650000000000);
        let read_result = fixture.write_and_read(&batches, &filters)?;

        //Only batches within timestamp range should return
        assert_eq!(read_result.event_batches.len(), 7); // Everything but the last batch is within the bounds
        assert_eq!(read_result.event_batches[0].server_timestamp, 1600000000000);
        assert_eq!(read_result.event_batches[6].server_timestamp, 1650000000000);
        Ok(())
    }

    #[test]
    fn test_min_max_event_timestamp_filtering() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;
        let batches = fixture.generate_test_batches();

        let mut filters = ReadFilters::new(1);
        filters.min_event_timestamp = Some(1640000000000);
        filters.max_event_timestamp = Some(1660000000000);
        let read_result = fixture.write_and_read(&batches, &filters)?;

        assert_eq!(read_result.event_batches.len(), 1);
        Ok(())
    }

    // 6. Pagination Testing

    #[test]
    fn test_max_bytes_pagination() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        // Create batches with known sizes
        let events = vec![
            fixture.create_event_with_data(1, vec![0u8; 1000]), // ~1KB events
            fixture.create_event_with_data(2, vec![0u8; 1000]),
            fixture.create_event_with_data(3, vec![0u8; 1000]),
        ];

        let batch1 = fixture.create_simple_batch(1, events.clone());
        let batch2 = fixture.create_simple_batch(2, events.clone());
        let batch3 = fixture.create_simple_batch(3, events);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch1)?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch2)?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch3)?;

        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;

        // Set max_bytes to approximately fit only the first batch
        let mut filters = ReadFilters::new(1);
        filters.max_bytes = Some(58); // Should fit only first batch
        let read_result =
            fixture.read_filtered(&mut event_batch_reader, &mut metadata_reader, &filters)?;

        assert_eq!(read_result.event_batches.len(), 1);
        assert_eq!(read_result.event_batches[0].event_batch_index, 1);
        assert_eq!(read_result.next_event_batch_index, Some(2));

        Ok(())
    }

    #[test]
    fn test_exact_boundary_pagination() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        // Create events with known data sizes
        let event1 = fixture.create_event_with_data(1, vec![0u8; 500]);
        let event2 = fixture.create_event_with_data(2, vec![0u8; 500]);
        let event3 = fixture.create_event_with_data(3, vec![0u8; 500]);

        let batch1 = fixture.create_simple_batch(1, vec![event1]);
        let batch2 = fixture.create_simple_batch(2, vec![event2]);
        let batch3 = fixture.create_simple_batch(3, vec![event3]);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        let metadata1 =
            fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch1)?;
        let metadata2 =
            fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch2)?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch3)?;

        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;

        // Set max_bytes to exactly fit the first two batches
        let mut filters = ReadFilters::new(1);
        filters.max_bytes = Some((metadata1.compressed_size + metadata2.compressed_size) as usize);
        let read_result =
            fixture.read_filtered(&mut event_batch_reader, &mut metadata_reader, &filters)?;

        assert_eq!(read_result.event_batches.len(), 2);
        assert_eq!(read_result.event_batches[0].event_batch_index, 1);
        assert_eq!(read_result.event_batches[1].event_batch_index, 2);
        assert_eq!(read_result.next_event_batch_index, Some(3));

        Ok(())
    }

    #[test]
    fn test_single_byte_under_boundary() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        let event1 = fixture.create_event_with_data(1, vec![0u8; 500]);
        let event2 = fixture.create_event_with_data(2, vec![0u8; 500]);
        let event3 = fixture.create_event_with_data(3, vec![0u8; 500]);

        let batch1 = fixture.create_simple_batch(1, vec![event1]);
        let batch2 = fixture.create_simple_batch(2, vec![event2]);
        let batch3 = fixture.create_simple_batch(3, vec![event3]);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        let metadata1 =
            fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch1)?;
        let metadata2 =
            fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch2)?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch3)?;

        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;

        // Set max_bytes to 1 byte under the exact boundary for two batches
        let mut filters = ReadFilters::new(1);
        filters.max_bytes =
            Some((metadata1.compressed_size + metadata2.compressed_size - 1) as usize);
        let read_result =
            fixture.read_filtered(&mut event_batch_reader, &mut metadata_reader, &filters)?;

        // Should only include the first batch since adding the second would exceed the limit
        assert_eq!(read_result.event_batches.len(), 1);
        assert_eq!(read_result.event_batches[0].event_batch_index, 1);
        assert_eq!(read_result.next_event_batch_index, Some(2));

        Ok(())
    }

    #[test]
    fn test_single_byte_over_boundary() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        let event1 = fixture.create_event_with_data(1, vec![0u8; 500]);
        let event2 = fixture.create_event_with_data(2, vec![0u8; 500]);
        let event3 = fixture.create_event_with_data(3, vec![0u8; 500]);

        let batch1 = fixture.create_simple_batch(1, vec![event1]);
        let batch2 = fixture.create_simple_batch(2, vec![event2]);
        let batch3 = fixture.create_simple_batch(3, vec![event3]);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        let metadata1 =
            fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch1)?;
        let metadata2 =
            fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch2)?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch3)?;

        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;

        // Set max_bytes to 1 byte over the exact boundary for two batches
        let mut filters = ReadFilters::new(1);
        filters.max_bytes =
            Some((metadata1.compressed_size + metadata2.compressed_size + 1) as usize);
        let read_result =
            fixture.read_filtered(&mut event_batch_reader, &mut metadata_reader, &filters)?;

        // Should include both batches since we have enough space
        assert_eq!(read_result.event_batches.len(), 2);
        assert_eq!(read_result.event_batches[0].event_batch_index, 1);
        assert_eq!(read_result.event_batches[1].event_batch_index, 2);
        assert_eq!(read_result.next_event_batch_index, Some(3));

        Ok(())
    }

    #[test]
    fn test_pagination_with_filtering() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        // Create batches with different client IDs
        let events = vec![fixture.create_event_with_data(1, vec![0u8; 1000])];
        let batch1 = EventBatchItem::new(1, 1600000000000, 111, Some(987654321), events.clone());
        let batch2 = EventBatchItem::new(2, 1600000000000, 222, Some(987654321), events.clone());
        let batch3 = EventBatchItem::new(3, 1600000000000, 111, Some(987654321), events.clone());
        let batch4 = EventBatchItem::new(4, 1600000000000, 222, Some(987654321), events);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch1)?;
        let metadata2 =
            fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch2)?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch3)?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch4)?;

        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;

        // Filter by client_id 111 with max_bytes that should fit only one batch
        let mut filters = ReadFilters::new(1);
        filters.include_client_id = Some(111);
        filters.max_bytes = Some(metadata2.compressed_size as usize); // Roughly one batch worth
        let read_result =
            fixture.read_filtered(&mut event_batch_reader, &mut metadata_reader, &filters)?;

        // Should only get one batch (batch1) and next_server_id should point to batch3 (next matching batch)
        assert_eq!(read_result.event_batches.len(), 1);
        assert_eq!(read_result.event_batches[0].event_batch_index, 1);
        assert_eq!(read_result.event_batches[0].client_id, 111);
        assert_eq!(read_result.next_event_batch_index, Some(3));

        Ok(())
    }

    #[test]
    fn test_pagination_no_limit() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        let events = vec![fixture.create_event_with_data(1, vec![0u8; 1000])];
        let batch1 = fixture.create_simple_batch(1, events.clone());
        let batch2 = fixture.create_simple_batch(2, events.clone());
        let batch3 = fixture.create_simple_batch(3, events);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch1)?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch2)?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch3)?;

        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;

        // No max_bytes limit should return all batches
        let filters = ReadFilters::new(1);
        let read_result =
            fixture.read_filtered(&mut event_batch_reader, &mut metadata_reader, &filters)?;

        assert_eq!(read_result.event_batches.len(), 3);
        assert_eq!(read_result.next_event_batch_index, None);

        Ok(())
    }

    #[test]
    fn test_pagination_very_small_limit() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        let events = vec![fixture.create_event_with_data(1, vec![0u8; 1000])];
        let batch1 = fixture.create_simple_batch(1, events);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch1)?;

        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;

        // Set max_bytes to a very small value that won't fit even one batch
        let mut filters = ReadFilters::new(1);
        filters.max_bytes = Some(10);
        let read_result =
            fixture.read_filtered(&mut event_batch_reader, &mut metadata_reader, &filters);

        // Should have error
        assert!(read_result.is_err());

        Ok(())
    }

    #[test]
    fn test_pagination_continue_from_next() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        let events = vec![fixture.create_event_with_data(1, vec![0u8; 1000])];
        let batch1 = fixture.create_simple_batch(1, events.clone());
        let batch2 = fixture.create_simple_batch(2, events.clone());
        let batch3 = fixture.create_simple_batch(3, events.clone());
        let batch4 = fixture.create_simple_batch(4, events);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        let metadata1 =
            fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch1)?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch2)?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch3)?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch4)?;

        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;

        // First page - limit to approximately one batch
        let mut filters = ReadFilters::new(1);
        filters.max_bytes = Some(metadata1.compressed_size as usize);
        let read_result1 =
            fixture.read_filtered(&mut event_batch_reader, &mut metadata_reader, &filters)?;

        assert_eq!(read_result1.event_batches.len(), 1);
        assert_eq!(read_result1.event_batches[0].event_batch_index, 1);
        assert_eq!(read_result1.next_event_batch_index, Some(2));

        // Second page - continue from next_server_id
        let mut filters2 = ReadFilters::new(2);
        filters2.max_bytes = Some(metadata1.compressed_size as usize);
        let read_result2 =
            fixture.read_filtered(&mut event_batch_reader, &mut metadata_reader, &filters2)?;

        assert_eq!(read_result2.event_batches.len(), 1);
        assert_eq!(read_result2.event_batches[0].event_batch_index, 2);
        assert_eq!(read_result2.next_event_batch_index, Some(3));

        // Third page
        let mut filters3 = ReadFilters::new(3);
        filters3.max_bytes = Some((metadata1.compressed_size * 2) as usize); // Allow two batches
        let read_result3 =
            fixture.read_filtered(&mut event_batch_reader, &mut metadata_reader, &filters3)?;

        assert_eq!(read_result3.event_batches.len(), 2);
        assert_eq!(read_result3.event_batches[0].event_batch_index, 3);
        assert_eq!(read_result3.event_batches[1].event_batch_index, 4);
        assert_eq!(read_result3.next_event_batch_index, None);

        Ok(())
    }

    #[test]
    fn test_pagination_with_large_batch() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        // Create one very large batch and one small batch
        let large_events = vec![
            fixture.create_event_with_data(1, vec![0u8; 5000]),
            fixture.create_event_with_data(2, vec![0u8; 5000]),
        ];
        let small_events = vec![fixture.create_event_with_data(3, vec![0u8; 100])];

        let large_batch = fixture.create_simple_batch(1, large_events);
        let small_batch = fixture.create_simple_batch(2, small_events);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &large_batch)?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &small_batch)?;

        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;

        // Set max_bytes smaller than the large batch
        let mut filters = ReadFilters::new(1);
        filters.max_bytes = Some(60);
        let read_result =
            fixture.read_filtered(&mut event_batch_reader, &mut metadata_reader, &filters)?;

        assert_eq!(read_result.event_batches.len(), 1);
        assert_eq!(read_result.next_event_batch_index, Some(2));

        let mut filters = ReadFilters::new(1);
        filters.max_bytes = Some(60);
        filters.from_event_batch_index = read_result.next_event_batch_index.unwrap();
        let read_result =
            fixture.read_filtered(&mut event_batch_reader, &mut metadata_reader, &filters)?;

        assert_eq!(read_result.event_batches.len(), 1);
        assert_eq!(read_result.event_batches[0].event_batch_index, 2);
        assert_eq!(read_result.next_event_batch_index, None);

        Ok(())
    }

    #[test]
    fn test_pagination_empty_after_filtering() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        let events = vec![fixture.create_event_with_data(1, vec![0u8; 1000])];
        let batch1 = EventBatchItem::new(1, 1600000000000, 111, Some(987654321), events.clone());
        let batch2 = EventBatchItem::new(2, 1600000000000, 222, Some(987654321), events);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch1)?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch2)?;

        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;

        // Filter by client_id that doesn't exist
        let mut filters = ReadFilters::new(1);
        filters.include_client_id = Some(99989);
        filters.max_bytes = Some(5000);
        let read_result =
            fixture.read_filtered(&mut event_batch_reader, &mut metadata_reader, &filters)?;

        // Should return empty result
        assert_eq!(read_result.event_batches.len(), 0);
        assert_eq!(read_result.next_event_batch_index, None);

        Ok(())
    }
}
