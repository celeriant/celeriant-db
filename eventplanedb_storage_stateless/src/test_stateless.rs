#[cfg(test)]
mod tests {
    // Platform-specific raw file descriptor traits
    #[cfg(target_os = "windows")]
    use std::os::windows::io::AsRawHandle;
    use std::sync::Arc;

    use crate::stateless_destructive::StatelessDestructive;
    use crate::stateless_engine::StatelessEngine;
    use crate::stateless_reader::{CorruptPositions, StatelessReader};
    use crate::stateless_writer::StatelessWriter;
    use crate::test_fixture::tests::TestFixture;
    use eventplanedb_storage_structures::constants::{
        BINCODE_CONFIG_FIXED, BLOOM_HASH_SEED, METADATA_BATCH_SIZE_BYTES,
    };
    use eventplanedb_storage_structures::{
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
            eventplanedb_storage_structures::event_batch_metadata::EventTypesData::Direct(
                ref types,
            ) => {
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
            &eventplanedb_storage_structures::read_filters::ReadFilters::default(),
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
        assert_eq!(
            event_batch_0.events[0].event_value,
            Arc::new(b"test event".to_vec())
        );

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
        event2.event_value = Arc::new(b"test event 2".to_vec());

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
            eventplanedb_storage_structures::event_batch_metadata::EventTypesData::Direct(
                ref types,
            ) => {
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
            &eventplanedb_storage_structures::read_filters::ReadFilters::default(),
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
        assert_eq!(
            event_batch_0.events[0].event_value,
            Arc::new(b"test event".to_vec())
        );
        assert_eq!(event_batch_0.events[1].event_index, 1);
        assert_eq!(event_batch_0.events[1].event_timestamp, 1550);
        assert_eq!(event_batch_0.events[1].client_event_index, 34);
        assert_eq!(event_batch_0.events[1].event_type_major, 20);
        assert_eq!(event_batch_0.events[1].event_type_minor, 23);
        assert_eq!(
            event_batch_0.events[1].event_value,
            Arc::new(b"test event 2".to_vec())
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
        event2.event_value = Arc::new(b"test event 2".to_vec());

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
            eventplanedb_storage_structures::event_batch_metadata::EventTypesData::Direct(
                ref types,
            ) => {
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
        event3.event_value = Arc::new(b"test event 3".to_vec());

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
            eventplanedb_storage_structures::event_batch_metadata::EventTypesData::Direct(
                ref types,
            ) => {
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
            &eventplanedb_storage_structures::read_filters::ReadFilters::default(),
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
        assert_eq!(
            event_batch_0.events[0].event_value,
            Arc::new(b"test event".to_vec())
        );
        assert_eq!(event_batch_0.events[1].event_index, 1);
        assert_eq!(event_batch_0.events[1].event_timestamp, 1550);
        assert_eq!(event_batch_0.events[1].client_event_index, 34);
        assert_eq!(event_batch_0.events[1].event_type_major, 20);
        assert_eq!(event_batch_0.events[1].event_type_minor, 23);
        assert_eq!(
            event_batch_0.events[1].event_value,
            Arc::new(b"test event 2".to_vec())
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
            Arc::new(b"test event 3".to_vec())
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
            &eventplanedb_storage_structures::read_filters::ReadFilters::default(),
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
            &eventplanedb_storage_structures::read_filters::ReadFilters::default(),
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
            Arc::new("你好世界".to_string().into_bytes())
        );
        assert_eq!(
            read_batch.events[1].event_value,
            Arc::new(vec![0u8, 1u8, 2u8, 255u8])
        );

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
            let mut event_batch_writer = File::create(&event_batch_path)?;
            let mut metadata_writer = File::create(&metadata_path)?;
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
                let mut event_batch_writer = File::create(&event_batch_path)?;
                let mut metadata_writer = File::create(&metadata_path)?;
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
            let mut event_batch_writer = File::create(&event_batch_path)?;
            let mut metadata_writer = File::create(&metadata_path)?;
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
            eventplanedb_storage_structures::event_batch_metadata::EventTypesData::Direct(_) => {
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
            eventplanedb_storage_structures::event_batch_metadata::EventTypesData::Bloom(_) => {
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
        filters.include_event_types = Some(event_types); // Batch 6 has types 10, 20, 30, 40, 50, 60, 70, 80
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
        filters.include_event_types = Some(event_types); // Batch 3 has event type 1
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
        filters.include_event_types = Some(event_types); // Batch 2 has these types
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
        filters.include_event_types = Some(event_types); // Batch 2 has these types
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

    // 7. Corruption Detection

    #[test]
    fn test_no_corruption_detection() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        // Write some valid data
        let events = vec![
            fixture.create_simple_event(1),
            fixture.create_simple_event(2),
        ];
        let batch1 = fixture.create_simple_batch(1, events.clone());
        let batch2 = fixture.create_simple_batch(2, events);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch1)?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch2)?;

        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;
        let corruption =
            fixture.detect_corruption(&mut event_batch_reader, &mut metadata_reader)?;

        // Should detect no corruption
        assert!(
            corruption.is_none(),
            "Should not detect any corruption in valid files"
        );

        Ok(())
    }

    #[test]
    fn test_truncated_metadata_file() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        // Write valid data first
        let events = vec![fixture.create_simple_event(1)];
        let batch1 = fixture.create_simple_batch(1, events.clone());
        let batch2 = fixture.create_simple_batch(2, events);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch1)?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch2)?;
        drop(event_batch_writer);
        drop(metadata_writer);

        // Truncate metadata file mid-entry (corrupt the second metadata entry)
        let metadata_path = fixture._temp_dir.path().join("metadata.bin");
        let mut metadata_file = std::fs::OpenOptions::new()
            .write(true)
            .open(&metadata_path)?;

        // Truncate to middle of second metadata entry
        let truncate_position =
            METADATA_BATCH_SIZE_BYTES as u64 + (METADATA_BATCH_SIZE_BYTES / 2) as u64;
        metadata_file.set_len(truncate_position)?;
        drop(metadata_file);

        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;
        let corruption =
            fixture.detect_corruption(&mut event_batch_reader, &mut metadata_reader)?;

        // Should detect corruption at the position of the first complete metadata entry
        assert!(
            corruption.is_some(),
            "Should detect corruption due to truncated metadata"
        );
        let corrupt_pos = corruption.unwrap();
        assert_eq!(
            corrupt_pos.metadata_position,
            METADATA_BATCH_SIZE_BYTES as u64
        );

        Ok(())
    }

    #[test]
    fn test_corrupted_metadata_deserialization() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        // Write valid data first
        let events = vec![fixture.create_simple_event(1)];
        let batch1 = fixture.create_simple_batch(1, events.clone());
        let batch2 = fixture.create_simple_batch(2, events);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch1)?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch2)?;
        drop(event_batch_writer);
        drop(metadata_writer);

        // Corrupt metadata bytes directly
        let metadata_path = fixture._temp_dir.path().join("metadata.bin");
        let mut metadata_file = std::fs::OpenOptions::new()
            .write(true)
            .open(&metadata_path)?;

        // Seek to middle of second metadata entry and corrupt some bytes
        metadata_file.seek(SeekFrom::Start(METADATA_BATCH_SIZE_BYTES as u64 + 10))?;
        metadata_file.write_all(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF])?; // Invalid data
        drop(metadata_file);

        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;
        let corruption =
            fixture.detect_corruption(&mut event_batch_reader, &mut metadata_reader)?;

        // Should detect corruption at the position after the first valid metadata entry
        assert!(
            corruption.is_some(),
            "Should detect corruption due to invalid metadata bytes"
        );
        let corrupt_pos = corruption.unwrap();
        assert_eq!(
            corrupt_pos.metadata_position,
            METADATA_BATCH_SIZE_BYTES as u64
        );

        Ok(())
    }

    #[test]
    fn test_event_batch_file_too_short() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        // Write valid data first
        let events = vec![fixture.create_simple_event(1)];
        let batch1 = fixture.create_simple_batch(1, events.clone());
        let batch2 = fixture.create_simple_batch(2, events);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        let metadata1 =
            fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch1)?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch2)?;
        drop(event_batch_writer);
        drop(metadata_writer);

        // Truncate event batch file so it doesn't contain the second batch
        let event_batch_path = fixture._temp_dir.path().join("event_batches.bin");
        let mut event_batch_file = std::fs::OpenOptions::new()
            .write(true)
            .open(&event_batch_path)?;

        // Truncate to only contain first batch minus a few bytes
        let truncate_position = metadata1.compressed_size - 10;
        event_batch_file.set_len(truncate_position)?;
        drop(event_batch_file);

        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;
        let corruption =
            fixture.detect_corruption(&mut event_batch_reader, &mut metadata_reader)?;

        // Should detect corruption at position 0 (no valid batches)
        assert!(
            corruption.is_some(),
            "Should detect corruption due to insufficient event batch data"
        );
        let corrupt_pos = corruption.unwrap();
        assert_eq!(corrupt_pos.metadata_position, 0);
        assert_eq!(corrupt_pos.event_batch_position, 0);

        Ok(())
    }

    #[test]
    fn test_crc_mismatch_detection() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        // Write valid data first
        let events = vec![fixture.create_simple_event(1)];
        let batch1 = fixture.create_simple_batch(1, events.clone());
        let batch2 = fixture.create_simple_batch(2, events);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        let metadata1 =
            fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch1)?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch2)?;
        drop(event_batch_writer);
        drop(metadata_writer);

        // Corrupt event batch data to cause CRC mismatch
        let event_batch_path = fixture._temp_dir.path().join("event_batches.bin");
        let mut event_batch_file = std::fs::OpenOptions::new()
            .write(true)
            .open(&event_batch_path)?;

        // Corrupt some bytes in the first batch
        event_batch_file.seek(SeekFrom::Start(10))?;
        event_batch_file.write_all(&[0xFF, 0xFF, 0xFF, 0xFF])?;
        drop(event_batch_file);

        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;
        let corruption =
            fixture.detect_corruption(&mut event_batch_reader, &mut metadata_reader)?;

        // Should detect corruption at position 0 due to CRC mismatch in first batch
        assert!(
            corruption.is_some(),
            "Should detect corruption due to CRC mismatch"
        );
        let corrupt_pos = corruption.unwrap();
        assert_eq!(corrupt_pos.metadata_position, 0);
        assert_eq!(corrupt_pos.event_batch_position, 0);

        Ok(())
    }

    #[test]
    fn test_empty_file_corruption() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        // Create empty files
        let (event_batch_writer, metadata_writer) = fixture.create_writers()?;
        drop(event_batch_writer);
        drop(metadata_writer);

        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;
        let corruption =
            fixture.detect_corruption(&mut event_batch_reader, &mut metadata_reader)?;

        // Should detect corruption at position 0 for empty files
        assert!(
            corruption.is_some(),
            "Should detect corruption in empty files"
        );
        let corrupt_pos = corruption.unwrap();
        assert_eq!(corrupt_pos.metadata_position, 0);
        assert_eq!(corrupt_pos.event_batch_position, 0);

        Ok(())
    }

    #[test]
    fn test_mismatched_file_lengths() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        // Write valid data first
        let events = vec![fixture.create_simple_event(1)];
        let batch1 = fixture.create_simple_batch(1, events.clone());
        let batch2 = fixture.create_simple_batch(2, events);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        let metadata1 =
            fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch1)?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch2)?;
        drop(event_batch_writer);
        drop(metadata_writer);

        // Truncate event batch file but leave metadata file intact
        let event_batch_path = fixture._temp_dir.path().join("event_batches.bin");
        let mut event_batch_file = std::fs::OpenOptions::new()
            .write(true)
            .open(&event_batch_path)?;

        // Truncate to only contain first batch
        event_batch_file.set_len(metadata1.compressed_size)?;
        drop(event_batch_file);

        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;
        let corruption =
            fixture.detect_corruption(&mut event_batch_reader, &mut metadata_reader)?;

        // Should detect corruption after the first valid batch
        assert!(
            corruption.is_some(),
            "Should detect corruption due to mismatched file lengths"
        );
        let corrupt_pos = corruption.unwrap();
        assert_eq!(
            corrupt_pos.metadata_position,
            METADATA_BATCH_SIZE_BYTES as u64
        );
        assert_eq!(corrupt_pos.event_batch_position, metadata1.compressed_size);

        Ok(())
    }

    #[test]
    fn test_corruption_detection_with_multiple_valid_batches() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        // Write multiple valid batches
        let events = vec![fixture.create_simple_event(1)];
        let batch1 = fixture.create_simple_batch(1, events.clone());
        let batch2 = fixture.create_simple_batch(2, events.clone());
        let batch3 = fixture.create_simple_batch(3, events);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        let metadata1 =
            fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch1)?;
        let metadata2 =
            fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch2)?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch3)?;
        drop(event_batch_writer);
        drop(metadata_writer);

        // Corrupt the third batch in event batch file
        let event_batch_path = fixture._temp_dir.path().join("event_batches.bin");
        let mut event_batch_file = std::fs::OpenOptions::new()
            .write(true)
            .open(&event_batch_path)?;

        // Seek to third batch and corrupt it
        let third_batch_position = metadata1.compressed_size + metadata2.compressed_size + 5;
        event_batch_file.seek(SeekFrom::Start(third_batch_position))?;
        event_batch_file.write_all(&[0xFF, 0xFF, 0xFF, 0xFF])?;
        drop(event_batch_file);

        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;
        let corruption =
            fixture.detect_corruption(&mut event_batch_reader, &mut metadata_reader)?;

        // Should detect corruption after the second valid batch
        assert!(
            corruption.is_some(),
            "Should detect corruption in third batch"
        );
        let corrupt_pos = corruption.unwrap();
        assert_eq!(
            corrupt_pos.metadata_position,
            2 * METADATA_BATCH_SIZE_BYTES as u64
        );
        assert_eq!(
            corrupt_pos.event_batch_position,
            metadata1.compressed_size + metadata2.compressed_size
        );

        Ok(())
    }

    #[test]
    fn test_corruption_detection_insufficient_event_batch_bytes() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        // Write valid data first
        let events = vec![fixture.create_simple_event(1)];
        let batch1 = fixture.create_simple_batch(1, events.clone());
        let batch2 = fixture.create_simple_batch(2, events);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        let metadata1 =
            fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch1)?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch2)?;
        drop(event_batch_writer);
        drop(metadata_writer);

        // Truncate event batch file so second batch is incomplete
        let event_batch_path = fixture._temp_dir.path().join("event_batches.bin");
        let mut event_batch_file = std::fs::OpenOptions::new()
            .write(true)
            .open(&event_batch_path)?;

        // Truncate to only partial second batch
        let truncate_position = metadata1.compressed_size + 10;
        event_batch_file.set_len(truncate_position)?;
        drop(event_batch_file);

        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;
        let corruption =
            fixture.detect_corruption(&mut event_batch_reader, &mut metadata_reader)?;

        // Should detect corruption after the first valid batch
        assert!(
            corruption.is_some(),
            "Should detect corruption due to insufficient bytes for second batch"
        );
        let corrupt_pos = corruption.unwrap();
        assert_eq!(
            corrupt_pos.metadata_position,
            METADATA_BATCH_SIZE_BYTES as u64
        );
        assert_eq!(corrupt_pos.event_batch_position, metadata1.compressed_size);

        Ok(())
    }

    #[test]
    fn test_corruption_detection_partial_metadata_read() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        // Write valid data first
        let events = vec![fixture.create_simple_event(1)];
        let batch1 = fixture.create_simple_batch(1, events);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch1)?;
        drop(event_batch_writer);
        drop(metadata_writer);

        // Append partial metadata entry (simulate interrupted write)
        let metadata_path = fixture._temp_dir.path().join("metadata.bin");
        let mut metadata_file = std::fs::OpenOptions::new()
            .append(true)
            .open(&metadata_path)?;

        // Write partial metadata (less than METADATA_BATCH_SIZE_BYTES)
        let partial_metadata = vec![0u8; METADATA_BATCH_SIZE_BYTES / 2];
        metadata_file.write_all(&partial_metadata)?;
        drop(metadata_file);

        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;
        let corruption =
            fixture.detect_corruption(&mut event_batch_reader, &mut metadata_reader)?;

        // Should detect corruption after the first valid batch
        assert!(
            corruption.is_some(),
            "Should detect corruption due to partial metadata entry"
        );
        let corrupt_pos = corruption.unwrap();
        assert_eq!(
            corrupt_pos.metadata_position,
            METADATA_BATCH_SIZE_BYTES as u64
        );

        Ok(())
    }

    #[test]
    fn test_corruption_detection_with_large_files() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        // Write many valid batches
        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;

        let mut last_metadata = None;
        for i in 1..=100 {
            let events = vec![fixture.create_simple_event(i)];
            let batch = fixture.create_simple_batch(i, events);
            last_metadata =
                Some(fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch)?);
        }
        drop(event_batch_writer);
        drop(metadata_writer);

        // Corrupt the last batch
        let event_batch_path = fixture._temp_dir.path().join("event_batches.bin");
        let mut event_batch_file = std::fs::OpenOptions::new()
            .write(true)
            .open(&event_batch_path)?;

        // Seek near end and corrupt
        let file_len = event_batch_file.seek(SeekFrom::End(0))?;
        event_batch_file.seek(SeekFrom::Start(file_len - 10))?;
        event_batch_file.write_all(&[0xFF, 0xFF, 0xFF, 0xFF])?;
        drop(event_batch_file);

        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;
        let corruption =
            fixture.detect_corruption(&mut event_batch_reader, &mut metadata_reader)?;

        // Should detect corruption in the last batch
        assert!(
            corruption.is_some(),
            "Should detect corruption in large file"
        );
        let corrupt_pos = corruption.unwrap();
        assert_eq!(
            corrupt_pos.metadata_position,
            99 * METADATA_BATCH_SIZE_BYTES as u64
        );

        Ok(())
    }

    // 8. Metadata Query Operations

    #[test]
    fn test_last_server_id_retrieval() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        // Write multiple batches
        let events = vec![fixture.create_simple_event(1)];
        let batch1 = fixture.create_simple_batch(5, events.clone()); // server_id 5
        let batch2 = fixture.create_simple_batch(10, events.clone()); // server_id 10  
        let batch3 = fixture.create_simple_batch(15, events); // server_id 15

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch1)?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch2)?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch3)?;

        let (_, mut metadata_reader) = fixture.create_readers()?;
        let last_server_id = fixture.last_event_batch_index(&mut metadata_reader)?;

        assert_eq!(
            last_server_id, 15,
            "Should return the most recent server ID"
        );

        Ok(())
    }

    #[test]
    fn test_last_server_id_single_batch() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        let events = vec![fixture.create_simple_event(1)];
        let batch = fixture.create_simple_batch(42, events);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch)?;

        let (_, mut metadata_reader) = fixture.create_readers()?;
        let last_server_id = fixture.last_event_batch_index(&mut metadata_reader)?;

        assert_eq!(last_server_id, 42, "Should return the single server ID");

        Ok(())
    }

    #[test]
    fn test_last_server_id_from_empty_file() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        // Create empty metadata file
        let (_, metadata_writer) = fixture.create_writers()?;
        drop(metadata_writer);

        let (_, mut metadata_reader) = fixture.create_readers()?;
        let result = fixture.last_event_batch_index(&mut metadata_reader);

        assert!(
            result.is_err(),
            "Should return error for empty metadata file"
        );

        let error_msg = result.unwrap_err().to_string();
        assert!(
            error_msg.contains("too small to contain any entries"),
            "Error should indicate insufficient data"
        );

        Ok(())
    }

    #[test]
    fn test_last_server_id_with_partial_metadata() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        // Write one complete batch
        let events = vec![fixture.create_simple_event(1)];
        let batch = fixture.create_simple_batch(1, events);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch)?;
        drop(event_batch_writer);
        drop(metadata_writer);

        // Append partial metadata to simulate corruption or incomplete write
        let metadata_path = fixture._temp_dir.path().join("metadata.bin");
        let mut metadata_file = std::fs::OpenOptions::new()
            .append(true)
            .open(&metadata_path)?;

        let partial_data = vec![0u8; METADATA_BATCH_SIZE_BYTES / 2]; // Half a metadata entry
        metadata_file.write_all(&partial_data)?;
        drop(metadata_file);

        let (_, mut metadata_reader) = fixture.create_readers()?;
        let last_server_id = fixture.last_event_batch_index(&mut metadata_reader);

        // Should still return the last complete entry
        assert!(last_server_id.is_err(), "Should error due to corruption");

        Ok(())
    }

    #[test]
    fn test_last_local_index_retrieval() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        // Create events with different local indices
        let mut event1 = fixture.create_simple_event(1);
        event1.client_event_index = 10;
        let mut event2 = fixture.create_simple_event(2);
        event2.client_event_index = 25;
        let mut event3 = fixture.create_simple_event(3);
        event3.client_event_index = 15; // Not the highest in the batch

        let batch1 = fixture.create_simple_batch(1, vec![event1, event2, event3]);

        // Second batch with higher indices
        let mut event4 = fixture.create_simple_event(4);
        event4.client_event_index = 50;
        let mut event5 = fixture.create_simple_event(5);
        event5.client_event_index = 75; // This should be the highest overall

        let batch2 = fixture.create_simple_batch(2, vec![event4, event5]);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch1)?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch2)?;

        let (_, mut metadata_reader) = fixture.create_readers()?;
        let last_local_index = fixture.engine.last_local_index(&mut metadata_reader)?;

        assert_eq!(
            last_local_index, 75,
            "Should return the highest local index from the last batch"
        );

        Ok(())
    }

    #[test]
    fn test_last_local_index_single_event() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        let mut event = fixture.create_simple_event(1);
        event.client_event_index = 42;
        let batch = fixture.create_simple_batch(1, vec![event]);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch)?;

        let (_, mut metadata_reader) = fixture.create_readers()?;
        let last_local_index = fixture.engine.last_local_index(&mut metadata_reader)?;

        assert_eq!(
            last_local_index, 42,
            "Should return the single event's local index"
        );

        Ok(())
    }

    #[test]
    fn test_last_local_index_from_empty_file() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        // Create empty metadata file
        let (_, metadata_writer) = fixture.create_writers()?;
        drop(metadata_writer);

        let (_, mut metadata_reader) = fixture.create_readers()?;
        let result = fixture.engine.last_local_index(&mut metadata_reader);

        assert!(
            result.is_err(),
            "Should return error for empty metadata file"
        );

        let error_msg = result.unwrap_err().to_string();
        assert!(
            error_msg.contains("too small to contain any entries"),
            "Error should indicate insufficient data"
        );

        Ok(())
    }

    #[test]
    fn test_last_local_index_with_zero_index() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        let mut event1 = fixture.create_simple_event(1);
        event1.client_event_index = 0; // Zero is a valid local index
        let mut event2 = fixture.create_simple_event(2);
        event2.client_event_index = 5;

        let batch = fixture.create_simple_batch(1, vec![event1, event2]);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch)?;

        let (_, mut metadata_reader) = fixture.create_readers()?;
        let last_local_index = fixture.engine.last_local_index(&mut metadata_reader)?;

        assert_eq!(
            last_local_index, 5,
            "Should return the maximum local index, not necessarily the last event"
        );

        Ok(())
    }

    #[test]
    fn test_last_local_index_multiple_batches() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        // First batch with high indices
        let mut event1 = fixture.create_simple_event(1);
        event1.client_event_index = 100;
        let batch1 = fixture.create_simple_batch(1, vec![event1]);

        // Second batch with lower indices (but this is the "last" batch)
        let mut event2 = fixture.create_simple_event(2);
        event2.client_event_index = 50;
        let mut event3 = fixture.create_simple_event(3);
        event3.client_event_index = 75;
        let batch2 = fixture.create_simple_batch(2, vec![event2, event3]);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch1)?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch2)?;

        let (_, mut metadata_reader) = fixture.create_readers()?;
        let last_local_index = fixture.engine.last_local_index(&mut metadata_reader)?;

        // Should return max from LAST batch, not the global maximum
        assert_eq!(
            last_local_index, 75,
            "Should return highest local index from the most recent batch only"
        );

        Ok(())
    }

    #[test]
    fn test_last_local_index_with_partial_metadata() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        // Write one complete batch
        let mut event = fixture.create_simple_event(1);
        event.client_event_index = 42;
        let batch = fixture.create_simple_batch(1, vec![event]);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch)?;
        drop(event_batch_writer);
        drop(metadata_writer);

        // Append partial metadata
        let metadata_path = fixture._temp_dir.path().join("metadata.bin");
        let mut metadata_file = std::fs::OpenOptions::new()
            .append(true)
            .open(&metadata_path)?;

        let partial_data = vec![0u8; METADATA_BATCH_SIZE_BYTES / 2];
        metadata_file.write_all(&partial_data)?;
        drop(metadata_file);

        let (_, mut metadata_reader) = fixture.create_readers()?;
        let result = fixture.engine.last_local_index(&mut metadata_reader);

        assert!(
            result.is_err(),
            "Should return error due to partial metadata"
        );

        Ok(())
    }

    #[test]
    fn test_metadata_queries_with_large_file() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;

        let num_batches = 1000;
        let mut expected_last_server_id = 0;
        let mut expected_last_local_index = 0;

        for i in 1..=num_batches {
            let mut event = fixture.create_simple_event(i);
            event.client_event_index = i * 10; // Make local indices predictable

            let batch = fixture.create_simple_batch(i, vec![event]);
            fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch)?;

            expected_last_server_id = i;
            expected_last_local_index = i * 10;
        }

        let (_, mut metadata_reader) = fixture.create_readers()?;

        // Test last server ID
        let last_server_id = fixture.last_event_batch_index(&mut metadata_reader)?;
        assert_eq!(
            last_server_id, expected_last_server_id,
            "Should handle large files correctly for server ID"
        );

        // Test last local index
        let last_local_index = fixture.engine.last_local_index(&mut metadata_reader)?;
        assert_eq!(
            last_local_index, expected_last_local_index,
            "Should handle large files correctly for local index"
        );

        Ok(())
    }

    #[test]
    fn test_metadata_queries_after_seek_operations() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        let mut event1 = fixture.create_simple_event(1);
        event1.client_event_index = 100;
        let batch1 = fixture.create_simple_batch(10, vec![event1]);

        let mut event2 = fixture.create_simple_event(2);
        event2.client_event_index = 200;
        let batch2 = fixture.create_simple_batch(20, vec![event2]);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch1)?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch2)?;

        let (_, mut metadata_reader) = fixture.create_readers()?;

        // Perform some seek operations to move the cursor
        metadata_reader.seek(SeekFrom::Start(10))?;
        metadata_reader.seek(SeekFrom::Current(20))?;

        // The methods should still work correctly regardless of cursor position
        let last_server_id = fixture.last_event_batch_index(&mut metadata_reader)?;
        assert_eq!(last_server_id, 20, "Should work after seek operations");

        // Seek again
        metadata_reader.seek(SeekFrom::End(-50))?;

        let last_local_index = fixture.engine.last_local_index(&mut metadata_reader)?;
        assert_eq!(
            last_local_index, 200,
            "Should work after multiple seek operations"
        );

        Ok(())
    }

    #[test]
    fn test_metadata_queries_consistency() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        // Create batches with specific patterns
        let batches_data = vec![
            (1, 10), // (server_id, max_local_index)
            (5, 25),
            (3, 15), // Out of order server_id but should still be considered "last" in file order
            (10, 50),
        ];

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;

        for (server_id, local_idx) in &batches_data {
            let mut event = fixture.create_simple_event(*local_idx);
            event.client_event_index = *local_idx;

            let batch = fixture.create_simple_batch(*server_id, vec![event]);
            fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch)?;
        }

        let (_, mut metadata_reader) = fixture.create_readers()?;

        // Last server ID should be from the last written batch (server_id 10)
        let last_server_id = fixture.last_event_batch_index(&mut metadata_reader)?;
        assert_eq!(
            last_server_id, 10,
            "Should return server_id from last written batch"
        );

        // Last local index should be from the last written batch (local_index 50)
        let last_local_index = fixture.engine.last_local_index(&mut metadata_reader)?;
        assert_eq!(
            last_local_index, 50,
            "Should return local_index from last written batch"
        );

        Ok(())
    }

    // 9. Destructive Operations

    #[test]
    fn test_trim_end_operation() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        // Write multiple batches
        let events = vec![fixture.create_event_with_data(1, vec![0u8; 100])];
        let batch1 = fixture.create_simple_batch(1, events.clone());
        let batch2 = fixture.create_simple_batch(2, events.clone());
        let batch3 = fixture.create_simple_batch(3, events);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        let metadata1 =
            fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch1)?;
        let metadata2 =
            fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch2)?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch3)?;

        // Calculate trim positions to remove the last batch
        let event_batch_trim_position = metadata1.compressed_size + metadata2.compressed_size;
        let metadata_trim_position = 2 * METADATA_BATCH_SIZE_BYTES as u64;

        // Perform trim_end
        let mut event_batch_writer  =
            std::fs::OpenOptions::new()
                .write(true)
                .open(fixture._temp_dir.path().join("event_batches.bin"),
        )?;
        let mut metadata_writer = 
            std::fs::OpenOptions::new()
                .write(true)
                .open(fixture._temp_dir.path().join("metadata.bin"),
        )?;

        fixture.engine.trim_end(
            &mut event_batch_writer,
            event_batch_trim_position,
            &mut metadata_writer,
            metadata_trim_position,
        )?;

        drop(event_batch_writer);
        drop(metadata_writer);

        // Verify only first two batches remain
        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;
        let read_result = fixture.read_filtered(
            &mut event_batch_reader,
            &mut metadata_reader,
            &ReadFilters::new(1),
        )?;

        assert_eq!(read_result.event_batches.len(), 2);
        assert_eq!(read_result.event_batches[0].event_batch_index, 1);
        assert_eq!(read_result.event_batches[1].event_batch_index, 2);

        // Verify no corruption detected
        let corruption =
            fixture.detect_corruption(&mut event_batch_reader, &mut metadata_reader)?;
        assert!(corruption.is_none());

        Ok(())
    }

    #[test]
    fn test_trim_end_at_file_boundaries() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        // Write exactly 3 batches
        let events = vec![fixture.create_event_with_data(1, vec![0u8; 200])];
        let batch1 = fixture.create_simple_batch(1, events.clone());
        let batch2 = fixture.create_simple_batch(2, events.clone());
        let batch3 = fixture.create_simple_batch(3, events);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        let metadata1 =
            fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch1)?;
        let metadata2 =
            fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch2)?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch3)?;

        // Trim at exact end of batch 2
        let event_batch_trim_position = metadata1.compressed_size + metadata2.compressed_size;
        let metadata_trim_position = 2 * METADATA_BATCH_SIZE_BYTES as u64;

        let mut event_batch_writer = 
            std::fs::OpenOptions::new()
                .write(true)
                .open(fixture._temp_dir.path().join("event_batches.bin"),
        )?;
        let mut metadata_writer = 
            std::fs::OpenOptions::new()
                .write(true)
                .open(fixture._temp_dir.path().join("metadata.bin"),
        )?;

        fixture.engine.trim_end(
            &mut event_batch_writer,
            event_batch_trim_position,
            &mut metadata_writer,
            metadata_trim_position,
        )?;

        drop(event_batch_writer);
        drop(metadata_writer);

        // Should have exactly 2 batches remaining
        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;
        let read_result = fixture.read_filtered(
            &mut event_batch_reader,
            &mut metadata_reader,
            &ReadFilters::new(1),
        )?;

        assert_eq!(read_result.event_batches.len(), 2);
        assert_eq!(read_result.event_batches[0].event_batch_index, 1);
        assert_eq!(read_result.event_batches[1].event_batch_index, 2);

        Ok(())
    }

    #[test]
    fn test_trim_end_to_zero() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        // Write one batch
        let events = vec![fixture.create_event_with_data(1, vec![0u8; 100])];
        let batch1 = fixture.create_simple_batch(1, events);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch1)?;

        // Trim to position 0 (remove everything)
        let mut event_batch_writer = 
            std::fs::OpenOptions::new()
                .write(true)
                .open(fixture._temp_dir.path().join("event_batches.bin"),
        )?;
        let mut metadata_writer = 
            std::fs::OpenOptions::new()
                .write(true)
                .open(fixture._temp_dir.path().join("metadata.bin"),
        )?;

        fixture
            .engine
            .trim_end(&mut event_batch_writer, 0, &mut metadata_writer, 0)?;

        drop(event_batch_writer);
        drop(metadata_writer);

        // Verify files are empty
        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;

        // Should detect corruption (empty files)
        let corruption =
            fixture.detect_corruption(&mut event_batch_reader, &mut metadata_reader)?;
        assert!(corruption.is_some());

        let corrupt_pos = corruption.unwrap();
        assert_eq!(corrupt_pos.metadata_position, 0);
        assert_eq!(corrupt_pos.event_batch_position, 0);

        Ok(())
    }

    #[test]
    fn test_trim_start_operation() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        // Write 5 batches
        let mut batches = Vec::new();
        let mut total_event_size = 0u64;
        let mut total_metadata_size = 0u64;

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;

        for i in 1..=5 {
            let events = vec![fixture.create_event_with_data(i, vec![0u8; 100])];
            let batch = fixture.create_simple_batch(i, events);
            let metadata =
                fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch)?;

            if i <= 2 {
                total_event_size += metadata.compressed_size;
                total_metadata_size += METADATA_BATCH_SIZE_BYTES as u64;
            }

            batches.push(batch);
        }

        drop(event_batch_writer);
        drop(metadata_writer);

        // Trim start to keep from batch 3 onwards
        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;
        fixture.trim_start(
            &mut event_batch_reader,
            total_event_size,
            &mut metadata_reader,
            total_metadata_size,
        )?;

        drop(event_batch_reader);
        drop(metadata_reader);

        // Verify only batches 3, 4, 5 remain
        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;
        let read_result = fixture.read_filtered(
            &mut event_batch_reader,
            &mut metadata_reader,
            &ReadFilters::new(3),
        )?;

        assert_eq!(read_result.event_batches.len(), 3);
        assert_eq!(read_result.event_batches[0].event_batch_index, 3);
        assert_eq!(read_result.event_batches[1].event_batch_index, 4);
        assert_eq!(read_result.event_batches[2].event_batch_index, 5);

        // Verify no corruption detected
        let corruption =
            fixture.detect_corruption(&mut event_batch_reader, &mut metadata_reader)?;
        assert!(corruption.is_none());

        Ok(())
    }

    #[test]
    fn test_trim_start_to_position_zero_rejection() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        // Write some batches
        let events = vec![fixture.create_event_with_data(1, vec![0u8; 100])];
        let batch = fixture.create_simple_batch(1, events);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch)?;

        drop(event_batch_writer);
        drop(metadata_writer);

        // Attempt to trim to position 0
        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;
        let result = fixture.trim_start(&mut event_batch_reader, 0, &mut metadata_reader, 0);

        // Should return error
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Cannot trim to position 0"));

        Ok(())
    }

    #[test]
    fn test_trim_start_with_temporary_files() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        // Write multiple batches
        let events = vec![fixture.create_event_with_data(1, vec![0u8; 500])];
        let batch1 = fixture.create_simple_batch(1, events.clone());
        let batch2 = fixture.create_simple_batch(2, events.clone());
        let batch3 = fixture.create_simple_batch(3, events);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        let metadata1 =
            fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch1)?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch2)?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch3)?;

        drop(event_batch_writer);
        drop(metadata_writer);

        // Trim start to keep from batch 2 onwards
        let event_batch_trim_position = metadata1.compressed_size;
        let metadata_trim_position = METADATA_BATCH_SIZE_BYTES as u64;

        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;
        fixture.trim_start(
            &mut event_batch_reader,
            event_batch_trim_position,
            &mut metadata_reader,
            metadata_trim_position,
        )?;

        drop(event_batch_reader);
        drop(metadata_reader);

        // Verify no temporary files remain
        let event_batch_temp_path = fixture._temp_dir.path().join("event_batches.bin.tmp");
        let metadata_temp_path = fixture._temp_dir.path().join("metadata.bin.tmp");

        assert!(
            !event_batch_temp_path.exists(),
            "Event batch temp file should be cleaned up"
        );
        assert!(
            !metadata_temp_path.exists(),
            "Metadata temp file should be cleaned up"
        );

        // Verify operation succeeded
        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;
        let read_result = fixture.read_filtered(
            &mut event_batch_reader,
            &mut metadata_reader,
            &ReadFilters::new(2),
        )?;

        assert_eq!(read_result.event_batches.len(), 2);
        assert_eq!(read_result.event_batches[0].event_batch_index, 2);
        assert_eq!(read_result.event_batches[1].event_batch_index, 3);

        Ok(())
    }

    #[test]
    fn test_trim_start_single_batch_remaining() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        // Write 3 batches
        let events = vec![fixture.create_event_with_data(1, vec![0u8; 100])];
        let batch1 = fixture.create_simple_batch(1, events.clone());
        let batch2 = fixture.create_simple_batch(2, events.clone());
        let batch3 = fixture.create_simple_batch(3, events);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        let metadata1 =
            fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch1)?;
        let metadata2 =
            fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch2)?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch3)?;

        drop(event_batch_writer);
        drop(metadata_writer);

        // Trim start to keep only the last batch
        let event_batch_trim_position = metadata1.compressed_size + metadata2.compressed_size;
        let metadata_trim_position = 2 * METADATA_BATCH_SIZE_BYTES as u64;

        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;
        fixture.trim_start(
            &mut event_batch_reader,
            event_batch_trim_position,
            &mut metadata_reader,
            metadata_trim_position,
        )?;

        drop(event_batch_reader);
        drop(metadata_reader);

        // Verify only the last batch remains
        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;
        let read_result = fixture.read_filtered(
            &mut event_batch_reader,
            &mut metadata_reader,
            &ReadFilters::new(3),
        )?;

        assert_eq!(read_result.event_batches.len(), 1);
        assert_eq!(read_result.event_batches[0].event_batch_index, 3);

        Ok(())
    }

    #[test]
    fn test_trim_start_large_file() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        // Write many batches
        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;

        let num_batches = 100;
        let trim_from_batch = 50;
        let mut trim_event_position = 0u64;
        let mut trim_metadata_position = 0u64;

        for i in 1..=num_batches {
            let events = vec![fixture.create_event_with_data(i, vec![0u8; 50])];
            let batch = fixture.create_simple_batch(i, events);
            let metadata =
                fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch)?;

            if i < trim_from_batch {
                trim_event_position += metadata.compressed_size;
                trim_metadata_position += METADATA_BATCH_SIZE_BYTES as u64;
            }
        }

        drop(event_batch_writer);
        drop(metadata_writer);

        // Trim start to keep from batch 50 onwards
        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;
        fixture.trim_start(
            &mut event_batch_reader,
            trim_event_position,
            &mut metadata_reader,
            trim_metadata_position,
        )?;

        drop(event_batch_reader);
        drop(metadata_reader);

        // Verify batches 50-100 remain
        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;
        let read_result = fixture.read_filtered(
            &mut event_batch_reader,
            &mut metadata_reader,
            &ReadFilters::new(trim_from_batch),
        )?;

        assert_eq!(
            read_result.event_batches.len(),
            (num_batches - trim_from_batch + 1) as usize
        );
        assert_eq!(
            read_result.event_batches[0].event_batch_index,
            trim_from_batch
        );
        assert_eq!(
            read_result.event_batches.last().unwrap().event_batch_index,
            num_batches
        );

        Ok(())
    }

    #[test]
    fn test_file_deletion_operation() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        // Write some data
        let events = vec![fixture.create_simple_event(1)];
        let batch = fixture.create_simple_batch(1, events);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch)?;

        drop(event_batch_writer);
        drop(metadata_writer);

        // Verify files exist
        let event_batch_path = fixture._temp_dir.path().join("event_batches.bin");
        let metadata_path = fixture._temp_dir.path().join("metadata.bin");
        assert!(event_batch_path.exists());
        assert!(metadata_path.exists());

        // Delete files
        fixture.engine.delete(&event_batch_path, &metadata_path)?;

        // Verify files no longer exist
        assert!(!event_batch_path.exists());
        assert!(!metadata_path.exists());

        Ok(())
    }

    #[test]
    fn test_delete_non_existent_files() -> io::Result<()> {
        let fixture = TestFixture::new()?;

        let non_existent_event_path = fixture._temp_dir.path().join("non_existent_events.bin");
        let non_existent_metadata_path = fixture._temp_dir.path().join("non_existent_metadata.bin");

        // Attempt to delete non-existent files
        let result = fixture
            .engine
            .delete(&non_existent_event_path, &non_existent_metadata_path);

        // Should return error
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_delete_partial_failure() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        // Create only the event batch file, not the metadata file
        let events = vec![fixture.create_simple_event(1)];
        let batch = fixture.create_simple_batch(1, events);

        let (mut event_batch_writer, _) = fixture.create_writers()?;
        // Create a temporary metadata file for writing, but we'll delete it later
        let temp_metadata_file = std::fs::File::create(fixture._temp_dir.path().join("temp_metadata.bin"))?;
        let mut temp_metadata_writer = temp_metadata_file;
        
        fixture.write_batch(&mut event_batch_writer, &mut temp_metadata_writer, &batch)?;
        drop(event_batch_writer);
        drop(temp_metadata_writer);

        let event_batch_path = fixture._temp_dir.path().join("event_batches.bin");
        let metadata_path = fixture._temp_dir.path().join("metadata.bin");

        //Delete metadata_path
        std::fs::remove_file(&metadata_path).ok();

        // Verify only event batch file exists
        assert!(event_batch_path.exists());
        assert!(!metadata_path.exists());

        // Attempt to delete both files
        let result = fixture.engine.delete(&event_batch_path, &metadata_path);

        // Should fail because metadata file doesn't exist
        assert!(result.is_err());

        // Event batch file should still exist (deletion is not atomic across both files)
        assert!(event_batch_path.exists());

        Ok(())
    }

    #[test]
    fn test_trim_operations_preserve_data_integrity() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        // Write batches with different data to ensure integrity after trim
        let batch1_events = vec![
            fixture.create_event_with_data(1, b"batch1_event1".to_vec()),
            fixture.create_event_with_data(2, b"batch1_event2".to_vec()),
        ];
        let batch2_events = vec![fixture.create_event_with_data(3, b"batch2_event1".to_vec())];
        let batch3_events = vec![
            fixture.create_event_with_data(4, b"batch3_event1".to_vec()),
            fixture.create_event_with_data(5, b"batch3_event2".to_vec()),
            fixture.create_event_with_data(6, b"batch3_event3".to_vec()),
        ];

        let batch1 = fixture.create_simple_batch(1, batch1_events);
        let batch2 = fixture.create_simple_batch(2, batch2_events);
        let batch3 = fixture.create_simple_batch(3, batch3_events);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        let metadata1 =
            fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch1)?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch2)?;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch3)?;

        drop(event_batch_writer);
        drop(metadata_writer);

        // First, trim end to remove batch3
        let mut event_batch_writer = 
            std::fs::OpenOptions::new()
                .write(true)
                .open(fixture._temp_dir.path().join("event_batches.bin"),
        )?;
        let mut metadata_writer = 
            std::fs::OpenOptions::new()
                .write(true)
                .open(fixture._temp_dir.path().join("metadata.bin"),
        )?;

        // Create temporary files to calculate compressed size
        let temp_event_file = std::fs::File::create(fixture._temp_dir.path().join("temp_events.bin"))?;
        let temp_metadata_file = std::fs::File::create(fixture._temp_dir.path().join("temp_metadata.bin"))?;
        let mut temp_event_writer = temp_event_file;
        let mut temp_metadata_writer = temp_metadata_file;

        let compressed_size = fixture
            .write_batch(&mut temp_event_writer, &mut temp_metadata_writer, &batch2)?
            .compressed_size;

        drop(temp_event_writer);
        drop(temp_metadata_writer);

        fixture.engine.trim_end(
            &mut event_batch_writer,
            metadata1.compressed_size + compressed_size,
            &mut metadata_writer,
            2 * METADATA_BATCH_SIZE_BYTES as u64,
        )?;

        drop(event_batch_writer);
        drop(metadata_writer);

        // Now trim start to remove batch1
        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;
        fixture.trim_start(
            &mut event_batch_reader,
            metadata1.compressed_size,
            &mut metadata_reader,
            METADATA_BATCH_SIZE_BYTES as u64,
        )?;

        drop(event_batch_reader);
        drop(metadata_reader);

        // Verify only batch2 remains and data is intact
        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;
        let read_result = fixture.read_filtered(
            &mut event_batch_reader,
            &mut metadata_reader,
            &ReadFilters::new(2),
        )?;

        assert_eq!(read_result.event_batches.len(), 1);
        let remaining_batch = &read_result.event_batches[0];
        assert_eq!(remaining_batch.event_batch_index, 2);
        assert_eq!(remaining_batch.events.len(), 1);
        assert_eq!(
            remaining_batch.events[0].event_value,
            Arc::new(b"batch2_event1".to_vec())
        );

        // Verify no corruption
        let corruption =
            fixture.detect_corruption(&mut event_batch_reader, &mut metadata_reader)?;
        assert!(corruption.is_none());

        Ok(())
    }

    #[test]
    fn test_trim_operations_with_different_compression() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        // Write batches with different compression types
        let events = vec![fixture.create_event_with_data(1, vec![0u8; 1000])]; // Compressible data

        let batch1 = fixture.create_simple_batch(1, events.clone());
        let batch2 = fixture.create_simple_batch(2, events.clone());
        let batch3 = fixture.create_simple_batch(3, events);

        // Use different compression for each batch by creating separate fixtures
        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;

        // Write with Zstd compression (default)
        let metadata1 =
            fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch1)?;

        // Change to Snappy compression
        fixture.compression_type = CompressionType::Snappy;
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch2)?;

        // Change to Gzip compression
        fixture.compression_type = CompressionType::Gzip { level: 6 };
        fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch3)?;

        drop(event_batch_writer);
        drop(metadata_writer);

        // Trim start to remove first batch
        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;
        fixture.trim_start(
            &mut event_batch_reader,
            metadata1.compressed_size,
            &mut metadata_reader,
            METADATA_BATCH_SIZE_BYTES as u64,
        )?;

        drop(event_batch_reader);
        drop(metadata_reader);

        // Verify remaining batches with different compression types work correctly
        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;
        let read_result = fixture.read_filtered(
            &mut event_batch_reader,
            &mut metadata_reader,
            &ReadFilters::new(2),
        )?;

        assert_eq!(read_result.event_batches.len(), 2);
        assert_eq!(read_result.event_batches[0].event_batch_index, 2);
        assert_eq!(read_result.event_batches[1].event_batch_index, 3);

        // Verify no corruption
        let corruption =
            fixture.detect_corruption(&mut event_batch_reader, &mut metadata_reader)?;
        assert!(corruption.is_none());

        Ok(())
    }

    #[test]
    fn test_positions_for_event_batch_index() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        // Create test batches with different sizes
        let events1 = vec![fixture.create_event_with_data(1, vec![0u8; 100])];
        let events2 = vec![fixture.create_event_with_data(2, vec![1u8; 200])];
        let events3 = vec![fixture.create_event_with_data(3, vec![2u8; 300])];

        let batch1 = fixture.create_simple_batch(10, events1); // Start from index 10
        let batch2 = fixture.create_simple_batch(11, events2);
        let batch3 = fixture.create_simple_batch(12, events3);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;

        // Write batches and capture metadata to know compressed sizes
        let metadata1 =
            fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch1)?;
        let metadata2 =
            fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch2)?;
        let metadata3 =
            fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch3)?;

        drop(event_batch_writer);
        drop(metadata_writer);

        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;

        // Test getting positions for first batch (index 10)
        let positions = fixture
            .engine
            .positions_for_event_batch_index(&mut metadata_reader, 10)?;
        assert!(positions.is_some());
        let pos = positions.unwrap();
        assert_eq!(pos.metadata_position, 0); // First metadata entry
        assert_eq!(pos.event_batch_position, 0); // First event batch

        // Test getting positions for second batch (index 11)
        let positions = fixture
            .engine
            .positions_for_event_batch_index(&mut metadata_reader, 11)?;
        assert!(positions.is_some());
        let pos = positions.unwrap();
        assert_eq!(pos.metadata_position, METADATA_BATCH_SIZE_BYTES as u64); // Second metadata entry
        assert_eq!(pos.event_batch_position, metadata1.compressed_size); // After first batch

        // Test getting positions for third batch (index 12)
        let positions = fixture
            .engine
            .positions_for_event_batch_index(&mut metadata_reader, 12)?;
        assert!(positions.is_some());
        let pos = positions.unwrap();
        assert_eq!(pos.metadata_position, 2 * METADATA_BATCH_SIZE_BYTES as u64); // Third metadata entry
        assert_eq!(
            pos.event_batch_position,
            metadata1.compressed_size + metadata2.compressed_size
        ); // After first two batches

        // Test getting positions for non-existent batch (too low)
        let positions = fixture
            .engine
            .positions_for_event_batch_index(&mut metadata_reader, 9)?;
        assert!(positions.is_none());

        // Test getting positions for non-existent batch (too high)
        let positions = fixture
            .engine
            .positions_for_event_batch_index(&mut metadata_reader, 15)?;
        assert!(positions.is_none());

        Ok(())
    }

    #[test]
    fn test_positions_for_event_batch_index_single_batch() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        // Create single batch starting from a high index
        let events = vec![fixture.create_event_with_data(1, vec![0u8; 500])];
        let batch = fixture.create_simple_batch(100, events);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;
        let metadata =
            fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch)?;

        drop(event_batch_writer);
        drop(metadata_writer);

        let (mut _event_batch_reader, mut metadata_reader) = fixture.create_readers()?;

        // Test getting positions for the single batch
        let positions = fixture
            .engine
            .positions_for_event_batch_index(&mut metadata_reader, 100)?;
        assert!(positions.is_some());
        let pos = positions.unwrap();
        assert_eq!(pos.metadata_position, 0);
        assert_eq!(pos.event_batch_position, 0);

        // Test positions for indices before the available range
        let positions = fixture
            .engine
            .positions_for_event_batch_index(&mut metadata_reader, 99)?;
        assert!(positions.is_none());

        // Test positions for indices after the available range
        let positions = fixture
            .engine
            .positions_for_event_batch_index(&mut metadata_reader, 101)?;
        assert!(positions.is_none());

        Ok(())
    }

    #[test]
    fn test_positions_after_trim_start_and_read() -> io::Result<()> {
        let mut fixture = TestFixture::new()?;

        // Create test batches with different sizes
        let events1 = vec![fixture.create_event_with_data(1, vec![0u8; 100])];
        let events2 = vec![fixture.create_event_with_data(2, vec![1u8; 200])];
        let events3 = vec![fixture.create_event_with_data(3, vec![2u8; 300])];
        let events4 = vec![fixture.create_event_with_data(4, vec![3u8; 400])];

        let batch1 = fixture.create_simple_batch(10, events1);
        let batch2 = fixture.create_simple_batch(11, events2);
        let batch3 = fixture.create_simple_batch(12, events3);
        let batch4 = fixture.create_simple_batch(13, events4);

        let (mut event_batch_writer, mut metadata_writer) = fixture.create_writers()?;

        // Write batches and capture metadata
        let metadata1 =
            fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch1)?;
        let metadata2 =
            fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch2)?;
        let metadata3 =
            fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch3)?;
        let metadata4 =
            fixture.write_batch(&mut event_batch_writer, &mut metadata_writer, &batch4)?;

        drop(event_batch_writer);
        drop(metadata_writer);

        // Get positions for batch 12 before trim
        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;
        let positions_before_trim = fixture
            .engine
            .positions_for_event_batch_index(&mut metadata_reader, 12)?;
        assert!(positions_before_trim.is_some());
        let pos_before = positions_before_trim.unwrap();

        // Expected positions before trim:
        // - Metadata position: 2 * METADATA_BATCH_SIZE_BYTES (third entry, index 12)
        // - Event batch position: metadata1.compressed_size + metadata2.compressed_size
        assert_eq!(
            pos_before.metadata_position,
            2 * METADATA_BATCH_SIZE_BYTES as u64
        );
        assert_eq!(
            pos_before.event_batch_position,
            metadata1.compressed_size + metadata2.compressed_size
        );

        // Trim start to remove first two batches (keep from batch 12 onwards)
        let trim_event_batch_position = metadata1.compressed_size + metadata2.compressed_size;
        let trim_metadata_position = 2 * METADATA_BATCH_SIZE_BYTES as u64;

        fixture.trim_start(
            &mut event_batch_reader,
            trim_event_batch_position,
            &mut metadata_reader,
            trim_metadata_position,
        )?;

        drop(event_batch_reader);
        drop(metadata_reader);

        // Get positions for batch 12 after trim
        let (mut event_batch_reader, mut metadata_reader) = fixture.create_readers()?;
        let positions_after_trim = fixture
            .engine
            .positions_for_event_batch_index(&mut metadata_reader, 12)?;
        assert!(positions_after_trim.is_some());
        let pos_after = positions_after_trim.unwrap();

        // After trim, batch 12 should now be at the beginning of both files
        assert_eq!(pos_after.metadata_position, 0);
        assert_eq!(pos_after.event_batch_position, 0);

        // Verify we can still read from event_batch_index 12 after trim
        let read_result = fixture.read_filtered(
            &mut event_batch_reader,
            &mut metadata_reader,
            &ReadFilters::new(12),
        )?;

        assert_eq!(read_result.event_batches.len(), 2); // Should have batches 12 and 13
        assert_eq!(read_result.event_batches[0].event_batch_index, 12);
        assert_eq!(read_result.event_batches[1].event_batch_index, 13);

        // Verify the content of batch 12 is correct
        assert_eq!(read_result.event_batches[0].events.len(), 1);
        assert_eq!(read_result.event_batches[0].events[0].client_event_index, 3);
        assert_eq!(
            read_result.event_batches[0].events[0].event_value,
            Arc::new(vec![2u8; 300])
        );

        // Verify positions for batch 13 after trim
        let positions_batch_13 = fixture
            .engine
            .positions_for_event_batch_index(&mut metadata_reader, 13)?;
        assert!(positions_batch_13.is_some());
        let pos_13 = positions_batch_13.unwrap();

        // Batch 13 should be at the second position after trim
        assert_eq!(pos_13.metadata_position, METADATA_BATCH_SIZE_BYTES as u64);
        assert_eq!(pos_13.event_batch_position, metadata3.compressed_size);

        // Verify trimmed batches are no longer accessible
        let positions_batch_10 = fixture
            .engine
            .positions_for_event_batch_index(&mut metadata_reader, 10)?;
        assert!(positions_batch_10.is_none());

        let positions_batch_11 = fixture
            .engine
            .positions_for_event_batch_index(&mut metadata_reader, 11)?;
        assert!(positions_batch_11.is_none());

        // Verify no corruption after trim
        let corruption =
            fixture.detect_corruption(&mut event_batch_reader, &mut metadata_reader)?;
        assert!(corruption.is_none());

        Ok(())
    }
}
