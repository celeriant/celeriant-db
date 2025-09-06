use std::sync::OnceLock;

pub struct StatelessEngine {
    pub io_uring_queue_depth: u32,
    pub io_uring_status: IoUringStatus,
}

static IO_URING_AVAILABLE: OnceLock<IoUringStatus> = OnceLock::new();

impl StatelessEngine {
    pub fn builder() -> StatelessEngineBuilder {
        StatelessEngineBuilder::new()
    }

    fn detect_io_uring_support() -> IoUringStatus {
        *IO_URING_AVAILABLE.get_or_init(|| {
            #[cfg(target_os = "linux")]
            {
                // Try to create a minimal io_uring instance
                match io_uring::IoUring::new(1) {
                    Ok(_) => IoUringStatus::Available,
                    Err(_) => IoUringStatus::Unavailable,
                }
            }
            #[cfg(not(target_os = "linux"))]
            {
                IoUringStatus::UnsupportedPlatform
            }
        })
    }

    pub fn is_io_uring_available(&self) -> bool {
        self.io_uring_status == IoUringStatus::Available
    }
}

pub struct StatelessEngineBuilder {
    io_uring_queue_depth: u32,
    io_uring_status: Option<IoUringStatus>,
}

impl Default for StatelessEngineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl StatelessEngineBuilder {
    pub fn new() -> Self {
        Self {
            io_uring_queue_depth: 32,
            io_uring_status: None,
        }
    }

    pub fn with_io_uring_queue_depth(mut self, depth: u32) -> Self {
        self.io_uring_queue_depth = depth;
        self
    }

    pub fn with_io_uring_disabled(mut self) -> Self {
        self.io_uring_status = Some(IoUringStatus::Unavailable);
        self
    }

    pub fn build(self) -> StatelessEngine {
        StatelessEngine {
            io_uring_queue_depth: self.io_uring_queue_depth,
            io_uring_status: self
                .io_uring_status
                .unwrap_or_else(|| StatelessEngine::detect_io_uring_support()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum IoUringStatus {
    Available,
    Unavailable,
    UnsupportedPlatform,
}

#[cfg(test)]
mod tests {
    use crate::stateless::stateless_engine::StatelessEngine;
    use crate::stateless::stateless_reader::StatelessReader;
    use crate::stateless::stateless_writer::StatelessWriter;
    use crate::structures::constants::BLOOM_HASH_SEED;
    use crate::structures::{
        compression_type::CompressionType,
        constants::{BLOOM_BYTES, BLOOM_HASH_COUNT},
        event_batch_item::EventBatchItem,
        event_item::EventItem,
        read_filters::ReadFilters,
    };
    use fastbloom::BloomFilter;
    use std::collections::HashSet;
    use std::fs::File;
    use std::io;
    use tempfile::tempdir;

    #[test]
    fn test_basic_write_read_flow() -> io::Result<()> {
        // Create a temporary directory for our test files
        let temp_dir = tempdir()?;
        let event_batch_path = temp_dir.path().join("event_batches.bin");
        let metadata_path = temp_dir.path().join("metadata.bin");

        // Create the stateless engine
        let engine = StatelessEngine::builder().build();

        // Create test data
        let event1 = EventItem::new(
            3, // local_index
            1,
            1000,                          // event_time
            42,                            // event_type_major
            1,                             // event_type_minor
            b"test event data 1".to_vec(), // value
        );

        let event2 = EventItem::new(4, 2, 1050, 42, 1, b"test event data 2".to_vec());

        let batch = EventBatchItem::new(
            2,                    // server_id
            1600000000000,        // server_time
            123456789,            // client_id
            Some(987654321),      // user_id
            vec![event1, event2], // events
        );

        // Setup file writers
        let mut event_batch_writer = File::create(&event_batch_path)?;
        let mut metadata_writer = File::create(&metadata_path)?;

        // Setup bloom filter with proper constants
        let mut bloom_filter = BloomFilter::with_num_bits(BLOOM_BYTES * 8)
            .seed(&BLOOM_HASH_SEED)
            .hashes(BLOOM_HASH_COUNT);
        let mut event_type_dedup = HashSet::new();

        // Write the batch using the trait method
        let compression_type = CompressionType::Zstd { level: 3 };
        let metadata = engine.append_event_batch(
            &mut event_batch_writer,
            &mut metadata_writer,
            &mut bloom_filter,
            &mut event_type_dedup,
            compression_type,
            &batch,
        )?;

        assert!(
            metadata.compressed_size > 0,
            "Compressed size should be greater than 0"
        );

        // Reopen files for reading
        let mut event_batch_reader = File::open(&event_batch_path)?;
        let mut metadata_reader = File::open(&metadata_path)?;

        let corruption = engine.detect_corruption(&mut event_batch_reader, &mut metadata_reader)?;
        assert!(
            corruption.is_none(),
            "Should not detect any corruption in the written files"
        );

        let last_server_id = engine.last_event_batch_index(&mut metadata_reader)?;
        assert_eq!(last_server_id, 2, "Last server ID should be 2");

        let last_local_index = engine.last_local_index(&mut metadata_reader)?;
        assert_eq!(last_local_index, 4, "Last local index should be 4");

        // Read with simple filter using the trait method
        let filters = ReadFilters::new(2); // Start from server_id 1

        let read_result =
            engine.read_filtered(&mut event_batch_reader, &mut metadata_reader, &filters)?;

        // Verify results
        assert_eq!(
            read_result.event_batches.len(),
            1,
            "Should read 1 event batch"
        );
        assert_eq!(
            read_result.next_event_batch_index, None,
            "Should have next_server_id of None"
        );

        let read_batch = &read_result.event_batches[0];
        assert_eq!(read_batch.event_batch_index, 2, "Server ID should match");
        assert_eq!(read_batch.client_id, 123456789, "Client ID should match");
        assert_eq!(read_batch.client_id, 123456789, "Client ID should match");
        assert_eq!(read_batch.user_id, Some(987654321), "User ID should match");
        assert_eq!(read_batch.events.len(), 2, "Should have 2 events");

        // Check individual events
        let read_event1 = &read_batch.events[0];
        assert_eq!(
            read_event1.client_event_index, 3,
            "Event 1 local_index should match"
        );
        assert_eq!(
            read_event1.event_type_major, 42,
            "Event 1 type should match"
        );

        let read_event2 = &read_batch.events[1];
        assert_eq!(
            read_event2.client_event_index, 4,
            "Event 2 local_index should match"
        );

        let filters = ReadFilters::new(3); // Start from server_id 1

        let read_result =
            engine.read_filtered(&mut event_batch_reader, &mut metadata_reader, &filters)?;

        assert_eq!(
            read_result.event_batches.len(),
            0,
            "Should read 0 event batches"
        );
        assert_eq!(
            read_result.next_event_batch_index, None,
            "Should have next_server_id of None"
        );

        Ok(())
    }

    #[test]
    fn test_metadata_at_event_batch_index() -> io::Result<()> {
        // Create a temporary directory for our test files
        let temp_dir = tempdir()?;
        let event_batch_path = temp_dir.path().join("event_batches.bin");
        let metadata_path = temp_dir.path().join("metadata.bin");

        // Create the stateless engine
        let engine = StatelessEngine::builder().build();

        // Create test data for multiple batches
        let event1 = EventItem::new(1, 1, 1000, 42, 1, b"test event data 1".to_vec());
        let batch1 = EventBatchItem::new(
            5,               // event_batch_index
            1600000000000,   // server_time
            123456789,       // client_id
            Some(987654321), // user_id
            vec![event1],    // events
        );

        let event2 = EventItem::new(2, 2, 1050, 43, 1, b"test event data 2".to_vec());
        let batch2 = EventBatchItem::new(
            6,               // event_batch_index
            1600000001000,   // server_time
            123456790,       // client_id
            Some(987654322), // user_id
            vec![event2],    // events
        );

        // Setup file writers
        let mut event_batch_writer = File::create(&event_batch_path)?;
        let mut metadata_writer = File::create(&metadata_path)?;

        // Setup bloom filter and dedup set
        let mut bloom_filter = BloomFilter::with_num_bits(BLOOM_BYTES * 8)
            .seed(&BLOOM_HASH_SEED)
            .hashes(BLOOM_HASH_COUNT);
        let mut event_type_dedup = HashSet::new();

        // Write both batches
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

        // Reopen metadata file for reading
        let mut metadata_reader = File::open(&metadata_path)?;

        // Test retrieving metadata for existing event_batch_index
        let metadata_5 = engine.metadata_at_event_batch_index(&mut metadata_reader, 5)?;
        assert!(
            metadata_5.is_some(),
            "Should find metadata for event_batch_index 5"
        );
        let metadata_5 = metadata_5.unwrap();
        assert_eq!(
            metadata_5.event_batch_index, 5,
            "Event batch index should match"
        );
        assert_eq!(metadata_5.client_id, 123456789, "Client ID should match");

        let metadata_6 = engine.metadata_at_event_batch_index(&mut metadata_reader, 6)?;
        assert!(
            metadata_6.is_some(),
            "Should find metadata for event_batch_index 6"
        );
        let metadata_6 = metadata_6.unwrap();
        assert_eq!(
            metadata_6.event_batch_index, 6,
            "Event batch index should match"
        );
        assert_eq!(metadata_6.client_id, 123456790, "Client ID should match");

        // Test retrieving metadata for non-existing event_batch_index (before range)
        let metadata_3 = engine.metadata_at_event_batch_index(&mut metadata_reader, 3)?;
        assert!(
            metadata_3.is_none(),
            "Should not find metadata for event_batch_index 3"
        );

        // Test retrieving metadata for non-existing event_batch_index (after range)
        let metadata_10 = engine.metadata_at_event_batch_index(&mut metadata_reader, 10)?;
        assert!(
            metadata_10.is_none(),
            "Should not find metadata for event_batch_index 10"
        );

        // Test retrieving metadata for gap in sequence
        let metadata_7 = engine.metadata_at_event_batch_index(&mut metadata_reader, 7)?;
        assert!(
            metadata_7.is_none(),
            "Should not find metadata for event_batch_index 7 (gap)"
        );

        Ok(())
    }
}
