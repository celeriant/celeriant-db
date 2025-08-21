pub mod serde_option_u128_base64;
pub mod serde_u128_base64;
pub mod stateless;
pub mod structures;

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stateless::{read, write};
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
    use std::io::{self, Read, Seek, SeekFrom};
    use tempfile::tempdir;

    #[test]
    fn test_basic_write_read_flow() -> io::Result<()> {
        // Create a temporary directory for our test files
        let temp_dir = tempdir()?;
        let event_batch_path = temp_dir.path().join("event_batches.bin");
        let metadata_path = temp_dir.path().join("metadata.bin");
        
        // Create test data
        let event1 = EventItem::new(
            1, // local_index
            1000, // event_time
            42, // event_type_major
            1, // event_type_minor
            b"test event data 1".to_vec(), // value
        );
        
        let event2 = EventItem::new(
            2,
            1050,
            42,
            1,
            b"test event data 2".to_vec(),
        );
        
        let batch = EventBatchItem::new(
            1, // server_id
            1600000000000, // server_time
            123456789, // client_id
            Some(987654321), // user_id
            vec![event1, event2], // events
        );
        
        // Setup file writers
        let mut event_batch_writer = File::create(&event_batch_path)?;
        let mut metadata_writer = File::create(&metadata_path)?;
        
        // Setup bloom filter with proper constants
        let mut bloom_filter = BloomFilter::with_num_bits(BLOOM_BYTES * 8).hashes(BLOOM_HASH_COUNT);
        let mut event_type_dedup = HashSet::new();
        
        // Write the batch
        let compression_type = CompressionType::Zstd { level: 3 };
        let metadata = write::append_event_batch(
            &mut event_batch_writer,
            &mut metadata_writer,
            &mut bloom_filter,
            &mut event_type_dedup,
            compression_type,
            &batch,
        )?;
        
        assert!(metadata.compressed_size > 0, "Compressed size should be greater than 0");
        
        // Reopen files for reading
        let mut event_batch_reader = File::open(&event_batch_path)?;
        let mut metadata_reader = File::open(&metadata_path)?;
        
        // Read with simple filter
        let filters = ReadFilters::new(1); // Start from server_id 1
        
        let read_result = read::filtered_read(
            &mut event_batch_reader,
            &mut metadata_reader,
            &filters,
        )?;
        
        // Verify results
        assert_eq!(read_result.event_batches.len(), 1, "Should read 1 event batch");
        assert_eq!(read_result.next_server_id, None, "Should not have next_server_id");
        
        let read_batch = &read_result.event_batches[0];
        assert_eq!(read_batch.server_id, 1, "Server ID should match");
        assert_eq!(read_batch.client_id, 123456789, "Client ID should match");
        assert_eq!(read_batch.user_id, Some(987654321), "User ID should match");
        assert_eq!(read_batch.events.len(), 2, "Should have 2 events");
        
        // Check individual events
        let read_event1 = &read_batch.events[0];
        assert_eq!(read_event1.local_index, 1, "Event 1 local_index should match");
        assert_eq!(read_event1.event_type_major, 42, "Event 1 type should match");
        
        let read_event2 = &read_batch.events[1];
        assert_eq!(read_event2.local_index, 2, "Event 2 local_index should match");
        
        Ok(())
    }

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}