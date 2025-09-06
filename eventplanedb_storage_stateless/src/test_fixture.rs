#[cfg(test)]
pub mod tests {
    // Platform-specific raw file descriptor traits
    #[cfg(unix)]
    use std::os::fd::AsRawFd;
    #[cfg(windows)]
    use std::os::windows::io::AsRawHandle;

    use eventplanedb_storage_structures::constants::BLOOM_HASH_SEED;
    use eventplanedb_storage_structures::event_batch_metadata::EventBatchMetadata;
    #[cfg(windows)]
    use eventplanedb_storage_structures::read_result::ReadResult;
    use eventplanedb_storage_structures::{
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
    use std::io::{Read, Seek, Write};
    use tempfile::tempdir;

    use crate::{
        stateless_destructive::StatelessDestructive,
        stateless_engine::StatelessEngine,
        stateless_reader::{CorruptPositions, StatelessReader},
        stateless_writer::StatelessWriter,
    };

    pub struct TestFixture {
        pub _temp_dir: tempfile::TempDir, // Keep temp dir alive
        event_batch_path: std::path::PathBuf,
        metadata_path: std::path::PathBuf,
        pub engine: StatelessEngine,
        bloom_filter: BloomFilter,
        event_type_dedup: HashSet<u64>,
        pub compression_type: CompressionType,
    }

    impl TestFixture {
        pub fn new() -> io::Result<Self> {
            let temp_dir = tempdir()?;
            let event_batch_path = temp_dir.path().join("event_batches.bin");
            let metadata_path = temp_dir.path().join("metadata.bin");

            let engine = StatelessEngine::builder().build();
            let bloom_filter = BloomFilter::with_num_bits(BLOOM_BYTES * 8)
                .seed(&BLOOM_HASH_SEED)
                .hashes(BLOOM_HASH_COUNT);
            let event_type_dedup = HashSet::new();
            let compression_type = CompressionType::Zstd { level: 3 };

            Ok(Self {
                _temp_dir: temp_dir,
                event_batch_path,
                metadata_path,
                engine,
                bloom_filter,
                event_type_dedup,
                compression_type,
            })
        }

        pub fn with_compression(mut self, compression_type: CompressionType) -> Self {
            self.compression_type = compression_type;
            self
        }

        pub fn write_batch_with_dedup(
            &mut self,
            event_batch_writer: &mut impl Write,
            metadata_writer: &mut impl Write,
            event_type_dedup: &mut HashSet<u64>,
            batch: &EventBatchItem,
        ) -> io::Result<EventBatchMetadata> {
            let engine = StatelessEngine::builder().build();

            let mut bloom_filter = BloomFilter::with_num_bits(BLOOM_BYTES * 8)
                .seed(&BLOOM_HASH_SEED)
                .hashes(BLOOM_HASH_COUNT);

            let compression_type = CompressionType::Zstd { level: 3 };

            let metadata = engine.append_event_batch(
                event_batch_writer,
                metadata_writer,
                &mut bloom_filter,
                event_type_dedup,
                compression_type,
                batch,
            )?;

            Ok(metadata)
        }

        pub fn create_writers(&self) -> io::Result<(File, File)> {
            let event_batch_writer = File::create(&self.event_batch_path)?;
            let metadata_writer = File::create(&self.metadata_path)?;
            Ok((event_batch_writer, metadata_writer))
        }

        pub fn create_readers(&self) -> io::Result<(File, File)> {
            let event_batch_reader = File::open(&self.event_batch_path)?;
            let metadata_reader = File::open(&self.metadata_path)?;
            Ok((event_batch_reader, metadata_reader))
        }

        pub fn write_batch(
            &mut self,
            event_batch_writer: &mut impl Write,
            metadata_writer: &mut impl Write,
            batch: &EventBatchItem,
        ) -> io::Result<EventBatchMetadata> {
            self.engine.append_event_batch(
                event_batch_writer,
                metadata_writer,
                &mut self.bloom_filter,
                &mut self.event_type_dedup,
                self.compression_type,
                batch,
            )
        }

        pub fn write_batches(
            &mut self,
            event_batch_writer: &mut impl Write,
            metadata_writer: &mut impl Write,
            batches: &[EventBatchItem],
        ) -> io::Result<Vec<EventBatchMetadata>> {
            let mut results = Vec::new();
            for batch in batches {
                let metadata = self.write_batch(event_batch_writer, metadata_writer, batch)?;
                results.push(metadata);
            }
            Ok(results)
        }

        #[cfg(windows)]
        pub fn read_filtered<R: Read + Seek + AsRawHandle>(
            &self,
            event_batch_reader: &mut R,
            metadata_reader: &mut R,
            filters: &ReadFilters,
        ) -> io::Result<ReadResult> {
            use crate::stateless_reader::StatelessReader;

            self.engine
                .read_filtered(event_batch_reader, metadata_reader, filters)
        }

        #[cfg(unix)]
        pub fn read_filtered<R: Read + Seek + AsRawFd>(
            &self,
            event_batch_reader: &mut R,
            metadata_reader: &mut R,
            filters: &ReadFilters,
        ) -> io::Result<crate::structures::read_result::ReadResult> {
            self.engine
                .read_filtered(event_batch_reader, metadata_reader, filters)
        }

        pub fn detect_corruption<R: Read + Seek>(
            &self,
            event_batch_reader: &mut R,
            metadata_reader: &mut R,
        ) -> io::Result<Option<CorruptPositions>> {
            self.engine
                .detect_corruption(event_batch_reader, metadata_reader)
        }

        pub fn last_event_batch_index<R: Read + Seek>(
            &self,
            metadata_reader: &mut R,
        ) -> io::Result<u64> {
            self.engine.last_event_batch_index(metadata_reader)
        }

        pub fn trim_start<R: Read + Seek>(
            &self,
            event_batch_reader: &mut R,
            event_batch_keep_from_position: u64,
            metadata_reader: &mut R,
            metadata_keep_from_position: u64,
        ) -> io::Result<()> {
            self.engine.trim_start(
                event_batch_reader,
                event_batch_keep_from_position,
                self.event_batch_path.to_str().unwrap(),
                metadata_reader,
                metadata_keep_from_position,
                self.metadata_path.to_str().unwrap(),
            )
        }

        // Helper methods for creating common test data
        pub fn create_simple_event(&self, index: u64) -> EventItem {
            EventItem::new(index, index, 1000, 1, 1, b"test event".to_vec())
        }

        pub fn create_event_with_type(&self, index: u64, event_type: u64) -> EventItem {
            EventItem::new(
                index,
                index,
                1000 + index,
                event_type,
                1,
                b"test event".to_vec(),
            )
        }

        pub fn create_event_with_timestamp(&self, index: u64, timestamp: u64) -> EventItem {
            EventItem::new(index, index, timestamp, 1, 1, b"test event".to_vec())
        }

        pub fn create_event_with_data(&self, index: u64, data: Vec<u8>) -> EventItem {
            EventItem::new(index, index, 1000, 1, 1, data)
        }

        pub fn create_simple_batch(
            &self,
            batch_index: u64,
            events: Vec<EventItem>,
        ) -> EventBatchItem {
            EventBatchItem::new(
                batch_index,
                1600000000000,
                123456789,
                Some(987654321),
                events,
            )
        }

        pub fn create_batch_with_timestamp(
            &self,
            batch_index: u64,
            timestamp: u64,
            events: Vec<EventItem>,
        ) -> EventBatchItem {
            EventBatchItem::new(batch_index, timestamp, 123456789, Some(987654321), events)
        }

        pub fn create_batch_with_client_id(
            &self,
            batch_index: u64,
            client_id: u128,
            events: Vec<EventItem>,
        ) -> EventBatchItem {
            EventBatchItem::new(batch_index, 1600000000000, client_id, None, events)
        }

        pub fn create_batch_with_user_id(
            &self,
            batch_index: u64,
            user_id: Option<u128>,
            events: Vec<EventItem>,
        ) -> EventBatchItem {
            EventBatchItem::new(batch_index, 1600000000000, 123456789, user_id, events)
        }

        pub fn create_multiple_events(&self, count: usize) -> Vec<EventItem> {
            (1..=count)
                .map(|i| self.create_simple_event(i as u64))
                .collect()
        }

        pub fn create_events_with_types(&self, types: &[u64]) -> Vec<EventItem> {
            types
                .iter()
                .enumerate()
                .map(|(i, &event_type)| self.create_event_with_type((i + 1) as u64, event_type))
                .collect()
        }

        // Convenience method for full write-then-read cycle
        pub fn write_and_read(
            &mut self,
            batches: &[EventBatchItem],
            filters: &ReadFilters,
        ) -> io::Result<ReadResult> {
            let (mut event_batch_writer, mut metadata_writer) = self.create_writers()?;
            self.write_batches(&mut event_batch_writer, &mut metadata_writer, batches)?;

            let (mut event_batch_reader, mut metadata_reader) = self.create_readers()?;
            self.read_filtered(&mut event_batch_reader, &mut metadata_reader, filters)
        }

        pub fn generate_test_batches(&self) -> Vec<EventBatchItem> {
            let mut batches = Vec::new();

            // Batch 1: Basic batch with simple events
            let events_batch_1 = self.create_multiple_events(5);
            let batch_1 = self.create_simple_batch(1, events_batch_1);
            batches.push(batch_1);

            // Batch 2: Batch with different event types
            let events_batch_2 = self.create_events_with_types(&[10, 20, 30, 40, 50]);
            let batch_2 = self.create_simple_batch(2, events_batch_2);
            batches.push(batch_2);

            // Batch 3: Batch with specific client and user IDs
            let events_batch_3 = self.create_multiple_events(3);
            let batch_3 = self.create_batch_with_client_id(3, 12345, events_batch_3);
            batches.push(batch_3);

            // Batch 4: Batch with a user ID
            let events_batch_4 = self.create_multiple_events(4);
            let batch_4 = self.create_batch_with_user_id(4, Some(67890), events_batch_4);
            batches.push(batch_4);

            // Batch 5: Batch with timestamps
            let events_batch_5 = self.create_multiple_events(2);
            let batch_5 = self.create_batch_with_timestamp(5, 1650000000000, events_batch_5);
            batches.push(batch_5);

            // Batch 6: Batch with mixed event types to test bloom filter
            let events_batch_6 = self.create_events_with_types(&[10, 20, 30, 40, 50, 60, 70, 80]);
            let batch_6 = self.create_simple_batch(6, events_batch_6);
            batches.push(batch_6);

            // Batch 7: Batch with events with specific timestamps
            let mut events_batch_7 = Vec::new();
            events_batch_7.push(self.create_event_with_timestamp(1, 1630000000000));
            events_batch_7.push(self.create_event_with_timestamp(2, 1640000000000));
            events_batch_7.push(self.create_event_with_timestamp(3, 1660000000000));
            events_batch_7.push(self.create_event_with_timestamp(4, 1670000000000));
            let mut batch_7 = self.create_batch_with_timestamp(7, 1650000000000, events_batch_7);
            batches.push(batch_7);

            // Batch 8: Batch with events with different client_event_index
            let mut events_batch_8 = Vec::new();
            events_batch_8.push(self.create_simple_event(1));
            events_batch_8.push(self.create_simple_event(2));
            let batch_8 = self.create_batch_with_timestamp(8, 1660000000000, events_batch_8);
            batches.push(batch_8);

            batches
        }
    }
}
