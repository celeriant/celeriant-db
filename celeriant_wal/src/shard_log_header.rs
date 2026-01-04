
use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;

use crate::constants::{AGGREGATE_BLOOM_BYTES, FIXED_BLOCK_SIZE_BYTES};

/// The header is written at the start and end of the 1GB fixed size file
/// Writing both, protected by crc checks, allows recovery on torn writes
#[derive(Debug, Clone, Encode, Decode, DeepSizeOf)]
pub struct ShardLogHeader {
    /// A metablock is 512 byte fixed size, written from the start of the file
    /// This position indicates the end of the last written metablock entry
    pub metablocks_position: u64,

    /// The position where new variable length payloads can be written to
    /// Note that event batches are written to end of the file
    /// so this position indicates the start of the most recently written batches
    pub datablocks_position: u64,

    /// Shard-global WAL index representing the last written metablock
    pub wal_index: u64,

    /// Bloom filter for aggregate keys written to this log segment.
    /// Used to quickly skip log segments during aggregate existence checks.
    /// A "definitely not in set" result means no metablocks for that aggregate exist.
    pub aggregate_bloom: [u64; AGGREGATE_BLOOM_BYTES / 8],
}

impl ShardLogHeader {
    // Wire format layout (bincode fixed-int encoding)
    // Update these if field order or types change!

    const WIRE_SIZE_METABLOCKS_POSITION: usize = 8;
    const WIRE_SIZE_DATABLOCKS_POSITION: usize = 8;
    const WIRE_SIZE_WAL_INDEX: usize = 8;
    const WIRE_SIZE_AGGREGATE_BLOOM: usize = AGGREGATE_BLOOM_BYTES;

    pub const OFFSET_METABLOCKS_POSITION: usize = 0;

    pub const OFFSET_DATABLOCKS_POSITION: usize = 
        Self::OFFSET_METABLOCKS_POSITION + Self::WIRE_SIZE_METABLOCKS_POSITION;

    pub const OFFSET_WAL_INDEX: usize = 
        Self::OFFSET_DATABLOCKS_POSITION + Self::WIRE_SIZE_DATABLOCKS_POSITION;

    pub const OFFSET_AGGREGATE_BLOOM: usize =
        Self::OFFSET_WAL_INDEX + Self::WIRE_SIZE_WAL_INDEX;

    /// Total wire size of ShardLogHeader
    pub const WIRE_SIZE_TOTAL: usize = 
        Self::OFFSET_AGGREGATE_BLOOM + Self::WIRE_SIZE_AGGREGATE_BLOOM; // = 152 bytes
        
    pub fn new(file_len: u64) -> Self {
        Self {
            metablocks_position: FIXED_BLOCK_SIZE_BYTES as u64,
            datablocks_position: file_len.saturating_sub(FIXED_BLOCK_SIZE_BYTES as u64),
            wal_index: 0,
            aggregate_bloom: [0u64; AGGREGATE_BLOOM_BYTES / 8],
        }
    }

    pub fn available_space(&self) -> u64 {
        self.datablocks_position.saturating_sub(self.metablocks_position)
    }

    pub fn has_space_for(&self, metablock_size: u64, datablock_size: u64) -> bool {
        self.available_space() >= metablock_size.saturating_add(datablock_size)
    }

    pub fn append_event_batches(
        &mut self,
        metablock_size: u64,
        datablock_size: u64,
    ) {
        self.metablocks_position = self
            .metablocks_position
            .saturating_add(metablock_size);
        self.datablocks_position = self
            .datablocks_position
            .saturating_sub(datablock_size);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_FILE_LEN: u64 = 1024 * 1024 * 1024; // 1GB

    #[test]
    fn test_new_initializes_positions_correctly() {
        let header = ShardLogHeader::new(TEST_FILE_LEN);

        assert_eq!(header.metablocks_position, FIXED_BLOCK_SIZE_BYTES as u64);
        assert_eq!(
            header.datablocks_position,
            TEST_FILE_LEN - FIXED_BLOCK_SIZE_BYTES as u64
        );
    }

    #[test]
    fn test_new_handles_small_file_len() {
        let small_file_len = (FIXED_BLOCK_SIZE_BYTES / 2) as u64;
        let header = ShardLogHeader::new(small_file_len);

        assert_eq!(header.metablocks_position, FIXED_BLOCK_SIZE_BYTES as u64);
        assert_eq!(header.datablocks_position, 0); // saturating_sub prevents underflow
    }

    #[test]
    fn test_available_space() {
        let header = ShardLogHeader::new(TEST_FILE_LEN);
        let expected = TEST_FILE_LEN - 2 * FIXED_BLOCK_SIZE_BYTES as u64;

        assert_eq!(header.available_space(), expected);
    }

    #[test]
    fn test_available_space_when_positions_overlap() {
        let header = ShardLogHeader {
            metablocks_position: 1000,
            datablocks_position: 500,
            wal_index: 0,
            aggregate_bloom: [0u64; AGGREGATE_BLOOM_BYTES / 8],
        };

        assert_eq!(header.available_space(), 0); // saturating_sub prevents underflow
    }

    #[test]
    fn test_has_space_for_returns_true_when_sufficient() {
        let header = ShardLogHeader::new(TEST_FILE_LEN);
        let available = header.available_space();

        assert!(header.has_space_for(100, 100));
        assert!(header.has_space_for(available / 2, available / 2));
        assert!(header.has_space_for(available, 0));
        assert!(header.has_space_for(0, available));
    }

    #[test]
    fn test_has_space_for_returns_false_when_insufficient() {
        let header = ShardLogHeader::new(TEST_FILE_LEN);
        let available = header.available_space();

        assert!(!header.has_space_for(available, 1));
        assert!(!header.has_space_for(1, available));
        assert!(!header.has_space_for(available / 2 + 1, available / 2 + 1));
    }

    #[test]
    fn test_append_event_batches_updates_positions() {
        let mut header = ShardLogHeader::new(TEST_FILE_LEN);
        let initial_meta = header.metablocks_position;
        let initial_data = header.datablocks_position;

        header.append_event_batches(512, 1024);

        assert_eq!(header.metablocks_position, initial_meta + 512);
        assert_eq!(header.datablocks_position, initial_data - 1024);
    }

    #[test]
    fn test_append_event_batches_multiple_times() {
        let mut header = ShardLogHeader::new(TEST_FILE_LEN);

        header.append_event_batches(100, 200);
        header.append_event_batches(150, 300);

        assert_eq!(
            header.metablocks_position,
            FIXED_BLOCK_SIZE_BYTES as u64 + 250
        );
        assert_eq!(
            header.datablocks_position,
            TEST_FILE_LEN - FIXED_BLOCK_SIZE_BYTES as u64 - 500
        );
    }

    #[test]
    fn test_append_reduces_available_space() {
        let mut header = ShardLogHeader::new(TEST_FILE_LEN);
        let initial_space = header.available_space();

        header.append_event_batches(100, 200);

        assert_eq!(header.available_space(), initial_space - 300);
    }
}