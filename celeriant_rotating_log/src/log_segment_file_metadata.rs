use celeriant_wal::shard_log_header::ShardLogHeader;

use crate::aggregate_key_bloom::AggregateKeyBloom;


#[derive(Clone)]
pub struct LogSegmentFileMetadata {
    /// One based incremented log id
    pub log_id: u64,

    /// Size of log file, typically fixed but could be smaller if log is being truncated
    pub file_len: u64,

    /// A metablock is 512 byte fixed size, written from the start of the file
    /// This position indicates the end of the last written metablock entry
    pub metablocks_position: u64,

    /// The position where new variable length payloads can be written to
    /// Note that event batches are written to end of the file
    /// so this position indicates the start of the most recently written batches
    pub datablocks_position: u64,

    /// Shard-global WAL index representing the last written metablock
    pub wal_index: u64,

    pub datablocks_carry_over: Option<Vec<u8>>,

    pub aggregate_key_bloom: AggregateKeyBloom,
}

impl LogSegmentFileMetadata {

    pub fn available_space(&self) -> u64 {
        self.datablocks_position.saturating_sub(self.metablocks_position)
    }

    pub fn new(log_id: u64, file_len: u64, datablocks_carry_over: Option<Vec<u8>>, shard_log_header: &ShardLogHeader) -> Self {
        let aggregate_key_bloom = AggregateKeyBloom::from_bytes(&shard_log_header.aggregate_bloom);

        LogSegmentFileMetadata {
            log_id,
            file_len,
            metablocks_position: shard_log_header.metablocks_position,
            datablocks_position: shard_log_header.datablocks_position,
            wal_index: shard_log_header.datablocks_position,
            aggregate_key_bloom,
            datablocks_carry_over,
        }
    }

    /// Convert this metadata into a WAL `ShardLogHeader` suitable for writing to disk.
    #[must_use]
    pub fn to_shard_log_header(&self) -> ShardLogHeader {
        ShardLogHeader {
            metablocks_position: self.metablocks_position,
            datablocks_position: self.datablocks_position,
            wal_index: self.wal_index,
            aggregate_bloom: self.aggregate_key_bloom.to_bytes(),
        }
    }
}