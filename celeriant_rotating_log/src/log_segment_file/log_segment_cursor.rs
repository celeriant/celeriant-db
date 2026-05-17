use celeriant_wal::{constants::EntryHashBytes, shard_log_header::ShardLogHeader};

use crate::log_segment_file::aggregate_key_bloom::AggregateKeyBloom;

/// Cursor tracking positions within a log segment file.
#[derive(Clone, Default)]
pub struct LogSegmentCursor {
    pub log_id: u64,
    /// End of the last metablock entry (metablocks grow from start of file)
    pub metablocks_position: u64,
    /// Start of the most recent datablocks (datablocks grow from end of file)
    pub datablocks_position: u64,
    /// Shard-global WAL sequence of the last entry at this cursor
    pub wal_seq: u64,
    /// Bloom filter state at this cursor
    pub aggregate_key_bloom: AggregateKeyBloom,
    /// blake3 hash chain up to latest written metablock for verification in distributed env
    pub tip_hash: EntryHashBytes,
}

impl LogSegmentCursor {
    pub fn from_shard_log_header(log_id: u64, header: &ShardLogHeader) -> Self {
        Self {
            log_id: log_id,
            metablocks_position: header.metablocks_position,
            datablocks_position: header.datablocks_position,
            wal_seq: header.wal_seq,
            aggregate_key_bloom: AggregateKeyBloom::from_bytes(&header.aggregate_bloom),
            tip_hash: header.tip_hash,
        }
    }

    pub fn to_shard_log_header(&self, last_received_replication_wal_seq: u64) -> ShardLogHeader {
        ShardLogHeader {
            metablocks_position: self.metablocks_position,
            datablocks_position: self.datablocks_position,
            wal_seq: self.wal_seq,
            aggregate_bloom: self.aggregate_key_bloom.to_bytes(),
            tip_hash: self.tip_hash,
            last_received_replication_wal_seq,
        }
    }
}