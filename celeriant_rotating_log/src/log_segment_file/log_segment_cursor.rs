use celeriant_wal::{
    constants::EntryHashBytes,
    shard_log_header::{HeaderCursor, ShardLogHeader},
};

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
    pub fn from_shard_log_header_write(log_id: u64, header: &ShardLogHeader) -> Self {
        Self::from_header_cursor(log_id, &header.write, &header.aggregate_bloom)
    }

    pub fn from_shard_log_header_read(log_id: u64, header: &ShardLogHeader) -> Self {
        // Read range is always a subset of write, so the write bloom is a valid filter.
        Self::from_header_cursor(log_id, &header.read, &header.aggregate_bloom)
    }

    fn from_header_cursor(log_id: u64, cursor: &HeaderCursor, bloom: &[u64]) -> Self {
        Self {
            log_id,
            metablocks_position: cursor.metablocks_position,
            datablocks_position: cursor.datablocks_position,
            wal_seq: cursor.wal_seq,
            aggregate_key_bloom: AggregateKeyBloom::from_bytes(bloom),
            tip_hash: cursor.tip_hash,
        }
    }

    fn to_header_cursor(&self) -> HeaderCursor {
        HeaderCursor {
            metablocks_position: self.metablocks_position,
            datablocks_position: self.datablocks_position,
            wal_seq: self.wal_seq,
            tip_hash: self.tip_hash,
        }
    }

    pub fn to_shard_log_header(
        &self,
        read: Option<&LogSegmentCursor>,
        last_received_replication_wal_seq: u64,
        last_self_acked_wal_seq: u64,
    ) -> ShardLogHeader {
        ShardLogHeader {
            write: self.to_header_cursor(),
            aggregate_bloom: self.aggregate_key_bloom.to_bytes(),
            last_received_replication_wal_seq,
            last_self_acked_wal_seq,
            // Zero sentinel: read has not advanced to this segment yet (post-rotation).
            read: read.map(LogSegmentCursor::to_header_cursor).unwrap_or_else(HeaderCursor::genesis),
        }
    }
}
