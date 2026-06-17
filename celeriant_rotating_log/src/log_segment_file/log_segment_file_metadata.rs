#[cfg(test)]
use celeriant_wal::shard_log_header::HeaderCursor;
use celeriant_wal::{constants::HEADER_BLOCK_SIZE_BYTES, shard_log_header::ShardLogHeader};

use crate::log_segment_file::log_segment_cursor::LogSegmentCursor;

#[derive(Clone)]
pub struct LogSegmentFileMetadata {
    /// One based incremented log id
    pub log_id: u64,
    /// Size of log file, typically fixed but could be smaller if log is being truncated
    pub file_len: u64,
    /// Writer's view - most recent writes (not yet replicated)
    pub write: LogSegmentCursor,
    /// Reader's view: replicated and visible. `None` until first replication advances
    /// into this segment (post-rotation gap). Zero in the persisted header maps to None.
    pub read: Option<LogSegmentCursor>,
    /// Used in write path as datablock file writes are not Direct I/O aligned
    pub datablocks_carry_over: Option<Vec<u8>>,
    /// Promotion-batch floor: `leader_confirmed_wal_seq + 1` from the highest-confirmed
    /// batch received via TCP replication while follower (monotonic max). On promotion to
    /// leader, entries from this index onward are uploaded to S3.
    pub last_received_replication_wal_seq: u64,
    /// See ShardLogHeader::last_self_acked_wal_seq.
    pub last_self_acked_wal_seq: u64,
}

impl LogSegmentFileMetadata {
    pub fn available_space(&self) -> u64 {
        self.write.datablocks_position.saturating_sub(self.write.metablocks_position)
    }

    /// `advance_read=true` restores read from the header (None if zero sentinel).
    /// `advance_read=false` forces read=None; used during rotation for a fresh segment.
    pub fn new(log_id: u64, file_len: u64, datablocks_carry_over: Option<Vec<u8>>, shard_log_header: &ShardLogHeader, advance_read: bool) -> Self {
        let write = LogSegmentCursor::from_shard_log_header_write(log_id, shard_log_header);
        // Read shares the write bloom (same persisted bytes; superset is valid for reads) so a
        // segment load allocates ONE bloom pair, not two.
        let read = if advance_read && shard_log_header.read.metablocks_position >= HEADER_BLOCK_SIZE_BYTES as u64 {
            Some(write.read_snapshot_sharing_bloom(&shard_log_header.read))
        } else {
            None
        };
        LogSegmentFileMetadata {
            log_id,
            file_len,
            write,
            read,
            datablocks_carry_over,
            last_received_replication_wal_seq: shard_log_header.last_received_replication_wal_seq,
            last_self_acked_wal_seq: shard_log_header.last_self_acked_wal_seq,
        }
    }

    #[must_use]
    pub fn to_shard_log_header(&self) -> ShardLogHeader {
        self.write
            .to_shard_log_header(self.read.as_ref(), self.last_received_replication_wal_seq, self.last_self_acked_wal_seq)
    }

    /// Advance visible position after successful replication (write -> read)
    pub fn advance_visible_position(&mut self) {
        self.read = Some(self.write.clone());
    }

    /// Returns true if write cursor is ahead of read cursor (pending replication)
    pub fn is_pending_advance(&self) -> bool {
        self.read.is_none() || self.write.wal_seq > self.read.as_ref().unwrap().wal_seq
    }

    /// Returns the end of the readable metablock region.
    /// Uses the read cursor if available (fully replicated), otherwise falls back to the write
    /// cursor (segment opened but not yet advanced). This is the correct upper bound for any
    /// forward or reverse scan of committed data.
    pub fn readable_metablocks_end(&self) -> u64 {
        match &self.read {
            Some(r) => r.metablocks_position,
            None => self.write.metablocks_position,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log_segment_file::aggregate_key_bloom::AggregateKeyBloom;
    use celeriant_wal::constants::GENESIS_HASH;

    fn make_header(meta_pos: u64, data_pos: u64, wal_idx: u64) -> ShardLogHeader {
        ShardLogHeader {
            write: HeaderCursor {
                metablocks_position: meta_pos,
                datablocks_position: data_pos,
                wal_seq: wal_idx,
                tip_hash: [0u8; 32],
            },
            aggregate_bloom: AggregateKeyBloom::new().to_bytes(),
            client_bloom: AggregateKeyBloom::new().to_bytes(),
            last_received_replication_wal_seq: 0,
            last_self_acked_wal_seq: 0,
            // read defaults to zero sentinel (not advanced)
            read: HeaderCursor::genesis(),
        }
    }

    fn make_header_with_read(meta_pos: u64, data_pos: u64, wal_idx: u64, read_meta: u64, read_data: u64, read_wal: u64) -> ShardLogHeader {
        ShardLogHeader {
            write: HeaderCursor {
                metablocks_position: meta_pos,
                datablocks_position: data_pos,
                wal_seq: wal_idx,
                tip_hash: [0u8; 32],
            },
            aggregate_bloom: AggregateKeyBloom::new().to_bytes(),
            client_bloom: AggregateKeyBloom::new().to_bytes(),
            last_received_replication_wal_seq: 0,
            last_self_acked_wal_seq: 0,
            read: HeaderCursor {
                metablocks_position: read_meta,
                datablocks_position: read_data,
                wal_seq: read_wal,
                tip_hash: GENESIS_HASH,
            },
        }
    }

    #[test]
    fn available_space_basic() {
        let meta = LogSegmentFileMetadata::new(1, 1000, None, &make_header(100, 900, 0), false);
        assert_eq!(meta.available_space(), 800);
    }

    #[test]
    fn available_space_saturates() {
        let meta = LogSegmentFileMetadata::new(1, 1000, None, &make_header(500, 400, 0), false);
        assert_eq!(meta.available_space(), 0);
    }

    #[test]
    fn new_without_advance_read() {
        let meta = LogSegmentFileMetadata::new(5, 2000, None, &make_header(50, 1950, 10), false);
        assert_eq!(meta.log_id, 5);
        assert_eq!(meta.file_len, 2000);
        assert!(meta.read.is_none());
        assert_eq!(meta.write.wal_seq, 10);
    }

    #[test]
    fn new_with_advance_read_but_sentinel_gives_none() {
        // read_metablocks_position == 0 is the sentinel; advance_read=true still gives None
        let meta = LogSegmentFileMetadata::new(5, 2000, None, &make_header(50, 1950, 10), true);
        assert!(meta.read.is_none());
    }

    #[test]
    fn new_with_advance_read_and_valid_read_cursor() {
        // read_metablocks_position >= HEADER_BLOCK_SIZE_BYTES means read was advanced
        let header = make_header_with_read(50, 1950, 10, HEADER_BLOCK_SIZE_BYTES as u64, 1900, 5);
        let meta = LogSegmentFileMetadata::new(5, 2000, None, &header, true);
        assert!(meta.read.is_some());
        assert_eq!(meta.read.unwrap().wal_seq, 5);
        assert_eq!(meta.write.wal_seq, 10);
    }

    #[test]
    fn read_behind_write_preserved_on_load() {
        // Simulates restart after leader crash with write=10, read=5
        let header = make_header_with_read(
            HEADER_BLOCK_SIZE_BYTES as u64 + 10 * 512,
            4_000_000 - HEADER_BLOCK_SIZE_BYTES as u64,
            10,
            HEADER_BLOCK_SIZE_BYTES as u64 + 5 * 512,
            4_000_000 - HEADER_BLOCK_SIZE_BYTES as u64,
            5,
        );
        let meta = LogSegmentFileMetadata::new(1, 4_000_000, None, &header, true);
        assert_eq!(meta.write.wal_seq, 10);
        assert_eq!(meta.read.as_ref().unwrap().wal_seq, 5);
    }

    #[test]
    fn advance_visible_position_copies_write() {
        let header = make_header_with_read(HEADER_BLOCK_SIZE_BYTES as u64, 900, 5, HEADER_BLOCK_SIZE_BYTES as u64, 900, 5);
        let mut meta = LogSegmentFileMetadata::new(1, 1000, None, &header, true);
        meta.write.wal_seq = 15;
        meta.advance_visible_position();
        assert_eq!(meta.read.unwrap().wal_seq, 15);
    }

    #[test]
    fn is_pending_when_read_none() {
        let meta = LogSegmentFileMetadata::new(1, 1000, None, &make_header(100, 900, 5), false);
        assert!(meta.is_pending_advance());
    }

    #[test]
    fn is_pending_when_write_ahead() {
        let header = make_header_with_read(HEADER_BLOCK_SIZE_BYTES as u64, 900, 10, HEADER_BLOCK_SIZE_BYTES as u64, 900, 5);
        let mut meta = LogSegmentFileMetadata::new(1, 1000, None, &header, true);
        meta.write.wal_seq = 10;
        assert!(meta.is_pending_advance());
    }

    #[test]
    fn not_pending_when_synced() {
        let header = make_header_with_read(HEADER_BLOCK_SIZE_BYTES as u64, 900, 5, HEADER_BLOCK_SIZE_BYTES as u64, 900, 5);
        let meta = LogSegmentFileMetadata::new(1, 1000, None, &header, true);
        assert!(!meta.is_pending_advance());
    }

    #[test]
    fn to_shard_log_header_roundtrip_with_read_gap() {
        // write=10, read=5 should survive a header roundtrip
        let header = make_header_with_read(
            HEADER_BLOCK_SIZE_BYTES as u64 + 5120,
            900_000,
            10,
            HEADER_BLOCK_SIZE_BYTES as u64 + 2560,
            900_000,
            5,
        );
        let meta = LogSegmentFileMetadata::new(1, 4_000_000, None, &header, true);
        let serialized = meta.to_shard_log_header();
        let restored = LogSegmentFileMetadata::new(1, 4_000_000, None, &serialized, true);
        assert_eq!(restored.write.wal_seq, 10);
        assert_eq!(restored.read.as_ref().unwrap().wal_seq, 5);
    }

    #[test]
    fn to_shard_log_header_none_read_writes_sentinel() {
        let meta = LogSegmentFileMetadata::new(1, 1000, None, &make_header(100, 900, 5), false);
        let serialized = meta.to_shard_log_header();
        assert_eq!(serialized.read.metablocks_position, 0);
        // Restoring with advance_read=true should give None (sentinel preserved)
        let restored = LogSegmentFileMetadata::new(1, 1000, None, &serialized, true);
        assert!(restored.read.is_none());
    }
}
