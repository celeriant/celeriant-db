use celeriant_wal::{shard_log_header::ShardLogHeader};

use crate::log_segment_file::log_segment_cursor::LogSegmentCursor;

#[derive(Clone)]
pub struct LogSegmentFileMetadata {
    /// One based incremented log id
    pub log_id: u64,
    /// Size of log file, typically fixed but could be smaller if log is being truncated
    pub file_len: u64,
    /// Writer's view - most recent writes (not yet replicated)
    pub write: LogSegmentCursor,
    /// Reader's view - replicated and visible to readers
    /// May be none if the read position is still on the previous log segment file
    pub read: Option<LogSegmentCursor>,
    /// Used in write path as datablock file writes are not Direct I/O aligned
    pub datablocks_carry_over: Option<Vec<u8>>,
    /// First WAL index of the last batch received via TCP replication while follower.
    /// On promotion to leader, entries from this index onward are uploaded to S3.
    pub last_received_replication_wal_index: u64,
}

impl LogSegmentFileMetadata {
    pub fn available_space(&self) -> u64 {
        self.write.datablocks_position.saturating_sub(self.write.metablocks_position)
    }

    pub fn new(log_id: u64, file_len: u64, datablocks_carry_over: Option<Vec<u8>>, shard_log_header: &ShardLogHeader, advance_read: bool) -> Self {
        LogSegmentFileMetadata {
            log_id,
            file_len,
            write: LogSegmentCursor::from_shard_log_header(log_id, shard_log_header),
            read: if advance_read { Some(LogSegmentCursor::from_shard_log_header(log_id, shard_log_header)) } else { None },
            datablocks_carry_over,
            last_received_replication_wal_index: shard_log_header.last_received_replication_wal_index,
        }
    }

    #[must_use]
    pub fn to_shard_log_header(&self) -> ShardLogHeader {
        self.write.to_shard_log_header(self.last_received_replication_wal_index)
    }

    /// Advance visible position after successful replication (write -> read)
    pub fn advance_visible_position(&mut self) {
        self.read = Some(self.write.clone());
    }

    /// Returns true if write cursor is ahead of read cursor (pending replication)
    pub fn is_pending_advance(&self) -> bool {
        self.read.is_none() || self.write.wal_index > self.read.as_ref().unwrap().wal_index
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

    fn make_header(meta_pos: u64, data_pos: u64, wal_idx: u64) -> ShardLogHeader {
        ShardLogHeader {
            metablocks_position: meta_pos,
            datablocks_position: data_pos,
            wal_index: wal_idx,
            tip_hash: [0u8; 32],
            aggregate_bloom: AggregateKeyBloom::new().to_bytes(),
            last_received_replication_wal_index: 0,
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
        assert_eq!(meta.write.wal_index, 10);
    }

    #[test]
    fn new_with_advance_read() {
        let meta = LogSegmentFileMetadata::new(5, 2000, None, &make_header(50, 1950, 10), true);
        assert!(meta.read.is_some());
        assert_eq!(meta.read.unwrap().wal_index, 10);
    }

    #[test]
    fn advance_visible_position_copies_write() {
        let mut meta = LogSegmentFileMetadata::new(1, 1000, None, &make_header(100, 900, 5), false);
        meta.write.wal_index = 15;
        meta.advance_visible_position();
        assert_eq!(meta.read.unwrap().wal_index, 15);
    }

    #[test]
    fn is_pending_when_read_none() {
        let meta = LogSegmentFileMetadata::new(1, 1000, None, &make_header(100, 900, 5), false);
        assert!(meta.is_pending_advance());
    }

    #[test]
    fn is_pending_when_write_ahead() {
        let mut meta = LogSegmentFileMetadata::new(1, 1000, None, &make_header(100, 900, 5), true);
        meta.write.wal_index = 10;
        assert!(meta.is_pending_advance());
    }

    #[test]
    fn not_pending_when_synced() {
        let meta = LogSegmentFileMetadata::new(1, 1000, None, &make_header(100, 900, 5), true);
        assert!(!meta.is_pending_advance());
    }
}