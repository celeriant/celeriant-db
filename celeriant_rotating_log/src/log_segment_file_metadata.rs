use celeriant_wal::{shard_log_header::ShardLogHeader};

use crate::log_segment_cursor::LogSegmentCursor;

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
        }
    }

    #[must_use]
    pub fn to_shard_log_header(&self) -> ShardLogHeader {
        self.write.to_shard_log_header()
    }

    /// Advance visible position after successful replication (write -> read)
    pub fn advance_visible_position(&mut self) {
        self.read = Some(self.write.clone());
    }

    /// Returns true if write cursor is ahead of read cursor (pending replication)
    pub fn is_pending_advance(&self) -> bool {
        self.read.is_none() || self.write.wal_index > self.read.as_ref().unwrap().wal_index
    }
}