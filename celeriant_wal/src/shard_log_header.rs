use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;

use crate::constants::{AGGREGATE_BLOOM_BYTES, EntryHashBytes, GENESIS_HASH, HEADER_BLOCK_SIZE_BYTES};

/// Position + sequence + hash snapshot for either the write cursor or the read cursor
/// in a log segment header. Two of these are nested in [`ShardLogHeader`]; on the wire
/// bincode encodes the inner fields inline (no tag/length prefix), so the layout is
/// byte-identical to the previous flat representation.
#[derive(Debug, Clone, Encode, Decode, DeepSizeOf)]
pub struct HeaderCursor {
    /// End of the last metablock entry (metablocks grow from start of file)
    pub metablocks_position: u64,
    /// Start of the most recent datablocks (datablocks grow from end of file)
    pub datablocks_position: u64,
    /// Shard-global WAL sequence at this cursor
    pub wal_seq: u64,
    /// Blake3 hash chain tip at this cursor
    pub tip_hash: EntryHashBytes,
}

impl HeaderCursor {
    const WIRE_SIZE_METABLOCKS_POSITION: usize = 8;
    const WIRE_SIZE_DATABLOCKS_POSITION: usize = 8;
    const WIRE_SIZE_WAL_SEQ: usize = 8;
    const WIRE_SIZE_TIP_HASH: usize = 32;

    pub const OFFSET_METABLOCKS_POSITION: usize = 0;
    pub const OFFSET_DATABLOCKS_POSITION: usize =
        Self::OFFSET_METABLOCKS_POSITION + Self::WIRE_SIZE_METABLOCKS_POSITION;
    pub const OFFSET_WAL_SEQ: usize =
        Self::OFFSET_DATABLOCKS_POSITION + Self::WIRE_SIZE_DATABLOCKS_POSITION;
    pub const OFFSET_TIP_HASH: usize =
        Self::OFFSET_WAL_SEQ + Self::WIRE_SIZE_WAL_SEQ;

    pub const WIRE_SIZE_TOTAL: usize =
        Self::OFFSET_TIP_HASH + Self::WIRE_SIZE_TIP_HASH;

    pub fn genesis() -> Self {
        Self {
            metablocks_position: 0,
            datablocks_position: 0,
            wal_seq: 0,
            tip_hash: GENESIS_HASH,
        }
    }
}

/// Written at both the start and end of the fixed-size log segment, each CRC-protected,
/// so a torn write can be recovered from whichever copy survived. Sized at
/// HEADER_BLOCK_SIZE_BYTES (512KB) to hold the aggregate bloom filter.
#[derive(Debug, Clone, Encode, Decode, DeepSizeOf)]
pub struct ShardLogHeader {
    /// Writer's tip: the most recently written metablock. Sits at or ahead of `read`
    /// (entries here may be written but not yet replicated/confirmed).
    pub write: HeaderCursor,

    /// Bloom filter of aggregate keys written to this segment, for fast "aggregate absent"
    /// checks. A negative result means no metablock for that aggregate exists here. It's
    /// built over the write range, which always covers the read range, so it's also a
    /// valid (superset) filter for reads.
    pub aggregate_bloom: Vec<u64>,

    /// Promotion-upload floor: the first wal_seq we'd push to S3 if we promoted. Set from
    /// each TCP batch received as follower to `leader_confirmed_wal_seq + 1` (monotonic
    /// max). On promotion, `[this, write]` is uploaded so a partitioned ex-leader — which
    /// may have rolled back everything above its confirmed point — can catch up via S3 from
    /// entries that might otherwise exist only over TCP. 0 means nothing to upload. Persisted
    /// here so a promote-after-restart still knows the gap.
    pub last_received_replication_wal_seq: u64,

    /// Highest wal_seq this node acked to a client while leader. The S3-catchup truncate
    /// barrier refuses to truncate at or below it — that data is owed to whoever wrote it.
    /// The barrier uses this value alone; `last_received_replication_wal_seq` and `read` are
    /// deliberately excluded, since they track receive/apply, not what we promised a client.
    ///
    /// Bumped after a confirmed replication, then header-fsynced (coalesced) before the
    /// client sees Ok, so it survives a crash-after-Ok. If that fsync fails the bump stays
    /// in-memory only (logged), and a later crash could then permit a truncate we'd refuse.
    pub last_self_acked_wal_seq: u64,

    /// Reader's tip: end of the last replicated/confirmed metablock — the reader visibility
    /// horizon. A zero `metablocks_position` is the sentinel for "read hasn't advanced into
    /// this segment yet" (just after rotation, before its first replication).
    pub read: HeaderCursor,
}

impl ShardLogHeader {
    // Wire format layout (bincode fixed-int encoding, nested structs encoded inline).
    // Update these if field order or types change!

    const WIRE_SIZE_WRITE_CURSOR: usize = HeaderCursor::WIRE_SIZE_TOTAL;
    const WIRE_SIZE_AGGREGATE_BLOOM: usize = AGGREGATE_BLOOM_BYTES;
    const WIRE_SIZE_LAST_RECEIVED_REPLICATION_WAL_SEQ: usize = 8;
    const WIRE_SIZE_LAST_SELF_ACKED_WAL_SEQ: usize = 8;
    const WIRE_SIZE_READ_CURSOR: usize = HeaderCursor::WIRE_SIZE_TOTAL;

    pub const OFFSET_WRITE_CURSOR: usize = 0;

    pub const OFFSET_AGGREGATE_BLOOM: usize =
        Self::OFFSET_WRITE_CURSOR + Self::WIRE_SIZE_WRITE_CURSOR;

    pub const OFFSET_LAST_RECEIVED_REPLICATION_WAL_SEQ: usize =
        Self::OFFSET_AGGREGATE_BLOOM + Self::WIRE_SIZE_AGGREGATE_BLOOM;

    pub const OFFSET_LAST_SELF_ACKED_WAL_SEQ: usize =
        Self::OFFSET_LAST_RECEIVED_REPLICATION_WAL_SEQ + Self::WIRE_SIZE_LAST_RECEIVED_REPLICATION_WAL_SEQ;

    pub const OFFSET_READ_CURSOR: usize =
        Self::OFFSET_LAST_SELF_ACKED_WAL_SEQ + Self::WIRE_SIZE_LAST_SELF_ACKED_WAL_SEQ;

    /// Total wire size of ShardLogHeader
    pub const WIRE_SIZE_TOTAL: usize =
        Self::OFFSET_READ_CURSOR + Self::WIRE_SIZE_READ_CURSOR;

    pub fn new(file_len: u64) -> Self {
        let metablocks_position = HEADER_BLOCK_SIZE_BYTES as u64;
        let datablocks_position = file_len.saturating_sub(HEADER_BLOCK_SIZE_BYTES as u64);
        let cursor = HeaderCursor {
            metablocks_position,
            datablocks_position,
            wal_seq: 0,
            tip_hash: GENESIS_HASH,
        };
        Self {
            write: cursor.clone(),
            aggregate_bloom: vec![],
            last_received_replication_wal_seq: 0,
            last_self_acked_wal_seq: 0,
            read: cursor,
        }
    }

    pub fn available_space(&self) -> u64 {
        self.write.datablocks_position.saturating_sub(self.write.metablocks_position)
    }

    pub fn has_space_for(&self, metablock_size: u64, datablock_size: u64) -> bool {
        self.available_space() >= metablock_size.saturating_add(datablock_size)
    }

    pub fn append_event_batches(
        &mut self,
        metablock_size: u64,
        datablock_size: u64,
    ) {
        self.write.metablocks_position = self
            .write
            .metablocks_position
            .saturating_add(metablock_size);
        self.write.datablocks_position = self
            .write
            .datablocks_position
            .saturating_sub(datablock_size);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constants::GENESIS_HASH;

    const TEST_FILE_LEN: u64 = 1024 * 1024 * 1024; // 1GB

    #[test]
    fn test_new_initializes_positions_correctly() {
        let header = ShardLogHeader::new(TEST_FILE_LEN);

        assert_eq!(header.write.metablocks_position, HEADER_BLOCK_SIZE_BYTES as u64);
        assert_eq!(
            header.write.datablocks_position,
            TEST_FILE_LEN - HEADER_BLOCK_SIZE_BYTES as u64
        );
    }

    #[test]
    fn test_new_handles_small_file_len() {
        let small_file_len = (HEADER_BLOCK_SIZE_BYTES / 2) as u64;
        let header = ShardLogHeader::new(small_file_len);

        assert_eq!(header.write.metablocks_position, HEADER_BLOCK_SIZE_BYTES as u64);
        assert_eq!(header.write.datablocks_position, 0); // saturating_sub prevents underflow
    }

    #[test]
    fn test_available_space() {
        let header = ShardLogHeader::new(TEST_FILE_LEN);
        let expected = TEST_FILE_LEN - 2 * HEADER_BLOCK_SIZE_BYTES as u64;

        assert_eq!(header.available_space(), expected);
    }

    #[test]
    fn test_available_space_when_positions_overlap() {
        let header = ShardLogHeader {
            write: HeaderCursor {
                metablocks_position: 1000,
                datablocks_position: 500,
                wal_seq: 0,
                tip_hash: GENESIS_HASH,
            },
            aggregate_bloom: vec![],
            last_received_replication_wal_seq: 0,
            last_self_acked_wal_seq: 0,
            read: HeaderCursor::genesis(),
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
        let initial_meta = header.write.metablocks_position;
        let initial_data = header.write.datablocks_position;

        header.append_event_batches(512, 1024);

        assert_eq!(header.write.metablocks_position, initial_meta + 512);
        assert_eq!(header.write.datablocks_position, initial_data - 1024);
    }

    #[test]
    fn test_append_event_batches_multiple_times() {
        let mut header = ShardLogHeader::new(TEST_FILE_LEN);

        header.append_event_batches(100, 200);
        header.append_event_batches(150, 300);

        assert_eq!(
            header.write.metablocks_position,
            HEADER_BLOCK_SIZE_BYTES as u64 + 250
        );
        assert_eq!(
            header.write.datablocks_position,
            TEST_FILE_LEN - HEADER_BLOCK_SIZE_BYTES as u64 - 500
        );
    }

    #[test]
    fn test_append_reduces_available_space() {
        let mut header = ShardLogHeader::new(TEST_FILE_LEN);
        let initial_space = header.available_space();

        header.append_event_batches(100, 200);

        assert_eq!(header.available_space(), initial_space - 300);
    }

    #[test]
    fn test_wire_size_fits_in_header_block() {
        assert!(
            ShardLogHeader::WIRE_SIZE_TOTAL <= HEADER_BLOCK_SIZE_BYTES,
            "ShardLogHeader wire size ({}) exceeds header block size ({})",
            ShardLogHeader::WIRE_SIZE_TOTAL,
            HEADER_BLOCK_SIZE_BYTES
        );
    }

}
