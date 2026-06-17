use std::cell::RefCell;
use std::rc::Rc;

use celeriant_wal::{
    constants::{CLIENT_BLOOM_BYTES, EntryHashBytes},
    shard_log_header::{HeaderCursor, ShardLogHeader},
};

use crate::log_segment_file::aggregate_key_bloom::AggregateKeyBloom;

pub type SharedBloom = Rc<RefCell<AggregateKeyBloom>>;

#[must_use]
pub fn shared_bloom(bloom: AggregateKeyBloom) -> SharedBloom {
    Rc::new(RefCell::new(bloom))
}

#[must_use]
pub fn fork_bloom(b: &SharedBloom) -> SharedBloom {
    shared_bloom(b.borrow().clone())
}

/// Cursor tracking positions within a log segment file.
#[derive(Clone)]
pub struct LogSegmentCursor {
    pub log_id: u64,
    /// End of the last metablock entry (metablocks grow from start of file)
    pub metablocks_position: u64,
    /// Start of the most recent datablocks (datablocks grow from end of file)
    pub datablocks_position: u64,
    /// Shard-global WAL sequence of the last entry at this cursor
    pub wal_seq: u64,
    /// Bloom filter state at this cursor
    pub aggregate_key_bloom: SharedBloom,
    /// Global client_id bloom at this cursor (negative client-seq short-circuit)
    pub client_id_bloom: SharedBloom,
    /// blake3 hash chain up to latest written metablock for verification in distributed env
    pub tip_hash: EntryHashBytes,
}

// Hand-written (not derived) so the client bloom defaults to its 128KB size, not the
// aggregate bloom's 256KB
impl Default for LogSegmentCursor {
    fn default() -> Self {
        Self {
            log_id: 0,
            metablocks_position: 0,
            datablocks_position: 0,
            wal_seq: 0,
            aggregate_key_bloom: shared_bloom(AggregateKeyBloom::new()),
            client_id_bloom: shared_bloom(AggregateKeyBloom::with_capacity_bytes(CLIENT_BLOOM_BYTES)),
            tip_hash: EntryHashBytes::default(),
        }
    }
}

impl LogSegmentCursor {
    pub fn from_shard_log_header_write(log_id: u64, header: &ShardLogHeader) -> Self {
        Self::from_header_cursor(log_id, &header.write, &header.aggregate_bloom, &header.client_bloom)
    }

    /// Build a read cursor but sharing the blooms
    pub fn read_snapshot_sharing_bloom(&self, read_hc: &HeaderCursor) -> Self {
        Self {
            log_id: self.log_id,
            metablocks_position: read_hc.metablocks_position,
            datablocks_position: read_hc.datablocks_position,
            wal_seq: read_hc.wal_seq,
            aggregate_key_bloom: Rc::clone(&self.aggregate_key_bloom),
            client_id_bloom: Rc::clone(&self.client_id_bloom),
            tip_hash: read_hc.tip_hash,
        }
    }

    fn from_header_cursor(log_id: u64, cursor: &HeaderCursor, bloom: &[u64], client_bloom: &[u64]) -> Self {
        Self {
            log_id,
            metablocks_position: cursor.metablocks_position,
            datablocks_position: cursor.datablocks_position,
            wal_seq: cursor.wal_seq,
            aggregate_key_bloom: shared_bloom(AggregateKeyBloom::from_bytes(bloom)),
            client_id_bloom: shared_bloom(AggregateKeyBloom::from_bytes(client_bloom)),
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
            aggregate_bloom: self.aggregate_key_bloom.borrow().to_bytes(),
            client_bloom: self.client_id_bloom.borrow().to_bytes(),
            last_received_replication_wal_seq,
            last_self_acked_wal_seq,
            // Zero sentinel: read has not advanced to this segment yet (post-rotation).
            read: read.map(LogSegmentCursor::to_header_cursor).unwrap_or_else(HeaderCursor::genesis),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use celeriant_wal::aggregate_key::AggregateKey;

    /// `fork_bloom` must produce an INDEPENDENT bloom: post-fork inserts on either side must not
    /// leak across. This guards the unwind/reset site that seeds a new segment from a sealed one.
    #[test]
    fn fork_bloom_is_independent() {
        let a = AggregateKey::new(1, 1, 1);
        let b = AggregateKey::new(1, 1, 2);
        let original = shared_bloom(AggregateKeyBloom::new());
        original.borrow_mut().insert(&a);

        let forked = fork_bloom(&original);
        original.borrow_mut().insert(&b); // must NOT reach `forked`

        assert!(forked.borrow().may_contain(&a), "fork keeps keys present at fork time");
        assert!(!forked.borrow().may_contain(&b), "fork must not see post-fork inserts");
    }

    /// Cloning a cursor SHARES the bloom (the intended behaviour for read = write.clone()):
    /// an insert on one is visible through the other. Documents the contract the scanner relies on.
    #[test]
    fn cursor_clone_shares_bloom() {
        let b = AggregateKey::new(1, 1, 2);
        let write = LogSegmentCursor::default();
        let read = write.clone();

        write.aggregate_key_bloom.borrow_mut().insert(&b);
        assert!(read.aggregate_key_bloom.borrow().may_contain(&b), "clone shares the live bloom");
    }
}
