use celeriant_msg::request::requests::ReplicationBatchItem;
use celeriant_wal::constants::{FIXED_BLOCK_SIZE_BYTES, WIRE_VERSION_WAL_METABLOCK};
use celeriant_wire::codec::codec_error::CodecError;
use celeriant_wire::disk::versioned_block::serialize_versioned_message;

use crate::shard_wal_sync::compute_entry_hash;

#[derive(Debug, Clone)]
pub struct IntraBatchChainBreak {
    pub at_index: usize,
    pub expected: [u8; 32],
    pub producer_wal_seq: u64,
    pub actual: [u8; 32],
    pub consumer_wal_seq: u64,
}

#[derive(Debug, Clone)]
pub enum ValidateChainError {
    ChainBreak(IntraBatchChainBreak),
    SerialiseMetablock(CodecError),
}

/// Validates that consecutive metablocks form a continuous Blake3 hash chain
pub(crate) fn validate_intra_batch_chain(items: &[ReplicationBatchItem]) -> Result<(), ValidateChainError> {
    for (i, w) in items.windows(2).enumerate() {
        let mut buf = [0u8; FIXED_BLOCK_SIZE_BYTES];
        serialize_versioned_message(&w[0].metablock, WIRE_VERSION_WAL_METABLOCK, &mut buf)
            .map_err(|e| ValidateChainError::SerialiseMetablock(e.into()))?;
        let expected = compute_entry_hash(&w[0].metablock.previous_tip_hash, &buf);
        if w[1].metablock.previous_tip_hash != expected {
            return Err(ValidateChainError::ChainBreak(IntraBatchChainBreak {
                at_index: i + 1,
                expected,
                producer_wal_seq: w[0].metablock.wal_seq,
                actual: w[1].metablock.previous_tip_hash,
                consumer_wal_seq: w[1].metablock.wal_seq,
            }));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use celeriant_wal::aggregate_key::AggregateKey;
    use celeriant_wal::constants::GENESIS_HASH;
    use celeriant_wal::metablocks::metablock::Metablock;

    fn item(wal_seq: u64, previous_tip_hash: [u8; 32]) -> ReplicationBatchItem {
        let mut mb = Metablock::default_inline_event_batch_metadata(AggregateKey::new(1, 1, 1));
        mb.wal_seq = wal_seq;
        mb.previous_tip_hash = previous_tip_hash;
        ReplicationBatchItem { metablock: mb, datablock: None }
    }

    /// Compute the tip hash item `i` produces (to chain the next item).
    fn tip_after(item: &ReplicationBatchItem) -> [u8; 32] {
        let mut buf = [0u8; FIXED_BLOCK_SIZE_BYTES];
        serialize_versioned_message(&item.metablock, WIRE_VERSION_WAL_METABLOCK, &mut buf).unwrap();
        compute_entry_hash(&item.metablock.previous_tip_hash, &buf)
    }

    #[test]
    fn validate_intra_batch_chain_empty() {
        assert!(validate_intra_batch_chain(&[]).is_ok());
    }

    #[test]
    fn validate_intra_batch_chain_single_item() {
        let items = [item(1, GENESIS_HASH)];
        assert!(validate_intra_batch_chain(&items).is_ok());
    }

    #[test]
    fn validate_intra_batch_chain_multi_item_happy_path() {
        let i0 = item(1, GENESIS_HASH);
        let tip = tip_after(&i0);
        let i1 = item(2, tip);
        let tip2 = tip_after(&i1);
        let i2 = item(3, tip2);
        assert!(validate_intra_batch_chain(&[i0, i1, i2]).is_ok());
    }

    fn unwrap_chain_break(err: ValidateChainError) -> IntraBatchChainBreak {
        match err {
            ValidateChainError::ChainBreak(b) => b,
            ValidateChainError::SerialiseMetablock(e) => panic!("expected chain break, got serialise failure: {e:?}"),
        }
    }

    #[test]
    fn validate_intra_batch_chain_break_at_index_1() {
        let i0 = item(1, GENESIS_HASH);
        let i1 = item(2, [0xAB; 32]);
        let b = unwrap_chain_break(validate_intra_batch_chain(&[i0, i1]).unwrap_err());
        assert_eq!(b.at_index, 1);
        assert_eq!(b.consumer_wal_seq, 2);
        assert_eq!(b.producer_wal_seq, 1);
    }

    #[test]
    fn validate_intra_batch_chain_break_at_last_position() {
        let i0 = item(1, GENESIS_HASH);
        let tip1 = tip_after(&i0);
        let i1 = item(2, tip1);
        let tip2 = tip_after(&i1);
        let i2 = item(3, tip2);
        let i3 = item(4, [0xFF; 32]);
        let b = unwrap_chain_break(validate_intra_batch_chain(&[i0, i1, i2, i3]).unwrap_err());
        assert_eq!(b.at_index, 3);
        assert_eq!(b.consumer_wal_seq, 4);
        assert_eq!(b.producer_wal_seq, 3);
    }
}
