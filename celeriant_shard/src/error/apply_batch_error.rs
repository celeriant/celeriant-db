use celeriant_wire::codec::codec_error::CodecError;

#[derive(Debug, Clone)]
pub enum ApplyBatchError {
    WalIndexMismatch { current: u64, batch_first: u64 },
    TipHashMismatch { current: [u8; 32], current_wal_index: u64, batch: [u8; 32], batch_wal_index: u64 },
    BatchWalIndexGap { index: usize, expected: u64, actual: u64 },
    MissingDatablock,
    BlockBecameInline,
    SerialiseDatablocks(CodecError),
}
