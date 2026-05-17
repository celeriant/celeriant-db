use celeriant_wire::codec::codec_error::CodecError;

#[derive(Debug, Clone)]
pub enum ApplyBatchError {
    WalSeqMismatch { current: u64, batch_first: u64 },
    TipHashMismatch { current: [u8; 32], current_wal_seq: u64, batch: [u8; 32], batch_wal_seq: u64 },
    BatchWalSeqGap { index: usize, expected: u64, actual: u64 },
    MissingDatablock,
    BlockBecameInline,
    SerialiseDatablocks(CodecError),
}
