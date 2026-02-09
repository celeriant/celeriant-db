use celeriant_wire::codec::codec_error::CodecError;

#[derive(Debug, Clone)]
pub enum ApplyBatchError {
    WalIndexMismatch { current: u64, batch_first: u64 },
    TipHashMismatch { current: [u8; 32], batch: [u8; 32] },
    MissingDatablock,
    SerialiseDatablocks(CodecError),
}
