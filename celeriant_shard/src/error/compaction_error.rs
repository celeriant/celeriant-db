use celeriant_rotating_log::errors::{
    open_or_create_error::OpenOrCreateError,
    scan_error::ScanError,
    write_dual_header_error::WriteDualHeaderError,
};
use celeriant_wire::disk::disk_format_error::DiskFormatError;

#[derive(Debug, Clone)]
pub enum CompactionError {
    OpenSegment(OpenOrCreateError),
    ActiveSegmentTarget { log_id: u64 },
    LockTimeout,
    SegmentUnavailable { log_id: u64 },
    ForwardScanIo { log_id: u64, source: String },
    MetablockDeserialise(DiskFormatError),
    ReverseScan(ScanError<DiskFormatError>),
    CreateTempFile { path: String, source: String },
    WriteHeader(WriteDualHeaderError),
    WriteFailed { step: &'static str, source: String },
    ReadDatablock { log_id: u64, position: u64, source: String },
    MetablockSerialise(String),
    LayoutArithmetic { kept_metablock_count: u64, kept_datablock_bytes: u64, alignment: u64 },
    ShortRead { log_id: u64, pos: u64, requested: usize, got: usize },
    AtomicSwap { temp_path: String, target_path: String, source: String },
    CleanupFailed { path: String, source: String },
}

impl From<OpenOrCreateError> for CompactionError {
    fn from(e: OpenOrCreateError) -> Self {
        CompactionError::OpenSegment(e)
    }
}

impl From<ScanError<DiskFormatError>> for CompactionError {
    fn from(e: ScanError<DiskFormatError>) -> Self {
        CompactionError::ReverseScan(e)
    }
}
