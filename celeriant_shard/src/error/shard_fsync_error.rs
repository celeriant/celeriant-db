use celeriant_wire::wire_format_error::WireFormatError;

/// Storage/infrastructure errors—may be transient.
#[derive(Debug, Clone)]
pub enum ShardFsyncError {
    /// Disk I/O failure.
    Io(String),
    
    /// Serialization or deserialization failure.
    Serialization(WireFormatError),
    
    /// DMA file handle not initialized (startup issue).
    DmaFileNotInitialized,
    
    /// Log file header corrupted beyond recovery.
    HeaderCorrupted { log_id: Option<u64> },
    
    /// Requested log file doesn't exist.
    LogFileNotFound { log_id: u64 },
    
    /// Previous sync failed, forcing durable mode.
    SyncFailurePending,
}