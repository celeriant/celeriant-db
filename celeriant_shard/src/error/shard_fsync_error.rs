use celeriant_rotating_log::errors::{open_or_create_error::OpenOrCreateError, write_dual_header_error::WriteDualHeaderError};

// /// Storage/infrastructure errors—may be transient.
#[derive(Debug, Clone)]
pub enum ShardFsyncError {
    /// We needed the carry over buffer during a datablocks write
    /// due to Direct IO alignment, but it wasn't available in the
    /// log segment file metadata.
    DatablocksCarryOverBufferNotPresent,

    /// A rollback occurred and invalidated pending writes.
    /// Writers should retry their operation. Rollback could be from
    /// a local failure or replication failure
    RollbackInvalidatedWrites,

    /// We accumulated so many writes from clients that it
    /// is impossible to write it to a single log segment file
    /// Server possibly mis-configured the preallocate_bytes
    BatchesTooLarge { preallocate_bytes: u64 },

    /// Possibly out of disk space, unable to create
    /// new log file and pre-allocate space for it
    UnableToRotateToNewLogSegmentFile(OpenOrCreateError),

    /// We need the writer DmaFile but it's gone,
    /// possible due to a server shutdown event
    ActiveWriteFileUnavailable,

    /// We tried to lock the writer DmaFile for the active log
    /// segment file but timed out. Shouldn't happen as we serialise
    /// a single leader for fsync batching
    WriteLockTimeout,

    /// Whatever we got in the metablock it failed to serialize
    /// this shouldn't happen as metablocks are always going to fit
    /// in the provided buffer
    MetablockSerialisationError(String),

    /// Failed trying to write batch of metablocks to the active file
    WriteMetablocksError(String),

    /// Failed trying to write the shard log header (front or back)
    LogSegmentFileHeaderWriteFailure(WriteDualHeaderError),

    /// Failed fsync to the disk
    FDataSyncError(String),

    /// Failed trying to write batch of datablocks to the active file
    WriteDatablocksError(String),
}