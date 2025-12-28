use crate::error::{shard_read_error::ShardReadError, shard_write_error::ShardWriteError, watch_session_error::WatchSessionError};

#[derive(Debug, Clone)]
pub enum ShardError {
    Read(ShardReadError),
    Write(ShardWriteError),
    WatchSession(WatchSessionError),
}

impl From<ShardReadError> for ShardError {
    fn from(error: ShardReadError) -> Self {
        ShardError::Read(error)
    }
}

impl From<ShardWriteError> for ShardError {
    fn from(error: ShardWriteError) -> Self {
        ShardError::Write(error)
    }
}

impl From<WatchSessionError> for ShardError {
    fn from(error: WatchSessionError) -> Self {
        ShardError::WatchSession(error)
    }
}