use crate::{shard_log_read_error::ShardLogReadError, shard_log_write_error::ShardLogWriteError};

#[derive(Debug, Clone)]
pub enum ReadWriteError {
    Read(ShardLogReadError),
    Write(ShardLogWriteError),
}

impl From<ShardLogReadError> for ReadWriteError {
    fn from(error: ShardLogReadError) -> Self {
        ReadWriteError::Read(error)
    }
}

impl From<ShardLogWriteError> for ReadWriteError {
    fn from(error: ShardLogWriteError) -> Self {
        ReadWriteError::Write(error)
    }
}