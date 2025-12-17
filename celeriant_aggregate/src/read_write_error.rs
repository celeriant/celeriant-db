use celeriant_filesystem::shard_log_write_error::ShardLogWriteError;

use crate::read_operations::read_error::ReadError;
use crate::write_operations::write_error::WriteError;

#[derive(Debug, Clone)]
pub enum ReadWriteError {
    Read(ReadError),
    Write(WriteError),
}

impl From<ReadError> for ReadWriteError {
    fn from(error: ReadError) -> Self {
        ReadWriteError::Read(error)
    }
}

impl From<WriteError> for ReadWriteError {
    fn from(error: WriteError) -> Self {
        ReadWriteError::Write(error)
    }
}