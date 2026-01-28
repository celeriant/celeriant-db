use crate::errors::open_or_create_error::OpenOrCreateError;


#[derive(Debug)]
pub enum ReadyUpError {
    InvalidPreallocatedBytes(u64),
    ActiveFileError(OpenOrCreateError),
    UnableToAccessDirectory { directory: String, source: std::io::Error },
    UnableToCreateDirectory { directory: String, source: std::io::Error },
}