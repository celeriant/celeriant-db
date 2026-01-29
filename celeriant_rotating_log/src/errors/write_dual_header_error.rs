
#[derive(Debug, Clone)]
pub enum WriteDualHeaderError {
    SerialiseError(String),
    FileWriteError { from_back: bool },
}

impl From<bincode::error::EncodeError> for WriteDualHeaderError {
    fn from(e: bincode::error::EncodeError) -> Self {
        Self::SerialiseError(e.to_string())
    }
}