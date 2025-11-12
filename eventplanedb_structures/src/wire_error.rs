use crate::wire_format::WireFormatError;

#[derive(Debug)]
pub enum WireError {
    BufferTooSmall {
        required: usize,
        available: usize,
    },
    WireFormat(WireFormatError),
    NetworkError(std::io::Error),
    UnknownRequestType(u32),
    UnknownResponseType(u32),
    MessageTooLarge {
        message_length: u32,
        max_message_size: u32,
    },
    UnsupportedProtocol(u32),
}

impl From<std::io::Error> for WireError {
    fn from(value: std::io::Error) -> Self {
        WireError::NetworkError(value)
    }
}

impl From<WireFormatError> for WireError {
    fn from(value: WireFormatError) -> Self {
        WireError::WireFormat(value)
    }
}
