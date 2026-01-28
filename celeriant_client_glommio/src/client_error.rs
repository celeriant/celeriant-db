use celeriant_msg::{read_wire_data_error::ReadWireDataError, response::responses::ErrorResponse};
use celeriant_wire::network::wire_error::WireError;

#[derive(Debug)]
pub enum ClientError {
    ConnectionTimeout,
    ConnectionFailed(glommio::GlommioError<()>),
    SetNoDelayError(glommio::GlommioError<()>),
    RequestTimeout,
    RequestProtocolError,
    CeleriantError(ErrorResponse),
    WriteRequestError(WireError),
    ReadResponseError(ReadWireDataError),
}