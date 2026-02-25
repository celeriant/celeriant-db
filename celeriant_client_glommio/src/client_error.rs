use celeriant_ktls::KtlsError;
use celeriant_msg::{read_wire_data_error::ReadWireDataError, response::responses::ErrorResponse};
use celeriant_wire::network::wire_error::WireError;

#[derive(Debug)]
pub enum ClientError {
    NoAddress,
    ConnectionTimeout,
    ConnectionFailed(glommio::GlommioError<()>),
    SetNoDelayError(glommio::GlommioError<()>),
    KtlsError(KtlsError),
    RequestTimeout,
    RequestProtocolError,
    NotLeader { leader_address: Option<String>, error: ErrorResponse },
    CeleriantError(ErrorResponse),
    WriteRequestError(WireError),
    ReadResponseError(ReadWireDataError),
}

impl ClientError {
    pub(crate) fn from_error_response(error: ErrorResponse) -> Self {
        if error.is_not_leader() {
            let leader_address = error.parse_leader_address();
            ClientError::NotLeader { leader_address, error }
        } else {
            ClientError::CeleriantError(error)
        }
    }
}