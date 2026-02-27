use celeriant_client_glommio::ClientError;

#[derive(Debug)]
pub enum SendHeartbeatError {
    NetworkError(ClientError),
    UnexpectedResponse,
    LockTimeout,
}

impl From<ClientError> for SendHeartbeatError {
    fn from(e: ClientError) -> Self {
        SendHeartbeatError::NetworkError(e)
    }
}
