use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use eventplanedb_crypto::CryptoError;
use serde::Serialize;

use crate::job_error::JobError;

#[derive(Debug, Serialize)]
struct ErrorBody {
    message: String,
}

#[derive(Debug)]
pub enum RouteError {
    JobError(JobError),
    CryptoError(CryptoError),
    Other(String),
    BadRequest(String),
}

impl From<JobError> for RouteError {
    fn from(e: JobError) -> Self {
        RouteError::JobError(e)
    }
}

impl From<eventplanedb_metadata::MetadataError> for RouteError {
    fn from(e: eventplanedb_metadata::MetadataError) -> Self {
        RouteError::Other(format!("Metadata error: {}", e))
    }
}

impl From<eventplanedb_storage_threaded::ThreadedError> for RouteError {
    fn from(e: eventplanedb_storage_threaded::ThreadedError) -> Self {
        RouteError::Other(format!("Storage error: {}", e))
    }
}

// Add conversion from CryptoError to RouteError
impl From<CryptoError> for RouteError {
    fn from(e: CryptoError) -> Self {
        match e {
            CryptoError::KeyDecodingFailed(msg) => RouteError::BadRequest(msg),
            _ => RouteError::CryptoError(e),
        }
    }
}

// Example of handling other potential errors:
impl From<std::io::Error> for RouteError {
    fn from(e: std::io::Error) -> Self {
        RouteError::Other(format!("IO error: {}", e))
    }
}

impl IntoResponse for RouteError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            RouteError::JobError(job_error) => match job_error {
                JobError::PermissionDenied(msg) => (StatusCode::FORBIDDEN, msg),
                JobError::NotFound(msg) => (StatusCode::NOT_FOUND, msg),
                JobError::Conflict(msg) => (StatusCode::CONFLICT, msg),
                JobError::InvalidParameters(msg) => (StatusCode::BAD_REQUEST, msg),
                JobError::AuthenticationFailed(msg) => (StatusCode::UNAUTHORIZED, msg),
                JobError::Other(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
            },
            RouteError::CryptoError(crypto_error) => {
                let msg = crypto_error.to_string();
                match crypto_error {
                    CryptoError::InvalidSignature | CryptoError::InvalidNonce => {
                        (StatusCode::BAD_REQUEST, msg)
                    }
                    _ => (StatusCode::INTERNAL_SERVER_ERROR, msg),
                }
            }
            RouteError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            RouteError::Other(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
        };

        let body = Json(ErrorBody { message });
        (status, body).into_response()
    }
}
