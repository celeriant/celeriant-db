use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use eventplanedb_access::job_error::JobError;
use serde::Serialize;

#[derive(Debug, Serialize)]
struct ErrorBody {
    message: String,
}

#[derive(Debug)]
pub enum RouteError {
    JobError(JobError),
    Other(String),
}

impl From<JobError> for RouteError {
    fn from(e: JobError) -> Self {
        RouteError::JobError(e)
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
                JobError::InvalidParameters(msg) => (StatusCode::BAD_REQUEST, msg),
                JobError::Other(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
            },
            RouteError::Other(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
        };

        let body = Json(ErrorBody { message });
        (status, body).into_response()
    }
}