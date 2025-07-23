use std::io;

#[derive(Debug)]
pub enum JobError {
    PermissionDenied(String),
    NotFound(String),
    InvalidParameters(String),
    AuthenticationFailed(String),
    Other(String),
}

impl From<io::Error> for JobError {
    fn from(error: io::Error) -> Self {
        match error.kind() {
            io::ErrorKind::NotFound => JobError::NotFound(error.to_string()),
            io::ErrorKind::PermissionDenied => JobError::PermissionDenied(error.to_string()),
            _ => JobError::Other(error.to_string()),
        }
    }
}
