#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("S3 not configured")]
    S3NotConfigured,

    #[error("Object not found: {path}")]
    NotFound { path: String },

    #[error("Object already exists: {path}")]
    AlreadyExists { path: String },

    #[error("Precondition failed: {path}")]
    PreconditionFailed { path: String },

    #[error("S3 error: {message}")]
    S3Error { message: String },

    #[error("Invalid path: {path}")]
    InvalidPath { path: String },

    #[error("Unknown error: {message}")]
    Unknown { message: String },
}

impl StoreError {
    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::NotFound { .. } => ErrorKind::NotFound,
            Self::AlreadyExists { .. } => ErrorKind::AlreadyExists,
            Self::PreconditionFailed { .. } => ErrorKind::PreconditionFailed,
            Self::S3NotConfigured => ErrorKind::Configuration,
            Self::S3Error { .. } => ErrorKind::S3,
            Self::InvalidPath { .. } => ErrorKind::InvalidPath,
            Self::Unknown { .. } => ErrorKind::Unknown,
        }
    }
}

/// Categories of object store errors for handling decisions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    NotFound,
    AlreadyExists,
    PreconditionFailed,
    Configuration,
    S3,
    InvalidPath,
    Unknown,
}

impl From<object_store::Error> for StoreError {
    fn from(err: object_store::Error) -> Self {
        match err {
            object_store::Error::NotFound { path, .. } => Self::NotFound { path },
            object_store::Error::AlreadyExists { path, .. } => Self::AlreadyExists { path },
            object_store::Error::Precondition { path, .. } => Self::PreconditionFailed { path },
            object_store::Error::InvalidPath { source } => Self::InvalidPath {
                path: source.to_string(),
            },
            other => Self::S3Error {
                message: other.to_string(),
            },
        }
    }
}

impl From<object_store::path::Error> for StoreError {
    fn from(err: object_store::path::Error) -> Self {
        Self::InvalidPath {
            path: err.to_string(),
        }
    }
}