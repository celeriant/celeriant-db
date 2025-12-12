//! Error types for object store operations.

use std::fmt;
use celeriant_sidecar::error::StoreError;

/// Categories of object store errors for handling decisions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    /// Permanent error that will not succeed on retry.
    TokioRuntimeFailure,
    ChannelClosed,
    StoreError,
}

/// Error returned from object store operations.
#[derive(Clone, Debug)]
pub struct SidecarError {
    pub kind: ErrorKind,
    pub message: String,
}

impl SidecarError {
    pub fn tokio_runtime_failure(message: String) -> Self {
        Self { kind: ErrorKind::TokioRuntimeFailure, message }
    }
    
    pub(crate) fn channel_closed(message: String) -> SidecarError {
        Self { kind: ErrorKind::ChannelClosed, message }
    }
}

impl From<StoreError> for SidecarError {
    fn from(value: StoreError) -> Self {
        SidecarError { kind: ErrorKind::StoreError, message: value.message }
    }
}

impl fmt::Display for SidecarError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for SidecarError {}