//! Error types for object store operations.

use std::fmt;

/// Categories of object store errors for handling decisions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    /// Transient error that may succeed on retry.
    Retryable,
    /// Permanent error that will not succeed on retry.
    Permanent,
    /// Authentication or authorization failure.
    Auth,
    /// Operation timed out.
    Timeout,
    /// Precondition failed (e.g., ETag mismatch for conditional PUT).
    PreconditionFailed,
    /// Resource not found.
    NotFound,
    /// The sidecar runtime is unavailable.
    SidecarUnavailable,
    /// Channel is full, backpressure should be applied.
    ChannelFull,
}

/// Error returned from object store operations.
#[derive(Clone, Debug)]
pub struct SidecarError {
    pub kind: ErrorKind,
    pub message: String,
    /// Suggested retry delay in milliseconds, if applicable.
    pub retry_after_ms: Option<u64>,
}

impl SidecarError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            retry_after_ms: None,
        }
    }

    pub fn with_retry_after(mut self, ms: u64) -> Self {
        self.retry_after_ms = Some(ms);
        self
    }

    pub fn retryable(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Retryable, message)
    }

    pub fn permanent(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Permanent, message)
    }

    pub fn auth(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Auth, message)
    }

    pub fn timeout(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Timeout, message)
    }

    pub fn precondition_failed(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::PreconditionFailed, message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::NotFound, message)
    }

    pub fn sidecar_unavailable(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::SidecarUnavailable, message)
    }

    pub fn channel_full(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::ChannelFull, message)
    }

    pub fn is_retryable(&self) -> bool {
        matches!(self.kind, ErrorKind::Retryable | ErrorKind::Timeout)
    }
}

impl fmt::Display for SidecarError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for SidecarError {}