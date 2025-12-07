use core::fmt;

#[derive(Clone, Debug)]
pub struct StoreError {
    pub kind: ErrorKind,
    pub message: String,
}

/// Categories of object store errors for handling decisions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    Unknown,
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for StoreError {}