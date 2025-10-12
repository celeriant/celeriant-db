use std::fmt;

#[derive(Debug)]
pub enum MetadataError {
    /// Database connection or execution error
    DatabaseError(async_sqlite::Error),
    /// Query preparation or execution failed
    QueryError(String),
    /// Row parsing or type conversion error
    RowError(String),
    /// Schema migration is not supported
    UnsupportedMigration(u32, u32),
    /// IO error (for file operations)
    IoError(std::io::Error),
    /// Configuration error
    ConfigError(String),
    /// Permission denied
    PermissionDenied(String),
    /// Resource not found
    NotFound(String),
    /// Invalid input or state
    InvalidInput(String),
}

impl fmt::Display for MetadataError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetadataError::DatabaseError(err) => {
                write!(f, "Database error: {}", err)
            }
            MetadataError::QueryError(msg) => {
                write!(f, "Query error: {}", msg)
            }
            MetadataError::RowError(msg) => {
                write!(f, "Row parsing error: {}", msg)
            }
            MetadataError::UnsupportedMigration(from, to) => {
                write!(f, "Unsupported migration from version {} to {}", from, to)
            }
            MetadataError::IoError(err) => {
                write!(f, "IO error: {}", err)
            }
            MetadataError::ConfigError(msg) => {
                write!(f, "Configuration error: {}", msg)
            }
            MetadataError::PermissionDenied(msg) => {
                write!(f, "Permission denied: {}", msg)
            }
            MetadataError::NotFound(msg) => {
                write!(f, "Not found: {}", msg)
            }
            MetadataError::InvalidInput(msg) => {
                write!(f, "Invalid input: {}", msg)
            }
        }
    }
}

impl std::error::Error for MetadataError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            MetadataError::DatabaseError(err) => Some(err),
            MetadataError::IoError(err) => Some(err),
            _ => None,
        }
    }
}

impl From<async_sqlite::Error> for MetadataError {
    fn from(err: async_sqlite::Error) -> Self {
        MetadataError::DatabaseError(err)
    }
}

// Conversion from std::io::Error
impl From<std::io::Error> for MetadataError {
    fn from(err: std::io::Error) -> Self {
        MetadataError::IoError(err)
    }
}

// Helper methods for common error creation
impl MetadataError {
    pub fn query_failed<S: Into<String>>(msg: S) -> Self {
        MetadataError::QueryError(msg.into())
    }

    pub fn row_parse_failed<S: Into<String>>(msg: S) -> Self {
        MetadataError::RowError(msg.into())
    }

    pub fn config_error<S: Into<String>>(msg: S) -> Self {
        MetadataError::ConfigError(msg.into())
    }

    pub fn permission_denied<S: Into<String>>(msg: S) -> Self {
        MetadataError::PermissionDenied(msg.into())
    }

    pub fn not_found<S: Into<String>>(msg: S) -> Self {
        MetadataError::NotFound(msg.into())
    }

    pub fn invalid_input<S: Into<String>>(msg: S) -> Self {
        MetadataError::InvalidInput(msg.into())
    }
}

/// Result type alias for metadata operations
pub type MetadataResult<T> = Result<T, MetadataError>;
