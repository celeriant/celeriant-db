
/// Errors that can occur during rollback operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RollbackError {
    /// Failed to fsync the rolled-back header (CRITICAL - durability loss).
    HeaderFsyncFailed(String),
}