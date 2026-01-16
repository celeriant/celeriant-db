/// Specifies which cache path to use for lookups
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CachePath {
    /// Read path - only sees replicated data
    Read,
    /// Write path - sees all durable data (for OCC, idempotency)
    Write,
}
