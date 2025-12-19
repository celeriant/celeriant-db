use std::rc::Rc;

use celeriant_wire::wire_format_error::WireFormatError;

use crate::aggregate_watchers::AggregateWatchers;

/// Error type for watch-related read operations
#[derive(Debug)]
pub enum WatchReadError {
    Io(String),
    Serialization(WireFormatError),
    Other(String),
}

impl std::fmt::Display for WatchReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WatchReadError::Io(s) => write!(f, "IO error: {}", s),
            WatchReadError::Serialization(s) => write!(f, "Serialization error: {:?}", s),
            WatchReadError::Other(s) => write!(f, "{}", s),
        }
    }
}

impl std::error::Error for WatchReadError {}

/// Trait for reading aggregates - implemented by ShardWriteAheadLog
pub trait AggregateReader {
    fn watched_aggregates(&self) -> Rc<AggregateWatchers>;
}