// use celeriant_disk::files::read_fixed_records_visit_const::ReadVisitError;
// use celeriant_rotating_log::{rotating_log_error::RotatingLogError, rwlock_timeout::LockTimeoutError};
// use celeriant_watch::aggregate_reader::WatchReadError;
// use celeriant_wire::wire_format_error::WireFormatError;
// use glommio::GlommioError;

// use crate::error::shard_cache_load_error::ShardCacheError;

// #[derive(Debug, Clone)]
// pub enum ShardReadError {
//     IoError(String),
//     MaxBytesTooSmall {
//         current_max_bytes: u64,
//         required_max_bytes: u64,
//     },
//     SerializationError(WireFormatError),
//     UnavailableBatchIndex {
//         minimum_available: u64,
//         requested: u64,
//     },
//     AggregateNotExists,
// }

// impl From<LockTimeoutError> for ShardReadError {
//     fn from(e: LockTimeoutError) -> Self {
//         Self::IoError(e.to_string())
//     }
// }

// impl From<RotatingLogError> for ShardReadError {
//     fn from(value: RotatingLogError) -> Self {
//         match value {
//             RotatingLogError::IoError(e) => ShardReadError::IoError(e),
//             _ => ShardReadError::IoError(value.to_string()),
//         }
//     }
// }

// impl From<ShardCacheError> for ShardReadError {
//     fn from(value: ShardCacheError) -> Self {
//         match value {
//             ShardCacheError::IoError(error) => ShardReadError::IoError(error.to_string()),
//         }
//     }
// }

// impl From<std::io::Error> for ShardReadError {
//     fn from(error: std::io::Error) -> Self {
//         ShardReadError::IoError(error.to_string())
//     }
// }

// impl From<WireFormatError> for ShardReadError {
//     fn from(error: WireFormatError) -> Self {
//         ShardReadError::SerializationError(error)
//     }
// }

// impl From<GlommioError<()>> for ShardReadError {
//     fn from(error: GlommioError<()>) -> Self {
//         ShardReadError::IoError(error.to_string())
//     }
// }

// /// Push the ReadVisitError (io or deserialisation errors) into ReadError
// impl From<ReadVisitError<ShardReadError>> for ShardReadError {
//     fn from(error: ReadVisitError<ShardReadError>) -> Self {
//         match error {
//             ReadVisitError::Io(glommio_error) => ShardReadError::IoError(glommio_error.to_string()),
//             ReadVisitError::Visitor(e) => e,
//         }
//     }
// }

// impl From<WatchReadError> for ShardReadError {
//     fn from(error: WatchReadError) -> Self {
//         match error {
//             WatchReadError::Io(msg) => ShardReadError::IoError(msg),
//             WatchReadError::Serialization(e) => ShardReadError::SerializationError(e),
//             WatchReadError::Other(msg) => ShardReadError::IoError(msg),
//         }
//     }
// }