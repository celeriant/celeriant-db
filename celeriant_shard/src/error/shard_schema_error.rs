use crate::error::{replication_error::ReplicationError, shard_cache_load_error::ShardCacheLoadError, shard_fsync_error::ShardFsyncError};

#[derive(Debug, Clone)]
pub enum ShardSchemaError {
    SchemaAlreadyExists {
        event_type_major: u64,
        event_type_minor: u64,
    },

    InvalidSchema {
        schema_type: u8,
        parse_error: String,
    },

    UnsupportedSchemaType {
        schema_type: u8,
    },

    ShardCannotAcceptWrites {
        leader_address: Option<String>,
    },

    SchemaCoordinationFailed {
        failed_shard_count: usize,
        total_shards: usize,
    },

    CacheLoadError(ShardCacheLoadError),

    FsyncError(ShardFsyncError),

    ReplicationError(ReplicationError),
}

impl ShardSchemaError {
    pub fn error_code(&self) -> &'static str {
        match self {
            Self::SchemaAlreadyExists { .. } => "schema_already_exists",
            Self::InvalidSchema { .. } => "invalid_schema",
            Self::UnsupportedSchemaType { .. } => "unsupported_schema_type",
            Self::ShardCannotAcceptWrites { .. } => "shard_cannot_accept_writes",
            Self::SchemaCoordinationFailed { .. } => "schema_coordination_failed",
            Self::CacheLoadError(_) => "cache_load_error",
            Self::FsyncError(_) => "fsync_error",
            Self::ReplicationError(_) => "replication_error",
        }
    }
}

impl From<ShardCacheLoadError> for ShardSchemaError {
    fn from(e: ShardCacheLoadError) -> Self {
        Self::CacheLoadError(e)
    }
}

impl From<ShardFsyncError> for ShardSchemaError {
    fn from(e: ShardFsyncError) -> Self {
        Self::FsyncError(e)
    }
}

impl From<ReplicationError> for ShardSchemaError {
    fn from(e: ReplicationError) -> Self {
        Self::ReplicationError(e)
    }
}
