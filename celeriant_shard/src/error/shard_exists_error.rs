use crate::error::shard_cache_load_error::ShardCacheLoadError;

#[derive(Debug, Clone)]
pub enum ShardAggregateDetailsError {
    AggregateExistsAndCacheError(ShardCacheLoadError),
    AggregateNotExists,
    MetablockReadError(String),
}

impl ShardAggregateDetailsError {
    pub fn error_code(&self) -> &'static str {
        match self {
            Self::AggregateExistsAndCacheError(_) => "cache_load_error",
            Self::AggregateNotExists => "aggregate_not_exists",
            Self::MetablockReadError(_) => "metablock_read_error",
        }
    }
}

impl From<ShardCacheLoadError> for ShardAggregateDetailsError {
    fn from(e: ShardCacheLoadError) -> Self {
        Self::AggregateExistsAndCacheError(e)
    }
}
