use crate::error::shard_cache_load_error::ShardCacheLoadError;

#[derive(Debug, Clone)]
pub enum ShardAggregateDetailsError {
    AggregateExistsAndCacheError(ShardCacheLoadError),
    AggregateNotExists,
    MetablockReadError(String),
}

impl From<ShardCacheLoadError> for ShardAggregateDetailsError {
    fn from(e: ShardCacheLoadError) -> Self {
        Self::AggregateExistsAndCacheError(e)
    }
}
