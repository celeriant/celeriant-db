use crate::error::shard_cache_load_error::ShardCacheLoadError;

#[derive(Debug, Clone)]
pub enum ShardExistsError {
    AggregateExistsAndCacheError(ShardCacheLoadError),
}

impl From<ShardCacheLoadError> for ShardExistsError {
    fn from(e: ShardCacheLoadError) -> Self {
        Self::AggregateExistsAndCacheError(e)
    }
}