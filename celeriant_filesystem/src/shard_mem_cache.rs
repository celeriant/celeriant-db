use std::collections::{HashMap};

use celeriant_wal::{aggregate_key::AggregateKey, shard_log::shard_log_checkpoint::ShardLogCheckpoint, wal::event_batch_metadata::EventBatchMetadata};

use crate::event_batch_cached_item::EventBatchCachedItem;


/// All this data needs to be updated atomically during the disk write / fsync step
/// Metadata will be in both aggregate_metadata and recent_writes_cache but we 
/// are ok with the clone cost
pub struct ShardMemCache {

    /// Used to determine the next indexes for each aggregate
    /// This struct is serializable and gets appended to the end of the file
    /// This only contains aggregates present in the latest active log
    pub active_shard_log_checkpoint: ShardLogCheckpoint,

    /// This cache has an option to be None for an aggregate
    /// This means the aggregate exists, but we don't have the cache hot
    pub aggregate_metadata: HashMap<AggregateKey, HashMap<u64, Option<Vec<EventBatchMetadata>>>>,

    pub aggregate_client_event_indexes: HashMap<AggregateKey, HashMap<u128, u64>>,

    /// Recent writes per aggregate. It's not an LRU as we need to control
    /// memory use at the shard level by what we have stored, not by batch count
    pub recent_writes_cache: HashMap<AggregateKey, Vec<EventBatchCachedItem>>,
}