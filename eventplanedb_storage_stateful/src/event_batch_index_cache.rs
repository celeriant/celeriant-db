use lru::LruCache;
use std::num::NonZeroUsize;

/// Simple cache for storing the last event batch index per aggregate
/// Uses LRU eviction based on number of aggregates
pub struct EventBatchIndexCache {
    cache: LruCache<u128, u64>,
}

impl EventBatchIndexCache {
    /// Create a new cache with a maximum number of aggregates to store
    pub fn new(max_aggregates: usize) -> Self {
        let capacity =
            NonZeroUsize::new(max_aggregates).unwrap_or_else(|| NonZeroUsize::new(1000).unwrap());

        Self {
            cache: LruCache::new(capacity),
        }
    }

    /// Get the last event batch index for an aggregate
    /// Returns None if the aggregate is not in cache
    pub fn get(&mut self, aggregate_id: u128) -> Option<u64> {
        self.cache.get(&aggregate_id).copied()
    }

    /// Set the last event batch index for an aggregate
    /// Returns the previous value if it existed
    pub fn set(&mut self, aggregate_id: u128, event_batch_index: u64) -> Option<u64> {
        self.cache.put(aggregate_id, event_batch_index)
    }

    /// Remove an aggregate from the cache
    /// Returns the previous value if it existed
    pub fn remove(&mut self, aggregate_id: u128) -> Option<u64> {
        self.cache.pop(&aggregate_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_operations() {
        let mut cache = EventBatchIndexCache::new(3);

        // Test set and get
        assert_eq!(cache.set(111, 5), None);
        assert_eq!(cache.get(111), Some(5));
        assert_eq!(cache.get(765), None);

        // Test update
        assert_eq!(cache.set(111, 10), Some(5));
        assert_eq!(cache.get(111), Some(10));

        // Test remove
        assert_eq!(cache.remove(111), Some(10));
        assert_eq!(cache.remove(111), None);
        assert_eq!(cache.get(111), None);
    }

    #[test]
    fn test_lru_eviction() {
        let mut cache = EventBatchIndexCache::new(2);

        // Fill cache to capacity
        cache.set(111, 1);
        cache.set(222, 2);

        // Access aggregate1 to make it most recently used
        cache.get(111);

        // Add another aggregate - should evict aggregate2 (LRU)
        cache.set(333, 3);

        // aggregate2 should be evicted, aggregate1 and aggregate3 should remain
        assert_eq!(cache.get(111), Some(1));
        assert_eq!(cache.get(222), None);
        assert_eq!(cache.get(333), Some(3));
    }
}
