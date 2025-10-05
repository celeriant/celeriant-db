use lru::LruCache;
use std::num::NonZeroUsize;

use crate::aggregate_key::AggregateKey;

/// Cache for storing the last event index per aggregate
/// Uses LRU eviction based on number of aggregates
/// Properly isolates by org_id and aggregate_type_id
pub struct EventIndexCache {
    cache: LruCache<AggregateKey, u64>,
}

impl EventIndexCache {
    /// Create a new cache with a maximum number of aggregates to store
    pub fn new(max_aggregates: usize) -> Self {
        let capacity =
            NonZeroUsize::new(max_aggregates).unwrap_or_else(|| NonZeroUsize::new(1000).unwrap());

        Self {
            cache: LruCache::new(capacity),
        }
    }

    /// Get the last event index for an aggregate
    /// Returns None if the aggregate is not in cache
    pub fn get(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
    ) -> Option<u64> {
        let key = AggregateKey::new(org_id, aggregate_type_id, aggregate_id);
        self.cache.get(&key).copied()
    }

    /// Set the last event index for an aggregate
    /// Returns the previous value if it existed
    pub fn set(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        event_index: u64,
    ) -> Option<u64> {
        let key = AggregateKey::new(org_id, aggregate_type_id, aggregate_id);
        self.cache.put(key, event_index)
    }

    /// Update the event_index only if the new value is higher
    /// Returns true if the value was updated, false if the existing value was higher or equal
    pub fn update_if_higher(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        event_index: u64,
    ) -> bool {
        let key = AggregateKey::new(org_id, aggregate_type_id, aggregate_id);

        match self.cache.get(&key) {
            Some(&existing_index) if existing_index >= event_index => {
                // Don't update, existing value is higher or equal
                false
            }
            _ => {
                // Update with new higher value or first value
                self.cache.put(key, event_index);
                true
            }
        }
    }

    /// Remove an aggregate from the cache
    /// Returns the previous value if it existed
    pub fn remove(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
    ) -> Option<u64> {
        let key = AggregateKey::new(org_id, aggregate_type_id, aggregate_id);
        self.cache.pop(&key)
    }

    /// Check if an aggregate exists in the cache without affecting LRU order
    pub fn contains(&self, org_id: u128, aggregate_type_id: u128, aggregate_id: u128) -> bool {
        let key = AggregateKey::new(org_id, aggregate_type_id, aggregate_id);
        self.cache.contains(&key)
    }

    /// Get the number of aggregates currently cached
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Check if the cache is empty
    pub fn is_empty(&self) -> bool {
        self.cache.len() == 0
    }

    /// Clear all cached data
    pub fn clear(&mut self) {
        self.cache.clear();
    }

    /// Get cache capacity
    pub fn capacity(&self) -> usize {
        self.cache.cap().get()
    }

    /// Peek at the least recently used entry without affecting LRU order
    pub fn peek_lru(&self) -> Option<(u128, u128, u128, u64)> {
        self.cache
            .peek_lru()
            .map(|(key, &value)| (key.org_id, key.aggregate_type_id, key.aggregate_id, value))
    }

    /// Force eviction of the least recently used entry
    /// Returns the evicted (org_id, aggregate_type_id, aggregate_id, event_index) if any
    pub fn force_evict_lru(&mut self) -> Option<(u128, u128, u128, u64)> {
        self.cache
            .pop_lru()
            .map(|(key, value)| (key.org_id, key.aggregate_type_id, key.aggregate_id, value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_operations() {
        let mut cache = EventIndexCache::new(3);

        // Test set and get
        assert_eq!(cache.set(554, 665, 111, 5), None);
        assert_eq!(cache.get(554, 665, 111), Some(5));
        assert_eq!(cache.get(554, 665, 765), None);
        assert_eq!(cache.get(554, 666, 111), None); // Different aggregate type
        assert_eq!(cache.get(555, 665, 111), None); // Different org

        // Test update
        assert_eq!(cache.set(554, 665, 111, 10), Some(5));
        assert_eq!(cache.get(554, 665, 111), Some(10));

        // Test remove
        assert_eq!(cache.remove(554, 665, 111), Some(10));
        assert_eq!(cache.remove(554, 665, 111), None);
        assert_eq!(cache.get(554, 665, 111), None);
    }

    #[test]
    fn test_update_if_higher() {
        let mut cache = EventIndexCache::new(100);

        // First update should succeed (no existing value)
        assert!(cache.update_if_higher(554, 665, 111, 5));
        assert_eq!(cache.get(554, 665, 111), Some(5));

        // Higher value should update
        assert!(cache.update_if_higher(554, 665, 111, 10));
        assert_eq!(cache.get(554, 665, 111), Some(10));

        // Lower value should not update
        assert!(!cache.update_if_higher(554, 665, 111, 7));
        assert_eq!(cache.get(554, 665, 111), Some(10));

        // Equal value should not update
        assert!(!cache.update_if_higher(554, 665, 111, 10));
        assert_eq!(cache.get(554, 665, 111), Some(10));

        // Higher value should update again
        assert!(cache.update_if_higher(554, 665, 111, 15));
        assert_eq!(cache.get(554, 665, 111), Some(15));
    }

    #[test]
    fn test_org_isolation() {
        let mut cache = EventIndexCache::new(100);

        // Same aggregate_type_id and aggregate_id across different orgs
        cache.set(1, 100, 200, 10);
        cache.set(2, 100, 200, 20);
        cache.set(3, 100, 200, 30);

        // Each org should have its own isolated entry
        assert_eq!(cache.get(1, 100, 200), Some(10));
        assert_eq!(cache.get(2, 100, 200), Some(20));
        assert_eq!(cache.get(3, 100, 200), Some(30));

        // Updating one org shouldn't affect others
        cache.set(1, 100, 200, 15);
        assert_eq!(cache.get(1, 100, 200), Some(15));
        assert_eq!(cache.get(2, 100, 200), Some(20));
        assert_eq!(cache.get(3, 100, 200), Some(30));
    }

    #[test]
    fn test_aggregate_type_isolation() {
        let mut cache = EventIndexCache::new(100);

        // Same org_id and aggregate_id across different aggregate_type_ids
        cache.set(1, 100, 200, 10);
        cache.set(1, 101, 200, 20);
        cache.set(1, 102, 200, 30);

        // Each aggregate type should have its own isolated entry
        assert_eq!(cache.get(1, 100, 200), Some(10));
        assert_eq!(cache.get(1, 101, 200), Some(20));
        assert_eq!(cache.get(1, 102, 200), Some(30));
    }
}