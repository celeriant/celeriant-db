use lru::LruCache;
use std::num::NonZeroUsize;

use crate::aggregate_key::AggregateKey;

/// Cache for storing the last event batch index per aggregate
/// Uses LRU eviction based on number of aggregates
/// Properly isolates by org_id and aggregate_type_id
pub struct EventBatchIndexCache {
    cache: LruCache<AggregateKey, u64>,
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
    pub fn get(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
    ) -> Option<u64> {
        let key = AggregateKey::new(org_id, aggregate_type_id, aggregate_id);
        self.cache.get(&key).copied()
    }

    /// Set the last event batch index for an aggregate
    /// Returns the previous value if it existed
    pub fn set(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        event_batch_index: u64,
    ) -> Option<u64> {
        let key = AggregateKey::new(org_id, aggregate_type_id, aggregate_id);
        self.cache.put(key, event_batch_index)
    }

    /// Update the event_batch_index only if the new value is higher
    /// Returns true if the value was updated, false if the existing value was higher or equal
    pub fn update_if_higher(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        event_batch_index: u64,
    ) -> bool {
        let key = AggregateKey::new(org_id, aggregate_type_id, aggregate_id);

        match self.cache.get(&key) {
            Some(&existing_index) if existing_index >= event_batch_index => {
                // Don't update, existing value is higher or equal
                false
            }
            _ => {
                // Update with new higher value or first value
                self.cache.put(key, event_batch_index);
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
    /// Returns the evicted (org_id, aggregate_type_id, aggregate_id, event_batch_index) if any
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
        let mut cache = EventBatchIndexCache::new(3);

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
        let mut cache = EventBatchIndexCache::new(100);

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
    fn test_lru_eviction() {
        let mut cache = EventBatchIndexCache::new(2);

        // Fill cache to capacity
        cache.set(554, 665, 111, 1);
        cache.set(554, 665, 222, 2);

        // Access first aggregate to make it most recently used
        cache.get(554, 665, 111);

        // Add another aggregate - should evict aggregate 222 (LRU)
        cache.set(554, 665, 333, 3);

        // aggregate 222 should be evicted, 111 and 333 should remain
        assert_eq!(cache.get(554, 665, 111), Some(1));
        assert_eq!(cache.get(554, 665, 222), None);
        assert_eq!(cache.get(554, 665, 333), Some(3));
    }

    #[test]
    fn test_contains() {
        let mut cache = EventBatchIndexCache::new(100);

        assert!(!cache.contains(554, 665, 111));

        cache.set(554, 665, 111, 5);
        assert!(cache.contains(554, 665, 111));
        assert!(!cache.contains(554, 665, 222));
        assert!(!cache.contains(554, 666, 111)); // Different aggregate type
        assert!(!cache.contains(555, 665, 111)); // Different org
    }

    #[test]
    fn test_org_isolation() {
        let mut cache = EventBatchIndexCache::new(100);

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

        // Remove one org's entry shouldn't affect others
        assert_eq!(cache.remove(2, 100, 200), Some(20));
        assert_eq!(cache.get(1, 100, 200), Some(15));
        assert_eq!(cache.get(2, 100, 200), None);
        assert_eq!(cache.get(3, 100, 200), Some(30));
    }

    #[test]
    fn test_aggregate_type_isolation() {
        let mut cache = EventBatchIndexCache::new(100);

        // Same org_id and aggregate_id across different aggregate_type_ids
        cache.set(1, 100, 200, 10);
        cache.set(1, 101, 200, 20);
        cache.set(1, 102, 200, 30);

        // Each aggregate type should have its own isolated entry
        assert_eq!(cache.get(1, 100, 200), Some(10));
        assert_eq!(cache.get(1, 101, 200), Some(20));
        assert_eq!(cache.get(1, 102, 200), Some(30));

        // Updating one aggregate type shouldn't affect others
        cache.set(1, 101, 200, 25);
        assert_eq!(cache.get(1, 100, 200), Some(10));
        assert_eq!(cache.get(1, 101, 200), Some(25));
        assert_eq!(cache.get(1, 102, 200), Some(30));

        // Remove one aggregate type's entry shouldn't affect others
        assert_eq!(cache.remove(1, 102, 200), Some(30));
        assert_eq!(cache.get(1, 100, 200), Some(10));
        assert_eq!(cache.get(1, 101, 200), Some(25));
        assert_eq!(cache.get(1, 102, 200), None);
    }

    #[test]
    fn test_cross_org_aggregate_type_isolation() {
        let mut cache = EventBatchIndexCache::new(100);

        // Matrix of different orgs and aggregate types with same aggregate_id
        cache.set(1, 100, 999, 11);
        cache.set(1, 101, 999, 12);
        cache.set(2, 100, 999, 21);
        cache.set(2, 101, 999, 22);

        // Verify complete isolation
        assert_eq!(cache.get(1, 100, 999), Some(11));
        assert_eq!(cache.get(1, 101, 999), Some(12));
        assert_eq!(cache.get(2, 100, 999), Some(21));
        assert_eq!(cache.get(2, 101, 999), Some(22));

        // Update one combination shouldn't affect others
        cache.set(1, 100, 999, 111);
        assert_eq!(cache.get(1, 100, 999), Some(111));
        assert_eq!(cache.get(1, 101, 999), Some(12));
        assert_eq!(cache.get(2, 100, 999), Some(21));
        assert_eq!(cache.get(2, 101, 999), Some(22));
    }

    #[test]
    fn test_update_if_higher_with_isolation() {
        let mut cache = EventBatchIndexCache::new(100);

        // Set initial values across isolated contexts
        cache.set(1, 100, 999, 10);
        cache.set(1, 101, 999, 20); // Same org, different aggregate type
        cache.set(2, 100, 999, 30); // Different org, same aggregate type

        // Update one context with higher value
        assert!(cache.update_if_higher(1, 100, 999, 15));
        assert_eq!(cache.get(1, 100, 999), Some(15));

        // Other contexts should be unaffected
        assert_eq!(cache.get(1, 101, 999), Some(20));
        assert_eq!(cache.get(2, 100, 999), Some(30));

        // Try to update with lower value - should fail
        assert!(!cache.update_if_higher(1, 100, 999, 12));
        assert_eq!(cache.get(1, 100, 999), Some(15));

        // Update different context with lower value than its current - should fail
        assert!(!cache.update_if_higher(1, 101, 999, 15));
        assert_eq!(cache.get(1, 101, 999), Some(20));

        // Update different context with higher value - should succeed
        assert!(cache.update_if_higher(2, 100, 999, 35));
        assert_eq!(cache.get(2, 100, 999), Some(35));
    }

    #[test]
    fn test_cache_stats() {
        let mut cache = EventBatchIndexCache::new(100);

        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
        assert_eq!(cache.capacity(), 100);

        cache.set(554, 665, 111, 5);
        assert_eq!(cache.len(), 1);
        assert!(!cache.is_empty());

        cache.clear();
        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
    }

    #[test]
    fn test_force_evict_lru() {
        let mut cache = EventBatchIndexCache::new(3);

        cache.set(554, 665, 111, 1);
        cache.set(554, 665, 222, 2);
        cache.set(554, 665, 333, 3);

        // Access middle entry to change LRU order
        cache.get(554, 665, 222);

        // Force evict should remove LRU entry
        let evicted = cache.force_evict_lru();
        assert!(evicted.is_some());
        let (org_id, aggregate_type_id, aggregate_id, index) = evicted.unwrap();
        assert_eq!(
            (org_id, aggregate_type_id, aggregate_id, index),
            (554, 665, 111, 1)
        );

        // Verify it was actually removed
        assert_eq!(cache.get(554, 665, 111), None);
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_cache_key_uniqueness() {
        let mut cache = EventBatchIndexCache::new(100);

        // Create entries that would collide if we didn't have proper isolation
        let combinations = vec![
            (1, 100, 200, 1000),
            (1, 100, 201, 1001),
            (1, 101, 200, 1002),
            (2, 100, 200, 1003),
        ];

        // Add all combinations
        for (org_id, aggregate_type_id, aggregate_id, value) in &combinations {
            cache.set(*org_id, *aggregate_type_id, *aggregate_id, *value);
        }

        // Verify all combinations are stored independently
        for (org_id, aggregate_type_id, aggregate_id, expected_value) in combinations {
            assert_eq!(
                cache.get(org_id, aggregate_type_id, aggregate_id),
                Some(expected_value)
            );
        }

        assert_eq!(cache.len(), 4);
    }
}
