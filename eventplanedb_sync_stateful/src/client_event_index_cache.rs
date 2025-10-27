use lru::LruCache;
use std::num::NonZeroUsize;

use crate::aggregate_client_key::AggregateClientKey;

/// Cache for tracking the highest client_event_index seen from each client within each aggregate
/// Uses LRU eviction based on number of client/aggregate combinations
/// Optimized for producer idempotency checks during writes
pub struct ClientEventIndexCache {
    cache: LruCache<AggregateClientKey, u64>,
}

impl ClientEventIndexCache {
    /// Create a new cache with a maximum number of client/aggregate combinations to store
    pub fn new(max_clients: usize) -> Self {
        let capacity =
            NonZeroUsize::new(max_clients).unwrap_or_else(|| NonZeroUsize::new(10000).unwrap());

        Self {
            cache: LruCache::new(capacity),
        }
    }

    /// Get the highest client_event_index seen for a specific client in an aggregate
    /// Returns None if this client hasn't written to this aggregate before
    pub fn get(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        client_id: u128,
    ) -> Option<u64> {
        let key = AggregateClientKey::new(org_id, aggregate_type_id, aggregate_id, client_id);
        self.cache.get(&key).copied()
    }

    /// Set the highest client_event_index for a client in an aggregate
    /// Returns the previous value if it existed
    pub fn set(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        client_id: u128,
        client_event_index: u64,
    ) -> Option<u64> {
        let key = AggregateClientKey::new(org_id, aggregate_type_id, aggregate_id, client_id);
        self.cache.put(key, client_event_index)
    }

    /// Update the client_event_index only if the new value is higher
    /// Returns true if the value was updated, false if the existing value was higher or equal
    pub fn update_if_higher(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        client_id: u128,
        client_event_index: u64,
    ) -> bool {
        let key = AggregateClientKey::new(org_id, aggregate_type_id, aggregate_id, client_id);

        match self.cache.get(&key) {
            Some(&existing_index) if existing_index >= client_event_index => {
                // Don't update, existing value is higher or equal
                false
            }
            _ => {
                // Update with new higher value or first value
                self.cache.put(key, client_event_index);
                true
            }
        }
    }

    /// Remove a specific client from an aggregate
    /// Returns the previous value if it existed
    pub fn remove_client(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        client_id: u128,
    ) -> Option<u64> {
        let key = AggregateClientKey::new(org_id, aggregate_type_id, aggregate_id, client_id);
        self.cache.pop(&key)
    }

    /// Remove all clients for a specific aggregate
    /// Returns the number of clients that were removed
    pub fn remove_aggregate(&mut self, aggregate_id: u128) -> usize {
        let keys_to_remove: Vec<AggregateClientKey> = self
            .cache
            .iter()
            .filter(|(key, _)| key.aggregate_id == aggregate_id)
            .map(|(key, _)| key.clone())
            .collect();

        let count = keys_to_remove.len();
        for key in keys_to_remove {
            self.cache.pop(&key);
        }
        count
    }

    /// Check if a client exists in the cache for a specific aggregate without affecting LRU order
    pub fn contains_client(
        &self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        client_id: u128,
    ) -> bool {
        let key = AggregateClientKey::new(org_id, aggregate_type_id, aggregate_id, client_id);
        self.cache.contains(&key)
    }

    /// Get all clients for a specific aggregate
    /// Returns a vector of (client_id, highest_client_event_index) pairs
    pub fn get_clients_for_aggregate(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
    ) -> Vec<(u128, u64)> {
        self.cache
            .iter()
            .filter(|(key, _)| {
                key.org_id == org_id
                    && key.aggregate_type_id == aggregate_type_id
                    && key.aggregate_id == aggregate_id
            })
            .map(|(key, &value)| (key.client_id, value))
            .collect()
    }

    /// Get the number of client/aggregate combinations currently cached
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
    pub fn peek_lru(&self) -> Option<(u128, u128, u64)> {
        self.cache
            .peek_lru()
            .map(|(key, &value)| (key.aggregate_id, key.client_id, value))
    }

    /// Force eviction of the least recently used entry
    /// Returns the evicted (aggregate_id, client_id, client_event_index) if any
    pub fn force_evict_lru(&mut self) -> Option<(u128, u128, u64)> {
        self.cache
            .pop_lru()
            .map(|(key, value)| (key.aggregate_id, key.client_id, value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_operations() {
        let mut cache = ClientEventIndexCache::new(100);

        // Test set and get
        assert_eq!(cache.set(554, 665, 111, 123, 5), None);
        assert_eq!(cache.get(554, 665, 111, 123), Some(5));
        assert_eq!(cache.get(554, 665, 111, 456), None);
        assert_eq!(cache.get(554, 665, 222, 123), None);

        // Test update
        assert_eq!(cache.set(554, 665, 111, 123, 10), Some(5));
        assert_eq!(cache.get(554, 665, 111, 123), Some(10));

        // Test different client same aggregate
        assert_eq!(cache.set(554, 665, 111, 456, 3), None);
        assert_eq!(cache.get(554, 665, 111, 456), Some(3));
        assert_eq!(cache.get(554, 665, 111, 123), Some(10)); // Should still exist

        // Test same client different aggregate
        assert_eq!(cache.set(554, 665, 222, 123, 7), None);
        assert_eq!(cache.get(554, 665, 222, 123), Some(7));
        assert_eq!(cache.get(554, 665, 111, 123), Some(10)); // Should still exist
    }

    #[test]
    fn test_update_if_higher() {
        let mut cache = ClientEventIndexCache::new(100);

        // First update should succeed (no existing value)
        assert!(cache.update_if_higher(554, 665, 111, 123, 5));
        assert_eq!(cache.get(554, 665, 111, 123), Some(5));

        // Higher value should update
        assert!(cache.update_if_higher(554, 665, 111, 123, 10));
        assert_eq!(cache.get(554, 665, 111, 123), Some(10));

        // Lower value should not update
        assert!(!cache.update_if_higher(554, 665, 111, 123, 7));
        assert_eq!(cache.get(554, 665, 111, 123), Some(10));

        // Equal value should not update
        assert!(!cache.update_if_higher(554, 665, 111, 123, 10));
        assert_eq!(cache.get(554, 665, 111, 123), Some(10));

        // Higher value should update again
        assert!(cache.update_if_higher(554, 665, 111, 123, 15));
        assert_eq!(cache.get(554, 665, 111, 123), Some(15));
    }

    #[test]
    fn test_remove_operations() {
        let mut cache = ClientEventIndexCache::new(100);

        // Setup test data
        cache.set(554, 665, 111, 123, 5);
        cache.set(554, 665, 111, 456, 3);
        cache.set(554, 665, 222, 123, 7);
        cache.set(554, 665, 222, 789, 9);

        // Test remove client
        assert_eq!(cache.remove_client(554, 665, 111, 123), Some(5));
        assert_eq!(cache.remove_client(554, 665, 111, 123), None);
        assert_eq!(cache.get(554, 665, 111, 456), Some(3)); // Should still exist

        // Test remove aggregate
        let removed_count = cache.remove_aggregate(222);
        assert_eq!(removed_count, 2);
        assert_eq!(cache.get(554, 665, 222, 123), None);
        assert_eq!(cache.get(554, 665, 222, 789), None);
        assert_eq!(cache.get(554, 665, 111, 456), Some(3)); // Should still exist
    }

    #[test]
    fn test_get_clients_for_aggregate() {
        let mut cache = ClientEventIndexCache::new(100);

        cache.set(554, 665, 111, 123, 5);
        cache.set(554, 665, 111, 456, 3);
        cache.set(554, 665, 111, 789, 8);
        cache.set(554, 665, 222, 123, 7);

        let mut clients = cache.get_clients_for_aggregate(554, 665, 111);
        clients.sort_by_key(|(client_id, _)| *client_id);

        assert_eq!(clients.len(), 3);
        assert_eq!(clients[0], (123, 5));
        assert_eq!(clients[1], (456, 3));
        assert_eq!(clients[2], (789, 8));

        let clients2 = cache.get_clients_for_aggregate(554, 665, 222);
        assert_eq!(clients2.len(), 1);
        assert_eq!(clients2[0], (123, 7));

        let clients3 = cache.get_clients_for_aggregate(554, 665, 765);
        assert_eq!(clients3.len(), 0);
    }

    #[test]
    fn test_lru_eviction() {
        let mut cache = ClientEventIndexCache::new(2);

        // Fill cache to capacity
        cache.set(554, 665, 111, 123, 1);
        cache.set(554, 665, 111, 456, 2);

        // Access first entry to make it most recently used
        cache.get(554, 665, 111, 123);

        // Add another entry - should evict (aggregate1, 456)
        cache.set(554, 665, 222, 789, 3);

        // Check that LRU was evicted
        assert_eq!(cache.get(554, 665, 111, 123), Some(1)); // Should still exist
        assert_eq!(cache.get(554, 665, 111, 456), None); // Should be evicted
        assert_eq!(cache.get(554, 665, 222, 789), Some(3)); // Should exist
    }

    #[test]
    fn test_contains_client() {
        let mut cache = ClientEventIndexCache::new(100);

        assert!(!cache.contains_client(554, 665, 111, 123));

        cache.set(554, 665, 111, 123, 5);
        assert!(cache.contains_client(554, 665, 111, 123));
        assert!(!cache.contains_client(554, 665, 111, 456));
        assert!(!cache.contains_client(554, 665, 222, 123));
    }

    #[test]
    fn test_client_key_equality_and_hashing() {
        let key1 = AggregateClientKey::new(554, 665, 111, 123);
        let key2 = AggregateClientKey::new(554, 665, 111, 123);
        let key3 = AggregateClientKey::new(554, 665, 111, 456);
        let key4 = AggregateClientKey::new(554, 665, 222, 123);

        // Test equality
        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
        assert_ne!(key1, key4);

        // Test that equal keys have same hash
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher1 = DefaultHasher::new();
        let mut hasher2 = DefaultHasher::new();
        key1.hash(&mut hasher1);
        key2.hash(&mut hasher2);

        assert_eq!(hasher1.finish(), hasher2.finish());
    }

    #[test]
    fn test_cache_stats() {
        let mut cache = ClientEventIndexCache::new(100);

        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
        assert_eq!(cache.capacity(), 100);

        cache.set(554, 665, 111, 123, 5);
        assert_eq!(cache.len(), 1);
        assert!(!cache.is_empty());

        cache.clear();
        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
    }

    #[test]
    fn test_force_evict_lru() {
        let mut cache = ClientEventIndexCache::new(3);

        cache.set(554, 665, 111, 123, 1);
        cache.set(554, 665, 111, 456, 2);
        cache.set(554, 665, 222, 789, 3);

        // Access middle entry to change LRU order
        cache.get(554, 665, 111, 456);

        // Force evict should remove LRU entry
        let evicted = cache.force_evict_lru();
        assert!(evicted.is_some());
        let (aggregate_id, client_id, index) = evicted.unwrap();
        assert_eq!((aggregate_id, client_id, index), (111, 123, 1));

        // Verify it was actually removed
        assert_eq!(cache.get(554, 665, 111, 123), None);
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_org_isolation() {
        let mut cache = ClientEventIndexCache::new(100);

        // Same aggregate_type_id, aggregate_id, and client_id across different orgs
        cache.set(1, 100, 200, 300, 10);
        cache.set(2, 100, 200, 300, 20);
        cache.set(3, 100, 200, 300, 30);

        // Each org should have its own isolated entry
        assert_eq!(cache.get(1, 100, 200, 300), Some(10));
        assert_eq!(cache.get(2, 100, 200, 300), Some(20));
        assert_eq!(cache.get(3, 100, 200, 300), Some(30));

        // Updating one org shouldn't affect others
        cache.set(1, 100, 200, 300, 15);
        assert_eq!(cache.get(1, 100, 200, 300), Some(15));
        assert_eq!(cache.get(2, 100, 200, 300), Some(20));
        assert_eq!(cache.get(3, 100, 200, 300), Some(30));

        // Remove one org's entry shouldn't affect others
        assert_eq!(cache.remove_client(2, 100, 200, 300), Some(20));
        assert_eq!(cache.get(1, 100, 200, 300), Some(15));
        assert_eq!(cache.get(2, 100, 200, 300), None);
        assert_eq!(cache.get(3, 100, 200, 300), Some(30));
    }

    #[test]
    fn test_aggregate_type_isolation() {
        let mut cache = ClientEventIndexCache::new(100);

        // Same org_id, aggregate_id, and client_id across different aggregate_type_ids
        cache.set(1, 100, 200, 300, 10);
        cache.set(1, 101, 200, 300, 20);
        cache.set(1, 102, 200, 300, 30);

        // Each aggregate type should have its own isolated entry
        assert_eq!(cache.get(1, 100, 200, 300), Some(10));
        assert_eq!(cache.get(1, 101, 200, 300), Some(20));
        assert_eq!(cache.get(1, 102, 200, 300), Some(30));

        // Updating one aggregate type shouldn't affect others
        cache.set(1, 101, 200, 300, 25);
        assert_eq!(cache.get(1, 100, 200, 300), Some(10));
        assert_eq!(cache.get(1, 101, 200, 300), Some(25));
        assert_eq!(cache.get(1, 102, 200, 300), Some(30));

        // Remove one aggregate type's entry shouldn't affect others
        assert_eq!(cache.remove_client(1, 102, 200, 300), Some(30));
        assert_eq!(cache.get(1, 100, 200, 300), Some(10));
        assert_eq!(cache.get(1, 101, 200, 300), Some(25));
        assert_eq!(cache.get(1, 102, 200, 300), None);
    }

    #[test]
    fn test_cross_org_aggregate_type_isolation() {
        let mut cache = ClientEventIndexCache::new(100);

        // Matrix of different orgs and aggregate types with same aggregate_id and client_id
        cache.set(1, 100, 999, 500, 11);
        cache.set(1, 101, 999, 500, 12);
        cache.set(2, 100, 999, 500, 21);
        cache.set(2, 101, 999, 500, 22);

        // Verify complete isolation
        assert_eq!(cache.get(1, 100, 999, 500), Some(11));
        assert_eq!(cache.get(1, 101, 999, 500), Some(12));
        assert_eq!(cache.get(2, 100, 999, 500), Some(21));
        assert_eq!(cache.get(2, 101, 999, 500), Some(22));

        // Update one combination shouldn't affect others
        cache.set(1, 100, 999, 500, 111);
        assert_eq!(cache.get(1, 100, 999, 500), Some(111));
        assert_eq!(cache.get(1, 101, 999, 500), Some(12));
        assert_eq!(cache.get(2, 100, 999, 500), Some(21));
        assert_eq!(cache.get(2, 101, 999, 500), Some(22));
    }

    #[test]
    fn test_get_clients_for_aggregate_with_isolation() {
        let mut cache = ClientEventIndexCache::new(100);

        // Setup data across different orgs and aggregate types
        // Org 1, Aggregate Type 100, Aggregate 999
        cache.set(1, 100, 999, 100, 10);
        cache.set(1, 100, 999, 101, 11);
        cache.set(1, 100, 999, 102, 12);

        // Org 1, Aggregate Type 101, Aggregate 999 (same aggregate_id!)
        cache.set(1, 101, 999, 100, 20);
        cache.set(1, 101, 999, 103, 23);

        // Org 2, Aggregate Type 100, Aggregate 999 (same aggregate_id and type!)
        cache.set(2, 100, 999, 100, 30);
        cache.set(2, 100, 999, 104, 34);

        // Get clients for org 1, aggregate type 100, aggregate 999
        let mut clients1 = cache.get_clients_for_aggregate(1, 100, 999);
        clients1.sort_by_key(|(client_id, _)| *client_id);
        assert_eq!(clients1.len(), 3);
        assert_eq!(clients1, vec![(100, 10), (101, 11), (102, 12)]);

        // Get clients for org 1, aggregate type 101, aggregate 999
        let mut clients2 = cache.get_clients_for_aggregate(1, 101, 999);
        clients2.sort_by_key(|(client_id, _)| *client_id);
        assert_eq!(clients2.len(), 2);
        assert_eq!(clients2, vec![(100, 20), (103, 23)]);

        // Get clients for org 2, aggregate type 100, aggregate 999
        let mut clients3 = cache.get_clients_for_aggregate(2, 100, 999);
        clients3.sort_by_key(|(client_id, _)| *client_id);
        assert_eq!(clients3.len(), 2);
        assert_eq!(clients3, vec![(100, 30), (104, 34)]);

        // Non-existent combination should return empty
        let clients4 = cache.get_clients_for_aggregate(3, 100, 999);
        assert_eq!(clients4.len(), 0);

        let clients5 = cache.get_clients_for_aggregate(1, 102, 999);
        assert_eq!(clients5.len(), 0);
    }

    #[test]
    fn test_remove_aggregate_with_isolation() {
        let mut cache = ClientEventIndexCache::new(100);

        // Setup data across different orgs and aggregate types but same aggregate_id
        cache.set(1, 100, 888, 100, 10);
        cache.set(1, 100, 888, 101, 11);
        cache.set(1, 101, 888, 100, 20); // Same org, different aggregate type
        cache.set(2, 100, 888, 100, 30); // Different org, same aggregate type
        cache.set(1, 100, 777, 100, 40); // Same org and type, different aggregate

        // Remove aggregate should only affect entries with matching aggregate_id
        // regardless of org or aggregate type
        let removed_count = cache.remove_aggregate(888);
        assert_eq!(removed_count, 4); // Should remove all entries with aggregate_id 888

        // Verify removals
        assert_eq!(cache.get(1, 100, 888, 100), None);
        assert_eq!(cache.get(1, 100, 888, 101), None);
        assert_eq!(cache.get(1, 101, 888, 100), None);
        assert_eq!(cache.get(2, 100, 888, 100), None);

        // Verify entry with different aggregate_id is still there
        assert_eq!(cache.get(1, 100, 777, 100), Some(40));
    }

    #[test]
    fn test_contains_client_with_isolation() {
        let mut cache = ClientEventIndexCache::new(100);

        cache.set(1, 100, 999, 500, 10);

        // Should only contain exact match
        assert!(cache.contains_client(1, 100, 999, 500));

        // Different org should not contain
        assert!(!cache.contains_client(2, 100, 999, 500));

        // Different aggregate type should not contain
        assert!(!cache.contains_client(1, 101, 999, 500));

        // Different aggregate_id should not contain
        assert!(!cache.contains_client(1, 100, 888, 500));

        // Different client_id should not contain
        assert!(!cache.contains_client(1, 100, 999, 501));
    }

    #[test]
    fn test_update_if_higher_with_isolation() {
        let mut cache = ClientEventIndexCache::new(100);

        // Set initial values across isolated contexts
        cache.set(1, 100, 999, 500, 10);
        cache.set(1, 101, 999, 500, 20); // Same org, different aggregate type
        cache.set(2, 100, 999, 500, 30); // Different org, same aggregate type

        // Update one context with higher value
        assert!(cache.update_if_higher(1, 100, 999, 500, 15));
        assert_eq!(cache.get(1, 100, 999, 500), Some(15));

        // Other contexts should be unaffected
        assert_eq!(cache.get(1, 101, 999, 500), Some(20));
        assert_eq!(cache.get(2, 100, 999, 500), Some(30));

        // Try to update with lower value - should fail
        assert!(!cache.update_if_higher(1, 100, 999, 500, 12));
        assert_eq!(cache.get(1, 100, 999, 500), Some(15));

        // Update different context with lower value than its current - should fail
        assert!(!cache.update_if_higher(1, 101, 999, 500, 15));
        assert_eq!(cache.get(1, 101, 999, 500), Some(20));

        // Update different context with higher value - should succeed
        assert!(cache.update_if_higher(2, 100, 999, 500, 35));
        assert_eq!(cache.get(2, 100, 999, 500), Some(35));
    }

    #[test]
    fn test_cache_key_uniqueness() {
        let mut cache = ClientEventIndexCache::new(100);

        // Create entries that would collide if we didn't have proper isolation
        let combinations = vec![
            (1, 100, 200, 300, 1000),
            (1, 100, 200, 301, 1001),
            (1, 100, 201, 300, 1002),
            (1, 101, 200, 300, 1003),
            (2, 100, 200, 300, 1004),
        ];

        // Add all combinations
        for (org_id, aggregate_type_id, aggregate_id, client_id, value) in &combinations {
            cache.set(
                *org_id,
                *aggregate_type_id,
                *aggregate_id,
                *client_id,
                *value,
            );
        }

        // Verify all combinations are stored independently
        for (org_id, aggregate_type_id, aggregate_id, client_id, expected_value) in combinations {
            assert_eq!(
                cache.get(org_id, aggregate_type_id, aggregate_id, client_id),
                Some(expected_value)
            );
        }

        assert_eq!(cache.len(), 5);
    }
}
