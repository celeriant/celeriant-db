use lru::LruCache;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;

/// High-performance composite key for client event index tracking
/// Optimized for hashing and comparison operations
#[derive(Clone, PartialEq, Eq)]
pub struct ClientKey {
    aggregate_id: String,
    client_id: u128,
    // Pre-computed hash for better performance
    hash: u64,
}

impl ClientKey {
    pub fn new(aggregate_id: String, client_id: u128) -> Self {
        let hash = Self::compute_hash(&aggregate_id, client_id);
        Self {
            aggregate_id,
            client_id,
            hash,
        }
    }

    fn compute_hash(aggregate_id: &str, client_id: u128) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        aggregate_id.hash(&mut hasher);
        client_id.hash(&mut hasher);
        hasher.finish()
    }

    pub fn aggregate_id(&self) -> &str {
        &self.aggregate_id
    }

    pub fn client_id(&self) -> u128 {
        self.client_id
    }
}

impl Hash for ClientKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Use pre-computed hash for better performance
        state.write_u64(self.hash);
    }
}

impl fmt::Debug for ClientKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientKey")
            .field("aggregate_id", &self.aggregate_id)
            .field("client_id", &self.client_id)
            .finish()
    }
}

/// Cache for tracking the highest client_event_index seen from each client within each aggregate
/// Uses LRU eviction based on number of client/aggregate combinations
/// Optimized for producer idempotency checks during writes
pub struct ClientEventIndexCache {
    cache: LruCache<ClientKey, u64>,
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
    pub fn get(&mut self, aggregate_id: &str, client_id: u128) -> Option<u64> {
        let key = ClientKey::new(aggregate_id.to_string(), client_id);
        self.cache.get(&key).copied()
    }

    /// Set the highest client_event_index for a client in an aggregate
    /// Returns the previous value if it existed
    pub fn set(
        &mut self,
        aggregate_id: &str,
        client_id: u128,
        client_event_index: u64,
    ) -> Option<u64> {
        let key = ClientKey::new(aggregate_id.to_string(), client_id);
        self.cache.put(key, client_event_index)
    }

    /// Update the client_event_index only if the new value is higher
    /// Returns true if the value was updated, false if the existing value was higher or equal
    pub fn update_if_higher(
        &mut self,
        aggregate_id: &str,
        client_id: u128,
        client_event_index: u64,
    ) -> bool {
        let key = ClientKey::new(aggregate_id.to_string(), client_id);

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
    pub fn remove_client(&mut self, aggregate_id: &str, client_id: u128) -> Option<u64> {
        let key = ClientKey::new(aggregate_id.to_string(), client_id);
        self.cache.pop(&key)
    }

    /// Remove all clients for a specific aggregate
    /// Returns the number of clients that were removed
    pub fn remove_aggregate(&mut self, aggregate_id: &str) -> usize {
        let keys_to_remove: Vec<ClientKey> = self
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
    pub fn contains_client(&self, aggregate_id: &str, client_id: u128) -> bool {
        let key = ClientKey::new(aggregate_id.to_string(), client_id);
        self.cache.contains(&key)
    }

    /// Get all clients for a specific aggregate
    /// Returns a vector of (client_id, highest_client_event_index) pairs
    pub fn get_clients_for_aggregate(&mut self, aggregate_id: &str) -> Vec<(u128, u64)> {
        self.cache
            .iter()
            .filter(|(key, _)| key.aggregate_id == aggregate_id)
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
    pub fn peek_lru(&self) -> Option<(&str, u128, u64)> {
        self.cache
            .peek_lru()
            .map(|(key, &value)| (key.aggregate_id.as_str(), key.client_id, value))
    }

    /// Force eviction of the least recently used entry
    /// Returns the evicted (aggregate_id, client_id, client_event_index) if any
    pub fn force_evict_lru(&mut self) -> Option<(String, u128, u64)> {
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
        assert_eq!(cache.set("aggregate1", 123, 5), None);
        assert_eq!(cache.get("aggregate1", 123), Some(5));
        assert_eq!(cache.get("aggregate1", 456), None);
        assert_eq!(cache.get("aggregate2", 123), None);

        // Test update
        assert_eq!(cache.set("aggregate1", 123, 10), Some(5));
        assert_eq!(cache.get("aggregate1", 123), Some(10));

        // Test different client same aggregate
        assert_eq!(cache.set("aggregate1", 456, 3), None);
        assert_eq!(cache.get("aggregate1", 456), Some(3));
        assert_eq!(cache.get("aggregate1", 123), Some(10)); // Should still exist

        // Test same client different aggregate
        assert_eq!(cache.set("aggregate2", 123, 7), None);
        assert_eq!(cache.get("aggregate2", 123), Some(7));
        assert_eq!(cache.get("aggregate1", 123), Some(10)); // Should still exist
    }

    #[test]
    fn test_update_if_higher() {
        let mut cache = ClientEventIndexCache::new(100);

        // First update should succeed (no existing value)
        assert!(cache.update_if_higher("aggregate1", 123, 5));
        assert_eq!(cache.get("aggregate1", 123), Some(5));

        // Higher value should update
        assert!(cache.update_if_higher("aggregate1", 123, 10));
        assert_eq!(cache.get("aggregate1", 123), Some(10));

        // Lower value should not update
        assert!(!cache.update_if_higher("aggregate1", 123, 7));
        assert_eq!(cache.get("aggregate1", 123), Some(10));

        // Equal value should not update
        assert!(!cache.update_if_higher("aggregate1", 123, 10));
        assert_eq!(cache.get("aggregate1", 123), Some(10));

        // Higher value should update again
        assert!(cache.update_if_higher("aggregate1", 123, 15));
        assert_eq!(cache.get("aggregate1", 123), Some(15));
    }

    #[test]
    fn test_remove_operations() {
        let mut cache = ClientEventIndexCache::new(100);

        // Setup test data
        cache.set("aggregate1", 123, 5);
        cache.set("aggregate1", 456, 3);
        cache.set("aggregate2", 123, 7);
        cache.set("aggregate2", 789, 9);

        // Test remove client
        assert_eq!(cache.remove_client("aggregate1", 123), Some(5));
        assert_eq!(cache.remove_client("aggregate1", 123), None);
        assert_eq!(cache.get("aggregate1", 456), Some(3)); // Should still exist

        // Test remove aggregate
        let removed_count = cache.remove_aggregate("aggregate2");
        assert_eq!(removed_count, 2);
        assert_eq!(cache.get("aggregate2", 123), None);
        assert_eq!(cache.get("aggregate2", 789), None);
        assert_eq!(cache.get("aggregate1", 456), Some(3)); // Should still exist
    }

    #[test]
    fn test_get_clients_for_aggregate() {
        let mut cache = ClientEventIndexCache::new(100);

        cache.set("aggregate1", 123, 5);
        cache.set("aggregate1", 456, 3);
        cache.set("aggregate1", 789, 8);
        cache.set("aggregate2", 123, 7);

        let mut clients = cache.get_clients_for_aggregate("aggregate1");
        clients.sort_by_key(|(client_id, _)| *client_id);

        assert_eq!(clients.len(), 3);
        assert_eq!(clients[0], (123, 5));
        assert_eq!(clients[1], (456, 3));
        assert_eq!(clients[2], (789, 8));

        let clients2 = cache.get_clients_for_aggregate("aggregate2");
        assert_eq!(clients2.len(), 1);
        assert_eq!(clients2[0], (123, 7));

        let clients3 = cache.get_clients_for_aggregate("nonexistent");
        assert_eq!(clients3.len(), 0);
    }

    #[test]
    fn test_lru_eviction() {
        let mut cache = ClientEventIndexCache::new(2);

        // Fill cache to capacity
        cache.set("aggregate1", 123, 1);
        cache.set("aggregate1", 456, 2);

        // Access first entry to make it most recently used
        cache.get("aggregate1", 123);

        // Add another entry - should evict (aggregate1, 456)
        cache.set("aggregate2", 789, 3);

        // Check that LRU was evicted
        assert_eq!(cache.get("aggregate1", 123), Some(1)); // Should still exist
        assert_eq!(cache.get("aggregate1", 456), None); // Should be evicted
        assert_eq!(cache.get("aggregate2", 789), Some(3)); // Should exist
    }

    #[test]
    fn test_contains_client() {
        let mut cache = ClientEventIndexCache::new(100);

        assert!(!cache.contains_client("aggregate1", 123));

        cache.set("aggregate1", 123, 5);
        assert!(cache.contains_client("aggregate1", 123));
        assert!(!cache.contains_client("aggregate1", 456));
        assert!(!cache.contains_client("aggregate2", 123));
    }

    #[test]
    fn test_client_key_equality_and_hashing() {
        let key1 = ClientKey::new("aggregate1".to_string(), 123);
        let key2 = ClientKey::new("aggregate1".to_string(), 123);
        let key3 = ClientKey::new("aggregate1".to_string(), 456);
        let key4 = ClientKey::new("aggregate2".to_string(), 123);

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

        cache.set("aggregate1", 123, 5);
        assert_eq!(cache.len(), 1);
        assert!(!cache.is_empty());

        cache.clear();
        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
    }

    #[test]
    fn test_force_evict_lru() {
        let mut cache = ClientEventIndexCache::new(3);

        cache.set("aggregate1", 123, 1);
        cache.set("aggregate1", 456, 2);
        cache.set("aggregate2", 789, 3);

        // Access middle entry to change LRU order
        cache.get("aggregate1", 456);

        // Force evict should remove LRU entry
        let evicted = cache.force_evict_lru();
        assert!(evicted.is_some());
        let (aggregate_id, client_id, index) = evicted.unwrap();
        assert_eq!(
            (aggregate_id.as_str(), client_id, index),
            ("aggregate1", 123, 1)
        );

        // Verify it was actually removed
        assert_eq!(cache.get("aggregate1", 123), None);
        assert_eq!(cache.len(), 2);
    }
}
