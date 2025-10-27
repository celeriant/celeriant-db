use lru::LruCache;
use std::num::NonZeroUsize;

use crate::aggregate_key::AggregateKey;
use eventplanedb_structures::event_batch_item::EventBatchItem;
use eventplanedb_structures::event_batch_metadata::EventBatchMetadata;

/// High-performance memory cache using LRU eviction
/// Each aggregate stores a vector of (EventBatchItem, EventBatchMetadata) pairs
/// Uses uncompressed_size for memory-based eviction with manual size tracking
/// Properly isolates by org_id and aggregate_type_id
pub struct LruMemoryCache {
    cache: LruCache<AggregateKey, Vec<(EventBatchItem, EventBatchMetadata)>>,
    max_memory_bytes: u64,
    current_memory_bytes: u64,
}

impl LruMemoryCache {
    /// Create a new cache with a memory limit in bytes
    /// The cache will evict least recently used aggregates when the total uncompressed size exceeds the limit
    pub fn new(max_memory_bytes: u64) -> Self {
        // Use a large capacity since we're managing eviction manually based on memory
        let cache = LruCache::new(NonZeroUsize::new(100_000).unwrap());

        Self {
            cache,
            max_memory_bytes,
            current_memory_bytes: 0,
        }
    }

    /// Calculate the memory usage of a batch vector
    fn calculate_memory_usage(batches: &[(EventBatchItem, EventBatchMetadata)]) -> u64 {
        batches
            .iter()
            .map(|(_, metadata)| metadata.uncompressed_size)
            .sum()
    }

    /// Evict least recently used aggregates until we're under the memory limit
    fn evict_if_needed(&mut self) {
        while self.current_memory_bytes > self.max_memory_bytes {
            if let Some((_, removed_batches)) = self.cache.pop_lru() {
                let removed_size = Self::calculate_memory_usage(&removed_batches);
                self.current_memory_bytes = self.current_memory_bytes.saturating_sub(removed_size);
            } else {
                break; // Cache is empty
            }
        }
    }

    /// Add a new event batch to the cache for the given aggregate
    /// This is highly optimized to avoid cloning when possible
    pub fn add(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        event_batch_item: EventBatchItem,
        event_batch_metadata: EventBatchMetadata,
    ) {
        let batch_size = event_batch_metadata.uncompressed_size;
        let new_batch_index = event_batch_item.event_batch_index;
        let key = AggregateKey::new(org_id, aggregate_type_id, aggregate_id);

        // Try to get mutable reference to existing batches
        if let Some(batches) = self.cache.get_mut(&key) {
            // Validate sequence continuity
            if !batches.is_empty() {
                let last_batch_index = batches.last().unwrap().0.event_batch_index;

                // New batch should be exactly the next sequential index
                if new_batch_index != last_batch_index + 1 {
                    return;
                }
            }
            // No cloning needed! Direct append to existing vector
            batches.push((event_batch_item, event_batch_metadata));
            self.current_memory_bytes += batch_size;
        } else {
            // Create new entry
            let new_batches = vec![(event_batch_item, event_batch_metadata)];
            self.cache.put(key, new_batches);
            self.current_memory_bytes += batch_size;
        }

        // Evict if we exceed memory limit
        self.evict_if_needed();
    }

    /// Add multiple batches at once for better performance
    pub fn add_batches(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        mut new_batches: Vec<(EventBatchItem, EventBatchMetadata)>,
    ) {
        let batch_total_size = Self::calculate_memory_usage(&new_batches);
        let new_batch_index = new_batches.first().unwrap().0.event_batch_index;
        let key = AggregateKey::new(org_id, aggregate_type_id, aggregate_id);

        if let Some(existing_batches) = self.cache.get_mut(&key) {
            // Validate sequence continuity
            if !existing_batches.is_empty() {
                let last_batch_index = existing_batches.last().unwrap().0.event_batch_index;

                // New batch should be exactly the next sequential index
                if new_batch_index != last_batch_index + 1 {
                    return;
                }
            }
            // Append all new batches without cloning the existing ones
            existing_batches.append(&mut new_batches);
            self.current_memory_bytes += batch_total_size;
        } else {
            // Create new entry
            self.cache.put(key, new_batches);
            self.current_memory_bytes += batch_total_size;
        }

        // Evict if we exceed memory limit
        self.evict_if_needed();
    }

    /// Get the array index position for a given event batch index
    /// Returns None if the event batch index is not found in cache
    pub fn get_pos(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        from_event_batch_index: u64,
    ) -> Option<usize> {
        let key = AggregateKey::new(org_id, aggregate_type_id, aggregate_id);
        let batches = self.cache.get(&key)?;

        if batches.is_empty() {
            return None;
        }

        // Get the first batch index to calculate offset
        let first_batch_index = batches[0].0.event_batch_index;

        // Calculate the expected position
        if from_event_batch_index >= first_batch_index {
            let position = (from_event_batch_index - first_batch_index) as usize;

            // Verify the position is within bounds and the batch index matches
            if position < batches.len()
                && batches[position].0.event_batch_index == from_event_batch_index
            {
                Some(position)
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Get an event batch item at the specified array index position
    /// Returns None if the aggregate or index doesn't exist
    pub fn get_batch(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        index_pos: usize,
    ) -> Option<&EventBatchItem> {
        let key = AggregateKey::new(org_id, aggregate_type_id, aggregate_id);
        let batches = self.cache.get(&key)?;
        batches.get(index_pos).map(|(batch, _)| batch)
    }

    /// Get event batch metadata at the specified array index position
    /// Returns None if the aggregate or index doesn't exist
    pub fn get_meta(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        index_pos: usize,
    ) -> Option<&EventBatchMetadata> {
        let key = AggregateKey::new(org_id, aggregate_type_id, aggregate_id);
        let batches = self.cache.get(&key)?;
        batches.get(index_pos).map(|(_, metadata)| metadata)
    }

    /// Get mutable reference to event batch item - enables in-place modifications
    pub fn get_batch_mut(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        index_pos: usize,
    ) -> Option<&mut EventBatchItem> {
        let key = AggregateKey::new(org_id, aggregate_type_id, aggregate_id);
        let batches = self.cache.get_mut(&key)?;
        batches.get_mut(index_pos).map(|(batch, _)| batch)
    }

    /// Get mutable reference to event batch metadata - enables in-place modifications
    pub fn get_meta_mut(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        index_pos: usize,
    ) -> Option<&mut EventBatchMetadata> {
        let key = AggregateKey::new(org_id, aggregate_type_id, aggregate_id);
        let batches = self.cache.get_mut(&key)?;
        batches.get_mut(index_pos).map(|(_, metadata)| metadata)
    }

    /// Get both batch and metadata as mutable references
    pub fn get_batch_and_meta_mut(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        index_pos: usize,
    ) -> Option<(&mut EventBatchItem, &mut EventBatchMetadata)> {
        let key = AggregateKey::new(org_id, aggregate_type_id, aggregate_id);
        let batches = self.cache.get_mut(&key)?;
        batches
            .get_mut(index_pos)
            .map(|(batch, metadata)| (batch, metadata))
    }

    /// Get immutable reference to all batches for an aggregate
    pub fn get_all_batches(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
    ) -> Option<&[(EventBatchItem, EventBatchMetadata)]> {
        let key = AggregateKey::new(org_id, aggregate_type_id, aggregate_id);
        self.cache.get(&key).map(|v| v.as_slice())
    }

    /// Get mutable reference to all batches for an aggregate
    pub fn get_all_batches_mut(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
    ) -> Option<&mut Vec<(EventBatchItem, EventBatchMetadata)>> {
        let key = AggregateKey::new(org_id, aggregate_type_id, aggregate_id);
        self.cache.get_mut(&key)
    }

    /// Clear all cached data for a specific aggregate and update memory tracking
    pub fn clear_aggregate(&mut self, org_id: u128, aggregate_type_id: u128, aggregate_id: u128) {
        let key = AggregateKey::new(org_id, aggregate_type_id, aggregate_id);
        if let Some(removed_batches) = self.cache.pop(&key) {
            let removed_size = Self::calculate_memory_usage(&removed_batches);
            self.current_memory_bytes = self.current_memory_bytes.saturating_sub(removed_size);
        }
    }

    /// Clear all cached data
    pub fn clear_all(&mut self) {
        self.cache.clear();
        self.current_memory_bytes = 0;
    }

    /// Check if we have any cached data for an aggregate (doesn't affect LRU order)
    pub fn contains_aggregate(
        &self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
    ) -> bool {
        let key = AggregateKey::new(org_id, aggregate_type_id, aggregate_id);
        self.cache.contains(&key)
    }

    /// Promote an aggregate to most recently used without accessing data
    pub fn touch_aggregate(
        &mut self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
    ) -> bool {
        let key = AggregateKey::new(org_id, aggregate_type_id, aggregate_id);
        self.cache.promote(&key)
    }

    /// Get the number of aggregates currently cached
    pub fn aggregate_count(&self) -> usize {
        self.cache.len()
    }

    /// Get the current total memory usage in bytes
    pub fn memory_usage_bytes(&self) -> u64 {
        self.current_memory_bytes
    }

    /// Get the maximum memory limit in bytes
    pub fn max_memory_bytes(&self) -> u64 {
        self.max_memory_bytes
    }

    /// Update the memory limit (may trigger eviction)
    pub fn set_max_memory_bytes(&mut self, max_memory_bytes: u64) {
        self.max_memory_bytes = max_memory_bytes;
        self.evict_if_needed();
    }

    /// Get memory utilization as a percentage (0.0 to 100.0)
    pub fn memory_utilization_percent(&self) -> f64 {
        if self.max_memory_bytes == 0 {
            0.0
        } else {
            (self.current_memory_bytes as f64 / self.max_memory_bytes as f64) * 100.0
        }
    }

    /// Peek at the least recently used aggregate without affecting LRU order
    pub fn peek_lru(
        &self,
    ) -> Option<(u128, u128, u128, &Vec<(EventBatchItem, EventBatchMetadata)>)> {
        self.cache
            .peek_lru()
            .map(|(key, batches)| (key.org_id, key.aggregate_type_id, key.aggregate_id, batches))
    }

    /// Peek at the most recently used aggregate without affecting LRU order
    pub fn peek_mru(
        &self,
    ) -> Option<(u128, u128, u128, &Vec<(EventBatchItem, EventBatchMetadata)>)> {
        self.cache
            .peek_mru()
            .map(|(key, batches)| (key.org_id, key.aggregate_type_id, key.aggregate_id, batches))
    }

    /// Force eviction of the least recently used aggregate
    /// Returns the evicted (org_id, aggregate_type_id, aggregate_id, batches) if any
    pub fn force_evict_lru(
        &mut self,
    ) -> Option<(u128, u128, u128, Vec<(EventBatchItem, EventBatchMetadata)>)> {
        if let Some((key, batches)) = self.cache.pop_lru() {
            let removed_size = Self::calculate_memory_usage(&batches);
            self.current_memory_bytes = self.current_memory_bytes.saturating_sub(removed_size);
            Some((key.org_id, key.aggregate_type_id, key.aggregate_id, batches))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eventplanedb_structures::event_item::EventItem;

    #[test]
    fn test_gap_detection() {
        let mut cache = LruMemoryCache::new(16 * 1024 * 1024);

        // Add first batch (index 1)
        let event1 = EventItem::new(1, 1, 1000, 42, 1, b"test1".to_vec());
        let batch1 = EventBatchItem::new(1, 2000, 123, Some(456), vec![event1]);
        let metadata1 = EventBatchMetadata {
            event_batch_index: 1,
            uncompressed_size: 1000,
            compressed_size: 500,
            ..Default::default()
        };
        cache.add(554, 665, 123, batch1, metadata1);

        // Try to add batch with gap (index 3 instead of 2)
        let event3 = EventItem::new(3, 3, 1000, 42, 1, b"test3".to_vec());
        let batch3 = EventBatchItem::new(3, 2000, 123, Some(456), vec![event3]);
        let metadata3 = EventBatchMetadata {
            event_batch_index: 3,
            uncompressed_size: 1000,
            compressed_size: 500,
            ..Default::default()
        };
        cache.add(554, 665, 123, batch3, metadata3);

        // Should only have 1 batch (the gap batch should be rejected)
        assert_eq!(cache.get_all_batches(554, 665, 123).unwrap().len(), 1);
        assert_eq!(cache.memory_usage_bytes(), 1000); // Only first batch

        // Should not be able to find batch index 3
        assert!(cache.get_pos(554, 665, 123, 3).is_none());

        // Now add correct sequential batch (index 2)
        let event2 = EventItem::new(2, 2, 1000, 42, 1, b"test2".to_vec());
        let batch2 = EventBatchItem::new(2, 2000, 123, Some(456), vec![event2]);
        let metadata2 = EventBatchMetadata {
            event_batch_index: 2,
            uncompressed_size: 1000,
            compressed_size: 500,
            ..Default::default()
        };
        cache.add(554, 665, 123, batch2, metadata2);

        // Now should have 2 batches
        assert_eq!(cache.get_all_batches(554, 665, 123).unwrap().len(), 2);
        assert_eq!(cache.memory_usage_bytes(), 2000);
        assert_eq!(cache.get_pos(554, 665, 123, 2), Some(1));
    }

    #[test]
    fn test_add_without_cloning() {
        let mut cache = LruMemoryCache::new(16 * 1024 * 1024); // 16MB limit

        let event = EventItem::new(1, 1, 1000, 42, 1, b"test".to_vec());
        let batch = EventBatchItem::new(5, 2000, 123, Some(456), vec![event]);
        let metadata = EventBatchMetadata {
            event_batch_index: 5,
            uncompressed_size: 1000,
            compressed_size: 500,
            ..Default::default()
        };

        cache.add(554, 665, 123, batch, metadata);

        // Should find the batch at index 5
        assert_eq!(cache.get_pos(554, 665, 123, 5), Some(0));
        assert_eq!(cache.memory_usage_bytes(), 1000);
        assert_eq!(cache.aggregate_count(), 1);
    }

    #[test]
    fn test_multiple_adds_same_aggregate() {
        let mut cache = LruMemoryCache::new(16 * 1024 * 1024);

        // Add multiple batches to same aggregate - tests the get_mut optimization
        for i in 1..=3 {
            let event = EventItem::new(i, i, 1000 + i, 42, 1, b"test".to_vec());
            let batch = EventBatchItem::new(i, 2000 + i, 123, Some(456), vec![event]);
            let metadata = EventBatchMetadata {
                event_batch_index: i,
                uncompressed_size: 500,
                compressed_size: 250,
                ..Default::default()
            };
            cache.add(554, 665, 123, batch, metadata);
        }

        // All batches should be present
        assert_eq!(cache.get_pos(554, 665, 123, 1), Some(0));
        assert_eq!(cache.get_pos(554, 665, 123, 2), Some(1));
        assert_eq!(cache.get_pos(554, 665, 123, 3), Some(2));

        // Memory usage should be accurate
        assert_eq!(cache.memory_usage_bytes(), 1500); // 3 * 500
        assert_eq!(cache.aggregate_count(), 1);
    }

    #[test]
    fn test_memory_based_eviction() {
        let mut cache = LruMemoryCache::new(2000); // 2KB limit

        // Add first aggregate with 1KB
        let event1 = EventItem::new(1, 1, 1000, 42, 1, b"test1".to_vec());
        let batch1 = EventBatchItem::new(1, 2000, 123, Some(456), vec![event1]);
        let metadata1 = EventBatchMetadata {
            event_batch_index: 1,
            uncompressed_size: 1000,
            compressed_size: 500,
            ..Default::default()
        };
        cache.add(554, 665, 111, batch1, metadata1);

        // Add second aggregate with 1KB (total: 2KB, at limit)
        let event2 = EventItem::new(1, 1, 1000, 42, 1, b"test2".to_vec());
        let batch2 = EventBatchItem::new(1, 2000, 789, Some(456), vec![event2]);
        let metadata2 = EventBatchMetadata {
            event_batch_index: 1,
            uncompressed_size: 1000,
            compressed_size: 500,
            ..Default::default()
        };
        cache.add(554, 665, 222, batch2, metadata2);

        assert_eq!(cache.memory_usage_bytes(), 2000);
        assert_eq!(cache.aggregate_count(), 2);

        // Add third aggregate with 1KB - should trigger eviction
        let event3 = EventItem::new(1, 1, 1000, 42, 1, b"test3".to_vec());
        let batch3 = EventBatchItem::new(1, 2000, 999, Some(456), vec![event3]);
        let metadata3 = EventBatchMetadata {
            event_batch_index: 1,
            uncompressed_size: 1000,
            compressed_size: 500,
            ..Default::default()
        };
        cache.add(554, 665, 333, batch3, metadata3);

        // Should have evicted first aggregate (LRU)
        assert!(!cache.contains_aggregate(554, 665, 111));
        assert!(cache.contains_aggregate(554, 665, 222));
        assert!(cache.contains_aggregate(554, 665, 333));
        assert_eq!(cache.memory_usage_bytes(), 2000);
        assert_eq!(cache.aggregate_count(), 2);
    }

    #[test]
    fn test_mutable_access() {
        let mut cache = LruMemoryCache::new(16 * 1024 * 1024);

        let event = EventItem::new(1, 1, 1000, 42, 1, b"test".to_vec());
        let batch = EventBatchItem::new(5, 2000, 123, Some(456), vec![event]);
        let metadata = EventBatchMetadata {
            event_batch_index: 5,
            uncompressed_size: 1000,
            compressed_size: 500,
            ..Default::default()
        };

        cache.add(554, 665, 123, batch, metadata);

        // Test mutable access
        if let Some(batch_mut) = cache.get_batch_mut(554, 665, 123, 0) {
            batch_mut.event_batch_index = 99;
        }

        // Verify the change
        if let Some(batch) = cache.get_batch(554, 665, 123, 0) {
            assert_eq!(batch.event_batch_index, 99);
        }
    }

    #[test]
    fn test_bulk_add() {
        let mut cache = LruMemoryCache::new(16 * 1024 * 1024);

        let mut batches = Vec::new();
        for i in 1..=3 {
            let event = EventItem::new(i, i, 1000 + i, 42, 1, b"test".to_vec());
            let batch = EventBatchItem::new(i, 2000 + i, 123, Some(456), vec![event]);
            let metadata = EventBatchMetadata {
                event_batch_index: i,
                uncompressed_size: 500,
                compressed_size: 250,
                ..Default::default()
            };
            batches.push((batch, metadata));
        }

        cache.add_batches(554, 665, 123, batches);

        assert_eq!(cache.aggregate_count(), 1);
        assert_eq!(cache.memory_usage_bytes(), 1500); // 3 * 500
        assert_eq!(cache.get_pos(554, 665, 123, 1), Some(0));
        assert_eq!(cache.get_pos(554, 665, 123, 2), Some(1));
        assert_eq!(cache.get_pos(554, 665, 123, 3), Some(2));
    }

    #[test]
    fn test_org_isolation() {
        let mut cache = LruMemoryCache::new(16 * 1024 * 1024);

        // Same aggregate_type_id and aggregate_id across different orgs
        let event1 = EventItem::new(1, 1, 1000, 42, 1, b"test1".to_vec());
        let batch1 = EventBatchItem::new(1, 2000, 200, Some(456), vec![event1]);
        let metadata1 = EventBatchMetadata {
            event_batch_index: 1,
            uncompressed_size: 1000,
            compressed_size: 500,
            ..Default::default()
        };
        cache.add(1, 100, 200, batch1, metadata1);

        let event2 = EventItem::new(1, 1, 1000, 42, 1, b"test2".to_vec());
        let batch2 = EventBatchItem::new(1, 2000, 200, Some(456), vec![event2]);
        let metadata2 = EventBatchMetadata {
            event_batch_index: 1,
            uncompressed_size: 1000,
            compressed_size: 500,
            ..Default::default()
        };
        cache.add(2, 100, 200, batch2, metadata2);

        // Each org should have its own isolated entry
        assert!(cache.contains_aggregate(1, 100, 200));
        assert!(cache.contains_aggregate(2, 100, 200));
        assert_eq!(cache.aggregate_count(), 2);
        assert_eq!(cache.memory_usage_bytes(), 2000);

        // Clear one org's entry shouldn't affect others
        cache.clear_aggregate(1, 100, 200);
        assert!(!cache.contains_aggregate(1, 100, 200));
        assert!(cache.contains_aggregate(2, 100, 200));
        assert_eq!(cache.memory_usage_bytes(), 1000);
    }

    #[test]
    fn test_aggregate_type_isolation() {
        let mut cache = LruMemoryCache::new(16 * 1024 * 1024);

        // Same org_id and aggregate_id across different aggregate_type_ids
        let event1 = EventItem::new(1, 1, 1000, 42, 1, b"test1".to_vec());
        let batch1 = EventBatchItem::new(1, 2000, 200, Some(456), vec![event1]);
        let metadata1 = EventBatchMetadata {
            event_batch_index: 1,
            uncompressed_size: 1000,
            compressed_size: 500,
            ..Default::default()
        };
        cache.add(1, 100, 200, batch1, metadata1);

        let event2 = EventItem::new(1, 1, 1000, 42, 1, b"test2".to_vec());
        let batch2 = EventBatchItem::new(1, 2000, 200, Some(456), vec![event2]);
        let metadata2 = EventBatchMetadata {
            event_batch_index: 1,
            uncompressed_size: 1000,
            compressed_size: 500,
            ..Default::default()
        };
        cache.add(1, 101, 200, batch2, metadata2);

        // Each aggregate type should have its own isolated entry
        assert!(cache.contains_aggregate(1, 100, 200));
        assert!(cache.contains_aggregate(1, 101, 200));
        assert_eq!(cache.aggregate_count(), 2);

        // Clear one aggregate type's entry shouldn't affect others
        cache.clear_aggregate(1, 100, 200);
        assert!(!cache.contains_aggregate(1, 100, 200));
        assert!(cache.contains_aggregate(1, 101, 200));
    }

    #[test]
    fn test_cross_org_aggregate_type_isolation() {
        let mut cache = LruMemoryCache::new(16 * 1024 * 1024);

        // Matrix of different orgs and aggregate types with same aggregate_id
        let combinations = vec![
            (1, 100, 999, 1001),
            (1, 101, 999, 1002),
            (2, 100, 999, 1003),
            (2, 101, 999, 1004),
        ];

        for (org_id, aggregate_type_id, aggregate_id, data_value) in &combinations {
            let event = EventItem::new(
                1,
                1,
                1000,
                42,
                1,
                format!("test{}", data_value).as_bytes().to_vec(),
            );
            let batch = EventBatchItem::new(1, 2000, *aggregate_id, Some(456), vec![event]);
            let metadata = EventBatchMetadata {
                event_batch_index: 1,
                uncompressed_size: 500,
                compressed_size: 250,
                ..Default::default()
            };
            cache.add(*org_id, *aggregate_type_id, *aggregate_id, batch, metadata);
        }

        // Verify complete isolation - all combinations should exist
        for (org_id, aggregate_type_id, aggregate_id, _) in combinations {
            assert!(cache.contains_aggregate(org_id, aggregate_type_id, aggregate_id));
        }

        assert_eq!(cache.aggregate_count(), 4);
        assert_eq!(cache.memory_usage_bytes(), 2000); // 4 * 500
    }

    #[test]
    fn test_force_evict_lru_with_isolation() {
        let mut cache = LruMemoryCache::new(3000); // 3KB limit

        // Add entries for different orgs/types
        let event1 = EventItem::new(1, 1, 1000, 42, 1, b"test1".to_vec());
        let batch1 = EventBatchItem::new(1, 2000, 200, Some(456), vec![event1]);
        let metadata1 = EventBatchMetadata {
            event_batch_index: 1,
            uncompressed_size: 1000,
            compressed_size: 500,
            ..Default::default()
        };
        cache.add(1, 100, 200, batch1, metadata1);

        let event2 = EventItem::new(1, 1, 1000, 42, 1, b"test2".to_vec());
        let batch2 = EventBatchItem::new(1, 2000, 201, Some(456), vec![event2]);
        let metadata2 = EventBatchMetadata {
            event_batch_index: 1,
            uncompressed_size: 1000,
            compressed_size: 500,
            ..Default::default()
        };
        cache.add(1, 101, 201, batch2, metadata2);

        let event3 = EventItem::new(1, 1, 1000, 42, 1, b"test3".to_vec());
        let batch3 = EventBatchItem::new(1, 2000, 202, Some(456), vec![event3]);
        let metadata3 = EventBatchMetadata {
            event_batch_index: 1,
            uncompressed_size: 1000,
            compressed_size: 500,
            ..Default::default()
        };
        cache.add(2, 100, 202, batch3, metadata3);

        // Access middle entry to change LRU order
        cache.get_batch(1, 101, 201, 0);

        // Force evict should remove LRU entry
        let evicted = cache.force_evict_lru();
        assert!(evicted.is_some());
        let (org_id, aggregate_type_id, aggregate_id, _) = evicted.unwrap();
        assert_eq!((org_id, aggregate_type_id, aggregate_id), (1, 100, 200));

        // Verify it was actually removed
        assert!(!cache.contains_aggregate(1, 100, 200));
        assert_eq!(cache.aggregate_count(), 2);
        assert_eq!(cache.memory_usage_bytes(), 2000);
    }

    #[test]
    fn test_touch_aggregate() {
        let mut cache = LruMemoryCache::new(16 * 1024 * 1024);

        let event = EventItem::new(1, 1, 1000, 42, 1, b"test".to_vec());
        let batch = EventBatchItem::new(1, 2000, 123, Some(456), vec![event]);
        let metadata = EventBatchMetadata {
            event_batch_index: 1,
            uncompressed_size: 1000,
            compressed_size: 500,
            ..Default::default()
        };

        cache.add(554, 665, 123, batch, metadata);

        // Touch should promote to MRU without accessing data
        assert!(cache.touch_aggregate(554, 665, 123));
        assert!(!cache.touch_aggregate(554, 665, 999)); // Non-existent aggregate

        // Verify aggregate is still there
        assert!(cache.contains_aggregate(554, 665, 123));
    }

    #[test]
    fn test_peek_operations() {
        let mut cache = LruMemoryCache::new(16 * 1024 * 1024);

        let event = EventItem::new(1, 1, 1000, 42, 1, b"test".to_vec());
        let batch = EventBatchItem::new(1, 2000, 123, Some(456), vec![event]);
        let metadata = EventBatchMetadata {
            event_batch_index: 1,
            uncompressed_size: 1000,
            compressed_size: 500,
            ..Default::default()
        };

        cache.add(554, 665, 123, batch, metadata);

        // Test peek operations
        if let Some((org_id, aggregate_type_id, aggregate_id, batches)) = cache.peek_lru() {
            assert_eq!((org_id, aggregate_type_id, aggregate_id), (554, 665, 123));
            assert_eq!(batches.len(), 1);
        }

        if let Some((org_id, aggregate_type_id, aggregate_id, batches)) = cache.peek_mru() {
            assert_eq!((org_id, aggregate_type_id, aggregate_id), (554, 665, 123));
            assert_eq!(batches.len(), 1);
        }
    }

    #[test]
    fn test_memory_utilization() {
        let mut cache = LruMemoryCache::new(2000); // 2KB limit

        assert_eq!(cache.memory_utilization_percent(), 0.0);

        let event = EventItem::new(1, 1, 1000, 42, 1, b"test".to_vec());
        let batch = EventBatchItem::new(1, 2000, 123, Some(456), vec![event]);
        let metadata = EventBatchMetadata {
            event_batch_index: 1,
            uncompressed_size: 1000, // 50% of capacity
            compressed_size: 500,
            ..Default::default()
        };

        cache.add(554, 665, 123, batch, metadata);
        assert_eq!(cache.memory_utilization_percent(), 50.0);

        // Test memory limit update
        cache.set_max_memory_bytes(4000);
        assert_eq!(cache.memory_utilization_percent(), 25.0);
    }

    #[test]
    fn test_cache_key_uniqueness() {
        let mut cache = LruMemoryCache::new(16 * 1024 * 1024);

        // Create entries that would collide if we didn't have proper isolation
        let combinations = vec![
            (1, 100, 200, "data1"),
            (1, 100, 201, "data2"),
            (1, 101, 200, "data3"),
            (2, 100, 200, "data4"),
        ];

        // Add all combinations
        for (org_id, aggregate_type_id, aggregate_id, data) in &combinations {
            let event = EventItem::new(1, 1, 1000, 42, 1, data.as_bytes().to_vec());
            let batch = EventBatchItem::new(1, 2000, *aggregate_id, Some(456), vec![event]);
            let metadata = EventBatchMetadata {
                event_batch_index: 1,
                uncompressed_size: 500,
                compressed_size: 250,
                ..Default::default()
            };
            cache.add(*org_id, *aggregate_type_id, *aggregate_id, batch, metadata);
        }

        // Verify all combinations are stored independently
        for (org_id, aggregate_type_id, aggregate_id, _) in combinations {
            assert!(cache.contains_aggregate(org_id, aggregate_type_id, aggregate_id));
            assert!(
                cache
                    .get_batch(org_id, aggregate_type_id, aggregate_id, 0)
                    .is_some()
            );
        }

        assert_eq!(cache.aggregate_count(), 4);
        assert_eq!(cache.memory_usage_bytes(), 2000); // 4 * 500
    }
}
