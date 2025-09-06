use lru::LruCache;
use std::num::NonZeroUsize;

use eventplanedb_storage_structures::event_batch_item::EventBatchItem;
use eventplanedb_storage_structures::event_batch_metadata::EventBatchMetadata;

/// High-performance memory cache using LRU eviction
/// Each aggregate stores a vector of (EventBatchItem, EventBatchMetadata) pairs
/// Uses uncompressed_size for memory-based eviction with manual size tracking
pub struct LruMemoryCache {
    cache: LruCache<String, Vec<(EventBatchItem, EventBatchMetadata)>>,
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
        aggregate_id: &str,
        event_batch_item: EventBatchItem,
        event_batch_metadata: EventBatchMetadata,
    ) {
        let batch_size = event_batch_metadata.uncompressed_size;
        let new_batch_index = event_batch_item.event_batch_index;

        // Try to get mutable reference to existing batches
        if let Some(batches) = self.cache.get_mut(aggregate_id) {
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
            self.cache.put(aggregate_id.to_string(), new_batches);
            self.current_memory_bytes += batch_size;
        }

        // Evict if we exceed memory limit
        self.evict_if_needed();
    }

    /// Add multiple batches at once for better performance
    pub fn add_batches(
        &mut self,
        aggregate_id: &str,
        mut new_batches: Vec<(EventBatchItem, EventBatchMetadata)>,
    ) {
        let batch_total_size = Self::calculate_memory_usage(&new_batches);
        let new_batch_index = new_batches.first().unwrap().0.event_batch_index;

        if let Some(existing_batches) = self.cache.get_mut(aggregate_id) {
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
            self.cache.put(aggregate_id.to_string(), new_batches);
            self.current_memory_bytes += batch_total_size;
        }

        // Evict if we exceed memory limit
        self.evict_if_needed();
    }

    /// Get the array index position for a given event batch index
    /// Returns None if the event batch index is not found in cache
    pub fn get_pos(&mut self, aggregate_id: &str, from_event_batch_index: u64) -> Option<usize> {
        let batches = self.cache.get(aggregate_id)?;

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
    pub fn get_batch(&mut self, aggregate_id: &str, index_pos: usize) -> Option<&EventBatchItem> {
        let batches = self.cache.get(aggregate_id)?;
        batches.get(index_pos).map(|(batch, _)| batch)
    }

    /// Get event batch metadata at the specified array index position
    /// Returns None if the aggregate or index doesn't exist
    pub fn get_meta(
        &mut self,
        aggregate_id: &str,
        index_pos: usize,
    ) -> Option<&EventBatchMetadata> {
        let batches = self.cache.get(aggregate_id)?;
        batches.get(index_pos).map(|(_, metadata)| metadata)
    }

    /// Get mutable reference to event batch item - enables in-place modifications
    pub fn get_batch_mut(
        &mut self,
        aggregate_id: &str,
        index_pos: usize,
    ) -> Option<&mut EventBatchItem> {
        let batches = self.cache.get_mut(aggregate_id)?;
        batches.get_mut(index_pos).map(|(batch, _)| batch)
    }

    /// Get mutable reference to event batch metadata - enables in-place modifications
    pub fn get_meta_mut(
        &mut self,
        aggregate_id: &str,
        index_pos: usize,
    ) -> Option<&mut EventBatchMetadata> {
        let batches = self.cache.get_mut(aggregate_id)?;
        batches.get_mut(index_pos).map(|(_, metadata)| metadata)
    }

    /// Get both batch and metadata as mutable references
    pub fn get_batch_and_meta_mut(
        &mut self,
        aggregate_id: &str,
        index_pos: usize,
    ) -> Option<(&mut EventBatchItem, &mut EventBatchMetadata)> {
        let batches = self.cache.get_mut(aggregate_id)?;
        batches
            .get_mut(index_pos)
            .map(|(batch, metadata)| (batch, metadata))
    }

    /// Get immutable reference to all batches for an aggregate
    pub fn get_all_batches(
        &mut self,
        aggregate_id: &str,
    ) -> Option<&[(EventBatchItem, EventBatchMetadata)]> {
        self.cache.get(aggregate_id).map(|v| v.as_slice())
    }

    /// Get mutable reference to all batches for an aggregate
    pub fn get_all_batches_mut(
        &mut self,
        aggregate_id: &str,
    ) -> Option<&mut Vec<(EventBatchItem, EventBatchMetadata)>> {
        self.cache.get_mut(aggregate_id)
    }

    /// Clear all cached data for a specific aggregate and update memory tracking
    pub fn clear_aggregate(&mut self, aggregate_id: &str) {
        if let Some(removed_batches) = self.cache.pop(aggregate_id) {
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
    pub fn contains_aggregate(&self, aggregate_id: &str) -> bool {
        self.cache.contains(aggregate_id)
    }

    /// Promote an aggregate to most recently used without accessing data
    pub fn touch_aggregate(&mut self, aggregate_id: &str) -> bool {
        self.cache.promote(aggregate_id)
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
    pub fn peek_lru(&self) -> Option<(&String, &Vec<(EventBatchItem, EventBatchMetadata)>)> {
        self.cache.peek_lru()
    }

    /// Peek at the most recently used aggregate without affecting LRU order
    pub fn peek_mru(&self) -> Option<(&String, &Vec<(EventBatchItem, EventBatchMetadata)>)> {
        self.cache.peek_mru()
    }

    /// Force eviction of the least recently used aggregate
    /// Returns the evicted aggregate ID and its batches, if any
    pub fn force_evict_lru(
        &mut self,
    ) -> Option<(String, Vec<(EventBatchItem, EventBatchMetadata)>)> {
        if let Some((aggregate_id, batches)) = self.cache.pop_lru() {
            let removed_size = Self::calculate_memory_usage(&batches);
            self.current_memory_bytes = self.current_memory_bytes.saturating_sub(removed_size);
            Some((aggregate_id, batches))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eventplanedb_storage_structures::event_item::EventItem;

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
        cache.add("test_aggregate", batch1, metadata1);

        // Try to add batch with gap (index 3 instead of 2)
        let event3 = EventItem::new(3, 3, 1000, 42, 1, b"test3".to_vec());
        let batch3 = EventBatchItem::new(3, 2000, 123, Some(456), vec![event3]);
        let metadata3 = EventBatchMetadata {
            event_batch_index: 3,
            uncompressed_size: 1000,
            compressed_size: 500,
            ..Default::default()
        };
        cache.add("test_aggregate", batch3, metadata3);

        // Should only have 1 batch (the gap batch should be rejected)
        assert_eq!(cache.get_all_batches("test_aggregate").unwrap().len(), 1);
        assert_eq!(cache.memory_usage_bytes(), 1000); // Only first batch

        // Should not be able to find batch index 3
        assert!(cache.get_pos("test_aggregate", 3).is_none());

        // Now add correct sequential batch (index 2)
        let event2 = EventItem::new(2, 2, 1000, 42, 1, b"test2".to_vec());
        let batch2 = EventBatchItem::new(2, 2000, 123, Some(456), vec![event2]);
        let metadata2 = EventBatchMetadata {
            event_batch_index: 2,
            uncompressed_size: 1000,
            compressed_size: 500,
            ..Default::default()
        };
        cache.add("test_aggregate", batch2, metadata2);

        // Now should have 2 batches
        assert_eq!(cache.get_all_batches("test_aggregate").unwrap().len(), 2);
        assert_eq!(cache.memory_usage_bytes(), 2000);
        assert_eq!(cache.get_pos("test_aggregate", 2), Some(1));
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

        cache.add("test_aggregate", batch, metadata);

        // Should find the batch at index 5
        assert_eq!(cache.get_pos("test_aggregate", 5), Some(0));
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
            cache.add("test_aggregate", batch, metadata);
        }

        // All batches should be present
        assert_eq!(cache.get_pos("test_aggregate", 1), Some(0));
        assert_eq!(cache.get_pos("test_aggregate", 2), Some(1));
        assert_eq!(cache.get_pos("test_aggregate", 3), Some(2));

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
        cache.add("aggregate1", batch1, metadata1);

        // Add second aggregate with 1KB (total: 2KB, at limit)
        let event2 = EventItem::new(1, 1, 1000, 42, 1, b"test2".to_vec());
        let batch2 = EventBatchItem::new(1, 2000, 789, Some(456), vec![event2]);
        let metadata2 = EventBatchMetadata {
            event_batch_index: 1,
            uncompressed_size: 1000,
            compressed_size: 500,
            ..Default::default()
        };
        cache.add("aggregate2", batch2, metadata2);

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
        cache.add("aggregate3", batch3, metadata3);

        // Should have evicted aggregate1 (LRU)
        assert!(!cache.contains_aggregate("aggregate1"));
        assert!(cache.contains_aggregate("aggregate2"));
        assert!(cache.contains_aggregate("aggregate3"));
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

        cache.add("test_aggregate", batch, metadata);

        // Test mutable access
        if let Some(batch_mut) = cache.get_batch_mut("test_aggregate", 0) {
            batch_mut.event_batch_index = 99;
        }

        // Verify the change
        if let Some(batch) = cache.get_batch("test_aggregate", 0) {
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

        cache.add_batches("test_aggregate", batches);

        assert_eq!(cache.aggregate_count(), 1);
        assert_eq!(cache.memory_usage_bytes(), 1500); // 3 * 500
        assert_eq!(cache.get_pos("test_aggregate", 1), Some(0));
        assert_eq!(cache.get_pos("test_aggregate", 2), Some(1));
        assert_eq!(cache.get_pos("test_aggregate", 3), Some(2));
    }
}
