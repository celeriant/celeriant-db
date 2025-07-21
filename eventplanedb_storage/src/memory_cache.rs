use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use crate::event_batch_item::EventBatchItem;

#[derive(Hash, Eq, PartialEq)]
struct CacheKey {
    file_path: String,
    si: u64,
}

#[derive(Clone)]
struct CacheEntry {
    event_batch_item: Arc<EventBatchItem>,
    timestamp: Instant,
    compressed_batch_size: usize
}

pub struct MemoryCache {
    cache: HashMap<CacheKey, CacheEntry>,
    ttl: Duration,
    enabled: bool,
}

impl MemoryCache {
    pub fn new(ttl_secs: u64) -> Self {
        Self {
            cache: HashMap::new(),
            ttl: Duration::from_secs(ttl_secs),
            enabled: ttl_secs > 0,
        }
    }

    fn cleanup_expired(&mut self) {
        let now = Instant::now();
        self.cache.retain(|_, entry| now.duration_since(entry.timestamp) < self.ttl);
    }

    fn make_cache_key(&self, file_path: &str, from_si: u64) -> CacheKey {
        CacheKey {
            file_path: file_path.to_string(),
            si: from_si,
        }
    }

    pub fn get(&mut self, file_path: &str, from_si: u64) -> Option<(Arc<EventBatchItem>, usize)> {
        if !self.enabled {
            return None;
        }

        self.cleanup_expired();
        
        let key = self.make_cache_key(file_path, from_si);
        self.cache.get(&key).map(|entry| (entry.event_batch_item.clone(), entry.compressed_batch_size))
    }

    pub fn put(&mut self, file_path: &str, from_si: u64, event_batch_item: Arc<EventBatchItem>, compressed_batch_size: usize) {
        if !self.enabled {
            return;
        }

        self.cleanup_expired();
        
        let key = self.make_cache_key(file_path, from_si);
        let entry = CacheEntry {
            event_batch_item,
            timestamp: Instant::now(),
            compressed_batch_size
        };
        self.cache.insert(key, entry);
    }

    pub fn invalidate_file(&mut self, file_path: &str) {
        if !self.enabled {
            return;
        }

        self.cache.retain(|key, _| key.file_path != file_path);
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.cache.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_item::tests::create_test_event_item;
    use crate::wire_format::tests::generate_short_client_identity;
    use std::thread;

    #[test]
    fn test_cache_basic_operations() {
        let mut cache = MemoryCache::new(30);

        let mut event_batch_item = EventBatchItem::new();
        event_batch_item.user_id = Some("test".to_string());
        event_batch_item.client_id = generate_short_client_identity("test2");
        event_batch_item.server_id = 0;
        event_batch_item.server_date = 23432;
        event_batch_item.events.push(create_test_event_item());
        
        // Test put and get
        cache.put("test.bin", 0, Arc::new( event_batch_item), 1024);
        let (cached_events, batch_size) = cache.get("test.bin", 0).unwrap();
        assert_eq!(cached_events.events.len(), 1);
        assert_eq!(batch_size, 1024);
        
        // Test cache miss
        let miss = cache.get("test.bin", 1);
        assert!(miss.is_none());
    }

    #[test]
    fn test_cache_file_invalidation() {
        let mut cache = MemoryCache::new(30);

        let mut event_batch_item = EventBatchItem::new();
        event_batch_item.user_id = Some("test".to_string());
        event_batch_item.client_id = generate_short_client_identity("test2");
        event_batch_item.server_id = 0;
        event_batch_item.server_date = 23432;
        event_batch_item.events.push(create_test_event_item());
        
        cache.put("test1.bin", 0, Arc::new(event_batch_item.clone()), 512);
        cache.put("test2.bin", 0, Arc::new(event_batch_item), 1024);
        assert_eq!(cache.len(), 2);
        
        cache.invalidate_file("test1.bin");
        assert_eq!(cache.len(), 1);
        
        let result = cache.get("test1.bin", 0);
        assert!(result.is_none());
        
        let (result, _) = cache.get("test2.bin", 0).unwrap();
        assert_eq!(result.events.len(), 1);
    }

    #[test]
    fn test_cache_ttl_cleanup() {
        let mut cache = MemoryCache {
            cache: HashMap::new(),
            ttl: Duration::from_millis(50),
            enabled: true
        };
        
        let mut event_batch_item = EventBatchItem::new();
        event_batch_item.user_id = Some("test".to_string());
        event_batch_item.client_id = generate_short_client_identity("test2");
        event_batch_item.server_id = 0;
        event_batch_item.server_date = 23432;
        event_batch_item.events.push(create_test_event_item());

        cache.put("test.bin", 0, Arc::new(event_batch_item), 256);
        assert_eq!(cache.len(), 1);
        
        thread::sleep(Duration::from_millis(60));
        
        let result = cache.get("test.bin", 0);
        assert!(result.is_none());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_cache_disabled_when_ttl_zero() {
        let mut cache = MemoryCache::new(0);

        let mut event_batch_item = EventBatchItem::new();
        event_batch_item.user_id = Some("test".to_string());
        event_batch_item.client_id = generate_short_client_identity("test2");
        event_batch_item.server_id = 0;
        event_batch_item.server_date = 23432;
        event_batch_item.events.push(create_test_event_item());
        
        // Test that put does nothing when cache is disabled
        cache.put("test.bin", 0, Arc::new(event_batch_item), 1024);
        assert_eq!(cache.len(), 0);
        
        // Test that get always returns None when cache is disabled
        let result = cache.get("test.bin", 0);
        assert!(result.is_none());
        
        // Test that invalidate_file does nothing when cache is disabled
        cache.invalidate_file("test.bin");
        assert_eq!(cache.len(), 0);
    }
}