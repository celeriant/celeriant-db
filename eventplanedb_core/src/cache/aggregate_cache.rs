use std::num::NonZeroUsize;
use std::cell::RefCell;
use std::rc::Rc;

use eventplanedb_structures::aggregate_key::AggregateKey;
use lru::LruCache;

use crate::cache::aggregate_resources::AggregateResources;

pub struct AggregateCache {
    aggregates_cache: Rc<RefCell<LruCache<AggregateKey, Rc<AggregateResources>>>>,
}

impl AggregateCache {
    pub fn new(capacity: NonZeroUsize) -> Self {
        Self {
            aggregates_cache: Rc::new(RefCell::new(LruCache::new(capacity)))
        }
    }

    pub fn get<F>(&self, aggregate_key: &AggregateKey) -> Rc<AggregateResources>
    where
        F: FnOnce() -> AggregateResources,
    {
        let mut cache = self.aggregates_cache.borrow_mut();
        
        if let Some(resources) = cache.get(aggregate_key) {
            return Rc::clone(resources);
        }
        
        let resources = Rc::new(AggregateResources::new());
        cache.put(aggregate_key.clone(), Rc::clone(&resources));
        resources
    }
}