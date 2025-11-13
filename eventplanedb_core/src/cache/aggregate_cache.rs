use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::rc::Rc;

use eventplanedb_structures::aggregate_key::AggregateKey;
use lru::LruCache;

use crate::cache::aggregate_resources::AggregateResources;
use crate::read_operations::read_structures::AggregateReadConfig;
use crate::write_operations::write_structures::AggregateWriteConfig;

pub struct AggregateCache {
    aggregates_cache: Rc<RefCell<LruCache<AggregateKey, Rc<AggregateResources>>>>,
    data_root_folder: String,
    aggregate_read_config: AggregateReadConfig,
    aggregate_write_config: AggregateWriteConfig,
}

impl AggregateCache {
    pub fn new(
        capacity: NonZeroUsize,
        data_root_folder: String,
        aggregate_read_config: AggregateReadConfig,
        aggregate_write_config: AggregateWriteConfig,
    ) -> Self {
        Self {
            aggregates_cache: Rc::new(RefCell::new(LruCache::new(capacity))),
            data_root_folder,
            aggregate_read_config,
            aggregate_write_config,
        }
    }

    pub fn get(&self, aggregate_key: &AggregateKey) -> Rc<AggregateResources> {
        let mut cache = self.aggregates_cache.borrow_mut();

        if let Some(resources) = cache.get(aggregate_key) {
            return Rc::clone(resources);
        }

        let resources = Rc::new(AggregateResources::new(
            aggregate_key.clone(),
            &self.data_root_folder,
            self.aggregate_read_config.clone(),
            self.aggregate_write_config.clone(),
        ));
        cache.put(aggregate_key.clone(), Rc::clone(&resources));
        resources
    }
}
