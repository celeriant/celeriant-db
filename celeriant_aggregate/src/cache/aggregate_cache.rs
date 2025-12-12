use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::rc::Rc;

use celeriant_wal::aggregate_key::AggregateKey;
use lru::LruCache;

use crate::cache::aggregate_resources::AggregateResources;
use crate::node_config::NodeConfig;
use crate::read_operations::read_error::ReadError;
use crate::read_operations::read_operations::ReadOperations;
use crate::read_operations::read_structures::AggregateReadConfig;
use crate::write_operations::write_operations::WriteOperations;
use crate::write_operations::aggregate_write_config::AggregateWriteConfig;

pub struct AggregateCache {
    aggregates_cache: Rc<RefCell<LruCache<AggregateKey, Rc<AggregateResources>>>>,
    node_config: NodeConfig,
    pub aggregate_read_config: Rc<RefCell<AggregateReadConfig>>,
    pub aggregate_write_config: Rc<RefCell<AggregateWriteConfig>>,
}

impl AggregateCache {
    pub fn new(
        capacity: NonZeroUsize,
        node_config: NodeConfig,
        aggregate_read_config: AggregateReadConfig,
        aggregate_write_config: AggregateWriteConfig,
    ) -> Self {
        Self {
            aggregates_cache: Rc::new(RefCell::new(LruCache::new(capacity))),
            node_config,
            aggregate_read_config: Rc::new(RefCell::new(aggregate_read_config)),
            aggregate_write_config: Rc::new(RefCell::new(aggregate_write_config)),
        }
    }

    pub fn get_aggregate_resources(&self, aggregate_key: &AggregateKey) -> Rc<AggregateResources> {
        let mut cache = self.aggregates_cache.borrow_mut();

        if let Some(resources) = cache.get(aggregate_key) {
            return Rc::clone(resources);
        }

        let resources = Rc::new(AggregateResources::new(
            aggregate_key.clone(),
            &self.node_config.data_root_folder,
            self.aggregate_read_config.borrow().clone(),
            self.aggregate_write_config.borrow().clone(),
        ));
        cache.put(aggregate_key.clone(), Rc::clone(&resources));
        resources
    }

    pub async fn pop(&self, aggregate_key: &AggregateKey) -> Result<(), ReadError> {
    
        let aggregate_resources = {
            let mut cache = self.aggregates_cache.borrow_mut();
            cache.pop(aggregate_key)
        };

        if let Some(aggregate_resources) = aggregate_resources {
            let mut reader = aggregate_resources.get_reader_mut(false).await?;
            let mut writer = aggregate_resources.get_writer_mut(false).await?;

            reader.close().await?;
            writer.close().await?;
        }

        Ok(())
    }

    pub async fn close(&self) {
        // Drain all entries from cache
        let all_resources: Vec<Rc<AggregateResources>> = {
            let mut cache = self.aggregates_cache.borrow_mut();
            let mut resources = Vec::new();
            while let Some((_, v)) = cache.pop_lru() {
                resources.push(v);
            }
            resources
        };

        // Spawn all close operations as concurrent tasks
        let tasks: Vec<_> = all_resources
            .into_iter()
            .map(|resources| {
                glommio::spawn_local(async move {
                    if let Ok(mut reader) = resources.get_reader_mut(false).await {
                        let _ = reader.close().await;
                    }
                    if let Ok(mut writer) = resources.get_writer_mut(false).await {
                        let _ = writer.close().await;
                    }
                })
            })
            .collect();

        // Wait for all tasks to complete
        for task in tasks {
            task.await;
        }
    }

    pub fn get_all_keys(&self) -> Vec<AggregateKey> {
        self.aggregates_cache
            .borrow()
            .iter()
            .map(|(k, _)| k.clone())
            .collect()
    }

    pub fn update_configs(&self, read_config: AggregateReadConfig, write_config: AggregateWriteConfig) {
        *self.aggregate_read_config.borrow_mut() = read_config;
        *self.aggregate_write_config.borrow_mut() = write_config;
    }
}