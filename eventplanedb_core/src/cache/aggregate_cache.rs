use std::cell::RefCell;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::rc::Rc;

use eventplanedb_structures::aggregate_key::AggregateKey;
use lru::LruCache;

use crate::cache::aggregate_resources::AggregateResources;
use crate::node_config::NodeConfig;
use crate::object_store::ObjectStoreGateway;
use crate::read_operations::read_error::ReadError;
use crate::read_operations::read_operations::ReadOperations;
use crate::read_operations::read_structures::AggregateReadConfig;
use crate::replication::aggregate_lease::AggregateLease;
use crate::write_operations::write_operations::WriteOperations;
use crate::write_operations::write_structures::AggregateWriteConfig;

pub struct AggregateCache {
    aggregates_cache: Rc<RefCell<LruCache<AggregateKey, Rc<AggregateResources>>>>,
    aggregates_lease: Rc<RefCell<HashMap<AggregateKey, Rc<AggregateLease>>>>,
    node_config: NodeConfig,
    pub aggregate_read_config: Rc<RefCell<AggregateReadConfig>>,
    pub aggregate_write_config: Rc<RefCell<AggregateWriteConfig>>,
    /// Optional gateway for S3 operations (None if S3 is disabled).
    object_store_gateway: Option<ObjectStoreGateway>,
    /// Optional subfolder prefix for S3 paths.
    s3_subfolder: Option<String>,
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
            aggregates_lease: Rc::new(RefCell::new(HashMap::new())),
            object_store_gateway: None,
            s3_subfolder: None,
        }
    }

    pub fn with_object_store(
        capacity: NonZeroUsize,
        node_config: NodeConfig,
        aggregate_read_config: AggregateReadConfig,
        aggregate_write_config: AggregateWriteConfig,
        gateway: ObjectStoreGateway,
        s3_subfolder: Option<String>,
    ) -> Self {
        Self {
            aggregates_cache: Rc::new(RefCell::new(LruCache::new(capacity))),
            node_config,
            aggregate_read_config: Rc::new(RefCell::new(aggregate_read_config)),
            aggregate_write_config: Rc::new(RefCell::new(aggregate_write_config)),
            aggregates_lease: Rc::new(RefCell::new(HashMap::new())),
            object_store_gateway: Some(gateway),
            s3_subfolder,
        }
    }

    pub fn get_aggregate_lease(&self, aggregate_key: &AggregateKey) -> Rc<AggregateLease> {
        let mut cache = self.aggregates_lease.borrow_mut();
        if let Some(aggregate_lease) = cache.get(aggregate_key) {
            return aggregate_lease.clone();
        }

        let aggregate_lease = match &self.object_store_gateway {
            Some(gateway) => Rc::new(AggregateLease::with_gateway(
                aggregate_key.clone(),
                self.node_config.clone(),
                gateway.clone(),
                self.s3_subfolder.clone(),
            )),
            None => Rc::new(AggregateLease::new(
                aggregate_key.clone(),
                self.node_config.clone(),
            )),
        };

        cache.insert(aggregate_key.clone(), aggregate_lease.clone());
        aggregate_lease
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
        let mut cache = self.aggregates_cache.borrow_mut();
        let aggregate_resources = cache.pop(aggregate_key);
        if let Some(aggregate_resources) = aggregate_resources {
            let mut reader = aggregate_resources.get_reader_mut(false).await?;
            let mut writer = aggregate_resources.get_writer_mut(false).await?;

            reader.as_mut().unwrap().close().await?;
            writer.as_mut().unwrap().close().await?;
        }

        // Also remove from lease cache
        self.aggregates_lease.borrow_mut().remove(aggregate_key);

        Ok(())
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

    /// Check if the object store gateway is healthy.
    pub fn is_object_store_healthy(&self) -> bool {
        self.object_store_gateway
            .as_ref()
            .map(|g| g.is_healthy())
            .unwrap_or(true) // If no gateway, consider it "healthy" (disabled)
    }
}