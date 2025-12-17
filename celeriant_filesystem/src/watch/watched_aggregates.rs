use std::{cell::RefCell, collections::HashMap, rc::Rc};

use celeriant_wal::aggregate_key::AggregateKey;

use crate::watch::{aggregate_watch_event::AggregateWatchEvent, aggregate_watchers::AggregateWatchers};

pub struct WatchedAggregates {
    aggregates: RefCell<HashMap<AggregateKey, Rc<AggregateWatchers>>>,
}

impl WatchedAggregates {
    pub fn new() -> Self {
        Self { 
            aggregates: RefCell::new(HashMap::new()),
        }
    }

    /// Get existing watchers for an aggregate, or create new ones
    pub fn get_or_create(&self, key: &AggregateKey) -> Rc<AggregateWatchers> {
        let mut aggregates = self.aggregates.borrow_mut();
        
        if let Some(watchers) = aggregates.get(key) {
            return Rc::clone(watchers);
        }
        
        let watchers = Rc::new(AggregateWatchers::new());
        aggregates.insert(key.clone(), Rc::clone(&watchers));
        watchers
    }

    /// Get existing watchers if any exist (for broadcasting events)
    pub fn get(&self, key: &AggregateKey) -> Option<Rc<AggregateWatchers>> {
        self.aggregates.borrow().get(key).cloned()
    }

    /// Remove watchers for an aggregate if they have no subscribers
    pub fn remove_if_empty(&self, key: &AggregateKey) {
        let mut aggregates = self.aggregates.borrow_mut();
        if let Some(watchers) = aggregates.get(key) {
            if watchers.is_empty() {
                aggregates.remove(key);
            }
        }
    }

    /// Broadcast an event to all watchers for an aggregate (if any exist)
    pub fn notify(&self, key: &AggregateKey, event: AggregateWatchEvent) {
        if let Some(watchers) = self.get(key) {
            watchers.broadcast(event);
        }
    }
}

impl Default for WatchedAggregates {
    fn default() -> Self {
        Self::new()
    }
}