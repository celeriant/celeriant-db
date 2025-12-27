//! Coordinates concurrent loading operations to prevent thundering herd.
//!
//! When multiple async tasks attempt to load the same data from disk,
//! this ensures only one performs the I/O while others wait.

use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::Hash;
use std::rc::Rc;

use glommio::sync::RwLock;

/// Per-key loading serialization for async operations.
///
/// Locks are created on-demand and cleaned up when no longer referenced.
/// Not thread-safe—designed for single-threaded async runtimes like glommio.
pub struct LoadingCoordinator<K> {
    pending: RefCell<HashMap<K, Rc<RwLock<()>>>>,
}

impl<K: Eq + Hash + Clone> Default for LoadingCoordinator<K> {
    fn default() -> Self {
        Self::new()
    }
}

pub struct LoadingGuard<'a, K: Eq + Hash + Clone> {
    coordinator: &'a LoadingCoordinator<K>,
    key: K,
    lock: Rc<RwLock<()>>,
}

impl<K: Eq + Hash + Clone> LoadingCoordinator<K> {
    pub fn new() -> Self {
        Self {
            pending: RefCell::new(HashMap::new()),
        }
    }

    pub fn acquire(&self, key: &K) -> LoadingGuard<'_, K> {
        let lock = self.pending
            .borrow_mut()
            .entry(key.clone())
            .or_insert_with(|| Rc::new(RwLock::new(())))
            .clone();
        
        LoadingGuard {
            coordinator: self,
            key: key.clone(),
            lock,
        }
    }
}

impl<K: Eq + Hash + Clone> Drop for LoadingGuard<'_, K> {
    fn drop(&mut self) {
        let mut pending = self.coordinator.pending.borrow_mut();
        if let Some(lock) = pending.get(&self.key) {
            // Count is 2 when only HashMap + this guard hold references
            // (checked before our Rc drops)
            if Rc::strong_count(lock) == 2 {
                pending.remove(&self.key);
            }
        }
    }
}

impl<K: Eq + Hash + Clone> std::ops::Deref for LoadingGuard<'_, K> {
    type Target = RwLock<()>;
    fn deref(&self) -> &Self::Target {
        &self.lock
    }
}
