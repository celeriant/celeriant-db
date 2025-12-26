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
///
/// # Usage Pattern
///
/// ```ignore
/// // 1. Fast path - check cache without lock
/// if cache.contains(&key) { return Ok(value); }
///
/// // 2. Acquire loading lock
/// let lock = coordinator.acquire(&key);
/// let _guard = lock.write().await?;
///
/// // 3. Re-check cache (another task may have loaded while we waited)
/// if cache.contains(&key) {
///     drop(_guard);
///     drop(lock);
///     coordinator.release(&key);
///     return Ok(value);
/// }
///
/// // 4. Load from disk and update cache
/// let value = load_from_disk(&key).await?;
/// cache.insert(key.clone(), value);
///
/// // 5. Cleanup
/// drop(_guard);
/// drop(lock);
/// coordinator.release(&key);
/// ```
pub struct LoadingCoordinator<K> {
    pending: RefCell<HashMap<K, Rc<RwLock<()>>>>,
}

impl<K: Eq + Hash + Clone> LoadingCoordinator<K> {
    pub fn new() -> Self {
        Self {
            pending: RefCell::new(HashMap::new()),
        }
    }

    /// Get or create a lock for serializing load operations on this key.
    ///
    /// Caller must call `release()` after dropping the lock Rc.
    pub fn acquire(&self, key: &K) -> Rc<RwLock<()>> {
        self.pending
            .borrow_mut()
            .entry(key.clone())
            .or_insert_with(|| Rc::new(RwLock::new(())))
            .clone()
    }

    /// Attempt to clean up the lock if no other tasks hold references.
    ///
    /// Must be called after dropping the Rc from `acquire()`.
    pub fn release(&self, key: &K) {
        let mut pending = self.pending.borrow_mut();
        if let Some(lock) = pending.get(key) {
            if Rc::strong_count(lock) == 1 {
                pending.remove(key);
            }
        }
    }
}

impl<K: Eq + Hash + Clone> Default for LoadingCoordinator<K> {
    fn default() -> Self {
        Self::new()
    }
}