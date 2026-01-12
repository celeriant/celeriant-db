---
name: glommio-locking-patterns
description: Locking patterns and concurrent operations for Glommio executors. Use when working with the shard read/write paths, replication, or any async code that requires synchronization. Critical for avoiding deadlocks from holding synchronous locks across async boundaries.
---

# Glommio Locking Patterns and Concurrency

## Core Principle

**Glommio is single-threaded per executor.** There is no thread contention, but there IS async contention. Multiple async tasks can be interleaved at `.await` points. The critical rule:

> **NEVER hold a `RefCell` borrow across an `.await` boundary.**

Violating this causes deadlocks under concurrent load because another task may try to borrow while the first task is suspended at an await point.

## When to Use RefCell vs Glommio RwLock

### Use `RefCell` When

- All operations on the data are **synchronous** (no `.await` while borrowed)
- You can **complete the operation and drop the borrow** before any async work
- The data is only accessed within a single logical operation

```rust
// GOOD: Borrow is dropped before async work
let snapshot = {
    let cache = self.shard_mem_cache.borrow_mut();
    cache.take_sync_positions_snapshot() // Returns owned data
}; // borrow dropped here

// Now safe to await
self.write_to_disk(snapshot).await?;
```

### Use Glommio `RwLock` When

- You need to hold a lock **across async operations** (e.g., holding a file handle during I/O)
- You need to **serialize concurrent async operations** on the same resource
- The resource requires **exclusive access during I/O** (file handles, network connections)

```rust
// Reference: celeriant_rotating_log/src/log_segment_file.rs
pub struct LogSegmentFile {
    // RwLock because we need to hold during async I/O
    writer: RwLock<Option<Rc<DmaFile>>>,
    reader: RwLock<Option<Rc<DmaFile>>>,

    // RefCell is fine - metadata updates are synchronous
    pub metadata: RefCell<LogSegmentFileMetadata>,
}
```

## Pattern: Snapshot Before Async Work

The most common pattern is to **snapshot state before async operations**, then **commit results after**. This avoids holding borrows across await points.

```rust
// Reference: celeriant_shard/src/shard_wal_sync.rs

// 1. Take snapshot (synchronous, short borrow)
let Some((required_disk_space, mut sync_positions_snapshot)) = take_sync_snapshot(&shard_mem_cache) else {
    return Ok(());
};

// 2. Perform async work with snapshot (no borrow held)
let result = sync(active_log_segment, &mut sync_positions_snapshot).await;

// 3. Commit or rollback (synchronous, short borrow)
match result {
    Ok(_) => commit_sync(shard_mem_cache, watched_aggregates, sync_positions_snapshot),
    Err(e) => rollback_sync(shard_mem_cache, &log_segments_cache),
}
```

### Implementation in ShardMemCache

```rust
// Reference: celeriant_memcache/src/shard_mem_cache.rs

pub fn take_sync_positions_snapshot(&mut self) -> SyncPositionsSnapshot {
    // Clone queue positions - they must remain visible for new writes
    // that arrive while this sync is in progress
    let aggregate_queue_positions = self.aggregate_queue_positions.clone();

    // Swap out the pending queue, ready for new writes
    let mut pending_append_queue = vec![];
    std::mem::swap(&mut pending_append_queue, &mut self.pending_append_queue);

    SyncPositionsSnapshot {
        aggregate_queue_positions,
        pending_append_queue,
    }
}
```

## Pattern: Loading Coordinator (Preventing Thundering Herd)

When multiple async tasks attempt to load the same data from disk, use a `LoadingCoordinator` to ensure only one performs the I/O while others wait.

```rust
// Reference: celeriant_shard/src/loading_coordinator.rs

pub struct LoadingCoordinator<K> {
    // RefCell for HashMap (synchronous access to get/create locks)
    // RwLock inside for the actual async serialization
    pending: RefCell<HashMap<K, Rc<RwLock<()>>>>,
}

impl<K: Eq + Hash + Clone> LoadingCoordinator<K> {
    pub fn acquire(&self, key: &K) -> LoadingGuard<'_, K> {
        // Synchronous: get or create a lock for this key
        let lock = self.pending
            .borrow_mut()
            .entry(key.clone())
            .or_insert_with(|| Rc::new(RwLock::new(())))
            .clone();

        LoadingGuard { coordinator: self, key: key.clone(), lock }
    }
}
```

### Usage Pattern

```rust
// Reference: celeriant_shard/src/shard_wal.rs:969-976

// 1. Acquire exclusive lock for this aggregate (returns immediately)
let aggregate_lock = self.aggregate_loading.acquire(searching_for_aggregate_key);

// 2. Wait for write lock (async - other tasks wait here)
let _ = write_with_timeout(&aggregate_lock, "move_aggregate_to_memcache").await?;

// 3. Double-check cache (another task may have completed while we waited)
if let (true, status) = self.shard_mem_cache.borrow_mut().aggregate_load_status(searching_for_aggregate_key) {
    return Ok(status == AggregateStatus::Found);
}

// 4. We have exclusive access, do the expensive work
// ... disk I/O to load aggregate ...
```

## Pattern: Amortization Coordinator (Fsync Batching)

For expensive operations like fsync, batch multiple requests together using a leader/follower pattern.

```rust
// Reference: celeriant_shard/src/amortisation/coordinator.rs

pub struct Coordinator<E: Clone> {
    // RwLock for leader election and event sharing
    lock_orchestrator: RwLock<Option<Rc<LocalEvent<SyncResult<E>>>>>,
    // RwLock to serialize actual sync operations
    sync_serializer: RwLock<()>,
}
```

### How It Works

1. First caller becomes **leader**, creates a shared event
2. Subsequent callers become **followers**, subscribe to the same event
3. Leader sleeps for `delay` (e.g., 5ms) to accumulate more followers
4. Leader clears the event slot and performs the sync
5. Leader broadcasts result to all followers

```rust
// Reference: celeriant_shard/src/amortisation/coordinator.rs:57-97

loop {
    let acquired = {
        match self.lock_orchestrator.try_write() {
            Ok(mut guard) => match guard.as_ref() {
                Some(event) => Acquired::Follower(event.clone()),
                None => {
                    let event = Rc::new(LocalEvent::new());
                    *guard = Some(event.clone());
                    Acquired::Leader(event)
                }
            },
            Err(_) => {
                // Couldn't get write lock, try read lock to become follower
                match read_with_timeout(&self.lock_orchestrator, "...").await {
                    Ok(guard) => match guard.as_ref() {
                        Some(event) => Acquired::Follower(event.clone()),
                        None => Acquired::Retry,
                    },
                    Err(_) => Acquired::Retry,
                }
            }
        }
    }; // Guards dropped here before any await

    match acquired {
        Acquired::Leader(event) => {
            glommio::timer::sleep(delay).await;
            // Clear slot, perform sync, broadcast result
            let result = sync_fn().await;
            event.notify(result.clone());
            return result;
        }
        Acquired::Follower(event) => return event.listen().await,
        Acquired::Retry => continue,
    }
}
```

### Key Properties

- **At most one leader** per batch - sync_serializer RwLock ensures sequential syncs
- **Followers never call sync_fn** - their closure is never invoked
- **Errors propagate to all waiters** - everyone gets the same result
- **New batch can start** while followers receive results

## Pattern: Local Event (Single-Thread Async Notification)

For broadcasting results within a single executor thread, use `LocalEvent` instead of channels.

```rust
// Reference: celeriant_shard/src/amortisation/local_event.rs

pub struct LocalEvent<T = ()> {
    listeners: Rc<RefCell<BTreeMap<u64, ListenerState<T>>>>,
    last_id: Cell<u64>,
}

impl<T: Clone> LocalEvent<T> {
    pub fn listen(&self) -> LocalEventListener<T> {
        // Synchronous: register a listener
        let id = self.last_id.get();
        self.last_id.set(id.wrapping_add(1));
        // ... add to map ...
        LocalEventListener { id, listeners: self.listeners.clone() }
    }

    pub fn notify(&self, result: T) {
        // Synchronous: wake all listeners with cloned result
        let mut listeners = self.listeners.borrow_mut();
        for listener in listeners.values_mut() {
            listener.result = Some(result.clone());
            if let Some(waker) = listener.waker.take() {
                waker.wake();
            }
        }
    }
}
```

LocalEventListener implements `Future` - when polled, it either returns the result (if available) or registers a waker.

## Pattern: RwLock Timeout (Deadlock Detection)

Wrap lock acquisitions with timeouts to detect potential deadlocks during development. And avoid blocked tasks during high server load (we error and force retry by client instead)

```rust
// Reference: celeriant_rotating_log/src/rwlock_timeout.rs

const DEADLOCK_TIMEOUT: Duration = Duration::from_secs(1);

pub async fn write_with_timeout<'a, T>(
    lock: &'a RwLock<T>,
    location: &'static str,
) -> Result<RwLockWriteGuard<'a, T>, LockTimeoutError> {
    let result = or(
        async { Some(lock.write().await) },
        async {
            Timer::new(DEADLOCK_TIMEOUT).await;
            None
        },
    ).await;

    match result {
        Some(Ok(guard)) => Ok(guard),
        Some(Err(e)) => Err(LockTimeoutError::LockError(e)),
        None => Err(LockTimeoutError::PotentialDeadlock {
            duration: DEADLOCK_TIMEOUT,
            operation: "write",
            location,
        }),
    }
}
```

**Always use these wrappers** for RwLock acquisitions. The `location` parameter helps identify where deadlocks occur.

## Architecture Summary

| Component | Lock Type | Why |
|-----------|-----------|-----|
| `ShardMemCache` | `RefCell` (wrapped externally) | Pure synchronous data structure |
| `LogSegmentFile.writer/reader` | `RwLock<Option<Rc<DmaFile>>>` | Held during async I/O |
| `LogSegmentFile.metadata` | `RefCell` | Synchronous metadata updates |
| `LoadingCoordinator.pending` | `RefCell<HashMap<K, Rc<RwLock>>>` | HashMap access sync, per-key lock async |
| `Coordinator.lock_orchestrator` | `RwLock` | Leader election across async boundary |
| `LocalEvent.listeners` | `RefCell` | All operations synchronous |
| `LogSegmentsCache.active_file` | `RefCell` | Synchronous Rc swaps |
| `LogSegmentsCache.lru_cache` | `RefCell` | Synchronous cache operations |

## Checklist for New Async Code

1. **Identify lock type needed:**
   - Will you `.await` while holding the lock? → Use `RwLock`
   - All operations synchronous? → Use `RefCell`

2. **Drop borrows before await:**
   ```rust
   // Extract data, drop borrow
   let data = {
       let guard = self.cache.borrow();
       guard.get_data().clone()
   }; // guard dropped

   // Now safe to await
   self.process(data).await?;
   ```

3. **Use snapshot pattern for state updates:**
   - Take snapshot (sync)
   - Perform async work with snapshot
   - Commit/rollback (sync)

4. **Use LoadingCoordinator for cache-miss loading:**
   - Prevents duplicate I/O for same key
   - Double-check cache after acquiring lock

5. **Use Coordinator for expensive operations:**
   - Batches multiple callers into single operation
   - Configurable delay for amortization

6. **Always use timeout wrappers:**
   - `read_with_timeout` / `write_with_timeout`
   - Pass descriptive location string

7. **Consider watch notifications:**
   - Use `LocalEvent` for single-thread broadcast
   - Notify after commit, not before

## Anti-Patterns

### Holding RefCell Across Await

```rust
// BAD: RefCell borrow held across await - WILL DEADLOCK
let mut cache = self.cache.borrow_mut();
let result = self.fetch_from_disk().await?; // Another task can't borrow!
cache.insert(key, result);

// GOOD: Drop borrow, await, re-borrow
let needs_fetch = {
    let cache = self.cache.borrow();
    !cache.contains(&key)
}; // borrow dropped

if needs_fetch {
    let result = self.fetch_from_disk().await?;
    self.cache.borrow_mut().insert(key, result);
}
```

### Mixing Lock Types Incorrectly

```rust
// BAD: Using RwLock for purely synchronous operations (overhead)
struct Cache {
    data: RwLock<HashMap<K, V>>,  // Unnecessary async lock
}

// GOOD: RefCell for synchronous operations
struct Cache {
    data: RefCell<HashMap<K, V>>,
}
```

### Forgetting Double-Check After Lock

```rust
// BAD: Race condition - another task may have loaded while we waited
let lock = self.loader.acquire(&key);
let _ = write_with_timeout(&lock, "load").await?;
// Missing check! Another task may have completed the load
let data = self.expensive_load().await?;

// GOOD: Double-check after acquiring lock
let lock = self.loader.acquire(&key);
let _ = write_with_timeout(&lock, "load").await?;
if let Some(data) = self.cache.borrow().get(&key) {
    return Ok(data.clone()); // Already loaded by another task
}
let data = self.expensive_load().await?;
```

## Files Reference

| File | Purpose |
|------|---------|
| `celeriant_shard/src/shard_wal.rs` | Main WAL orchestrator, shows RefCell + LoadingCoordinator usage |
| `celeriant_shard/src/shard_wal_sync.rs` | Sync path with snapshot pattern |
| `celeriant_shard/src/loading_coordinator.rs` | Per-key async serialization |
| `celeriant_shard/src/amortisation/coordinator.rs` | Fsync batching with leader/follower |
| `celeriant_shard/src/amortisation/local_event.rs` | Single-thread async notification |
| `celeriant_rotating_log/src/log_segment_file.rs` | RwLock for file handles |
| `celeriant_rotating_log/src/log_segments_cache.rs` | RefCell for cache, LRU management |
| `celeriant_rotating_log/src/rwlock_timeout.rs` | Timeout wrappers for deadlock detection |
| `celeriant_memcache/src/shard_mem_cache.rs` | Pure synchronous cache (wrapped in RefCell) |
