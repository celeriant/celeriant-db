# celeriant_memcache

In-memory caching layer for the Celeriant WAL. Manages recent writes, aggregate positions, client idempotency tracking, and pending write queues.

## Architecture

```
Write Path:
┌─────────────────────┐     ┌──────────────────────┐     ┌─────────────┐
│ add_to_pending_     │────>│ take_sync_positions_ │────>│   fsync     │
│ append_queue        │     │ snapshot             │     │   (disk)    │
└─────────────────────┘     └──────────────────────┘     └──────┬──────┘
                                                                │
┌─────────────────────┐     ┌──────────────────────┐            │
│ cache_recent_write  │<────│ commit_sync_         │<───────────┘
│ (hot data)          │     │ positions_snapshot   │
└─────────────────────┘     └──────────────────────┘

Read Path:
┌───────────────────────┐
│ aggregate_load_status │─── Check queue, then LRU cache
└───────────────────────┘
           │
           ▼
┌───────────────────────┐
│ get_cached_writes_from│─── Recent writes (size-bounded)
└───────────────────────┘
           │
           ▼
┌───────────────────────┐
│ get_event_indexes     │─── Queue positions or LRU snapshot
└───────────────────────┘
```

## Key Types

| Type | Purpose |
|------|---------|
| `ShardMemCache` | Main cache coordinating all sub-caches |
| `AggregateRecentWrites` | VecDeque of recent writes for one aggregate |
| `MemSnapshotAggregate` | Cached aggregate position, status, and metadata |
| `QueueAggregatePositions` | In-flight write positions before disk commit |
| `RecentWrite` | Cached metablock + datablock + size |
| `ShardLogQueueItem` | Pending write awaiting fsync |
| `SyncPositionsSnapshot` | Atomic snapshot for two-phase commit |
| `MetablockPosition` | Log file position for an aggregate |
| `AggregateStatus` | Found / NotFound / Deleted |

## Key Functions

| Function | Purpose |
|----------|---------|
| `ShardMemCache::new` | Create cache with size limits |
| `add_to_pending_append_queue` | Queue write, update in-memory indexes |
| `add_pending_delete_to_queue` | Queue soft delete |
| `add_pending_trim_to_queue` | Queue trim operation |
| `take_sync_positions_snapshot` | Clone queue state for fsync |
| `commit_sync_positions_snapshot` | Merge synced positions into LRU |
| `rollback_queue_positions` | Clear queue on fsync failure |
| `cache_recent_write` | Add to hot cache after durable write |
| `get_cached_writes_from` | Iterate cached writes from batch index |
| `aggregate_load_status` | Check if aggregate is in memory |
| `get_event_indexes` | Get latest indexes (queue → LRU) |
| `get_client_event_index` | Get client's last event index |

## Usage

```rust
// Initialize cache with size limits
let cache = ShardMemCache::new(
    64 * 1024 * 1024,  // 64MB recent write cache
    16 * 1024 * 1024,  // 16MB aggregate snapshots
    8 * 1024 * 1024,   // 8MB client snapshots  
    1024 * 1024,       // 1MB WAL index cache
);

// Write path: queue → snapshot → fsync → commit
cache.add_to_pending_append_queue(
    &aggregate_key,
    event_index,
    event_batch_index,
    client_id,
    client_event_index,
    queue_item,
);

let snapshot = cache.take_sync_positions_snapshot();
// ... fsync to disk ...
cache.commit_sync_positions_snapshot(snapshot);

// Cache hot data after durable write
cache.cache_recent_write(aggregate_key, batch_index, metablock, datablock, size);

// Read path: check cache before disk
let (is_loaded, status) = cache.aggregate_load_status(&key);
if is_loaded && status == AggregateStatus::Found {
    for (batch_idx, write) in cache.get_cached_writes_from(&key, from_batch) {
        // Use cached data
    }
}
```

## Design Decisions

### Two-phase commit with queue visibility

```rust
// Queue positions cloned, not moved - new writes can continue
let snapshot = cache.take_sync_positions_snapshot();
// Pending queue cleared, ready for next batch
```

During fsync, the queue snapshot is cloned so new writes see correct indexes. The pending queue is cleared immediately, allowing new writes to accumulate for the next sync.

### Size-bounded recent write cache

```rust
pub struct ShardMemCache {
    recent_write_cache_bytes: u64,      // Max size
    cache_current_bytes: u64,           // Current size
    cache_eviction_queue: VecDeque<_>,  // FIFO eviction order
}
```

Recent writes are evicted FIFO when size limit exceeded. Each entry tracks its byte size for accurate accounting. Eviction happens before insertion to ensure space.

### LRU with priority insertion

```rust
fn put_with_priority<K, V>(cache: &mut LruCache<K, V>, key: K, value: V, low_priority: bool)
```

Low-priority inserts (eager caching during scans) only happen when there's spare capacity and immediately demote to LRU position. Prevents scan pollution of the cache.

### Queue vs snapshot separation

| Layer | Mutability | Purpose |
|-------|------------|---------|
| `aggregate_queue_positions` | Unbounded HashMap | In-flight writes, always latest |
| `aggregate_snapshots` | Bounded LRU | Committed positions from disk |

Queue is checked first for reads—it has the most recent uncommitted state. After fsync, positions move to LRU.

### Rollback on failure

```rust
pub fn rollback_queue_positions(&mut self) {
    self.aggregate_queue_positions.clear();
}
```

On fsync failure, queue is cleared. Next reads will reload from disk. Prevents serving uncommitted data.

### Client idempotency tracking

```rust
// 0 is sentinel for "checked disk, client never wrote"
if client_event_index == 0 { None } else { Some(client_event_index) }
```

Client event indexes cached with sentinel value to distinguish "never wrote" from "not in cache". Enables idempotency checks without repeated disk scans.

### Aggregate status enum

```rust
pub enum AggregateStatus {
    Found,      // Exists with data
    NotFound,   // Never created
    Deleted,    // Soft deleted (may allow recreate)
}
```

Explicit status prevents conflating "not in cache" with "doesn't exist". Deleted aggregates track `allow_recreate` and `allow_index_continuation` flags.

### Contiguous batch index optimization

```rust
pub struct AggregateRecentWrites {
    pub first_batch_index: u64,
    pub writes: VecDeque<RecentWrite>,
}
```

Batch indexes are monotonic with no gaps. VecDeque with tracked starting index enables O(1) lookup by batch index instead of HashMap overhead.

### WAL index position cache

```rust
wal_index_positions: LruCache<u64, WalIndexPosition>
```

Caches WAL index → file position mappings for list pagination. `find_nearest_wal_index_position` finds closest cached position to avoid full scans.

## Dependencies

- `celeriant_wal` - WAL data structures (Metablock, Datablock, keys)
- `lru` - LRU cache implementation