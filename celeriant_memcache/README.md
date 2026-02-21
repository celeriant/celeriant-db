# celeriant_memcache

In-memory caching layer for the Celeriant WAL. Manages recent writes, aggregate positions, client idempotency tracking, pending write queues, and replication visibility.

## Architecture

```
Write Path (Leader):
┌─────────────────────┐     ┌──────────────────────┐     ┌─────────────┐
│ add_to_pending_     │────>│ take_sync_positions_ │────>│   fsync     │
│ append_queue        │     │ snapshot             │     │   (disk)    │
└─────────────────────┘     └──────────────────────┘     └──────┬──────┘
                                                                │
┌─────────────────────┐     ┌──────────────────────┐            │
│ aggregate_write_    │<────│ commit_sync_         │<───────────┘
│ snapshots (LRU)     │     │ positions_snapshot   │  (always)
└─────────────────────┘     └──────────┬───────────┘
                                       │ push_pending_replication
                                       ▼
                            ┌──────────────────────┐
                            │ pending_replication_ │
                            │ batches (queue)      │
                            └──────────┬───────────┘
                                       │ take_pending_replication
                                       ▼
┌─────────────────────┐     ┌──────────────────────┐
│ aggregate_read_     │<────│ commit_read_position_│
│ snapshots (LRU)     │     │ snapshot             │
└─────────────────────┘     └──────────────────────┘

Write Path (Non-Leader / Single Node):
commit_sync_positions_snapshot also updates aggregate_read_snapshots directly
(no pending_replication step needed)

Read Path:
┌───────────────────────┐
│ aggregate_load_status │─── CachePath::Read -> aggregate_read_snapshots
│ (CachePath)           │─── CachePath::Write -> queue, then aggregate_write_snapshots
└───────────────────────┘
           │
           ▼
┌───────────────────────┐
│ get_cached_writes_    │─── Filter by visible_wal_index (replication boundary)
│ from                  │
└───────────────────────┘
           │
           ▼
┌───────────────────────┐
│ get_write_event_      │─── Queue positions or write snapshot
│ indexes               │
└───────────────────────┘
```

## Key Types

| Type | Purpose |
|------|---------|
| `ShardMemCache` | Main cache coordinating all sub-caches |
| `CachePath` | `Read` or `Write` - controls which snapshot LRU is accessed |
| `AggregateRecentWrites` | VecDeque of recent writes for one aggregate |
| `MemSnapshotAggregate` | Cached aggregate position, status, and metadata |
| `QueueAggregatePositions` | In-flight write positions before disk commit |
| `RecentWrite` | Cached metablock + datablock + size |
| `ShardLogQueueItem` | Pending write awaiting fsync (includes serialized bytes) |
| `PendingCacheItem` | Metablock/datablock pair post-replication (no serialized bytes) |
| `PendingCommitData` | Log metadata + pending queue for completing commit after replication |
| `SyncPositionsSnapshot` | Atomic snapshot for two-phase commit |
| `MetablockPosition` | Log file position for an aggregate |
| `AggregateStatus` | `Found` / `NotFound` / `Deleted` |
| `EventIndexes` | Latest indexes from queue or write snapshot |
| `WalIndexPosition` | Cached WAL index → log file position mapping |

## Key Functions

| Function | Purpose |
|----------|---------|
| `ShardMemCache::new` | Create cache with size limits including replication high water mark |
| `add_to_pending_append_queue` | Queue write, update in-memory indexes |
| `add_pending_delete_to_queue` | Queue soft delete |
| `add_pending_trim_to_queue` | Queue trim operation |
| `add_to_pending_queue` | Add prepared items directly (replication path, no index tracking) |
| `take_sync_positions_snapshot` | Clone queue state for fsync |
| `commit_sync_positions_snapshot` | Merge synced positions into write LRU; also updates read LRU on non-leader |
| `execute_fsync_rollback` | Clear queue on fsync failure, set rollback flag |
| `execute_replication_rollback` | Clear write snapshots and pending replication queue, set rollback flag |
| `cache_recent_write` | Add to hot cache after durable write |
| `get_cached_writes_from` | Iterate cached writes from batch index, filtered by `visible_wal_index` |
| `aggregate_load_status` | Check if aggregate is in memory (takes `CachePath`) |
| `aggregate_client_load_status` | Check if client has written to aggregate |
| `get_write_event_indexes` | Get latest indexes (queue → write LRU), returns `EventIndexes` |
| `get_client_event_index` | Get client's last event index (queue → write LRU) |
| `get_aggregate_last_metablock_pos` | Get last known metablock position (takes `CachePath`) |
| `get_aggregate_snapshot` | Retrieve cloned snapshot (takes `CachePath`) |
| `put_aggregate_into_cache` | Insert snapshot with client tracking (takes `CachePath`) |
| `put_aggregate_into_cache_as_not_found` | Mark aggregate as never created |
| `put_aggregate_into_cache_as_deleted` | Mark as soft-deleted; clears recent writes on read path |
| `put_aggregate_client_into_cache` | Insert client event index |
| `commit_read_position_snapshot` | Update read LRU from replicated batch |
| `copy_write_to_read_snapshot` | Copy single aggregate write→read snapshot on commit |
| `update_aggregate_min_event_batch_index` | Update trim position; evicts stale recent writes on read path |
| `push_pending_replication` | Add batch to pending replication queue, returns true if high water mark exceeded |
| `take_pending_replication` | Take all pending batches (clears queue and byte counter) |
| `peek_pending_replication` | Peek at oldest batch (for timeout checking) |
| `is_replication_queue_pressured` | Check if high water mark exceeded |
| `take_fsync_rollback_flag` | Check and clear fsync rollback flag |
| `take_replication_rollback_flag` | Check and clear replication rollback flag |
| `cache_wal_index_position` | Cache WAL index → file position mapping |
| `get_wal_index_position` | Retrieve exact cached position |
| `find_nearest_wal_index_position` | Find nearest cached position ≤ target (for list pagination) |

## Usage

```rust
// Initialize cache with size limits
let cache = ShardMemCache::new(
    64 * 1024 * 1024,  // 64MB recent write cache
    16 * 1024 * 1024,  // 16MB aggregate snapshots (shared cap for read+write LRUs)
    8 * 1024 * 1024,   // 8MB client snapshots
    1024 * 1024,       // 1MB WAL index cache
    256 * 1024 * 1024, // 256MB replication high water mark
);

// Write path: queue → snapshot → fsync → commit
cache.add_to_pending_append_queue(
    &aggregate_key,
    event_index,
    event_batch_index,
    min_event_batch_index,
    client_id,
    client_event_index,
    queue_item,
);

let snapshot = cache.take_sync_positions_snapshot();
// ... fsync to disk ...
cache.commit_sync_positions_snapshot(node_status, snapshot);
// On leader: push to pending replication queue
// On non-leader: read snapshots updated immediately

// Leader: after replication completes
cache.commit_read_position_snapshot(&event_batch, log_id, metablock_absolute_pos);

// Cache hot data after commit
cache.cache_recent_write(aggregate_key, batch_index, metablock, datablock, size);

// Read path: check appropriate cache before going to disk
let (is_loaded, status) = cache.aggregate_load_status(&key, CachePath::Read);
if is_loaded && status == AggregateStatus::Found {
    for (batch_idx, write) in cache.get_cached_writes_from(&key, from_batch, visible_wal_index) {
        // Use cached data
    }
}
```

## Design Decisions

### Dual snapshot caches: write vs read visibility

```
aggregate_write_snapshots  ← updated after every successful fsync
aggregate_read_snapshots   ← updated after successful replication (leader)
                             updated with fsync (non-leader / single-node)
```

The write cache is visible to the leader for OCC and idempotency checks. The read cache is visible to read requests and only advances when data is confirmed replicated. This prevents followers or readers from observing writes that might be rolled back due to a failed replication round.

`CachePath::Write` checks the pending queue first, then `aggregate_write_snapshots`. `CachePath::Read` skips the queue entirely and goes straight to `aggregate_read_snapshots`.

### Pending replication queue with high water mark backpressure

```rust
pub struct ShardMemCache {
    pending_replication_batches: Vec<PendingCommitData>, // post-fsync, pre-commit
    pending_replication_bytes: u64,
    pending_replication_high_water_bytes: u64,           // S3 fallback threshold
}
```

After a successful fsync, data waits in `pending_replication_batches` until followers confirm receipt. `push_pending_replication` returns `true` when the high water mark is exceeded, signalling the replication coordinator to trigger an S3 fallback path rather than keep buffering. The queue is intentionally unbounded here—backpressure is applied at the coordinator level, not by eviction.

### Rollback flags distinguish cause of empty queue

```rust
take_fsync_rollback_flag()        // true if fsync failure cleared the queue
take_replication_rollback_flag()  // true if replication failure cleared the queue
```

After a rollback, the caller needs to know whether an empty queue means "nothing to do" or "something went wrong and we lost data". The flags are set on rollback and cleared (consumed) on read. Without them, a coordinator polling an empty queue after a failure would incorrectly treat it as a normal idle state.

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

Recent writes are evicted FIFO when the size limit is exceeded. Each entry tracks its byte size for accurate accounting. Eviction happens before insertion to ensure space. When a trim commits on the read path, `update_aggregate_min_event_batch_index` also proactively evicts now-trimmed entries.

### LRU with priority insertion

```rust
fn put_with_priority<K, V>(cache: &mut LruCache<K, V>, key: K, value: V, low_priority: bool)
```

Low-priority inserts (eager caching during scans) only happen when there is spare capacity and immediately demote to LRU position. Prevents scan pollution of the hot working set.

### Queue vs snapshot separation

| Layer | Mutability | Visibility |
|-------|------------|------------|
| `aggregate_queue_positions` | Unbounded HashMap | In-flight writes, always latest (write path only) |
| `aggregate_write_snapshots` | Bounded LRU | Committed to disk (write path) |
| `aggregate_read_snapshots` | Bounded LRU | Confirmed replicated (read path) |

Queue is checked first for write-path reads—it has the most recent uncommitted state. After fsync, positions move to the write LRU. After replication, they move to the read LRU.

### Rollback on failure

```rust
pub fn execute_fsync_rollback(&mut self) {
    self.aggregate_queue_positions.clear();
    if !self.pending_append_queue.is_empty() {
        self.pending_append_queue.clear();
        self.fsync_rollback_occurred = true;
    }
}

pub fn execute_replication_rollback(&mut self) {
    self.execute_fsync_rollback();
    self.aggregate_write_snapshots.clear();
    self.aggregate_write_client_snapshots.clear();
    // ...also clears pending_replication_batches
}
```

Replication rollback is a superset of fsync rollback: it also wipes the write snapshots because those positions are not yet visible to readers and must not be served. Next reads will reload from disk.

### Client idempotency tracking

```rust
// 0 is sentinel for "checked disk, client never wrote"
if client_event_index == 0 { None } else { Some(client_event_index) }
```

Client event indexes are cached with a sentinel to distinguish "never wrote" from "not in cache". Enables idempotency checks without repeated disk scans. Client cache is only populated from the write path—`put_aggregate_into_cache` with `CachePath::Write` updates client tracking; `CachePath::Read` does not.

### WAL index position cache for list pagination

```rust
wal_index_positions: LruCache<u64, WalIndexPosition>
```

Caches WAL index → file position mappings. `find_nearest_wal_index_position` finds the closest cached position at or below the target, enabling list pagination to skip ahead rather than scanning from the beginning of the log. The scan is O(n) over the bounded LRU, which is acceptable given the small cache size.

### Contiguous batch index optimization

```rust
pub struct AggregateRecentWrites {
    pub first_batch_index: u64,
    pub writes: VecDeque<RecentWrite>,
}
```

Batch indexes are monotonic with no gaps. VecDeque with tracked starting index enables O(1) lookup by batch index instead of HashMap overhead.

### `visible_wal_index` filtering in recent write reads

```rust
pub fn get_cached_writes_from(
    &self,
    aggregate_key: &AggregateKey,
    from_batch_index: u64,
    visible_wal_index: u64,
) -> impl Iterator<Item = (u64, &RecentWrite)>
```

Each `RecentWrite` carries the `wal_index` of the write that produced it. The reader supplies the highest `wal_index` that is safe to serve (i.e. confirmed replicated). Writes beyond that boundary are silently excluded, ensuring readers never see data ahead of the replication frontier even if it is already cached in memory.

## Dependencies

- `celeriant_wal` - WAL data structures (Metablock, Datablock, keys, constants)
- `celeriant_distributed` - `NodeStatus` for leader vs non-leader behavior in `commit_sync_positions_snapshot`
- `celeriant_rotating_log` - `LogSegmentFileMetadata` stored in `PendingCommitData`
- `lru` - LRU cache implementation
- `deepsize` - Memory size estimation for queue items
