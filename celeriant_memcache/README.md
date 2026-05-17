# celeriant_memcache

In-memory caching layer for the Celeriant WAL. Manages recent writes, aggregate positions, client idempotency tracking, pending write queues, replication visibility, and schema validation caching.

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
│ aggregate_read_     │<────│ commit_position_     │
│ snapshots (LRU)     │     │ snapshot (Read)      │
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
│ get_cached_writes_    │─── Filter by visible_wal_seq (replication boundary)
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
| `ShardMemCache<V: Validate>` | Main cache coordinating all sub-caches, generic over schema validator |
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
| `WalSeqPosition` | Cached WAL sequence → log file position mapping |
| `Validate` | Trait for schema validators used by `ShardMemCache<V>` |
| `CachedSchema<V>` | `Validated(CachedValidator<V>)` or `CompilationFailed(String)` |
| `CachedValidator<V>` | Wraps an `Rc<V>` validator with size estimate |
| `UniqueSchemaKeys` | Small-vec optimized set of schema keys (inline up to 2, then Vec) |

## Invariants

- Two separate LRU caches exist: `aggregate_write_snapshots` (updated after fsync, used for OCC/idempotency) and `aggregate_read_snapshots` (updated after replication on leader, used by reads).
- OCC checks use the write-ahead snapshot, not the read snapshot. A concurrent write fsynced but not yet replicated still triggers an OCC conflict.
- The recent-write cache filters by `visible_wal_seq`. Entries with `wal_seq > visible_wal_seq` are excluded from reads.
- Rollback flags (`fsync_rollback_occurred`, `replication_rollback_occurred`) are one-time-consumption. The next capture phase reads and resets the flag.
- Replication rollback is a superset of fsync rollback: it also wipes write snapshots because those positions are not yet visible to readers.
- Low-priority LRU inserts (scan-driven) only populate spare capacity and immediately demote to LRU tail. Scans must not evict hot entries.
- `aggregate_queue_positions` and `pending_append_queue` are intentionally unbounded (transient in-flight state that drains every fsync cycle).

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
pub struct ShardMemCache<V: Validate> {
    pending_replication_batches: Vec<PendingCommitData>, // post-fsync, pre-commit
    pending_replication_bytes: u64,
    pending_replication_high_water_bytes: u64,           // S3 fallback threshold
}
```

After a successful fsync, data waits in `pending_replication_batches` until followers confirm receipt. `push_pending_replication` returns `true` when the high water mark is exceeded, signalling the replication coordinator to trigger an S3 fallback path rather than keep buffering. The queue is intentionally unbounded here. Backpressure is applied at the coordinator level, not by eviction.

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
pub struct ShardMemCache<V: Validate> {
    recent_write_cache_bytes: u64,      // Max size
    cache_current_bytes: u64,           // Current size
    cache_eviction_queue: VecDeque<_>,  // FIFO eviction order
}
```

Recent writes are evicted FIFO when the size limit is exceeded. Each entry tracks its byte size for accurate accounting. Eviction happens before insertion to ensure space. When a trim commits on the read path, `update_aggregate_min_aggregate_version` also proactively evicts now-trimmed entries.

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

Queue is checked first for write-path reads. It has the most recent uncommitted state. After fsync, positions move to the write LRU. After replication, they move to the read LRU.

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
if client_seq == 0 { None } else { Some(client_seq) }
```

Client event sequences are cached with a sentinel to distinguish "never wrote" from "not in cache". Enables idempotency checks without repeated disk scans. Client cache is only populated from the write path, `put_aggregate_into_cache` with `CachePath::Write` updates client tracking; `CachePath::Read` does not.

### WAL sequence position cache for list pagination

```rust
wal_seq_positions: LruCache<u64, WalSeqPosition>
```

Caches WAL sequence → file position mappings. `find_nearest_wal_seq_position` finds the closest cached position at or below the target, enabling list pagination to skip ahead rather than scanning from the beginning of the log. The scan is O(n) over the bounded LRU, which is acceptable given the small cache size.

### Contiguous aggregate version optimization

```rust
pub struct AggregateRecentWrites {
    pub first_version: u64,
    pub writes: VecDeque<RecentWrite>,
}
```

Aggregate versions are monotonic with no gaps. VecDeque with tracked starting index enables O(1) lookup by version instead of HashMap overhead.

### `visible_wal_seq` filtering in recent write reads

```rust
pub fn get_cached_writes_from(
    &self,
    aggregate_key: &AggregateKey,
    from_version: u64,
    visible_wal_seq: u64,
) -> impl Iterator<Item = (u64, &RecentWrite)>
```

Each `RecentWrite` carries the `wal_seq` of the write that produced it. The reader supplies the highest `wal_seq` that is safe to serve (i.e. confirmed replicated). Writes beyond that boundary are silently excluded, ensuring readers never see data ahead of the replication frontier even if it is already cached in memory.

### Schema validation caching

```rust
schema_cache: LruCache<SchemaKey, CachedSchema<V>>   // compiled validators
no_schema_cache: LruCache<SchemaKey, ()>              // confirmed no-schema-registered
pending_schema_registrations: HashSet<SchemaKey>      // awaiting fsync
```

Two-tier schema cache: `schema_cache` holds compiled validators (or compilation failures), while `no_schema_cache` records keys confirmed to have no schema. This avoids repeated disk lookups for unschema'd aggregates. `pending_schema_registrations` tracks registrations that are queued but not yet fsynced, preventing duplicate registrations within the same batch.
