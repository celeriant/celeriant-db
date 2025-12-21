# celeriant_memcache

In-memory state management for Celeriant shards. Tracks write positions, pending queues, and recent write caches. No I/O—just state coordination for the durability layer.

**README WAS LLM GENERATED AND HUMAN REVIEWED 2025-12-21**

## Purpose

Manages the gap between "client sent write" and "write durably on disk". Provides:

1. **Position tracking** - Where to write next, what's committed
2. **Idempotency** - Client deduplication before hitting disk
3. **Recent write cache** - Serve reads from memory when possible
4. **Atomic commit/rollback** - Snapshot state before disk write, restore on failure

## Architecture

```
Client Write
    │
    ▼
┌─────────────────────────────────────────────────────┐
│  ShardMemCache                                      │
│  ┌───────────────────┐  ┌────────────────────────┐  │
│  │ Queue Positions   │  │ Pending Append Queue   │  │
│  │ (uncommitted)     │  │ (waiting for disk)     │  │
│  └─────────┬─────────┘  └───────────┬────────────┘  │
│            │                        │               │
│            ▼ take_sync_snapshot()   ▼               │
│  ┌─────────────────────────────────────────────┐    │
│  │         SyncPositionsSnapshot               │    │
│  │         (frozen state for disk write)       │    │
│  └─────────────────────────────────────────────┘    │
│            │                                        │
│            ├── success ──► commit_sync_snapshot()   │
│            │                      │                 │
│            │                      ▼                 │
│            │              ┌───────────────────┐     │
│            │              │ File Positions    │     │
│            │              │ (committed)       │     │
│            │              └───────────────────┘     │
│            │                      │                 │
│            │                      ▼                 │
│            │              ┌───────────────────┐     │
│            │              │ Recent Write Cache│     │
│            │              │ (LRU eviction)    │     │
│            │              └───────────────────┘     │
│            │                                        │
│            └── failure ──► rollback_queue_positions │
└─────────────────────────────────────────────────────┘
```

## Key Types

| Type | Purpose |
|------|---------|
| `ShardMemCache` | Main coordinator for shard memory state |
| `AggregatePositions` | Tracks event_index, event_batch_index, client indexes per aggregate |
| `SyncPositionsSnapshot` | Frozen state taken before disk write begins |
| `ShardLogQueueItem` | Single write waiting in pending queue |
| `RecentWrite` | Cached metablock + datablock after durable write |
| `InternalShardConfig` | Shard configuration (cache size, fsync delay, etc.) |

## Two-Position Design

Each aggregate has two position stores:

| Store | Updated When | Purpose |
|-------|--------------|---------|
| `aggregate_queue_positions` | On `add_to_pending_append_queue()` | Idempotency checks before disk write |
| `aggregate_file_positions` | On `commit_sync_snapshot()` | Confirmed durable state |

Lookups check queue first, fall back to file. This allows immediate idempotency rejection without waiting for disk.

```rust
// Idempotency check flow
fn get_client_event_index(&self, aggregate_key, client_id) -> Option<u64> {
    self.aggregate_queue_positions.get(...)  // Check pending first
        .or_else(|| self.aggregate_file_positions.get(...))  // Fall back to committed
}
```

## Write Flow

### 1. Add to Queue

```rust
cache.add_to_pending_append_queue(
    &aggregate_key,
    event_index,
    event_batch_index,
    client_id,
    client_event_index,
    queue_item,
);
```

Updates `aggregate_queue_positions` immediately for idempotency. Appends to `pending_append_queue`.

### 2. Take Snapshot

```rust
let snapshot = cache.take_sync_positions_snapshot();
// Queue is now empty, positions moved to snapshot
// New writes can arrive while disk I/O happens
```

### 3. Commit or Rollback

```rust
// Success: merge snapshot into file positions
cache.commit_sync_positions_snapshot(snapshot);

// Failure: discard queue positions, set fsync failure flag
cache.rollback_queue_positions();
```

## Recent Write Cache

LRU-style cache for serving reads from memory:

```rust
// After durable write
cache.cache_recent_write(
    aggregate_key,
    batch_index,
    metablock,
    datablock,
    size_bytes,
);

// On read
if let Some(writes) = cache.get_cached_writes_from(&key, from_batch_index) {
    // Serve from memory, skip disk
}
```

**Eviction**: Size-based. When `cache_current_bytes + new_size > max_bytes`, oldest entries evicted via `cache_eviction_queue`.

**Configuration**: Set via `InternalShardConfig::recent_write_cache_bytes`. Zero disables caching.

## Position Tracking

Shard log file has metablocks growing forward, datablocks growing backward:

```
┌──────────────────────────────────────────────┐
│ [metablocks →]  [free space]  [← datablocks] │
│        ↑                            ↑        │
│  metablocks_position      datablocks_position│
└──────────────────────────────────────────────┘
```

When the file is full, a new log is created and the old one is rotated.

```rust
// Check if pending writes will fit
cache.has_enough_free_space()  // datablocks_position - metablocks_position > required

// After log rotation
cache.rotate_to_next_log(new_log_id, meta_pos, data_pos, file_len);
```

## Fsync Failure Handling

If fsync fails:

1. `had_fsync_failure` flag set
2. `aggregate_queue_positions` cleared (fall back to file positions)
3. In durable mode, clients are notified of the failure
3. Next write checks `force_durable_on_next_write()` to notify clients not using durable mode

```rust
if cache.force_durable_on_next_write() {
    // Force sync even in async mode, client needs to know about failure
}
```

## InternalShardConfig Fields

| Field | Purpose |
|-------|---------|
| `node_id` | Unique identifier for this node |
| `max_open_files` | File descriptor limit |
| `shard_log_preallocate_bytes` | Preallocate log file size |
| `fsync_delay` | Amortised fsync batch time (typically 5-10ms) |
| `recent_write_cache_bytes` | Max cache size (0 = disabled) |
| `non_durable_writes` | Ack back to clients after in-memory queuing, dont wait for fsync |
| `shard_dir` | Directory for shard log files |

## Thread Safety

`ShardMemCache` is **not thread-safe**. Designed for single-threaded access per shard in a thread-per-core architecture. Each CPU core owns its shards exclusively.

## Dependencies

- `celeriant_wal` - Types for metablocks, datablocks, aggregate keys