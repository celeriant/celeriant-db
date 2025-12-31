# celeriant_shard

Shard-level write-ahead log orchestrator. Coordinates validation, queue management, durability, caching, and read filtering for a single shard.

**README WAS LLM GENERATED AND HUMAN REVIEWED [2025-12-30]**

## Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              ShardWal                                   │
├─────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────────┐  ┌──────────────────┐  ┌───────────────────────┐  │
│  │  ShardMemCache   │  │ RotatingLogCache │  │  AggregateWatchers    │  │
│  │  (positions,     │  │  (DMA file I/O)  │  │  (watch subscribers)  │  │
│  │   queues, cache) │  │                  │  │                       │  │
│  └────────┬─────────┘  └────────┬─────────┘  └───────────┬───────────┘  │
│           │                     │                        │              │
│           ▼                     ▼                        ▼              │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                     Coordinator (fsync batching)                 │   │
│  │            leader/follower coalescing with delay                 │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  ┌─────────────────────┐  ┌─────────────────────┐                       │
│  │ LoadingCoordinator  │  │  BloomFilterCache   │                       │
│  │ (thundering herd)   │  │  (reusable filter)  │                       │
│  └─────────────────────┘  └─────────────────────┘                       │
└─────────────────────────────────────────────────────────────────────────┘
```

## Key Types

| Type | Purpose |
|------|---------|
| `ShardWal` | Main orchestrator: validation, queuing, durability, reads |
| `Coordinator` | Fsync batching with leader/follower coalescing |
| `LocalEvent` | Single-threaded async event for result broadcasting |
| `BloomFilterCache` | Reusable bloom filter to avoid allocations |
| `LoadingCoordinator` | Serializes concurrent loads per key |
| `ShardWriteError` | Validation failures (idempotency, OCC, empty events) |
| `ShardReadError` | Read failures (not found, size limits, I/O) |
| `ShardFsyncError` | Durability failures (I/O, space, corruption) |

## Write Flow

```
Client Write Request
        │
        ▼
┌───────────────────────────────────────┐
│  Phase 1: Validation (can fail)       │
│  • Empty events check                 │
│  • Zero event type check              │
│  • Aggregate exists / allow_create    │
│  • Client idempotency check           │
│  • Optimistic concurrency check       │
│  • Build metablock + datablock        │
└───────────────────┬───────────────────┘
                    │ all validations pass
                    ▼
┌───────────────────────────────────────┐
│  Phase 2: Queue (cannot fail)         │
│  • Append to pending_append_queue     │
│  • Update queue positions             │
└───────────────────┬───────────────────┘
                    │
                    ▼
┌───────────────────────────────────────┐
│  Phase 3: Durability                  │
│  • Coordinator batches writers        │
│  • Leader performs fsync              │
│  • Followers receive result           │
│  • Watch events broadcast             │
└───────────────────┬───────────────────┘
                    │
                    ▼
             Client ACK
```

### Validation Details

| Check | Error | Purpose |
|-------|-------|---------|
| Empty events | `EmptyEventsList` | At least one event required |
| Event type = 0 | `ZeroEventType` | Reserved sentinel value |
| Aggregate missing | `AggregateNotExists` | Unless `allow_create = true` |
| Client idempotency | `ClientIdempotencyViolation` | Reject duplicate client_event_index |
| OCC | `OptimisticConcurrencyViolation` | Expected batch index mismatch |

## Read Flow

```
Client Read Request
        │
        ▼
┌───────────────────────────────────────┐
│  1. Ensure aggregate cached           │
│     (LoadingCoordinator serializes)   │
└───────────────────┬───────────────────┘
                    │
                    ▼
┌───────────────────────────────────────┐
│  2. Collect metablocks (size-bounded) │
│     • Check recent write cache first  │
│     • Scan disk backwards if needed   │
│     • Apply metablock-level filters   │
│     • Evict newest when over budget   │
└───────────────────┬───────────────────┘
                    │
                    ▼
┌───────────────────────────────────────┐
│  3. Fetch datablocks                  │
│     • From cache: already have data   │
│     • Inline: deserialize immediately │
│     • Block: batch I/O per log file   │
└───────────────────┬───────────────────┘
                    │
                    ▼
┌───────────────────────────────────────┐
│  4. Apply event-level filters         │
│     • Event type whitelist            │
│     • Event index range               │
│     • Event timestamp range           │
│     • Client event index range        │
└───────────────────┬───────────────────┘
                    │
                    ▼
             ReadResponse
```

## Fsync Amortisation

The `Coordinator` batches multiple writers to amortise fsync cost:

```
Writer 1 ─┐
Writer 2 ─┼──► Coordinator ──► delay ──► Leader calls sync_fn()
Writer 3 ─┘                                     │
    ▲                                           │
    └───────── all receive same result ─────────┘
```

| Mode | Behavior |
|------|----------|
| Durable | Wait for fsync, batched by delay (typically 5-10ms) |
| Non-durable | Spawn fsync task, return immediately to client |
| Force immediate | Skip delay (after previous fsync failure in non-durable mode) |

### FSync Leader Election

1. First writer acquires write lock → becomes leader
2. Subsequent writers acquire read lock → become followers
3. Leader sleeps for configured delay
4. Leader clears orchestrator, calls sync function
5. All waiters receive cloned result via `LocalEvent`

## In-Memory Filtering

### Metablock-Level (skip disk I/O)

| Filter | Check Against |
|--------|---------------|
| `from_event_batch_index` | `event_batch_index >= from` |
| `to_event_batch_index` | `event_batch_index <= to` |
| `min/max_server_timestamp` | `server_timestamp` range |
| `include/exclude_client_id` | `client_id` match |
| `include/exclude_user_id` | `user_id` match |
| `min/max_client_event_index` | Overlaps with batch range |
| `min/max_event_timestamp` | Overlaps with batch range |
| `min/max_event_index` | Overlaps with batch range |
| `include_event_types` | Direct array or bloom filter |

### Event-Level (after deserialize)

Batches are filtered, but some filters are also applied to individual events within kept batches.

## Loading Coordinators

Prevent thundering herd when multiple async tasks request the same data:

```rust
// Only one task loads; others wait
let guard = self.aggregate_loading.acquire(&aggregate_key);
let _ = write_with_timeout(&guard, "context").await?;

// Check again (another task may have loaded while we waited)
if already_loaded { return Ok(()); }

// Perform expensive load...
```

Two coordinators:
- `aggregate_loading` - Aggregate snapshot loading
- `aggregate_client_loading` - Client idempotency index loading

## Watch Integration

After successful fsync, watch events are broadcast:

```rust
// Accumulate events during commit
let mut write_events: HashMap<AggregateKey, AggregateWatchEventOperation> = HashMap::new();
let mut create_events: HashMap<AggregateKey, AggregateWatchEventOperation> = HashMap::new();

// Broadcast after commit
for (aggregate_key, operation) in create_events {
    watched_aggregates.broadcast(AggregateWatchEvent { aggregate_key, operation });
}
for (aggregate_key, operation) in write_events {
    watched_aggregates.broadcast(AggregateWatchEvent { aggregate_key, operation });
}
```

Multiple batches to same aggregate in one sync are merged into range `(from_event_batch_index, to_event_batch_index)`. This keeps our memory usage reasonable when working with low latency watchers.

## Error Recovery

### Fsync Failure

```
sync() fails
    │
    ▼
rollback_queue_positions()
    │
    ├── Clears aggregate_queue_positions
    ├── Sets had_fsync_failure flag
    └── Next write forces a durable write to surface errors to clients
```

### Log Rotation

When log file is full:

```rust
if !shard_mem_cache.has_enough_free_space() {
    rotating_log_cache.rotate_to_next_log(...).await?;
    shard_mem_cache.rotate_to_next_log(new_log_id, meta_pos, data_pos, file_len);
}
```

## Configuration via InternalShardConfig

| Field | Purpose |
|-------|---------|
| `fsync_delay` | Amortisation batch window |
| `non_durable_writes` | Ack before fsync completes |
| `max_response_size` | Size bound for read responses |
| `read_max_chunk_size` | Disk read chunk size |
| `shard_log_preallocate_bytes` | Log file size |
| `max_open_files` | LRU cache capacity |

## Thread Safety

`ShardWal` is **not thread-safe**. Designed for single-threaded async execution per shard (thread-per-core architecture). Uses:

- `Rc<RefCell<_>>` for interior mutability
- `glommio::sync::RwLock` for async coordination
- `Cell` for lock-free primitives

## Dependencies

- `celeriant_memcache` - In-memory state (queues, positions, cache)
- `celeriant_rotating_log` - Direct I/O log file management
- `celeriant_wal` - Metablock/datablock types
- `celeriant_wire` - Serialization
- `celeriant_watch` - Watch subscription system
- `celeriant_msg` - Request/response types
- `celeriant_disk` - DMA read utilities
- `glommio` - Async runtime
- `fastbloom` - Bloom filter implementation