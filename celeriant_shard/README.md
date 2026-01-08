# celeriant_shard

Shard-level write-ahead log orchestrator. Coordinates validation, queue management, durability, caching, and read filtering for a single shard.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              ShardWal                                   │
├─────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────────┐  ┌──────────────────┐  ┌───────────────────────┐  │
│  │  ShardMemCache   │  │ LogSegmentsCache │  │  AggregateWatchers    │  │
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
│  ┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────┐  │
│  │ LoadingCoordinator  │  │  BloomFilterCache   │  │ TimestampConfig │  │
│  │ (thundering herd)   │  │  (reusable filter)  │  │ (precision/epoch│  │
│  └─────────────────────┘  └─────────────────────┘  └─────────────────┘  │
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
| `TimestampConfig` | Configurable precision (ms/μs/ns) and epoch offset |
| `InternalShardConfig` | All shard configuration parameters |
| `ShardWriteError` | Validation failures (idempotency, OCC, empty events) |
| `ShardReadError` | Read failures (not found, size limits, I/O) |
| `ShardFsyncError` | Durability failures (I/O, space, corruption) |

## Key Functions

| Function | Purpose |
|----------|---------|
| `ShardWal::open` | Open or create shard WAL from config |
| `ShardWal::process_request` | Route request to appropriate handler |
| `ShardWal::read` | Read event batches with filtering |
| `ShardWal::write` | Append events to aggregates |
| `ShardWal::delete` | Soft-delete aggregates |
| `ShardWal::trim_start` | Remove old event batches |
| `ShardWal::exists` | Check aggregate existence |
| `ShardWal::list_orgs/aggregate_types/aggregates` | Discovery with pagination |
| `ShardWal::close` | Flush and close shard |
| `Coordinator::request_sync` | Batched fsync with delay |

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

### Validation Errors

| Check | Error | Purpose |
|-------|-------|---------|
| Empty events | `EmptyEventsList` | At least one event required |
| Event type = 0 | `ZeroEventType` | Reserved sentinel value |
| Aggregate missing | `AggregateNotExists` | Unless `allow_create = true` |
| Client idempotency | `ClientIdempotencyViolation` | Reject duplicate client_event_index |
| OCC | `OptimisticConcurrencyViolation` | Expected batch index mismatch |
| Deleted aggregate | `AggregateRecreateNotAllowed` | Unless `allow_recreate = true` |

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
| Force immediate | Skip delay (after previous fsync failure) |

## In-Memory Filtering

### Metablock-Level (skip disk I/O)

| Filter | Check Against |
|--------|---------------|
| `from/to_event_batch_index` | `event_batch_index` bounds |
| `min/max_server_timestamp` | `server_timestamp` range |
| `include/exclude_client_id` | `client_id` match |
| `include/exclude_user_id` | `user_id` match |
| `min/max_client_event_index` | Overlaps with batch range |
| `min/max_event_timestamp` | Overlaps with batch range |
| `min/max_event_index` | Overlaps with batch range |
| `include_event_types` | Direct array or bloom filter |

### Event-Level (after deserialize)

Applied to individual events within kept batches for final filtering.

## List Operations

Reverse WAL scanning with pagination for discovery:

```rust
// List all orgs in shard
list_orgs(ListOrgsRequest { cursor: None, .. })

// List aggregate types filtered by org
list_aggregate_types(ListAggregateTypesRequest { org_id: Some(123), .. })

// List aggregates with full metadata
list_aggregates(ListAggregatesRequest { org_id: Some(123), aggregate_type_id: Some(456), .. })
```

Features:
- Time-bounded scans (`list_max_duration`)
- LRU deduplication within page
- WAL index position caching for fast cursor resumption
- Returns metadata: batch counts, index ranges, timestamps, sizes

## Timestamp Configuration

```rust
pub struct TimestampConfig {
    pub precision: TimestampPrecision,  // Milliseconds, Microseconds, Nanoseconds
    pub epoch_offset_secs: i64,         // Custom epoch offset from Unix epoch
}

// Usage
let config = TimestampConfig {
    precision: TimestampPrecision::Microseconds,
    epoch_offset_secs: 1704067200,  // Custom epoch: 2024-01-01
};
let timestamp = config.now();  // Microseconds since custom epoch
```

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
- `aggregate_loading` - Aggregate snapshot loading from disk
- `aggregate_client_loading` - Client idempotency index loading

## Watch Integration

After successful fsync, watch events are broadcast:

| Event Type | Trigger |
|------------|---------|
| `Create` | First event batch for aggregate |
| `Write` | Event batch appended (merged to range) |
| `Delete` | Soft delete committed |
| `TrimStart` | Trim operation committed |

## Error Recovery

### Fsync Failure

```
sync() fails
    │
    ▼
rollback_queue_positions()  → Clear uncommitted state
    │
    ├── force_immediate = true  → Next sync skips delay
    └── Error propagated to all waiting writers
```

## Configuration

| Field | Purpose |
|-------|---------|
| `fsync_delay` | Amortisation batch window |
| `non_durable_writes` | Ack before fsync completes |
| `max_response_size` | Size bound for read responses |
| `read_max_chunk_size` | Disk read chunk size |
| `shard_log_preallocate_bytes` | Log file size |
| `max_open_files` | LRU cache for log files |
| `recent_write_cache_bytes` | Hot write cache size |
| `aggregate_snapshots_cache_bytes` | Position cache size |
| `list_page_size` | Results per list page |
| `list_max_duration` | Max time for list scan |
| `timestamp_config` | Precision and epoch settings |

## Thread Safety

`ShardWal` is **not thread-safe**. Designed for single-threaded async execution per shard (thread-per-core architecture). Uses:

- `Rc<RefCell<_>>` for interior mutability
- `glommio::sync::RwLock` for async coordination within single thread
- `Cell` for lock-free flags

## Dependencies

| Crate | Purpose |
|-------|---------|
| `celeriant_memcache` | In-memory state (queues, positions, cache) |
| `celeriant_rotating_log` | Direct I/O log file management |
| `celeriant_wal` | Metablock/datablock types |
| `celeriant_wire` | Serialization |
| `celeriant_watch` | Watch subscription system |
| `celeriant_msg` | Request/response types |
| `celeriant_disk` | DMA read utilities |
| `glommio` | Async runtime |
| `fastbloom` | Bloom filter implementation |
| `lru` | LRU cache for bounded collections |