# celeriant_aggregate

Storage engine for Celeriant aggregates. This crate handles reading, writing, caching, and lifecycle management of per-aggregate event streams. It's the core of Celeriant's data path.

## Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        LocalAggregate                           │
│  High-level API for process_request(), read(), write()          │
├─────────────────────────────────────────────────────────────────┤
│                       AggregateCache                            │
│  LRU cache of open aggregates (configurable capacity)           │
├─────────────────────────────────────────────────────────────────┤
│                     AggregateResources                          │
│  Per-aggregate: reader, writer, sync coordination               │
├──────────────────────────┬──────────────────────────────────────┤
│   ReadOperationsWithDma  │     WriteOperationsWithDma           │
│   metadata + event files │     memory queue + fsync batching    │
├──────────────────────────┴──────────────────────────────────────┤
│                     DMA Files (io_uring)                        │
│  O_DIRECT bypass, aligned I/O, no page cache pollution          │
└─────────────────────────────────────────────────────────────────┘
```

Each aggregate is stored as two files:

```
{data_root}/{org_id}/{aggregate_type_id}/{aggregate_id}/
├── metadata.bin       # Fixed 256-byte records, one per batch
└── event_batches.bin  # Variable-length compressed event data
```

## Key Types

### LocalAggregate

Top-level entry point. Processes all request types and coordinates reads/writes.

```rust
pub struct LocalAggregate {
    aggregate_cache: AggregateCache,
    node_config: NodeConfig,
}
```

Use `LocalAggregateTrait::process_request()` to handle incoming requests, or call individual methods like `read()`, `write()`, `trim_start()` directly.

### AggregateCache

LRU cache of `AggregateResources`. Limits memory by evicting least-recently-used aggregates when capacity is reached.

```rust
let cache = AggregateCache::new(
    NonZeroUsize::new(10_000).unwrap(),  // Max open aggregates
    node_config,
    read_config,
    write_config,
);

let resources = cache.get_aggregate_resources(&aggregate_key);
```

Cache entries are created lazily on first access. Evicted aggregates close their file handles.

### AggregateResources

Per-aggregate state: file handles, locks, sync coordination.

```rust
pub struct AggregateResources {
    reader: RwLock<Option<ReadOperationsWithDmaFiles>>,
    writer: RwLock<Option<WriteOperationsWithDmaFile>>,
    wal_sync_event: RwLock<Option<Rc<LocalEvent<SyncResult>>>>,
    has_pending_sync_error: Cell<bool>,
    // paths, configs...
}
```

Reader and writer are initialized lazily. The `RwLock` is glommio's single-threaded async lock (not `std::sync`).

### WriteOperationsWithDmaFile

Handles append operations with in-memory buffering and cache.

```rust
pub struct WriteOperationsWithDmaFile {
    pub data_cache: VecDeque<CacheItem>,        // Recent batches in memory
    pub append_event_batch_queue: Vec<...>,     // Pending writes
    pub client_event_indexes: HashMap<u128, u64>, // Idempotency tracking
    pub next_event_batch_index: u64,
    pub next_event_index: u64,
    // file handles, buffers...
}
```

Writes are queued in memory via `queue_events_in_memory()`, then flushed to disk via `sync_with_rollback()`.

### ReadOperationsWithDmaFiles

Handles filtered reads from disk.

```rust
pub struct ReadOperationsWithDmaFiles {
    pub metadata_dma_file: Option<DmaFile>,
    pub event_batches_dma_file: Option<DmaFile>,
    pub config: AggregateReadConfig,
}
```

Reads metadata first to filter batches, then reads only matching event data.

## Data Path

### Write Flow

```
WriteRequest
    │
    ├─ Validate: concurrency check, idempotency check, non-zero event types
    │
    ├─ queue_events_in_memory()
    │  ├─ Assign event_index to each event
    │  ├─ Serialize + compress event batch
    │  ├─ Build metadata (bloom filter, min/max ranges)
    │  └─ Add to append_event_batch_queue
    │
    ├─ sync_with_delay() or background task
    │  ├─ Coalesce concurrent sync requests via LocalEvent
    │  ├─ Write event_batches.bin (events first, crash-safe)
    │  ├─ fdatasync()
    │  ├─ Write metadata.bin
    │  ├─ fdatasync()
    │  └─ Move queued items to data_cache
    │
    └─ WriteResponse (batch_index, timestamps, CRC)
```

On sync failure, `sync_with_rollback()` reverts in-memory state (next indexes, client indexes).

### Read Flow

```
ReadRequest
    │
    ├─ Try writer cache (maybe_read_cached_events)
    │  └─ Cache hit? Apply filters, return immediately
    │
    ├─ Cache miss: fall through to disk read
    │
    ├─ Read metadata records (get_metadata_range)
    │  ├─ Calculate byte offset from batch index
    │  └─ Deserialize fixed-size metadata
    │
    ├─ Apply metadata filters (is_include_batch)
    │  ├─ Batch index range
    │  ├─ Server timestamp range
    │  ├─ Client/user ID include/exclude
    │  ├─ Event timestamp/index ranges
    │  └─ Event type bloom filter check
    │
    ├─ Apply max_bytes pagination (trim_end_if_exceeds_max_bytes)
    │
    ├─ Read event data at calculated positions (read_objects_absolute)
    │
    ├─ Decompress + deserialize each batch
    │
    ├─ Apply event-level filters (apply_event_filters)
    │  ├─ Event type whitelist (bloom may have false positives)
    │  └─ Event timestamp/index fine-grained filtering
    │
    └─ ReadResponse (event_batches, next_event_batch_index)
```

## Design Decisions

### Thread-Per-Core Architecture

This crate is **not** `Send` or `Sync`. It uses:

- `Rc<RefCell<...>>` for shared mutable state
- `Cell<bool>` for flags
- glommio's `RwLock` (single-threaded async)

Each CPU core runs one glommio executor with its own `LocalAggregate` instance. Aggregates are sharded across cores by `aggregate_id % num_cores`. No cross-thread coordination on the hot path.

### DMA Files (O_DIRECT)

All disk I/O uses Direct Memory Access:

```rust
let writer_dma = create_and_write_only_dma(&path).await?;
let reader_dma = existing_file_read_only_dma(&path).await?;
```

Benefits:
- Bypass OS page cache (no double-buffering)
- Predictable latency (no cache eviction surprises)
- Better memory utilization (cache only what you need)

Constraints:
- Aligned I/O (handled by maintaining carry-over buffers)
- Can't memory-map

### Separate Metadata File

Metadata is stored separately from event data:

| File | Record Size | Access Pattern |
|------|-------------|----------------|
| `metadata.bin` | Fixed 256 bytes | Sequential scan, offset calculation |
| `event_batches.bin` | Variable | Random access by position |

Fixed-size metadata enables:
- **Offset calculation**: `position = (batch_index - min_available) * 256`
- **Quick filtering**: Read metadata without touching event data
- **Crash recovery**: Detect incomplete writes by checking alignment

### Writer Cache

Recent event batches are cached in memory after successful sync:

```rust
pub struct CacheItem {
    pub event_batch_item: EventBatchItem,
    pub event_batch_metadata: EventBatchMetadata,
}
```

Cache sizing is controlled by `max_data_cache_size_bytes`. When exceeded, oldest batches are evicted (FIFO). The cache is the first place reads check before hitting disk.

Cache misses return:
```rust
WriteError::CacheMiss {
    missing_from_event_batch_index: u64,
    missing_to_event_batch_index: Option<u64>,
}
```

Callers should then read from disk for the missing range.

### Amortized Fsync

Multiple concurrent writes share a single fsync via `sync_with_delay()`:

```rust
pub async fn sync_with_delay(&self, delay: Option<Duration>) -> SyncResult {
    // First caller becomes coordinator
    // Other callers wait on LocalEvent
    // Coordinator sleeps for delay, then fsyncs
    // All waiters get the result
}
```

With 100µs delay, ~100 concurrent writes share one fdatasync. This reduces disk IOPS while maintaining durability guarantees.

For immediate durability with lowest latency, pass `delay_us: 0`. For best throughput, pass `None`.

### Optimistic Concurrency Control

Writes can specify `expected_event_batch_index`:

```rust
if let Some(expected) = write_request.expected_event_batch_index {
    if expected != self.next_event_batch_index {
        return Err(WriteError::OptimisticConcurrencyViolation { ... });
    }
}
```

This enables conflict detection without locking. Clients read current state, compute new events, then write with expected index. If another write interleaved, they get a conflict error and retry.

### Client Idempotency

Per-client deduplication via `client_event_index`:

```rust
pub client_event_indexes: HashMap<u128, u64>,  // client_id -> max seen index
```

When `enforce_client_idempotency` is true:
- Server tracks highest `client_event_index` per `client_id`
- Rejects writes with index ≤ last seen
- Prevents duplicate events from client retries

This is stored in memory and rebuilt from metadata on startup.

### Bloom Filter Event Type Filtering

Batches with >4 unique event types use a 256-bit bloom filter:

```rust
pub enum EventTypesData {
    Bloom([u64; 4]),   // 256-bit bloom filter
    Direct([u64; 4]),  // Up to 4 types stored directly
}
```

Filtering happens in two passes:
1. **Metadata pass**: Check bloom/direct array, skip non-matching batches entirely
2. **Event pass**: After decompression, filter out false positives

This avoids decompressing batches that definitely don't contain requested event types.

### Crash Recovery

On startup, `get_write_operations_data_requirements()` scans metadata:

1. **Partial metadata record**: Truncate to aligned length
2. **All-zero metadata**: Skip (preallocated but unwritten)
3. **Valid metadata**: Build client index map, find next indexes

Event data corruption is detected on read via CRC:
```rust
if actual_crc != metadata.events_crc {
    return Err(ReadError::CorruptEventBatch { ... });
}
```

Writes always persist events before metadata. If crash occurs between, metadata won't reference the partial event data.

### Trim and Prepend

For retention and restore operations:

**Trim**: Remove old batches from the start
```rust
async fn trim_start(
    &mut self,
    keep_from_event_batch_index: u64,
    ...
) -> Result<(), WriteError>
```
Creates new files, copies retained data, renames atomically.

**Prepend**: Restore trimmed batches
```rust
async fn prepend_batches(
    &mut self,
    event_batches: &Vec<EventBatchItem>,
    ...
) -> Result<(), WriteError>
```
Validates contiguous indexes, creates new files with prepended data.

Both operations update `minimum_available_event_batch_index`.

## Configuration

### AggregateReadConfig

```rust
pub struct AggregateReadConfig {
    pub max_chunk_size: u64,  // Max bytes per disk read
}
```

### AggregateWriteConfig

```rust
pub struct AggregateWriteConfig {
    pub max_data_cache_size_bytes: usize,  // Writer cache limit
    pub cache_trim_factor: usize,          // Trim hysteresis (e.g., 25 = trim at 104%)
    pub max_chunk_size: usize,             // Max bytes per disk write
}
```

### NodeConfig

```rust
pub struct NodeConfig {
    pub data_root_folder: String,
    pub node_id: u128,
    pub async_flush_ms: u64,                    // Background sync delay
    pub max_open_aggregates: usize,             // Cache capacity
    pub max_event_batches_response_size: Option<usize>,  // Pagination limit
    // ...
}
```

## Error Handling

### ReadError

| Variant | Cause |
|---------|-------|
| `NotExists` | Aggregate files don't exist |
| `UnavailableBatchIndex` | Requested batch was trimmed |
| `MaxBytesTooSmall` | Pagination limit smaller than first batch |
| `CorruptEventBatch` | CRC mismatch on read |
| `CorruptMetadata` | Deserialization failed |

### WriteError

| Variant | Cause |
|---------|-------|
| `OptimisticConcurrencyViolation` | Expected batch index doesn't match |
| `ClientIdempotencyViolation` | Client event index already seen |
| `EmptyEventsList` | Write request has no events |
| `ZeroEventType` | Event type 0 is reserved |
| `CacheMiss` | Requested range not in writer cache |
| `PrependCreatesEventBatchIndexGap` | Prepend doesn't connect to existing data |

## Usage

### Embedding Celeriant

For embedding without the TCP server:

```rust
use celeriant_aggregate::{
    local_aggregate::{LocalAggregate, LocalAggregateTrait},
    read_operations::read_structures::AggregateReadConfig,
    write_operations::aggregate_write_config::AggregateWriteConfig,
    node_config::NodeConfig,
};

// Configure
let read_config = AggregateReadConfig { max_chunk_size: 1 << 20 };
let write_config = AggregateWriteConfig {
    max_data_cache_size_bytes: 1 << 25,
    cache_trim_factor: 25,
    max_chunk_size: 1 << 20,
};
let node_config = NodeConfig {
    data_root_folder: "/var/lib/myapp/events".into(),
    ..Default::default()
};

// Create engine (must run on glommio executor)
let engine = LocalAggregate::new(read_config, write_config, node_config);

// Write events
let response = engine.write(lease_index, write_request).await?;

// Read events
let response = engine.read(&read_request).await?;
```

### Low-Level Access

For direct file operations:

```rust
let cache = AggregateCache::new(capacity, node_config, read_config, write_config);
let resources = cache.get_aggregate_resources(&aggregate_key);

// Get writer with exclusive access
let mut writer = resources.get_writer_mut(true).await?;
writer.queue_events_in_memory(node_id, lease_index, timestamp, &mut request)?;
writer.sync_with_rollback().await?;

// Get reader
let reader = resources.get_reader(false).await?;
let response = reader.read(
    correlation_id,
    writer.minimum_available_event_batch_index,
    writer.file_len_metadata,
    writer.file_len_event_batch,
    &filters,
    max_bytes,
).await?;
```

## Dependencies

- `glommio` - Thread-per-core async runtime with io_uring
- `celeriant_wal` - Event and batch structures
- `celeriant_wire` - Serialization
- `celeriant_msg` - Request/response types
- `celeriant_disk` - DMA file utilities
- `lru` - Cache implementation
- `fastbloom` - Bloom filter
- `crc32c` - Hardware-accelerated CRC