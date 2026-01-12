---
name: database-architecture
description: Core database architecture patterns for Celeriant. Covers memory management with LRU for infinite cardinality, WAL durability guarantees, and tracing best practices. Use when implementing new features, reviewing code, or understanding why certain patterns exist.
---

# Celeriant Database Architecture Patterns

This skill covers critical architectural patterns that maintain Celeriant's correctness and performance guarantees. Code that violates these patterns can cause memory exhaustion, data loss, or operational issues.

## Related Skills

- **[glommio-locking-patterns](../glommio-locking-patterns/SKILL.md)**: Async concurrency, RefCell vs RwLock, deadlock avoidance
- **[understanding-celeriant-structure](../understanding-celeriant-structure/SKILL.md)**: Crate organization, write/read paths, file layout

---

## Memory Management: Bounded Structures for Infinite Cardinality

Celeriant supports **infinite cardinality**—millions of aggregates per shard. This requires strict memory bounds on all data structures.

### Rule: All Long-Lived Caches Must Be Bounded

Use `LruCache` from the `lru` crate for caches that persist across requests. Configure byte-based limits derived from configured memory budgets.

```rust
// Reference: celeriant_memcache/src/shard_mem_cache.rs:549-570

pub fn new(
    recent_write_cache_bytes: u64,
    aggregate_snapshots_cache_bytes: u64,
    aggregate_client_snapshots_cache_bytes: u64,
    list_wal_index_cache_bytes: u64,
) -> Self {
    // Calculate capacity from byte budget and entry size
    let aggregate_cap = NonZeroUsize::new(
        (aggregate_snapshots_cache_bytes / 112) as usize
    ).unwrap_or(NonZeroUsize::new(10_000).unwrap());

    Self {
        aggregate_snapshots: LruCache::new(aggregate_cap),
        // ... other bounded caches
    }
}
```

### Pattern: Size-Bounded Eviction Queue

For caches that need byte-accurate size tracking, maintain an eviction queue alongside the data:

```rust
// Reference: celeriant_memcache/src/shard_mem_cache.rs:18-29

pub struct ShardMemCache {
    recent_write_cache_bytes: u64,       // Maximum size
    cache_current_bytes: u64,            // Current size (tracked)
    cache_eviction_queue: VecDeque<(AggregateKey, u64, u64)>,  // FIFO order
    aggregate_recent_writes: HashMap<AggregateKey, AggregateRecentWrites>,
}
```

Eviction before insertion ensures we never exceed the limit:

```rust
// Reference: celeriant_memcache/src/shard_mem_cache.rs:102-114

pub fn cache_recent_write(&mut self, ..., size_bytes: u64) {
    // Evict until we have room
    while self.cache_current_bytes + size_bytes > self.recent_write_cache_bytes {
        if !self.evict_oldest_cache_entry() {
            break;
        }
    }
    // Now safe to insert
}
```

### Pattern: LRU File Handle Cache

Open file handles are expensive. Cache them with bounded LRU:

```rust
// Reference: celeriant_rotating_log/src/log_segments_cache.rs:10-24

pub struct LogSegmentsCache {
    /// Active file is always open (not in LRU)
    active_file: RefCell<Rc<LogSegmentFile>>,

    /// Older files cached with bounded LRU
    lru_cache: RefCell<LruCache<u64, Rc<LogSegmentFile>>>,
}
```

### Pattern: Priority-Based Cache Insertion

Prevent cache pollution from scans by using priority insertion:

```rust
// Reference: celeriant_memcache/src/shard_mem_cache.rs:592-607

fn put_with_priority<K, V>(cache: &mut LruCache<K, V>, key: K, value: V, low_priority: bool) {
    if low_priority {
        // Only insert if there's spare capacity
        if cache.len() < cache.cap().get() {
            cache.put(key.clone(), value);
            cache.demote(&key);  // Immediately move to LRU position
        }
    } else {
        cache.put(key, value);  // Normal MRU insertion
    }
}
```

Use `low_priority: true` for speculative caching during scans. Use `low_priority: false` for targeted access.

---

## Anti-Patterns: Unbounded Allocations

### AVOID: Unbounded HashMap for Per-Aggregate Data

```rust
// BAD: HashMap grows without bound as aggregates are accessed
struct BadCache {
    positions: HashMap<AggregateKey, Position>,  // Will exhaust memory
}

// GOOD: LRU-bounded cache
struct GoodCache {
    positions: LruCache<AggregateKey, Position>,  // Fixed capacity
}
```

### AVOID: Unbounded Vec for User-Controlled Data

```rust
// BAD: User can send unlimited events
fn process_events(events: Vec<Event>) {
    let all_events: Vec<_> = events.iter().collect();  // Unbounded
}

// GOOD: Process in bounded batches, stream results
fn process_events(events: Vec<Event>, batch_size: usize) {
    for chunk in events.chunks(batch_size) {
        process_chunk(chunk);
    }
}
```

### Acceptable Exceptions

The **pending append queue** is intentionally unbounded because it's drained quickly via fsync batching:

```rust
// Reference: celeriant_memcache/src/shard_mem_cache.rs:40-45

/// These are writes yet to be written to disk
/// This is unbounded as we expect quick flush to disk
aggregate_queue_positions: HashMap<AggregateKey, QueueAggregatePositions>,
pending_append_queue: Vec<ShardLogQueueItem>,
```

This is acceptable because:
1. The queue is drained every fsync (typically within milliseconds)
2. Backpressure is applied at the connection layer if queue grows too large
3. The alternative (bounded queue) would cause write rejections

### Memory Reclamation

Periodically shrink collections that may have grown temporarily:

```rust
// Reference: celeriant_memcache/src/shard_mem_cache.rs:145-151

// Periodically reclaim memory from data structures
if self.cache_eviction_queue.capacity() > self.cache_eviction_queue.len() * 2 {
    self.cache_eviction_queue.shrink_to_fit();
}
if self.aggregate_recent_writes.capacity() > self.aggregate_recent_writes.len() * 2 {
    self.aggregate_recent_writes.shrink_to_fit();
}
```

---

## Write-Ahead Log: Durability Guarantees

### Critical Rule: Acknowledge ONLY After fdatasync

**Never acknowledge a write to a client before it's durable on disk.** Violating this causes data loss on crash.

```rust
// Reference: celeriant_shard/src/shard_wal_sync.rs:126-263

pub(crate) async fn sync(...) -> Result<(), ShardFsyncError> {
    // 1. Write datablocks to disk
    dma_file_writer.write_at(buffer_datablocks, ...).await?;

    // 2. Write metablocks to disk
    dma_file_writer.write_at(buffer_metablocks, ...).await?;

    // 3. Write dual headers (for crash recovery)
    write_dual_shard_log_header(&dma_file_writer, ...).await?;

    // 4. CRITICAL: fdatasync before returning success
    dma_file_writer.fdatasync().await?;

    // 5. NOW safe to update in-memory state
    *updated_log_segment_file_metadata = log_segment_file_metadata;

    Ok(())
}
```

### Pattern: Two-Phase Commit with Rollback

The sync process uses snapshot-based two-phase commit:

```rust
// Reference: celeriant_shard/src/shard_wal_sync.rs:41-63

pub(crate) async fn sync_with_rollback(...) -> Result<(), ShardFsyncError> {
    // Phase 1: Take snapshot (synchronous, quick)
    let Some((required_disk_space, mut sync_positions_snapshot)) =
        take_sync_snapshot(&shard_mem_cache) else {
        return Ok(());
    };

    // Phase 2: Write to disk (async, may fail)
    match sync(active_log_segment, &mut sync_positions_snapshot).await {
        Ok(_) => {
            // Success: commit in-memory state, broadcast events
            commit_sync(shard_mem_cache, watched_aggregates, sync_positions_snapshot);
            Ok(())
        }
        Err(e) => {
            // Failure: rollback, clear queue, force log rotation
            rollback_sync(shard_mem_cache, &log_segments_cache);
            Err(e)
        }
    }
}
```

### Pattern: Queue Visibility During Sync

New writes must see correct indexes even while a sync is in progress:

```rust
// Reference: celeriant_memcache/src/shard_mem_cache.rs:280-295

pub fn take_sync_positions_snapshot(&mut self) -> SyncPositionsSnapshot {
    // Clone instead of swap - queue positions must remain visible
    // for new writes that arrive while this sync is in progress
    let aggregate_queue_positions = self.aggregate_queue_positions.clone();

    // Clear pending queue, ready for new writes
    let mut pending_append_queue = vec![];
    std::mem::swap(&mut pending_append_queue, &mut self.pending_append_queue);

    SyncPositionsSnapshot {
        aggregate_queue_positions,
        pending_append_queue,
    }
}
```

### Dual Header Pattern

Write headers at both start and end of file for crash recovery:

```rust
// Reference: celeriant_shard/src/shard_wal_sync.rs:252-255

// Write header at start (offset 0) AND end (file_len - header_size)
let header_end_start_pos = log_segment_file_metadata.file_len
    .saturating_sub(HEADER_BLOCK_SIZE_BYTES as u64);
write_dual_shard_log_header(&dma_file_writer, header_end_start_pos, &header).await?;
```

If one header is corrupted during crash, the other can be used for recovery.

---

## Tracing and Logging Best Practices

### Rule: No info! or debug! in Hot Paths

Hot paths (write validation, read filtering, per-request logic) must not log at `info` or `debug` level in production. This:
- Generates gigabytes of logs per hour
- Masks important operational events
- Degrades performance

```rust
// BAD: Logs on every write
pub async fn process_write(&self, request: &WriteRequest) -> Result<WriteResponse> {
    tracing::info!("Processing write for {:?}", request.aggregate_key);  // SPAM
    // ...
}

// GOOD: No logging in hot path, or trace! only
pub async fn process_write(&self, request: &WriteRequest) -> Result<WriteResponse> {
    // No logging here - this runs thousands of times per second
    // ...
}
```

### When to Use Each Level

| Level | When to Use | Examples |
|-------|-------------|----------|
| `error!` | Unrecoverable failures, data integrity issues | Fsync failure, corruption detected |
| `warn!` | Recoverable issues, unexpected conditions | Client disconnect, timeout, retry |
| `info!` | Startup, shutdown, configuration, rare events | Server started, config loaded |
| `debug!` | Development debugging only | Never in production code |
| `trace!` | Extremely detailed, disabled by default | Per-request flow, detailed state |

### Current Codebase Examples

```rust
// Reference: celeriant_runtimes/src/lib.rs
// GOOD: info! for startup only
tracing::info!("Starting sharded runtime with {} shards", num_shards);

// Reference: celeriant_runtimes/src/sharded/connection_handler.rs
// GOOD: warn! for unexpected client behavior
use tracing::warn;
// Used when client sends malformed request or disconnects unexpectedly

// Reference: celeriant/src/server_config.rs
// GOOD: info! for configuration at startup
tracing::info!("Server starting with custom configuration:");
for (name, value) in &config_entries {
    tracing::info!("  {}: {}", name, value);
}
```

### Pattern: Structured Fields Over String Interpolation

```rust
// BAD: String formatting
tracing::warn!("Write failed for aggregate {} with error {}", key, err);

// GOOD: Structured fields (searchable, parseable)
tracing::warn!(
    aggregate_key = ?key,
    error = %err,
    "Write failed"
);
```

### Pattern: Spans for Request Tracing

For operations that span multiple async calls, use spans:

```rust
// GOOD: Span captures entire operation duration
async fn handle_request(&self, req: Request) {
    let span = tracing::info_span!(
        "handle_request",
        request_id = %req.id,
        aggregate = ?req.aggregate_key,
    );
    let _guard = span.enter();

    // All operations within this scope are associated with the span
}
```

---

## Checklist for New Code

### Memory Management
- [ ] All per-aggregate caches use `LruCache` with byte-based capacity
- [ ] No unbounded `HashMap<AggregateKey, _>` or `Vec` that grows with user data
- [ ] Temporary allocations are bounded or documented as acceptable
- [ ] Collections call `shrink_to_fit()` when capacity greatly exceeds length

### Durability
- [ ] Writes acknowledged ONLY after `fdatasync()` completes
- [ ] Snapshot taken before async I/O, committed after success
- [ ] Rollback path handles fsync failures
- [ ] In-memory state never updated before disk write confirmed

### Tracing
- [ ] No `info!` or `debug!` in per-request paths
- [ ] `error!` for unrecoverable failures only
- [ ] `warn!` for recoverable issues
- [ ] Structured fields used instead of string interpolation
- [ ] Startup/shutdown events logged at `info!`

---

## Files Reference

| File | Purpose |
|------|---------|
| `celeriant_memcache/src/shard_mem_cache.rs` | Main cache with LRU patterns, eviction, size tracking |
| `celeriant_memcache/README.md` | Cache architecture overview |
| `celeriant_rotating_log/src/log_segments_cache.rs` | LRU file handle cache |
| `celeriant_shard/src/shard_wal_sync.rs` | Sync path, fdatasync, two-phase commit |
| `celeriant_shard/src/shard_wal.rs` | WAL orchestrator, write validation |
| `celeriant_runtimes/src/sharded/connection_handler.rs` | Example of appropriate warn! usage |
