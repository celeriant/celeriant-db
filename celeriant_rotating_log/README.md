# celeriant_rotating_log

Manages rotating WAL log segments with LRU caching, DMA I/O, and bloom filter optimization. Handles file lifecycle, crash recovery, and efficient reverse scanning.

## Overview

```
Shard Directory
├── log_1.wal (oldest, cached on-demand)
├── log_2.wal (cached on-demand)
├── ...
└── log_N.wal (active, always open for writes)

LogSegmentsCache
├── active_file     → Current log being written (Rc<LogSegmentFile>)
├── lru_cache       → Older logs opened on-demand (LRU<log_id, Rc<LogSegmentFile>>)
└── shard_dir       → Path for lazy-loading

LogSegmentFile
├── writer          → RwLock<DmaFile> for appends
├── reader          → RwLock<DmaFile> for concurrent reads (dup'd fd)
└── metadata        → Positions, bloom filter, wal_index
```

**Active file**: Always open, receives all writes, rotates when full.  
**Cached files**: Opened lazily, evicted via LRU when cache is full.  
**Dual file handles**: Separate reader/writer DmaFiles allow concurrent read/write without blocking.

## Key Types

| Type | Purpose |
|------|---------|
| `LogSegmentsCache` | Manages active + cached log files with LRU eviction |
| `LogSegmentFile` | Single log file with reader/writer handles and metadata |
| `LogSegmentFileMetadata` | In-memory state: positions, bloom filter, wal_index |
| `AggregateKeyBloom` | Per-segment bloom filter for aggregate key filtering |
| `ReverseMetablockScanner` | Scans metablocks backwards across log files |
| `RotatingLogError` | Error types (IO, corruption, validation) |

## Key Functions

| Function | Purpose |
|----------|---------|
| `LogSegmentsCache::ready_up` | Initialize shard, open/create active log file |
| `LogSegmentsCache::active` | Get active log segment for writing |
| `LogSegmentsCache::get` | Get log segment by ID (from cache or disk) |
| `LogSegmentsCache::rotate_to_next_log` | Create new active log when space exhausted |
| `LogSegmentsCache::close` | Close all file handles |
| `LogSegmentFile::open_or_create` | Open existing or create new log file |
| `LogSegmentFile::open_existing` | Open existing log file (errors if missing) |
| `ReverseMetablockScanner::scan` | Scan metablocks in reverse with visitor |
| `write_dual_shard_log_header` | Write header to both start and end of file |

## Usage

```rust
// Initialize shard
let cache = LogSegmentsCache::ready_up(
    shard_dir,
    1 << 30,        // 1GB preallocate
    8,              // max cached files
).await?;

// Write path: get active log
let active = cache.active();
let mut guard = active.lock_writer("append").await?;
// ... write metablocks/datablocks ...

// Rotate when needed
cache.rotate_to_next_log(required_space).await?;

// Read path: get any log by ID
let log_file = cache.get(log_id).await?;
let guard = log_file.lock_reader("read").await?;
// ... read data ...

// Reverse scan with bloom filter optimization
let mut scanner = ReverseMetablockScanner::new(
    &cache,
    cache.active_log_id(),
    None,           // start from end
    64 * 1024,      // chunk size
).with_bloom_filter(&aggregate_key);

let result = scanner.scan(|log_id, pos, block| {
    // Process 512-byte metablock
    if found_what_we_need(block) {
        Ok(Some(result))  // Stop scanning
    } else {
        Ok(None)          // Continue
    }
}).await?;

// Cleanup
cache.close().await?;
```

## Design Decisions

### Dual headers for crash recovery

```
Log File Layout:
[Header @ 0]           ← Primary header
[Metablocks →]
[Free space]
[← Datablocks]
[Header @ EOF-512]     ← Backup header
```

Both headers are written on every fsync. On open, if primary is corrupted, backup is used. If both are corrupted, the file requires repair.

### Separate reader/writer file handles

```rust
pub struct LogSegmentFile {
    writer: RwLock<Option<Rc<DmaFile>>>,  // For appends
    reader: RwLock<Option<Rc<DmaFile>>>,  // For concurrent reads
}
```

Using `DmaFile::dup()` creates independent file descriptors. Readers never block writers and vice versa. The RwLock protects the Option (for close semantics), not concurrent I/O.

### Metadata in separate RefCell

```rust
pub struct LogSegmentFile {
    writer: RwLock<Option<Rc<DmaFile>>>,
    reader: RwLock<Option<Rc<DmaFile>>>,
    pub metadata: RefCell<LogSegmentFileMetadata>,  // Not inside the RwLock!
}
```

Metadata (positions, bloom filter, wal_index) is stored in a separate `RefCell`, not inside the `RwLock` with the file handles. This enables a critical optimization:

```rust
// After fsync completes, update metadata without blocking readers:
let mut metadata = log_segment_file.metadata.borrow_mut();
metadata.metablocks_position = new_metablocks_position;
metadata.datablocks_position = new_datablocks_position;
metadata.wal_index = new_wal_index;
// Readers can immediately see new data boundaries
```

**Why this matters:**
- After a write is fsynced, readers need to know the new `metablocks_position` to read freshly written data
- If metadata lived inside the writer's `RwLock`, updating it would require a write lock
- That write lock would block all concurrent readers until the update completes
- With separate `RefCell`, metadata updates are instant and readers see new positions immediately

**Single-threaded safety:** Celeriant uses glommio's thread-per-core model. Each shard runs on exactly one thread, so `RefCell` is safe—no cross-thread access occurs. The `RwLock` on file handles exists for async coordination (multiple tasks on same thread), not thread safety.

### Bloom filter per log segment

Each log segment maintains a bloom filter of all aggregate keys written to it. When scanning backwards for an aggregate:

```rust
scanner.with_bloom_filter(&aggregate_key)
```

Entire log segments where the bloom filter says "definitely not present" are skipped, potentially avoiding many disk reads.

### LRU cache with active file bypass

```rust
pub async fn get(&self, log_id: u64) -> Result<Rc<LogSegmentFile>> {
    if log_id == self.active_log_id() {
        return Ok(self.active());  // Direct return, no cache
    }
    // Check LRU cache, open from disk if needed
}
```

The active file is always accessible without cache lookup. Older files go through LRU with configurable capacity.

### Rotation triggers

```rust
pub async fn rotate_to_next_log(&self, required_disk_space: u64) -> Result<bool> {
    if available_space.saturating_sub(required_disk_space) > 0 {
        return Ok(false);  // No rotation needed
    }
    // Create log_{N+1}.wal, move current to cache
}
```

Rotation is caller-driven based on required space. The old active file moves to the LRU cache; the new file becomes active.

### Deadlock detection

```rust
pub async fn read_with_timeout<T>(lock: &RwLock<T>, location: &'static str) 
    -> Result<RwLockReadGuard<T>, LockTimeoutError>
```

All lock acquisitions use 1-second timeouts. If exceeded, returns `PotentialDeadlock` error with the location string for debugging. Essential for diagnosing async deadlocks in production.

### Preallocated files

```rust
LogSegmentsCache::ready_up(shard_dir, preallocate_bytes, max_cached)
```

Files are preallocated to `preallocate_bytes` (typically 1GB) on creation. This:
- Reduces fragmentation
- Enables efficient DMA alignment
- Makes available space calculation simple

Size must be multiple of 512 bytes and large enough for dual headers.

### Datablocks carry-over

When `datablocks_position` is not aligned to DMA boundaries, the bytes between position and next alignment boundary are read on open:

```rust
pub datablocks_carry_over: Option<Vec<u8>>
```

This allows writers to continue appending at unaligned positions without losing data.

## Dependencies

- `glommio` - Thread-per-core async runtime with DMA support
- `lru` - LRU cache implementation
- `celeriant_wal` - WAL data structures (Metablock, ShardLogHeader)
- `celeriant_wire` - Serialization for headers
- `celeriant_disk` - Low-level DMA read utilities
- `fastbloom` - Bloom filter implementation