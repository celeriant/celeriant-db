# celeriant_rotating_log

Manages rotating WAL log segments with LRU caching, DMA I/O, and bloom filter optimization. Handles file lifecycle, crash recovery, replication-aware position tracking, and efficient reverse scanning.

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
└── metadata        → RefCell<LogSegmentFileMetadata>

LogSegmentFileMetadata
├── write           → LogSegmentCursor (writer's view, not yet replicated)
└── read            → Option<LogSegmentCursor> (reader's view, replicated & visible)
```

**Active file**: Always open, receives all writes, rotates when full.
**Cached files**: Opened lazily, evicted via LRU when cache is full.
**Dual file handles**: Separate reader/writer DmaFiles allow concurrent read/write without blocking.
**Dual cursors**: `write` tracks in-progress writes; `read` tracks what is replicated and visible.

## Key Types

| Type | Purpose |
|------|---------|
| `LogSegmentsCache` | Manages active + cached log files with LRU eviction |
| `LogSegmentFile` | Single log file with reader/writer handles and metadata |
| `LogSegmentFileMetadata` | In-memory state: dual cursors, file_len, carry-over bytes |
| `LogSegmentCursor` | Snapshot of positions, wal_index, bloom filter, tip_hash |
| `AggregateKeyBloom` | Per-segment bloom filter for aggregate key filtering |
| `ReverseMetablockScanner` | Scans metablocks backwards across log files |
| `OpenOrCreateError` | Errors opening or creating log files |
| `ReadyUpError` | Errors during shard initialization |
| `ScanError` | Errors during reverse metablock scanning |
| `WriteDualHeaderError` | Errors writing primary/backup headers |

## Key Functions

| Function | Purpose |
|----------|---------|
| `LogSegmentsCache::ready_up` | Initialize shard, open/create active log file |
| `LogSegmentsCache::active` | Get active log segment for writing |
| `LogSegmentsCache::active_log_id` | Get the log_id of the active file |
| `LogSegmentsCache::get` | Get log segment by ID (from cache or disk) |
| `LogSegmentsCache::get_if_cached` | Non-async check if log_id is already cached, no I/O |
| `LogSegmentsCache::evict_from_lru` | Evict a log segment from the LRU cache |
| `LogSegmentsCache::rotate_to_next_log` | Create new active log, move current to LRU cache |
| `LogSegmentsCache::rollback_write_position` | Rollback write cursor after failed replication |
| `LogSegmentsCache::get_latest_read_cursor` | Get replicated read position (handles rotation boundary) |
| `LogSegmentsCache::active_log_available_space` | Quick space check against write cursor |
| `LogSegmentsCache::shard_dir` | Get the shard directory path |
| `LogSegmentsCache::close` | Close all file handles |
| `LogSegmentFile::open_or_create_first_file_for_shard` | Open existing or create new log file |
| `LogSegmentFile::open_existing` | Open existing log file (errors if missing) |
| `LogSegmentFile::lock_reader` | Acquire read lock on the DmaFile with timeout |
| `LogSegmentFile::lock_writer` | Acquire write lock on the DmaFile with timeout |
| `LogSegmentFile::close` | Close both reader and writer file handles |
| `LogSegmentFileMetadata::advance_visible_position` | Promote write cursor to read (post-replication) |
| `LogSegmentFileMetadata::is_pending_advance` | True if write cursor is ahead of read cursor |
| `LogSegmentFileMetadata::to_shard_log_header` | Convert metadata to ShardLogHeader for persistence |
| `LogSegmentFileMetadata::available_space` | Remaining bytes between metablocks and datablocks |
| `LogSegmentFileMetadata::readable_metablocks_end` | End position of metablocks visible to readers |
| `ReverseMetablockScanner::scan` | Scan metablocks in reverse with visitor |
| `ReverseMetablockScanner::with_bloom_filter` | Skip segments where aggregate is definitely absent |
| `ReverseMetablockScanner::with_bloom_filter_hash` | Same as above but with pre-computed hash bytes |
| `write_dual_shard_log_header` | Write header to both start and end of file |
| `read_datablocks_carry_over_bytes` | Read unaligned bytes at datablocks boundary on open |

## Usage

```rust
// Initialize shard
let cache = LogSegmentsCache::ready_up(
    shard_dir,
    1 << 30,        // 1GB preallocate (must be multiple of 512KB)
    8,              // max cached files
    shard_id,       // shard identifier for metrics labels
).await?;

// Write path: get active log
let active = cache.active();
let mut guard = active.lock_writer("append").await?;
// ... write metablocks/datablocks ...
drop(guard);

// After successful replication: advance read cursor
active.metadata.borrow_mut().advance_visible_position();

// After failed replication: rollback write cursor
cache.rollback_write_position();

// Check space before writing
let space = cache.active_log_available_space();

// Rotate when space is insufficient (caller decides when)
cache.rotate_to_next_log().await?;

// Read path: get any log by ID
let log_file = cache.get(log_id).await?;
let guard = log_file.lock_reader("read").await?;
// ... read data ...

// Non-async cache check (no I/O)
if let Some(file) = cache.get_if_cached(log_id) {
    // already loaded
}

// Get the latest replicated position (safe to serve reads)
let read_cursor = cache.get_latest_read_cursor();

// Reverse scan with bloom filter optimization
let mut scanner = ReverseMetablockScanner::new(
    &cache,
    cache.active_log_id(),
    None,           // start from end
    64 * 1024,      // chunk size
).with_bloom_filter(&aggregate_key);

let result = scanner.scan(|log_id, pos, block| {
    // Process 1024-byte metablock
    if found_what_we_need(block) {
        Ok(Some(result))  // Stop scanning
    } else {
        Ok(None)          // Continue
    }
}).await?;

// Cleanup
cache.close().await;
```

## Design Decisions

### Dual headers for crash recovery

```
Log File Layout:
[Header @ 0]               ← Primary header (512KB)
[Metablocks →]             ← 1024 bytes each, grow from header end
[Free space]
[← Datablocks]             ← Grow from rear header inward
[Header @ EOF-512KB]       ← Backup header (512KB)
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

### Dual cursors: write and read

```rust
pub struct LogSegmentFileMetadata {
    pub write: LogSegmentCursor,        // Writer's view - most recent writes
    pub read: Option<LogSegmentCursor>, // Reader's view - replicated, visible to readers
}
```

Write and read advance independently:

- `write` cursor is updated immediately after each fsync
- `read` cursor is promoted from `write` only after successful replication via `advance_visible_position()`
- `read` is `None` on a freshly rotated log file until the first replication completes for that file
- `is_pending_advance()` returns true when `write` is ahead of `read`
- `rollback_write_position()` resets `write` back to `read` after replication failure

The scanner uses `read` exclusively, ensuring readers only see durably replicated data.

### LogSegmentCursor

```rust
pub struct LogSegmentCursor {
    pub log_id: u64,
    pub metablocks_position: u64,   // End of last metablock (grows from header end)
    pub datablocks_position: u64,   // Start of most recent datablock (grows from rear)
    pub wal_index: u64,             // Shard-global WAL index at this cursor
    pub aggregate_key_bloom: AggregateKeyBloom,
    pub tip_hash: EntryHashBytes,   // blake3 hash chain for distributed verification
}
```

A cursor is a full snapshot of log state at a point in time. It converts bidirectionally with `ShardLogHeader` for persistence.

### Metadata in separate RefCell

```rust
pub struct LogSegmentFile {
    writer: RwLock<Option<Rc<DmaFile>>>,
    reader: RwLock<Option<Rc<DmaFile>>>,
    pub metadata: RefCell<LogSegmentFileMetadata>,  // Not inside the RwLock!
}
```

Metadata (cursors, bloom filter, wal_index) is stored in a separate `RefCell`, not inside the `RwLock` with the file handles. This enables a critical optimization:

```rust
// After fsync completes, update metadata without blocking readers:
let mut metadata = log_segment_file.metadata.borrow_mut();
metadata.write.metablocks_position = new_metablocks_position;
metadata.write.datablocks_position = new_datablocks_position;
metadata.write.wal_index = new_wal_index;
// Readers can immediately see new data boundaries
```

**Why this matters:** If metadata lived inside the writer's `RwLock`, updating it would require a write lock, blocking all concurrent readers. With separate `RefCell`, metadata updates are instant.

**Single-threaded safety:** Celeriant uses glommio's thread-per-core model. Each shard runs on exactly one thread, so `RefCell` is safe. The `RwLock` on file handles exists for async coordination (multiple tasks on same thread), not thread safety.

### Rotation carry-over: wal_index and tip_hash

When rotating to a new log file, `wal_index` and `tip_hash` are read from the current active file's `write` cursor and written into the new file's header:

```rust
pub async fn rotate(&self, shard_dir: &PathBuf, preallocate_bytes: u64) -> Result<Self, OpenOrCreateError> {
    let (new_log_id, wal_index, tip_hash) = {
        let meta = self.metadata.borrow();
        (meta.log_id + 1, meta.write.wal_index, meta.write.tip_hash)
    };
    // ... create new file with wal_index and tip_hash ...
}
```

This maintains the global WAL sequence and hash chain continuity across file boundaries. The new file's `read` cursor starts as `None`—readers see the previous file's data until replication confirms the new file's entries.

### Rotation triggers

```rust
pub async fn rotate_to_next_log(&self) -> Result<(), OpenOrCreateError>
```

Rotation is caller-driven. The caller checks `active_log_available_space()` and calls `rotate_to_next_log()` when space is insufficient. The old active file moves to the LRU cache; the new file becomes active.

### Bloom filter per log segment

Each log segment maintains a bloom filter of all aggregate keys written to it (stored in both cursors). When scanning backwards for an aggregate:

```rust
scanner.with_bloom_filter(&aggregate_key)
```

Entire log segments where the bloom filter says "definitely not present" are skipped, potentially avoiding many disk reads. The bloom filter uses the `read` cursor so only replicated segments are considered.

### LRU cache with active file bypass

```rust
pub async fn get(&self, log_id: u64) -> Result<Rc<LogSegmentFile>> {
    if log_id == self.active_log_id() {
        return Ok(self.active());  // Direct return, no cache lookup
    }
    // Check LRU cache, open from disk if needed
}
```

The active file is always accessible without cache lookup. Older files go through LRU with configurable capacity. `get_if_cached()` provides a synchronous no-I/O variant for cases where the caller only wants to act on already-loaded files.

### get_latest_read_cursor: rotation boundary handling

```rust
pub fn get_latest_read_cursor(&self) -> LogSegmentCursor
```

After rotation, the new active file's `read` cursor is `None` until first replication. During this window, the latest replicated position is still on the previous log file in the LRU cache. `get_latest_read_cursor()` handles this transparently: if active `read` is `None`, it falls back to the previous file's `read` (or `write` if `read` is also unavailable there).

### rollback_write_position: replication failure recovery

```rust
pub fn rollback_write_position(&self)
```

If replication fails after writes have been fsynced, the write cursor must be reset to the last known replicated state. Two cases are handled:

- **Read on active file**: Reset `write` to `read` directly.
- **Read still on previous file** (just rotated): Reset active file's `write` cursor to an empty state (positions at header boundaries) carrying over `wal_index`, `tip_hash`, and bloom from the previous file's last known cursor. Also resets the previous file's `write` to its `read`.

### Deadlock detection

```rust
pub async fn read_with_timeout<T>(lock: &RwLock<T>, location: &'static str)
    -> Result<RwLockReadGuard<T>, LockTimeoutError>
```

All lock acquisitions use 1-second timeouts. If exceeded, returns `PotentialDeadlock` error with the location string for debugging. Essential for diagnosing async deadlocks in production.

### Preallocated files

```rust
LogSegmentsCache::ready_up(shard_dir, preallocate_bytes, max_cached, shard_id)
```

Files are preallocated to `preallocate_bytes` (typically 1GB) on creation. Size must be a multiple of `HEADER_BLOCK_SIZE_BYTES` (512KB) and large enough for two headers. This:
- Reduces fragmentation
- Enables efficient DMA alignment
- Makes available space calculation simple (`datablocks_position - metablocks_position`)

### Datablocks carry-over

When `datablocks_position` is not aligned to DMA boundaries, the bytes between position and next alignment boundary are read on open:

```rust
pub datablocks_carry_over: Option<Vec<u8>>
```

This allows writers to continue appending at unaligned positions without losing data.

## Dependencies

- `glommio` - Thread-per-core async runtime with DMA support
- `lru` - LRU cache implementation
- `celeriant_wal` - WAL data structures (Metablock, ShardLogHeader, constants)
- `celeriant_wire` - Serialization for headers
- `celeriant_disk` - Low-level DMA read utilities
- `fastbloom` - Bloom filter implementation
- `bincode` - Binary serialization
- `metrics` - Runtime metrics collection
- `tracing` - Structured logging and diagnostics
- `futures-lite` - Lightweight async utilities
