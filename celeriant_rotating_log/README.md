# celeriant_rotating_log

Rotating log file management for the Celeriant write-ahead log (WAL). This crate handles log file lifecycle, rotation, caching, and crash recovery using direct I/O.

## Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    RotatingLogCache                         │
├─────────────────────────────────────────────────────────────┤
│  active_file: Rc<RwLock<ShardLogDmaFile>>  ← writers here   │
│  active_log_id: Cell<u64>                  ← lock-free read │
│  lru_cache: LruCache<log_id, ShardLogDmaFile>  ← readers    │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    ShardLogDmaFile                          │
├─────────────────────────────────────────────────────────────┤
│  dma_file: Option<DmaFile>     ← direct I/O handle          │
│  log_id: u64                   ← monotonic file identifier  │
│  file_len: u64                 ← preallocated size          │
│  shard_log_header: ShardLogHeader  ← write positions        │
└─────────────────────────────────────────────────────────────┘
```

## File Layout

Each log file is preallocated to a fixed size with dual headers for crash recovery:

```
┌─────────────────────────────────────────────────────────────┐
│ Header (512 bytes) - metablocks_position, datablocks_position│
├─────────────────────────────────────────────────────────────┤
│ Metablocks (512 bytes each, growing forward →)              │
│                                                             │
├ ─ ─ ─ ─ ─ ─ ─ ─ ─ Free Space ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─┤
│                                                             │
│              Datablocks (variable, growing ← backward)      │
├─────────────────────────────────────────────────────────────┤
│ Header (512 bytes) - duplicate for torn write recovery      │
└─────────────────────────────────────────────────────────────┘

File: log_1.wal, log_2.wal, log_3.wal, ...
```

When metablocks and datablocks meet in the middle, the file is full and rotation occurs.

## Key Types

### RotatingLogCache

Manages the active log file and LRU cache of older log files.

```rust
let cache = RotatingLogCache::new(
    shard_dir,           // Directory for log files
    preallocate_bytes,   // Size of each log file (must be block-aligned)
    max_cached_files,    // LRU cache capacity for reader access
).await?;

// Writers: get the active file with write lock
let active = cache.active();
let mut guard = active.write().await?;

// Readers: get any log file by ID (from cache or disk)
let log_file = cache.get(log_id).await?;
let guard = log_file.read().await?;
```

**Design decisions:**

| Feature | Rationale |
|---------|-----------|
| Separate active file | Writers don't contend with readers on old files |
| `Cell<u64>` for active_log_id | Avoid blocking readers when write lock is present |
| LRU cache | Respect linux fd limits while keeping hot files open |
| `Rc<RwLock<_>>` handles | Uniform access pattern for both active and cached files |

### ShardLogDmaFile

Represents a single physical log file with direct I/O support.

```rust
// Open or create (for active file on startup)
let mut dma_file = ShardLogDmaFile::open_or_create(&shard_dir, preallocate_bytes, log_id).await?;

// Open existing (for reader cache)
let dma_file = ShardLogDmaFile::open_existing(&shard_dir, log_id).await?;

// Rotate to next file (returns previous file for caching)
let previous = dma_file.rotate_to_next_log(&shard_dir, preallocate_bytes).await?;
cache.rotate_to_next_log(dma_file.log_id, previous);

// Update headers after writes
dma_file.write_new_headers_and_fsync(new_datablocks_pos, new_metablocks_pos).await?;
```

### ShardLogHeader

Tracks write positions within a log file:

```rust
pub struct ShardLogHeader {
    pub metablocks_position: u64,  // End of last written metablock
    pub datablocks_position: u64,  // Start of last written datablock
}

// Available space for new writes
let space = header.available_space(); // datablocks_position - metablocks_position
```

## Usage Patterns

### Writer Path

```rust
// Acquire exclusive access to active file
let lockable_active = rotating_log_cache.active();
let mut shard_log_dma_file = lockable_active.write().await?;

// Check if rotation needed
if !has_enough_free_space {
    let previous = shard_log_dma_file
        .rotate_to_next_log(&shard_dir, preallocate_bytes)
        .await?;
    rotating_log_cache.rotate_to_next_log(shard_log_dma_file.log_id, previous);
}

// Write data...
let dma_file = shard_log_dma_file.dma_file.as_mut().unwrap();
dma_file.write_at(buffer, position).await?;

// Commit with fsync
shard_log_dma_file
    .write_new_headers_and_fsync(new_datablocks_pos, new_metablocks_pos)
    .await?;
```

### Reader Path

```rust
// Get file by log_id (cache hit or disk open)
let log_file = cache.get(log_id).await?;
let guard = log_file.read().await?;

// Read data
let dma_file = guard.dma_file.as_ref().unwrap();
let data = dma_file.read_at(position, size).await?;
```

### Startup Recovery

```rust
// Automatically finds latest log file and opens it
let cache = RotatingLogCache::new(shard_dir, preallocate_bytes, max_cached).await?;

// If front header is corrupted, recovers from back header
// If both headers corrupted, returns HeaderCorrupted error
```

## Crash Recovery

Dual headers enable recovery from torn writes:

1. **Normal state**: Front and back headers match
2. **Torn write during header update**: Back header has last-known-good state
3. **Recovery**: If front header CRC fails, use back header

```rust
// Header write order (in write_new_headers_and_fsync):
// 1. Write front header
// 2. Write back header  
// 3. fdatasync()

// Recovery order (in open_existing):
// 1. Try front header
// 2. If CRC fails, try back header
// 3. If both fail, return HeaderCorrupted
```

## Configuration

### Preallocate Bytes

Must be:
- Block-aligned (multiple of 512 bytes)
- At least 3 blocks (1536 bytes) for dual headers + minimal data
- Typically 1 GB depending on workload

```rust
// Valid
let size = 64 * 1024; // 64 KiB

// Invalid - not aligned
let size = 64 * 1024 + 100; // Error: InvalidPreallocatedBytes

// Invalid - too small
let size = 512; // Error: InvalidPreallocatedBytes (need > 2 blocks)
```

### Max Cached Files

Controls OS file descriptor usage for reader access to old log files:

```rust
// Minimum 1 (clamped internally)
let cache = RotatingLogCache::new(dir, size, 0).await?; // Uses 1

// Typical: 2-10 depending on read patterns
let cache = RotatingLogCache::new(dir, size, 5).await?;
```

## Error Handling

```rust
pub enum RotatingLogError {
    InvalidPreallocatedBytes(u64),  // Size validation failed
    IoError(String),                 // Disk I/O failure
    WireFormat(WireFormatError),     // Serialization failure
    HeaderCorrupted { log_id: Option<u64> },  // Both headers invalid
    LogFileNotFound { log_id: u64 }, // Requested log doesn't exist
}
```

## File Naming

Log files follow a strict naming convention:

```
log_{id}.wal

Examples:
  log_1.wal   ← First log file
  log_2.wal   ← After first rotation
  log_999.wal ← After 998 rotations
```

The cache scans for `log_*.wal` files on startup and opens the highest ID as active.

## Dependencies

- `glommio` - Async direct I/O runtime (DmaFile, RwLock)
- `lru` - LRU cache implementation
- `celeriant_wal` - Header structures and constants
- `celeriant_wire` - Serialization for headers
- `celeriant_disk` - DmaFile open helpers

## Thread Model

This crate is designed for single-threaded async executors (glommio):

- `Rc` instead of `Arc` for reference counting
- `RefCell` instead of `Mutex` for interior mutability
- `Cell` for lock-free primitive updates
- `glommio::sync::RwLock` for async reader-writer locking

Not `Send` or `Sync` - each shard runs on a dedicated CPU core.