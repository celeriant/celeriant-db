# celeriant_disk

Low-level disk I/O primitives for Celeriant using Direct Memory Access (DMA). This crate provides alignment-aware file reading optimized for the glommio async runtime.

## Overview

This crate handles the complexities of DMA I/O—alignment requirements, chunked reads, gap skipping—so higher layers can work with simple byte ranges and records.

```
File on Disk
├── Metablocks (fixed 1024B records)
└── Event Batches (variable size, compressed)

celeriant_disk provides:
├── read_objects_absolute    → Read variable-size objects by position
├── read_fixed_records_visit → Stream fixed-size records with visitor (forward or reverse)
├── open_dma_files           → DMA file opening helpers
└── rwlock_timeout           → Deadlock-detecting RwLock wrappers
```

## Key Functions

### read_objects_absolute

Read multiple variable-sized objects from absolute byte positions, efficiently skipping gaps.

```rust
pub async fn read_objects_absolute(
    file: &DmaFile,
    file_size: u64,
    object_positions: &[AbsoluteObjectPosition],
    max_chunk_size: u64,
) -> glommio::Result<Vec<Vec<u8>>, ()>

pub struct AbsoluteObjectPosition {
    pub start_pos: u64,
    pub end_pos: u64,
}
```

**Key features:**
- Reads objects in a single pass with minimal I/O operations
- Automatically aligns reads to device requirements (typically 512 bytes)
- Skips gaps between requested objects efficiently
- Handles objects spanning multiple chunks
- Pre-allocates buffers based on known object sizes
- Clamps reads to actual file size when `end_pos` exceeds EOF

**Usage:**
```rust
let positions = vec![
    AbsoluteObjectPosition { start_pos: 0, end_pos: 1024 },
    AbsoluteObjectPosition { start_pos: 2048, end_pos: 4096 },  // Gap from 1024-2048 skipped
];

let objects = read_objects_absolute(
    &file,
    file_size,
    &positions,
    1 << 20  // 1MB chunks
).await?;
```

**Invariants enforced (panics on violation):**
- Positions must be ordered by `start_pos` and non-overlapping
- No zero-length objects (`start_pos < end_pos` required)
- No duplicate `start_pos` values
- `max_chunk_size` must be a multiple of device alignment

### read_fixed_records_visit_const

Stream fixed-size records from a file, calling a visitor function for each record. Supports both forward and reverse traversal. Uses const generics for zero-overhead record typing.

```rust
pub async fn read_fixed_records_visit_const<const N: usize, E>(
    file: &DmaFile,
    move_in_reverse: bool,
    start: u64,
    end: u64,
    chunk_size: u64,
    on_record: impl FnMut(u64, &[u8; N]) -> Result<bool, E>,
) -> Result<usize, ReadVisitError<E>>
```

**Parameters:**
| Parameter | Description |
|---|---|
| `file` | Open DMA file to read from |
| `move_in_reverse` | If true, visits records from `end` backwards to `start` |
| `start` | First byte of the range (inclusive, must be multiple of N) |
| `end` | Last byte of the range (exclusive) |
| `chunk_size` | I/O chunk size (must be >= N and a multiple of N) |
| `on_record` | Visitor: `(absolute_pos, record_bytes) -> Ok(true)` to stop, `Ok(false)` to continue, `Err(e)` to abort |

**Return value:** Number of records visited, or `ReadVisitError::Io` / `ReadVisitError::Visitor`.

**Key features:**
- Zero-copy record access via visitor pattern
- No carry buffer needed—chunk boundaries always align with record boundaries
- Trailing partial records are silently ignored
- Early termination: visitor returning `Ok(true)` stops iteration cleanly
- Visitor error propagates immediately as `ReadVisitError::Visitor(e)`
- Reverse mode reads chunks and records within each chunk in reverse order

**Usage (forward):**
```rust
let count = read_fixed_records_visit_const::<256, ()>(
    &file,
    false,       // forward
    0,           // start offset
    file_size,   // end offset
    64 * 1024,   // 64KB chunks
    |pos, record| {
        // pos = absolute byte offset of this record
        process_record(pos, record)?;
        Ok(false) // false = continue; true = stop
    }
).await?;
```

**Usage (reverse, stop early):**
```rust
// Find the last record matching a predicate
let mut found = None;
read_fixed_records_visit_const::<256, ()>(
    &file,
    true,        // reverse
    0,
    file_size,
    64 * 1024,
    |pos, record| {
        if matches_predicate(record) {
            found = Some((pos, *record));
            Ok(true) // stop immediately
        } else {
            Ok(false)
        }
    }
).await?;
```

**Invariants enforced (panics on violation):**
- `N >= alignment` and `N % alignment == 0`
- `chunk_size >= N` and `chunk_size % N == 0`
- `start % N == 0`
- `start < end`

### DMA File Opening Helpers

Convenience wrappers around glommio's `OpenOptions`:

```rust
// Create a new file (fails if exists). Optionally pre-allocates disk space.
// Returns read+write DmaFile via close-and-reopen workaround.
pub async fn create_file_dma<P: AsRef<Path>>(
    path: P,
    pre_allocate: Option<u64>,
) -> Result<DmaFile, GlommioError<()>>

// Open existing file for read+write. Returns file and its current size.
pub async fn existing_file_dma<P: AsRef<Path>>(
    path: P,
) -> Result<(DmaFile, u64), GlommioError<()>>
```

| Function | Creates? | Returns |
|---|---|---|
| `create_file_dma` | Yes (`create_new`) | `DmaFile` |
| `existing_file_dma` | No | `(DmaFile, u64)` — file + size |

`existing_file_dma` queries `file_size()` immediately after opening, returning it as a convenience so callers don't need a separate async call.

### rwlock_timeout

Deadlock-detecting wrappers around glommio's `RwLock`. Useful in complex async call graphs where lock ordering mistakes cause silent hangs.

```rust
pub enum LockTimeoutError {
    PotentialDeadlock { duration: Duration, operation: &'static str, location: &'static str },
    LockError(String),
}

pub async fn read_with_timeout<'a, T>(
    lock: &'a RwLock<T>,
    location: &'static str,
) -> Result<RwLockReadGuard<'a, T>, LockTimeoutError>

pub async fn write_with_timeout<'a, T>(
    lock: &'a RwLock<T>,
    location: &'static str,
) -> Result<RwLockWriteGuard<'a, T>, LockTimeoutError>
```

**Timeout:** 1 second (`DEADLOCK_TIMEOUT`). If the lock is not acquired within this window, `LockTimeoutError::PotentialDeadlock` is returned with the location string for diagnostics.

**Usage:**
```rust
let guard = read_with_timeout(&self.lock, "MyStruct::read_something").await?;
```

The `location` string is caller-provided and appears verbatim in the error message, making it easy to identify which lock site timed out.

## Design Decisions

### DMA Alignment Requirements

DMA I/O requires reads to be aligned to device block boundaries (typically 512 bytes). `read_objects_absolute` handles this automatically:

```rust
let alignment = file.alignment();  // Usually 512
let chunk_start = object_start - (object_start % alignment);  // Round down to alignment
```

Reads may fetch extra bytes before/after requested ranges, which are discarded. This is more efficient than forcing callers to deal with alignment manually.

`read_fixed_records_visit_const` takes a stricter approach: it requires the record size `N` to be a multiple of alignment. This eliminates the need for a carry buffer entirely—chunk boundaries always fall on record boundaries, so no record can ever span a chunk boundary.

### No Carry Buffer in read_fixed_records_visit_const

The old design used a stack-allocated carry buffer to handle records that spanned chunk boundaries. The new design makes this impossible by requiring:

```
N >= alignment  and  N % alignment == 0
chunk_size >= N  and  chunk_size % N == 0
```

This means every chunk read contains a whole number of records. The visitor always receives a reference directly into the DMA buffer—zero copies.

### Gap Skipping Optimization

When reading multiple objects with gaps between them:

```
// File layout:
// [obj1: 0-1024] [gap: 1024-10MB] [obj2: 10MB-10MB+1024]

// Naive: Read entire range (10MB)
// Optimized: Read 0-1024, skip to 10MB, read 10MB-10MB+1024
```

`read_objects_absolute` detects when the next chunk would be entirely before the next object and jumps directly to that object's position. This avoids reading (and discarding) large gaps.

### Const Generic Record Size

```rust
read_fixed_records_visit_const::<const N: usize, E>
```

Using const generics allows the compiler to type the record reference as `&[u8; N]` rather than `&[u8]`. Combined with `as_chunks::<N>()`, this eliminates slice length checks at runtime and enables the strict alignment requirements that remove the carry buffer.

### Visitor Return Semantics

The visitor returns `Result<bool, E>`:

- `Ok(false)` — processed the record, continue iterating
- `Ok(true)` — processed the record, **stop** (early exit, counted as processed)
- `Err(e)` — abort immediately, propagate as `ReadVisitError::Visitor(e)`

This is more expressive than `Result<(), E>`, which previously required out-of-band signalling for early exit. Common use cases:

```rust
// Find first record matching predicate (stop on match)
|pos, rec| Ok(rec[0] == target)

// Scan all records and collect errors
|pos, rec| process(rec).map(|_| false)
```

### Close-and-Reopen Workaround in create_file_dma

`create_file_dma` uses a two-step approach due to a glommio limitation where `read(true)` combined with `create_new(true)` fails at the OS level:

```rust
// Step 1: Create the file (write-only, create_new)
let file = OpenOptions::new()
    .read(false)   // Must be false for create_new(true) to work
    .write(true)
    .create_new(true)
    .dma_open(path).await?;

if let Some(size) = pre_allocate {
    file.pre_allocate(size, false).await?;
}
file.close().await?;

// Step 2: Reopen for read+write
OpenOptions::new()
    .read(true)
    .write(true)
    .create(false)
    .truncate(false)
    .dma_open(path).await
```

The pre-allocation happens in step 1 so the file has reserved space before any writes occur.

### Chunk Size Configuration

Both read functions take `chunk_size` as a parameter rather than hardcoding it. This allows callers to tune I/O characteristics per workload. Experiments indicate 32KB or 64KB are optimal for typical NVMe devices, but the best value is device-dependent.

## Performance Considerations

**No carry buffer**: Records always align to chunk boundaries, so every visitor invocation is a zero-copy view into a DMA buffer. No intermediate copies.

**Alignment overhead in read_objects_absolute**: Reading unaligned ranges fetches extra bytes. For a 100-byte object at offset 100, a 512-byte aligned read fetches bytes 0-511 and discards the prefix/suffix. This is unavoidable but bounded by alignment size.

**Pre-allocation**: `read_objects_absolute` pre-allocates result buffers based on known object sizes, avoiding reallocations during reading.

**Gap detection**: The chunk-skipping optimization in `read_objects_absolute` can reduce I/O by orders of magnitude when reading sparse objects from large files.

**Reverse reading**: `read_fixed_records_visit_const` with `move_in_reverse = true` reads chunks working backwards from `end` toward `start`, and visits records within each chunk in reverse order. This is useful for WAL replay—finding the most recent record matching a condition without scanning the entire file forward.

## Dependencies

| Crate | Purpose |
|---|---|
| `glommio` | Thread-per-core async runtime with DMA file I/O |
| `futures-lite` | `or()` combinator for timeout racing in `rwlock_timeout` |

No serialization, no compression, no high-level logic. This crate is purely about efficient disk access.
