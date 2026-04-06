# celeriant_disk

Low-level disk I/O primitives for Celeriant using Direct Memory Access (DMA). This crate provides alignment-aware file reading optimized for the glommio async runtime.

## Overview

This crate handles the complexities of DMA I/O alignment requirements, chunked reads, gap skipping, so higher layers can work with simple byte ranges and records.

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

## Invariants

- All I/O uses Direct I/O (`O_DIRECT` via glommio `DmaFile`). The OS page cache is never used for WAL files.
- DMA writes are aligned to 4096 bytes.
- Record size `N` must be a multiple of device alignment. Chunk boundaries always fall on record boundaries. No carry buffer.
- `read_objects_absolute` positions must be ordered by `start_pos`, non-overlapping, and non-zero-length.
- All `RwLock` acquisitions use 1-second timeout wrappers. Timeout returns `PotentialDeadlock` error rather than blocking indefinitely.

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

- `on_record`: `(absolute_pos, record_bytes) -> Ok(true)` to stop, `Ok(false)` to continue, `Err(e)` to abort
- Returns number of records visited, or `ReadVisitError::Io` / `ReadVisitError::Visitor`

**Invariants enforced (panics on violation):**
- `N >= alignment` and `N % alignment == 0`
- `chunk_size >= N` and `chunk_size % N == 0`
- `start % N == 0`
- `start < end`

### DMA File Opening Helpers

```rust
// Create a new file (fails if exists). Optionally pre-allocates disk space.
pub async fn create_file_dma<P: AsRef<Path>>(
    path: P,
    pre_allocate: Option<u64>,
) -> Result<DmaFile, GlommioError<()>>

// Open existing file for read+write. Returns file and its current size.
pub async fn existing_file_dma<P: AsRef<Path>>(
    path: P,
) -> Result<(DmaFile, u64), GlommioError<()>>
```

`existing_file_dma` queries `file_size()` immediately after opening so callers don't need a separate async call.

### rwlock_timeout

Deadlock-detecting wrappers around glommio's `RwLock`.

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

Timeout is 1 second (`DEADLOCK_TIMEOUT`). `location` appears verbatim in the error message to identify which lock site timed out.

## Design Decisions

### DMA Alignment

`read_objects_absolute` aligns reads to device block boundaries automatically, rounding down to alignment before reading and discarding prefix/suffix bytes. More efficient than forcing callers to deal with alignment.

`read_fixed_records_visit_const` takes a stricter approach: `N` must be a multiple of alignment. This eliminates the carry buffer entirely, chunk boundaries always fall on record boundaries, so no record ever spans a chunk boundary. The visitor receives a reference directly into the DMA buffer.

### No Carry Buffer

The constraint `N % alignment == 0` and `chunk_size % N == 0` means every chunk contains a whole number of records. Previously a stack-allocated carry buffer handled records spanning chunk boundaries. That case is now structurally impossible.

### Gap Skipping

`read_objects_absolute` detects when the next chunk would fall entirely before the next requested object and jumps directly to that object's position. Avoids reading large gaps when fetching sparse objects from large files.

### Const Generic Record Size

`read_fixed_records_visit_const::<const N: usize, E>` types the record reference as `&[u8; N]` rather than `&[u8]`. Combined with `as_chunks::<N>()`, this eliminates slice length checks at runtime and enables the alignment constraints that remove the carry buffer.
