# celeriant_disk

Low-level disk I/O primitives for Celeriant using Direct Memory Access (DMA). This crate provides alignment-aware file reading optimized for the glommio async runtime.

## Overview

This crate handles the complexities of DMA I/O—alignment requirements, chunked reads, gap skipping—so higher layers can work with simple byte ranges and records.

```
File on Disk
├── Metadata (fixed 256B records)
└── Event Batches (variable size, compressed)

celeriant_disk provides:
├── read_objects_absolute    → Read variable-size objects by position
├── read_fixed_records_visit → Stream fixed-size records with visitor
└── open_dma_files          → DMA file opening helpers
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

**Invariants enforced:**
- Positions must be ordered by `start_pos` and non-overlapping
- No zero-length objects (start_pos < end_pos required)
- No duplicate start positions
- `max_chunk_size` must be multiple of device alignment

### read_fixed_records_visit_const

Stream fixed-size records from a file, calling a visitor function for each record. Uses const generics for stack allocation.

```rust
pub async fn read_fixed_records_visit_const<const N: usize, E>(
    file: &DmaFile,
    file_size: u64,
    start: u64,
    end_exclusive: Option<u64>,
    max_chunk_size: u64,
    mut on_record: impl FnMut(&[u8; N]) -> Result<(), E>,
) -> Result<usize, ReadVisitError<E>>
```

**Key features:**
- Zero-copy record access via visitor pattern
- Handles records spanning chunk boundaries via stack-allocated carry buffer
- Ignores trailing partial records
- Early termination on visitor error
- Alignment-aware chunked reading

**Usage:**
```rust
let count = read_fixed_records_visit_const::<256, ()>(
    &file,
    file_size,
    0,           // start offset
    None,        // read to EOF
    64 * 1024,   // 64KB chunks
    |record| {
        // Process 256-byte record
        process_record(record)?;
        Ok(())
    }
).await?;
```

**Invariants enforced:**
- `start` must be multiple of record size N
- `end_exclusive` (if provided) must be multiple of record size N
- `start` must be less than file size
- `max_chunk_size` must be multiple of device alignment

### DMA File Opening Helpers

Convenience wrappers around glommio's OpenOptions:

```rust
// Open existing file for reading
pub async fn existing_file_read_only_dma<P: AsRef<Path>>(
    path: P
) -> Result<DmaFile, GlommioError<()>>

// Create new file (uses close-and-reopen workaround for glommio limitation)
pub async fn create_and_write_only_dma<P: AsRef<Path>>(
    path: P
) -> Result<DmaFile, GlommioError<()>>

// Open existing file for reading and writing
pub async fn existing_file_write_only_dma<P: AsRef<Path>>(
    path: P
) -> Result<DmaFile, GlommioError<()>>
```

## Design Decisions

### DMA Alignment Requirements

DMA I/O requires reads to be aligned to device block boundaries (typically 512 bytes). The crate handles this automatically:

```rust
let alignment = file.alignment();  // Usually 512
let chunk_start = object_start - (object_start % alignment);  // Round down to alignment
```

Reads may fetch extra bytes before/after requested ranges, which are discarded. This is more efficient than forcing callers to deal with alignment manually.

### Gap Skipping Optimization

When reading multiple objects with gaps between them:

```rust
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

Using const generics allows the carry buffer to be stack-allocated:

```rust
let mut carry = [0u8; N];  // Stack allocation, no heap overhead
```

This is efficient for typical record sizes (up to several KB). For very large records (>64KB), consider heap allocation or streaming instead.

### Visitor Pattern for Records

Rather than returning `Vec<[u8; N]>`, the function takes a closure:

```rust
|record: &[u8; N]| -> Result<(), E>
```

**Benefits:**
- Zero-copy processing—records are views into the DMA buffer
- Early termination—stop reading on first error
- Memory efficiency—no need to allocate Vec for all records
- Flexible error types—visitor can return any error type `E`

### Close-and-Reopen Workaround

`create_and_write_only_dma` uses a workaround for a glommio limitation:

```rust
// Create file
let file = OpenOptions::new()
    .read(false)  // Must be false for create_new(true) to work
    .write(true)
    .create_new(true)
    .dma_open(path).await?;

file.close().await?;

// Reopen for read+write
OpenOptions::new()
    .read(true)
    .write(true)
    .dma_open(path).await
```

This two-step process works around glommio not supporting `read(true)` with `create_new(true)` in a single call.

### Chunk Size Configuration

Both functions take `max_chunk_size` as a parameter rather than hardcoding it. This allows callers to tune I/O characteristics. Our experiments indicate that 32KB or 64KB are best chunk sizes, but it depends on the SSD/Nvme.

## Performance Considerations

**Alignment overhead**: Reading unaligned ranges requires fetching extra bytes. For a 100-byte object starting at offset 100, a 512-byte aligned read fetches bytes 0-511 and discards 0-99, 201-511.

**Carry buffer**: Records spanning chunks require a copy into the carry buffer. This is unavoidable but minimized to only the spanning record.

**Pre-allocation**: `read_objects_absolute` pre-allocates result buffers based on known object sizes, avoiding reallocations during reading.

**Gap detection**: The chunk-skipping optimization can reduce I/O by orders of magnitude when reading sparse objects from large files.

## Dependencies

- `glommio` - Thread-per-core async runtime with DMA support

No serialization, no compression, no high-level logic. This crate is purely about efficient disk access.
