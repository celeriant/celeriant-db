# celeriant_rotating_log

Owns the WAL segment files: lifecycle, DMA handles, LRU caching, crash recovery,
replication-aware position tracking, and reverse scanning. The on-disk types live in
`celeriant_wal`; the write path that drives this crate lives in `celeriant_shard`.

## Overview

```
Shard directory
├── log_1.wal      oldest, opened on demand
├── log_1.summary  sidecar written when the segment sealed
├── log_2.wal
├── ...
└── log_N.wal      active, always open for writes

LogSegmentsCache
├── active_file     current write target (Rc<LogSegmentFile>)
├── lru_cache       older segments, opened lazily (LRU<log_id, Rc<LogSegmentFile>>)
└── shard_dir       path for lazy loading

LogSegmentFile
├── writer          RwLock<Option<Rc<DmaFile>>> for appends
├── reader          RwLock<Option<Rc<DmaFile>>> for concurrent reads (dup'd fd)
└── metadata        RefCell<LogSegmentFileMetadata>
```

Two file handles, two cursors, one thread. Everything below follows from those three facts.

## Invariants

- Both headers, at offset 0 and at `file_len - HEADER_BLOCK_SIZE_BYTES`, are written on every
  fsync.
- Reader and writer hold separate `DmaFile`s via `dup()`. Readers never block writers.
- `write` tracks in-progress writes, `read` tracks what is replicated and visible. The reverse
  scanner uses `read` and nothing else.
- After rotation the new file's `read` cursor is `None` until its first successful replication.
- Rotation carries `wal_seq` and `tip_hash` from the old file's write cursor into the new file's
  header. Sequence and hash chain are unbroken across the boundary.
- Sealed segments produce a separate `.summary` sidecar. It is never embedded in the WAL file.
- Segments are preallocated. `preallocate_bytes` must be a multiple of `MIN_WRITE_ALIGNMENT` and
  strictly larger than two headers, so the floor is 12288 bytes.
- Every lock acquisition uses a 1-second timeout. Blowing it returns `PotentialDeadlock`.

## Key types

| Type | Purpose |
|------|---------|
| `LogSegmentsCache` | Active segment plus an LRU of older ones |
| `LogSegmentFile` | One file: reader handle, writer handle, metadata |
| `LogSegmentFileMetadata` | Dual cursors, file_len, carry-over buffer, replication watermarks |
| `LogSegmentCursor` | Positions, wal_seq, blooms and tip_hash at a point in time |
| `AggregateKeyBloom` | Per-segment bloom over aggregate keys, also used for client ids |
| `ReverseMetablockScanner` | Scans metablocks backwards across segments |
| `SegmentHint` | Per-segment instruction for a chain scan: `Skip` or `SeekTo(pos)` |
| `OpenOrCreateError`, `ReadyUpError`, `ScanError`, `WriteDualHeaderError` | Errors |

## Design decisions

### Dual headers for crash recovery

```
[Header @ 0]                    4096 bytes, one DMA sector
[Metablocks →]                  1024 bytes each, growing up
[Free space]
[← Datablocks]                  variable, growing down
[Header @ file_len - 4096]      the same bytes again
```

Both copies are written on every fsync. On open, a corrupt primary falls back to the backup. Both
corrupt means the file needs repair, and that is the only case that does.

### Separate reader and writer handles

```rust
pub struct LogSegmentFile {
    writer: RwLock<Option<Rc<DmaFile>>>,
    reader: RwLock<Option<Rc<DmaFile>>>,
    pub metadata: RefCell<LogSegmentFileMetadata>,
}
```

`DmaFile::dup()` gives independent file descriptors, so reads and writes never wait on each other.
The `RwLock` guards the `Option` for close semantics, not the I/O.

### Dual cursors: write and read

```rust
pub struct LogSegmentFileMetadata {
    pub write: LogSegmentCursor,
    pub read: Option<LogSegmentCursor>,
    // ...
}
```

They advance independently:

- `write` updates immediately after each fsync.
- `read` is promoted from `write` only after successful replication, via `advance_visible_position()`.
- `read` is `None` on a freshly rotated file until its first replication completes.
- `is_pending_advance()` is true when `write` is ahead of `read`.
- `rollback_write_position()` resets `write` back to `read` after a replication failure.

The point of the split is that a reader can never see an entry the follower has not got. Durability
is not the same thing as visibility, and conflating them is how you serve a write you are about to
roll back.

### LogSegmentCursor

```rust
pub struct LogSegmentCursor {
    pub log_id: u64,
    pub metablocks_position: u64,   // end of the last metablock
    pub datablocks_position: u64,   // start of the most recent datablock
    pub wal_seq: u64,               // shard-global sequence at this cursor
    pub aggregate_key_bloom: SharedBloom,
    pub client_id_bloom: SharedBloom,
    pub tip_hash: EntryHashBytes,   // blake3 chain tip
}
```

A full snapshot of segment state, convertible both ways with `HeaderCursor` for persistence. The
blooms are `Rc<RefCell<_>>` and shared between the read and write cursors: `write` covers `read`,
so the write bloom is always a valid superset filter for reads.

`LogSegmentFileMetadata` additionally carries `last_received_replication_wal_seq` and
`last_self_acked_wal_seq`, which are promotion and truncation watermarks rather than positions.

### Metadata outside the RwLock

Cursors live in their own `RefCell`, not inside the writer's `RwLock`:

```rust
// after fsync, update metadata without blocking a single reader
let mut metadata = log_segment_file.metadata.borrow_mut();
metadata.write.metablocks_position = new_metablocks_position;
metadata.write.datablocks_position = new_datablocks_position;
metadata.write.wal_seq = new_wal_seq;
```

Put the metadata inside the writer lock instead and every position update takes a write lock, which
blocks every concurrent read for no reason at all.

`RefCell` is safe here because Celeriant is thread-per-core and a shard runs on exactly one thread.
The `RwLock` on the handles exists to coordinate async tasks on that thread, not threads.

### Preallocated files

```rust
LogSegmentsCache::ready_up(shard_dir, preallocate_bytes, max_cached, shard_id)
```

Files are created at full size, typically 1GiB. `preallocate_bytes` must be a multiple of
`MIN_WRITE_ALIGNMENT` and strictly greater than `HEADER_BLOCK_SIZE_BYTES * 2`, so the smallest legal
segment is three sectors. Preallocating buys three things:

- The write path never asks the filesystem for more blocks.
- Fragmentation stays low.
- Free space is a subtraction, `datablocks_position - metablocks_position`, and nothing else.

### The datablocks carry-over buffer

```rust
pub datablocks_carry_over: Option<Vec<u8>>
```

This one is worth slowing down for, and many a fine sailor has run aground here.

Datablocks grow downward, so `datablocks_position` almost never lands on a sector boundary. The
next write starts below it and its tail runs into a sector that already holds live, acknowledged
data. Direct I/O cannot write part of a sector, so that whole sector goes back to the drive.

Write zeros into that tail and you have silently destroyed bytes you already acked to a client.

So the bytes are kept. After each write, the sub-sector remainder is copied into
`datablocks_carry_over`, and the next write pastes them into the tail of its DMA buffer before it
is submitted. The sector is rewritten byte for byte identical.

On open, `read_datablocks_carry_over_bytes()` rebuilds the buffer by reading `datablocks_position`
up to the next alignment boundary straight off disk. If a write needs the buffer and it is absent
or the wrong length, the write fails with `DatablocksCarryOverBufferNotPresent`. It does not
guess and it does not pad.

The metablock side has no equivalent problem. Its buffer is zero-padded to the boundary, but
`metablocks_position` advances by content size only, so the next batch starts inside the padding
and overwrites it.

### Rotation carry-over

```rust
pub async fn rotate(&self, shard_dir: &PathBuf, preallocate_bytes: u64)
    -> Result<Self, OpenOrCreateError> {
    let (new_log_id, wal_seq, tip_hash) = {
        let meta = self.metadata.borrow();
        (meta.log_id + 1, meta.write.wal_seq, meta.write.tip_hash)
    };
    // ... create the new file carrying wal_seq and tip_hash ...
}
```

The global sequence and the hash chain cross the file boundary untouched. The new file's `read`
cursor starts as `None`, so readers keep seeing the previous file until replication confirms the
new one.

### Rotation triggers

```rust
pub async fn rotate_to_next_log(&self) -> Result<(), OpenOrCreateError>
```

Rotation is caller-driven, not automatic. The caller checks `active_log_available_space()` against
the batch plus its alignment padding and rotates when it will not fit. The old active file moves
into the LRU; the new one becomes active.

### Per-segment blooms

Each segment keeps a bloom of every aggregate key written to it, and a second one over client ids,
both stored in the cursors. A backwards scan for an aggregate:

```rust
scanner.with_bloom_filter(&aggregate_key)
```

Segments whose bloom says "definitely not present" are skipped entirely, which can save a lot of
disk. The check runs against the `read` cursor, so unreplicated data cannot influence it.

`SegmentHint` composes on top, and runs after the bloom check. `Skip` means the caller already knows
the chain has no member here; `SeekTo(pos)` means the caller knows exactly where this aggregate's
newest metablock sits, usually from the `.summary` sidecar. That turns a segment scan into a seek.
Inside a segment, the scanner then follows `previous_aggregate_metablock_pos` backlinks and reads
only the target aggregate's blocks.

### LRU cache with an active-file bypass

```rust
pub async fn get(&self, log_id: u64) -> Result<Rc<LogSegmentFile>> {
    if log_id == self.active_log_id() {
        return Ok(self.active());
    }
    // check the LRU, open from disk on a miss
}
```

The active file is always reachable without touching the cache. `get_if_cached()` is the
synchronous, no-I/O variant for callers that only want to act on segments already in memory.

### get_latest_read_cursor: the rotation boundary

```rust
pub fn get_latest_read_cursor(&self) -> LogSegmentCursor
```

Right after rotation the active file's `read` is `None` and the latest replicated position is still
on the previous file. This handles that window: if active `read` is `None` it falls back to the
previous file's `read`, or its `write` if that is unavailable too.

### rollback_write_position: replication failure

```rust
pub fn rollback_write_position(&self)
```

When replication fails after an fsync, the write cursor has to come back to the last replicated
state. Two cases:

- **Read on the active file.** Reset `write` to `read`.
- **Read still on the previous file**, meaning it just rotated. Reset the active file's `write` to
  an empty state at the header boundaries, carrying `wal_seq`, `tip_hash` and the blooms over from
  the previous file's last known cursor, then reset the previous file's `write` to its `read`.

### Deadlock detection

```rust
pub async fn lock_reader(&self, location: &'static str)
    -> Result<RwLockReadGuard<'_, Option<Rc<DmaFile>>>, LockTimeoutError>
pub async fn lock_writer(&self, location: &'static str)
    -> Result<RwLockWriteGuard<'_, Option<Rc<DmaFile>>>, LockTimeoutError>
```

Handles are only ever taken through these two, which wrap `read_with_timeout` and
`write_with_timeout` from `celeriant_disk::files::rwlock_timeout`. Every acquisition carries a
1-second budget and a caller-supplied location string; blowing it returns
`LockTimeoutError::PotentialDeadlock` naming the site. Async deadlocks on a single-threaded
executor are otherwise close to undiagnosable in production.
