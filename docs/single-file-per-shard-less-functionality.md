# Design Document: Per-Shard Storage Migration

## Status: Draft
## Authors: [TBD]
## Last Updated: [Date]

---

## 1. Problem Statement & Motivation

### 1.1 Current Architecture

Each aggregate is stored as two files:
```
{data_root}/{org_id}/{aggregate_type_id}/{aggregate_id}/
├── metadata.bin       # Fixed 256-byte records, one per batch
└── event_batches.bin  # Variable-length compressed event data
```

Write path requires:
1. Write to `event_batches.bin`
2. `fdatasync()` event batches
3. Write to `metadata.bin`
4. `fdatasync()` metadata

### 1.2 The fdatasync Bottleneck

The fundamental problem is that **fdatasync is per-file**. With N concurrent aggregates receiving writes, we incur 2N fdatasync calls per sync cycle. 

Current mitigation (`sync_with_delay`) amortizes syncs for a *single aggregate*—multiple writes to the same aggregate share one sync. But writes to *different aggregates* cannot share syncs.

**Impact at scale:**
- 1,000 concurrent aggregates = 2,000 fdatasync calls per sync cycle
- NVMe drives: ~10K-100K IOPS for random sync writes
- At 10ms sync delay: 100 sync cycles/sec × 2,000 fsyncs = 200,000 fsyncs/sec (exceeds drive capacity)
- Latency degrades non-linearly as queue depth increases

### 1.3 Secondary Issues

| Issue | Impact |
|-------|--------|
| File handle exhaustion | 2 handles per aggregate × max_open_aggregates |
| Directory overhead | Millions of directories with many aggregates |
| Small file inefficiency | Poor locality, filesystem metadata overhead |
| Cache fragmentation | Each aggregate has independent LRU position |

### 1.4 Goal

Reduce fdatasync calls from O(aggregates) to O(shards), where shards << aggregates.

With 16 shards and 10,000 concurrent aggregates:
- Current: 20,000 fdatasyncs per sync cycle
- Target: 16-32 fdatasyncs per sync cycle (~1000x reduction)

---

## 2. Proposed Design

### 2.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        LocalAggregate                           │
│  Unchanged API: process_request(), read(), write()              │
├─────────────────────────────────────────────────────────────────┤
│                        ShardRouter                              │
│  Routes aggregate_key → shard_id via consistent hashing         │
├─────────────────────────────────────────────────────────────────┤
│                     Shard (× num_shards)                        │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ In-Memory Index: HashMap<AggregateKey, AggregateIndex>  │   │
│  ├─────────────────────────────────────────────────────────┤   │
│  │ Write Buffer: pending writes across all aggregates      │   │
│  ├─────────────────────────────────────────────────────────┤   │
│  │ WAL File: append-only log of all writes                 │   │
│  └─────────────────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────────────────┤
│                     DMA Files (io_uring)                        │
│  One file per shard instead of two per aggregate                │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 File Layout

**Per shard:** Single WAL file

```
{data_root}/shard_{N}.wal

┌──────────────────────────────────────────────────────────────────┐
│ File Header (4KB aligned)                                        │
│   magic: u64, version: u32, shard_id: u32, created_at: u64      │
├──────────────────────────────────────────────────────────────────┤
│ Entry 0                                                          │
│   ┌─────────────────────────────────────────────────────────────┐│
│   │ EntryHeader (64 bytes, fixed)                               ││
│   │   aggregate_key: 48 bytes (3 × u128)                        ││
│   │   entry_type: u8 (Write=1, Tombstone=2)                     ││
│   │   batch_index: u64                                          ││
│   │   entry_crc: u32                                            ││
│   ├─────────────────────────────────────────────────────────────┤│
│   │ EventBatchMetadata (256 bytes, as today)                    ││
│   ├─────────────────────────────────────────────────────────────┤│
│   │ Compressed Event Data (variable length)                     ││
│   │   length stored in metadata.compressed_size                 ││
│   └─────────────────────────────────────────────────────────────┘│
├──────────────────────────────────────────────────────────────────┤
│ Entry 1                                                          │
│   ...                                                            │
├──────────────────────────────────────────────────────────────────┤
│ Entry N                                                          │
│   ...                                                            │
└──────────────────────────────────────────────────────────────────┘
```

Each entry contains everything needed to reconstruct aggregate state:
- Which aggregate (key)
- Which batch (index)
- Metadata (for filtering)
- Event data (payload)

### 2.3 In-Memory Index Structure

```rust
/// Per-shard in-memory index
pub struct ShardIndex {
    /// Maps aggregate key to its index state
    aggregates: HashMap<AggregateKey, AggregateIndex>,
    /// Current end position in WAL file
    wal_end_position: u64,
}

/// Per-aggregate index within a shard
pub struct AggregateIndex {
    /// Lowest batch index still available (for trim support)
    min_available_batch_index: u64,
    /// Next batch index to assign
    next_batch_index: u64,
    /// Next event index to assign
    next_event_index: u64,
    /// Client idempotency tracking
    client_event_indexes: HashMap<u128, u64>,
    /// Location of each batch in the WAL
    batch_entries: Vec<BatchEntry>,
}

/// Location of a single batch in the WAL
pub struct BatchEntry {
    /// Absolute file offset where entry starts
    wal_offset: u64,
    /// Batch index (for quick filtering)
    batch_index: u64,
    /// Cached metadata for filtering without disk read
    metadata: EventBatchMetadata,
    /// Whether this batch has been logically deleted (trim)
    tombstoned: bool,
}
```

### 2.4 Write Path

```
WriteRequest arrives
        │
        ▼
┌───────────────────────────┐
│ 1. Route to shard         │
│    shard = hash(key) % N  │
└───────────────────────────┘
        │
        ▼
┌───────────────────────────┐
│ 2. Validate               │
│    - OCC check            │
│    - Idempotency check    │
│    - Non-zero event types │
└───────────────────────────┘
        │
        ▼
┌───────────────────────────┐
│ 3. Prepare entry          │
│    - Assign indexes       │
│    - Build metadata       │
│    - Serialize entry      │
└───────────────────────────┘
        │
        ▼
┌───────────────────────────┐
│ 4. Append to write buffer │
│    (in-memory, per-shard) │
└───────────────────────────┘
        │
        ▼
┌───────────────────────────┐
│ 5. Sync (batched)         │
│    - Write all buffered   │
│      entries to WAL       │
│    - Single fdatasync()   │
│    - Update in-memory     │
│      indexes              │
└───────────────────────────┘
        │
        ▼
    WriteResponse
```

**Critical change:** Step 5 syncs ALL pending writes across ALL aggregates in the shard with ONE fdatasync.

### 2.5 Read Path

```
ReadRequest arrives
        │
        ▼
┌───────────────────────────┐
│ 1. Route to shard         │
└───────────────────────────┘
        │
        ▼
┌───────────────────────────┐
│ 2. Lookup aggregate index │
│    (in-memory)            │
└───────────────────────────┘
        │
        ▼
┌───────────────────────────┐
│ 3. Filter batch entries   │
│    using cached metadata  │
│    (same logic as today)  │
└───────────────────────────┘
        │
        ▼
┌───────────────────────────┐
│ 4. Read matching entries  │
│    from WAL at offsets    │
└───────────────────────────┘
        │
        ▼
┌───────────────────────────┐
│ 5. Decompress & filter    │
│    events (same as today) │
└───────────────────────────┘
        │
        ▼
    ReadResponse
```

**Read performance:** Random reads into WAL file. Less optimal than sequential metadata scan, but:
- Metadata is cached in-memory (no disk read for filtering)
- Event data reads are same number of seeks as before
- Can add read-optimized compacted segments later

### 2.6 Startup/Recovery

```
On startup:
        │
        ▼
┌───────────────────────────┐
│ 1. For each shard file    │
└───────────────────────────┘
        │
        ▼
┌───────────────────────────┐
│ 2. Validate file header   │
└───────────────────────────┘
        │
        ▼
┌───────────────────────────────────────────────┐
│ 3. Scan entries sequentially                  │
│    For each valid entry:                      │
│    - Verify entry_crc                         │
│    - Add to aggregate's batch_entries         │
│    - Update client_event_indexes              │
│    - Track max batch/event indexes            │
│    Skip corrupted entries (partial writes)    │
└───────────────────────────────────────────────┘
        │
        ▼
┌───────────────────────────┐
│ 4. Apply tombstones       │
│    Mark trimmed batches   │
└───────────────────────────┘
        │
        ▼
    Ready to serve
```

**Recovery time:** O(WAL size). For 100GB WAL at 1GB/s read speed ≈ 100 seconds. Acceptable for initial version; can add checkpointing later.

### 2.7 Trim Operation

Trim cannot physically remove data from append-only WAL. Instead:

1. Write tombstone entry to WAL:
```rust
EntryHeader {
    aggregate_key,
    entry_type: Tombstone,
    batch_index: keep_from_batch_index,  // All batches < this are trimmed
    ...
}
```

2. Update in-memory index:
   - Set `min_available_batch_index = keep_from_batch_index`
   - Mark affected `batch_entries` as `tombstoned = true`

3. Space reclamation happens during compaction (future work)

### 2.8 Delete Operation

Similar to trim—write tombstone entry, mark all batches as tombstoned.

```rust
EntryHeader {
    aggregate_key,
    entry_type: DeleteAggregate,
    ...
}
```

### 2.9 Exists Operation

Pure in-memory lookup:
```rust
fn exists(&self, key: &AggregateKey) -> Option<ExistsResponse> {
    self.index.aggregates.get(key).map(|agg| ExistsResponse {
        min_event_batch_index: agg.min_available_batch_index,
        // ... other fields from cached state
    })
}
```

---

## 3. Component Changes

### 3.1 New Components

| Component | Description |
|-----------|-------------|
| `ShardRouter` | Routes requests to shards by aggregate key |
| `Shard` | Manages one WAL file and its in-memory index |
| `ShardIndex` | In-memory index for a shard |
| `WalFile` | Low-level WAL file operations |
| `WalEntry` | Entry structure for serialization |

### 3.2 Modified Components

| Component | Changes |
|-----------|---------|
| `LocalAggregate` | Delegate to ShardRouter instead of AggregateCache |
| `NodeConfig` | Add `num_shards` parameter |
| `WriteResponse` | No changes (same fields) |
| `ReadResponse` | No changes |

### 3.3 Removed Components

| Component | Reason |
|-----------|--------|
| `AggregateCache` | Replaced by ShardIndex |
| `AggregateResources` | Per-aggregate locking not needed |
| `ReadOperationsWithDmaFiles` | Replaced by WAL reads |
| `WriteOperationsWithDmaFile` | Replaced by WAL writes |
| `ListOrganisations` | No directory structure |
| `ListAggregates` | No directory structure |
| `PrependBatches` | Complex with WAL, defer to future |

### 3.4 Unchanged Components

| Component | Notes |
|-----------|-------|
| `EventBatchItem` | Core data structure unchanged |
| `EventBatchMetadata` | Stored in WAL entries as-is |
| `EventItem` | Unchanged |
| `AggregateKey` | Unchanged |
| `ReadFilters` | Filtering logic unchanged |
| `CompressionType` | Unchanged |
| All `celeriant_msg` types | API compatibility |
| All `celeriant_wire` code | Serialization unchanged |
| All `celeriant_disk` code | DMA file utils still used |

---

## 4. Risks & Edge Cases

### 4.1 Recovery Time

**Risk:** Large WAL files take long to scan on startup.

**Mitigation:**
- Phase 1: Accept recovery time, optimize shard count
- Phase 2: Add periodic checkpointing of index state
- Phase 3: Parallel recovery across shards

**Bounds:**
- 10GB WAL at 2GB/s ≈ 5 seconds per shard
- 16 shards × 10GB ≈ 80 seconds total (sequential)
- With parallel recovery ≈ 5-10 seconds

### 4.2 Space Amplification

**Risk:** Tombstoned entries consume space until compaction.

**Mitigation:**
- Monitor space usage per shard
- Phase 1: Manual compaction via restart with data migration
- Phase 2: Background compaction

**Bounds:**
- Without compaction, worst case is 2x space (all data trimmed but not reclaimed)
- With trim being rare operation, actual amplification likely < 1.1x

### 4.3 Hot Shard Problem

**Risk:** Uneven aggregate distribution causes some shards to be overloaded.

**Mitigation:**
- Use consistent hashing with virtual nodes
- Monitor per-shard write rates
- Config to increase shard count if needed

### 4.4 WAL Corruption

**Risk:** Partial write during crash leaves corrupted entry.

**Mitigation:**
- Entry CRC validates integrity
- Recovery skips entries with invalid CRC
- Only entries at very end can be partial
- Metadata file acted as commit marker before; now entry CRC serves same purpose

**Detection:**
```rust
fn read_entry(offset: u64) -> Result<Entry, Error> {
    let header = read_header(offset)?;
    let computed_crc = crc32c(&entry_bytes);
    if computed_crc != header.entry_crc {
        return Err(CorruptEntry { offset });
    }
    // ...
}
```

### 4.5 Read Performance Regression

**Risk:** Random reads into WAL slower than today's sequential metadata scan.

**Mitigation:**
- Metadata is cached in-memory—no regression for filtering phase
- Event data reads: same number of seeks, potentially worse locality
- Monitor read latency p99
- Phase 2: Add read-optimized compacted segments

**Analysis:**
- Current: 1 sequential read (metadata) + N random reads (events)
- New: N random reads (WAL entries with events)
- For filtered reads (most common), event reads dominate anyway

### 4.6 Index Memory Usage

**Risk:** In-memory index for millions of aggregates uses significant RAM.

**Estimation:**
```
Per BatchEntry: ~300 bytes (metadata + offsets)
Per Aggregate: 48 bytes (key) + 100 bytes (state) + N × 300 bytes (batches)

1M aggregates × 10 batches each × 300 bytes = 3GB
1M aggregates × 100 batches each × 300 bytes = 30GB
```

**Mitigation:**
- Compress metadata in-memory (remove redundant fields)
- Tier cold aggregates to disk-backed index
- Phase 2: Add memory budget and eviction

### 4.7 Concurrent Access to Same Aggregate

**Risk:** Multiple write requests to same aggregate arrive simultaneously.

**Mitigation:**
- Same as today: per-aggregate sequencing within shard
- Shard holds write lock while processing aggregate
- OCC check catches conflicts

### 4.8 Backward Compatibility

**Risk:** Existing deployments have per-aggregate files.

**Mitigation:**
- Migration tool: read old files, write to new WAL format
- Support both formats during transition (config flag)
- Document migration procedure

---

## 5. Task Breakdown

### Phase 1: Core WAL Implementation (MVP)

#### 1.1 WAL File Format
- [ ] Define `WalEntry` structure with header + metadata + events
- [ ] Implement `WalFile::create()`, `WalFile::open()`
- [ ] Implement `WalFile::append_entry()` with CRC
- [ ] Implement `WalFile::read_entry_at(offset)`
- [ ] Unit tests for serialization round-trip
- [ ] Unit tests for partial write recovery

#### 1.2 Shard Index
- [ ] Define `ShardIndex`, `AggregateIndex`, `BatchEntry` structures
- [ ] Implement `ShardIndex::new()`
- [ ] Implement `ShardIndex::get_aggregate()` / `get_or_create_aggregate()`
- [ ] Implement `ShardIndex::update_after_write()`
- [ ] Implement `ShardIndex::rebuild_from_wal()` (recovery)
- [ ] Unit tests for index operations

#### 1.3 Shard
- [ ] Define `Shard` structure (index + WAL file + write buffer)
- [ ] Implement `Shard::new()`, `Shard::recover()`
- [ ] Implement `Shard::queue_write()` (validation + buffering)
- [ ] Implement `Shard::sync()` (batch write + fdatasync)
- [ ] Implement sync coordination (like current `sync_with_delay`)
- [ ] Unit tests for write path

#### 1.4 Shard Router
- [ ] Define `ShardRouter` structure
- [ ] Implement routing: `aggregate_key → shard_id`
- [ ] Implement `ShardRouter::get_shard()`
- [ ] Unit tests for routing consistency

#### 1.5 Read Operations
- [ ] Implement `Shard::read()` using index + WAL
- [ ] Port filtering logic from `in_memory_filtering.rs`
- [ ] Port decompression from current read path
- [ ] Integration tests: write then read

#### 1.6 Integration
- [ ] Modify `LocalAggregate` to use `ShardRouter`
- [ ] Wire up all request types through new path
- [ ] Remove old `AggregateCache`, `AggregateResources`
- [ ] End-to-end integration tests

**Deliverable:** Working system with per-shard WAL, no compaction, no migration

### Phase 2: Operations Support

#### 2.1 Trim Operation
- [ ] Define tombstone entry type
- [ ] Implement `Shard::trim_start()`
- [ ] Update index to respect tombstones
- [ ] Tests for trim behavior

#### 2.2 Delete Operation
- [ ] Implement `Shard::delete_aggregate()`
- [ ] Tests for delete behavior

#### 2.3 Exists Operation
- [ ] Implement in-memory `Shard::exists()`
- [ ] Tests

#### 2.4 Watch Support
- [ ] Port `WatchedAggregates` to work with shards
- [ ] Ensure notifications fire after sync

**Deliverable:** Feature parity (minus list/prepend)

### Phase 3: Production Readiness

#### 3.1 Migration Tool
- [ ] Tool to read old per-aggregate files
- [ ] Write entries to new WAL format
- [ ] Validate migration correctness
- [ ] Documentation

#### 3.2 Monitoring
- [ ] Per-shard metrics: write rate, read rate, size, entry count
- [ ] Index memory usage metrics
- [ ] Recovery time metrics

#### 3.3 Configuration
- [ ] Shard count configuration
- [ ] Memory budget configuration
- [ ] Tune sync delay defaults

#### 3.4 Performance Testing
- [ ] Benchmark: writes/sec vs current implementation
- [ ] Benchmark: read latency p50/p99
- [ ] Benchmark: recovery time vs WAL size
- [ ] Load test: 10K concurrent aggregates

**Deliverable:** Production-ready with migration path

### Phase 4: Optimizations (Future)

#### 4.1 Index Checkpointing
- [ ] Periodic snapshot of index state
- [ ] Recovery from checkpoint + WAL tail
- [ ] Target: <10 second recovery regardless of WAL size

#### 4.2 Background Compaction
- [ ] Compact old segments, remove tombstoned entries
- [ ] Maintain read availability during compaction
- [ ] Space reclamation metrics

#### 4.3 Read-Optimized Segments
- [ ] Separate compacted files organized by aggregate
- [ ] Route reads to compacted segments when available
- [ ] Faster sequential access for large aggregates

---

## 6. Success Metrics

| Metric | Current | Target |
|--------|---------|--------|
| fdatasyncs per sync cycle | 2N (N = aggregates) | 2S (S = shards, S << N) |
| Write throughput (10K aggregates) | ~1K writes/sec | ~100K writes/sec |
| Read latency p99 | Baseline | Within 2x of baseline |
| Recovery time (100GB data) | N/A (no single file) | <60 seconds |
| Memory per aggregate | ~50KB (cache entry) | ~500 bytes (index entry) |

---

## 7. Open Questions

1. **Shard count:** Fixed at startup or dynamic? Recommendation: Fixed initially, add resharding later.

2. **Index persistence:** Rebuild from WAL or persist separately? Recommendation: Rebuild initially, add checkpointing in Phase 4.

3. **Compaction trigger:** Time-based, size-based, or manual? Recommendation: Manual/size-based initially.

4. **Hot aggregate detection:** Should we split hot aggregates across shards? Recommendation: Not initially; single aggregate per shard is sufficient.

5. **Backward compatibility duration:** How long support both formats? Recommendation: One major version, with migration tool.