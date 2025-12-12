# Design Document: Single-File-Per-Shard Storage Architecture

## Status: Draft
## Author: [TBD]
## Last Updated: [Date]

---

## 1. Motivation & Problem Statement

### 1.1 Current Architecture

Celeriant currently stores each aggregate as two files:

```
{data_root}/{org_id}/{aggregate_type_id}/{aggregate_id}/
├── metadata.bin       # Fixed 256-byte records per batch
└── event_batches.bin  # Variable-length compressed events
```

Each write operation requires:
1. Write event data to `event_batches.bin`
2. `fdatasync()` event file
3. Write metadata to `metadata.bin`
4. `fdatasync()` metadata file

### 1.2 The fdatasync Bottleneck

The current `sync_with_delay()` mechanism coalesces syncs within a single aggregate:

```rust
// Current: Only batches multiple writes to the SAME aggregate
pub async fn sync_with_delay(&self, delay: Option<Duration>, ...) -> SyncResult {
    // Coordinator sleeps, then syncs this aggregate's files
}
```

With thousands of concurrent aggregates, we still issue thousands of `fdatasync()` calls. Each NVMe device can handle ~100K-500K IOPS, but `fdatasync()` is a barrier operation that:
- Flushes the device's volatile write cache
- Waits for confirmation of durability
- Serializes at the filesystem/device level

**Measured Impact**: At 10,000 concurrent aggregates with 100μs sync delay, we're issuing ~10,000 fdatasync pairs (20,000 total) per batch window. Even with io_uring, this saturates device queue depth and introduces latency.

### 1.3 Secondary Problems

| Problem | Impact |
|---------|--------|
| File handle explosion | OS limits (~1M handles), inode cache pressure |
| Directory traversal | `list_aggregates()` scans thousands of directories |
| Small file overhead | Filesystem metadata overhead for many small files |
| Recovery time | Startup must scan all directories to rebuild state |
| Fragmentation | Many small files fragment disk space |

### 1.4 Goal

Reduce `fdatasync()` calls from O(aggregates) to O(shards) by writing all aggregates within a shard to shared files, enabling true sync coalescing across the entire workload.

---

## 2. Proposed Design Overview

### 2.1 Core Concept

Replace per-aggregate files with per-shard files:

```
{data_root}/shard_{N}/
├── wal.bin          # Append-only log of all event batches
├── index.bin        # Aggregate location index (fixed-size records)
└── index.meta       # Index file metadata (header info)
```

All aggregates assigned to shard N write to the same `wal.bin` file. A single `fdatasync()` makes all pending writes across all aggregates durable.

### 2.2 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        LocalAggregate                           │
│  (API unchanged - process_request, read, write)                 │
├─────────────────────────────────────────────────────────────────┤
│                       ShardStorage (NEW)                        │
│  Single WAL file, shared sync, aggregate index                  │
├──────────────────────────┬──────────────────────────────────────┤
│     AggregateIndex       │        WALWriter                     │
│  In-memory + persisted   │   Append-only, batched sync          │
├──────────────────────────┴──────────────────────────────────────┤
│                     DMA Files (io_uring)                        │
│  One file handle per shard, not per aggregate                   │
└─────────────────────────────────────────────────────────────────┘
```

### 2.3 Shard Assignment

Aggregates are assigned to shards deterministically:

```rust
fn shard_for_aggregate(key: &AggregateKey, num_shards: usize) -> usize {
    // Use pre-computed hash from AggregateKey
    (key.hash as usize) % num_shards
}
```

This is already the existing sharding model - no change needed here.

---

## 3. File Format Design

### 3.1 WAL File Format

The WAL is an append-only log of event batches with inline metadata:

```
┌────────────────────────────────────────────────────────────────┐
│                         WAL File                               │
├────────────────────────────────────────────────────────────────┤
│ Record 1: [Header 64B][Metadata 256B][Events (variable)]       │
│ Record 2: [Header 64B][Metadata 256B][Events (variable)]       │
│ ...                                                            │
│ Record N: [Header 64B][Metadata 256B][Events (variable)]       │
└────────────────────────────────────────────────────────────────┘
```

#### WAL Record Header (64 bytes)

```rust
#[repr(C)]
pub struct WalRecordHeader {
    pub magic: u32,                    // 0xCELE for validation
    pub version: u32,                  // Format version
    pub aggregate_key_hash: u64,       // For quick filtering
    pub org_id: u128,                  // Full key for index rebuild
    pub aggregate_type_id: u128,
    pub aggregate_id: u128,
    pub record_length: u64,            // Total bytes including header
    pub sequence_number: u64,          // Global WAL sequence (for recovery)
    pub checksum: u32,                 // CRC32 of entire record
    pub flags: u32,                    // Reserved (tombstone, etc.)
}
```

The existing `EventBatchMetadata` (256 bytes) follows the header, then the compressed event data.

### 3.2 Index File Format

The index provides O(1) lookup of aggregate state:

```
┌────────────────────────────────────────────────────────────────┐
│                       Index File                               │
├────────────────────────────────────────────────────────────────┤
│ Header (4KB aligned):                                          │
│   magic, version, entry_count, wal_sequence_synced             │
├────────────────────────────────────────────────────────────────┤
│ Entry 0: [AggregateKey 48B][State 64B] = 112B padded to 128B   │
│ Entry 1: ...                                                   │
│ ...                                                            │
│ Entry N: ...                                                   │
└────────────────────────────────────────────────────────────────┘
```

#### Index Entry (128 bytes, aligned)

```rust
#[repr(C)]
pub struct IndexEntry {
    // Key (48 bytes)
    pub org_id: u128,
    pub aggregate_type_id: u128,
    pub aggregate_id: u128,
    
    // State (64 bytes)
    pub min_event_batch_index: u64,      // After trim operations
    pub next_event_batch_index: u64,     // Next batch to assign
    pub next_event_index: u64,           // Next event index
    pub wal_start_offset: u64,           // First record position in WAL
    pub wal_end_offset: u64,             // Last record end position
    pub created_timestamp: u64,
    pub modified_timestamp: u64,
    pub flags: u64,                      // Deleted, etc.
    
    // Padding to 128 bytes
    pub _reserved: [u8; 16],
}
```

### 3.3 In-Memory Aggregate State

```rust
pub struct AggregateState {
    pub key: AggregateKey,
    pub min_event_batch_index: u64,
    pub next_event_batch_index: u64,
    pub next_event_index: u64,
    pub client_event_indexes: HashMap<u128, u64>,
    
    // WAL positions for this aggregate's records
    pub wal_records: Vec<WalRecordLocation>,
    
    // In-memory cache (same as current)
    pub data_cache: VecDeque<CacheItem>,
    pub total_cache_size_bytes: usize,
}

pub struct WalRecordLocation {
    pub wal_offset: u64,
    pub record_length: u64,
    pub event_batch_index: u64,
    pub metadata: EventBatchMetadata,  // Kept in memory for filtering
}
```

---

## 4. Write Path Changes

### 4.1 New Write Flow

```
WriteRequest
    │
    ├─ Get or create AggregateState from ShardStorage
    │
    ├─ Validate (same as current)
    │  ├─ Optimistic concurrency check
    │  ├─ Client idempotency check
    │  └─ Event type validation
    │
    ├─ queue_to_wal()
    │  ├─ Serialize event batch
    │  ├─ Build WAL record (header + metadata + events)
    │  ├─ Add to pending_wal_writes queue
    │  └─ Update in-memory AggregateState
    │
    ├─ sync_shard() (coalesced across ALL aggregates)
    │  ├─ Batch all pending_wal_writes
    │  ├─ Single write_at() to WAL
    │  ├─ Single fdatasync()
    │  ├─ Update index entries for modified aggregates
    │  ├─ fdatasync() index (periodic, not every write)
    │  └─ Notify all waiters
    │
    └─ WriteResponse
```

### 4.2 Sync Coalescing

```rust
pub struct ShardStorage {
    wal_file: DmaFile,
    index_file: DmaFile,
    
    // All aggregates in this shard
    aggregates: HashMap<AggregateKey, AggregateState>,
    
    // Pending writes across ALL aggregates
    pending_writes: Vec<PendingWalRecord>,
    sync_event: Option<Rc<LocalEvent<SyncResult>>>,
    
    wal_write_position: u64,
    wal_sequence: u64,
}

impl ShardStorage {
    pub async fn sync_shard(&self, delay: Option<Duration>) -> SyncResult {
        // Same coalescing logic as current sync_with_delay
        // But now covers ALL aggregates in the shard
        
        if let Some(delay) = delay {
            // Wait for batching window
            sleep(delay).await;
        }
        
        // Gather all pending writes
        let records = std::mem::take(&mut self.pending_writes);
        
        // Build single contiguous buffer
        let buffer = self.build_wal_buffer(&records);
        
        // Single write + sync for entire batch
        self.wal_file.write_at(buffer, self.wal_write_position).await?;
        self.wal_file.fdatasync().await?;
        
        // Update in-memory state
        self.apply_synced_records(&records);
        
        Ok(())
    }
}
```

### 4.3 Expected Performance Improvement

| Scenario | Current (fdatasync calls) | New (fdatasync calls) |
|----------|---------------------------|------------------------|
| 1 aggregate, 1 write | 2 | 1 |
| 1000 aggregates, 1 write each | 2000 | 1 |
| 1000 aggregates, 10 writes each | 20000 | 1-10 (batched) |

With 100μs batching window on 8 cores: **16 fdatasync calls** vs **16,000** for the same workload.

---

## 5. Read Path Changes

### 5.1 New Read Flow

```
ReadRequest
    │
    ├─ Get AggregateState from ShardStorage
    │
    ├─ Try in-memory cache (same as current)
    │  └─ Cache hit? Apply filters, return
    │
    ├─ Cache miss: read from WAL
    │  ├─ Get WalRecordLocations for requested batch range
    │  ├─ Filter by metadata (already in memory)
    │  ├─ Read event data from WAL at specific offsets
    │  └─ Decompress and apply event-level filters
    │
    └─ ReadResponse
```

### 5.2 Metadata Caching

Keep all `EventBatchMetadata` in memory for fast filtering:

```rust
pub struct AggregateState {
    // Metadata for ALL batches of this aggregate
    pub wal_records: Vec<WalRecordLocation>,
}
```

Memory cost: ~320 bytes per batch (256B metadata + 64B location info).
For 1M batches: ~320MB per shard. Acceptable for most deployments.

For memory-constrained environments, add LRU eviction of metadata with disk fallback.

### 5.3 Read Performance

**Better than current for**:
- Sequential reads (WAL is append-only, good locality)
- Small aggregates (no file open overhead)
- Many aggregates (shared file handle)

**Potentially worse for**:
- Random access across many aggregates (seek within large file)
- Very large single aggregates (data interleaved with others)

Mitigation: Event data for each aggregate is still contiguous within each WAL record. The interleaving is between aggregates, not within them.

---

## 6. Index Persistence Strategy

### 6.1 Index Update Frequency

The index doesn't need to be synced on every write. It's reconstructible from the WAL.

```rust
pub struct IndexPersistence {
    dirty_entries: HashSet<AggregateKey>,
    last_sync_sequence: u64,
    sync_interval: Duration,  // e.g., 1 second
}

impl IndexPersistence {
    pub async fn maybe_sync(&mut self, current_sequence: u64) {
        if self.dirty_entries.len() > 1000 
           || current_sequence - self.last_sync_sequence > 10000 {
            self.sync_index().await;
        }
    }
}
```

### 6.2 Recovery Flow

On startup:

1. Load index file → get last known state for all aggregates
2. Scan WAL from `index.wal_sequence_synced` to end
3. Replay any records newer than index
4. Rebuild client_event_indexes from metadata

```rust
async fn recover_shard(shard_path: &Path) -> Result<ShardStorage, Error> {
    let index = load_index(shard_path)?;
    let wal = open_wal(shard_path)?;
    
    // Replay WAL from last index sync point
    let mut state = index.to_aggregate_states();
    
    for record in wal.scan_from(index.wal_sequence_synced)? {
        state.apply_record(&record)?;
    }
    
    Ok(ShardStorage::new(state, wal, index))
}
```

---

## 7. Trim, Delete, and Prepend Operations

### 7.1 Trim Start

Current behavior: Rewrite files without trimmed data.

New behavior: Mark records as trimmed, compact lazily.

```rust
pub async fn trim_start(&mut self, key: &AggregateKey, keep_from: u64) -> Result<()> {
    let state = self.aggregates.get_mut(key)?;
    
    // Update in-memory state
    state.min_event_batch_index = keep_from;
    state.wal_records.retain(|r| r.event_batch_index >= keep_from);
    
    // Mark for compaction (don't rewrite WAL immediately)
    self.compaction_candidates.insert(key.clone());
    
    // Update index
    self.mark_index_dirty(key);
    
    Ok(())
}
```

### 7.2 Compaction

Background process to reclaim space from trimmed/deleted aggregates:

```rust
pub async fn compact_wal(&mut self) -> Result<()> {
    if self.dead_space_ratio() < 0.3 {
        return Ok(()); // Not worth compacting yet
    }
    
    // Create new WAL with only live data
    let new_wal = create_wal(&self.path.with_extension("wal.new"))?;
    
    for (key, state) in &self.aggregates {
        if state.is_deleted() { continue; }
        
        for record in &state.wal_records {
            let data = self.wal.read_record(record)?;
            new_wal.append(data)?;
        }
    }
    
    new_wal.fdatasync().await?;
    
    // Atomic swap
    rename(new_wal.path(), self.wal.path())?;
    
    // Rebuild index
    self.rebuild_index()?;
    
    Ok(())
}
```

### 7.3 Delete

```rust
pub async fn delete(&mut self, key: &AggregateKey) -> Result<()> {
    let state = self.aggregates.get_mut(key)?;
    state.flags |= FLAG_DELETED;
    state.wal_records.clear();
    
    self.mark_index_dirty(key);
    self.compaction_candidates.insert(key.clone());
    
    Ok(())
}
```

### 7.4 Prepend

Prepending becomes more complex. Options:

**Option A**: Write prepended data as new WAL records with special flag
```rust
pub struct WalRecordHeader {
    pub flags: u32,  // FLAG_PREPENDED indicates out-of-order batch index
}
```

**Option B**: Require compaction after prepend to maintain order

**Recommendation**: Option A for simplicity, with compaction as optimization.

---

## 8. Components to Change

### 8.1 New Components

| Component | Purpose |
|-----------|---------|
| `ShardStorage` | Manages single shard's WAL, index, and aggregates |
| `WalFile` | Append-only log with record framing |
| `AggregateIndex` | In-memory index with persistence |
| `ShardCompactor` | Background compaction process |

### 8.2 Modified Components

| Component | Changes |
|-----------|---------|
| `LocalAggregate` | Delegate to `ShardStorage` instead of per-aggregate files |
| `AggregateCache` | Replace with `ShardStorage.aggregates` (already shard-local) |
| `AggregateResources` | Remove - state moves to `AggregateState` |
| `ReadOperations` | Read from WAL instead of dedicated event file |
| `WriteOperations` | Queue to WAL instead of per-aggregate file |
| `WatchedAggregates` | No change (already works with AggregateKey) |

### 8.3 Removed Components

| Component | Reason |
|-----------|---------|
| `aggregate_resources.rs` | Replaced by ShardStorage |
| Per-aggregate file handling | No longer needed |

### 8.4 Unchanged Components

| Component | Reason |
|-----------|---------|
| `celeriant_wal` types | Event/batch structures unchanged |
| `celeriant_wire` | Serialization unchanged |
| `celeriant_msg` | Request/response types unchanged |
| Filter logic | Same filtering, different data source |
| Client idempotency | Same logic, different storage location |

---

## 9. Risks & Edge Cases

### 9.1 High Risk

| Risk | Impact | Mitigation |
|------|--------|------------|
| WAL corruption | Data loss for all aggregates in shard | Per-record checksums, recovery from replicas |
| Index corruption | Slow startup (full WAL replay) | Periodic index checkpoints, WAL is authoritative |
| Large aggregate interleaving | Read amplification | Consider per-aggregate WAL segments for hot aggregates |
| Compaction during writes | Complexity, potential data races | Copy-on-write compaction, atomic swap |

### 9.2 Medium Risk

| Risk | Impact | Mitigation |
|------|--------|------------|
| Memory pressure from metadata | OOM for many aggregates | LRU eviction of cold aggregate metadata |
| Hot aggregate contention | Single aggregate blocks shard sync | Separate sync coordination per aggregate |
| WAL file growth | Disk space exhaustion | Automatic compaction triggers |
| Index rebuild time | Slow recovery for large WALs | Frequent index checkpoints |

### 9.3 Edge Cases

| Case | Handling |
|------|----------|
| Crash during sync | WAL records are atomic (checksum validation), partial writes detected and truncated |
| Crash during compaction | Old WAL still valid, new WAL incomplete and discarded |
| Aggregate deleted during read | Check deleted flag, return appropriate error |
| Prepend creates gaps | Allow gaps in WAL, track in metadata |
| Very long-running read | Snapshot WAL positions at read start, ignore subsequent writes |

### 9.4 Backwards Compatibility

**Migration strategy required**:

1. New code can read old format (per-aggregate files)
2. Migration tool converts old → new format
3. No automatic upgrade path (requires explicit migration)

Consider:
- Offline migration (stop server, migrate, restart)
- Online migration (dual-write during transition)

---

## 10. Task Breakdown

### Phase 1: Core Infrastructure (2-3 weeks)

| Task | Estimate | Dependencies |
|------|----------|--------------|
| Design WAL record format | 2d | None |
| Implement `WalFile` with append/read | 3d | WAL format |
| Implement `AggregateIndex` in-memory | 2d | None |
| Implement index persistence | 2d | Index in-memory |
| Implement `ShardStorage` skeleton | 3d | WalFile, Index |
| Unit tests for new components | 3d | All above |

### Phase 2: Write Path (1-2 weeks)

| Task | Estimate | Dependencies |
|------|----------|--------------|
| Implement `queue_to_wal()` | 2d | ShardStorage |
| Implement `sync_shard()` | 2d | ShardStorage |
| Integrate with `LocalAggregate.write()` | 2d | queue/sync |
| Write path tests | 2d | Integration |

### Phase 3: Read Path (1-2 weeks)

| Task | Estimate | Dependencies |
|------|----------|--------------|
| Implement WAL record reading | 2d | WalFile |
| Implement filtered reads from WAL | 3d | Record reading |
| Integrate with `LocalAggregate.read()` | 2d | Filtered reads |
| Cache integration | 2d | Read path |
| Read path tests | 2d | All above |

### Phase 4: Recovery & Startup (1 week)

| Task | Estimate | Dependencies |
|------|----------|--------------|
| Implement WAL replay | 2d | Read path |
| Implement index recovery | 1d | WAL replay |
| Implement client index rebuild | 1d | Recovery |
| Recovery tests | 2d | All above |

### Phase 5: Lifecycle Operations (1-2 weeks)

| Task | Estimate | Dependencies |
|------|----------|--------------|
| Implement trim_start | 2d | Core infra |
| Implement delete | 1d | Core infra |
| Implement prepend | 3d | Core infra |
| Implement compaction | 3d | All lifecycle |
| Lifecycle tests | 2d | All above |

### Phase 6: Integration & Migration (1-2 weeks)

| Task | Estimate | Dependencies |
|------|----------|--------------|
| Remove old components | 2d | All phases |
| Update `AggregateCache` / remove | 1d | Phase 2-4 |
| Migration tool (old → new) | 3d | All phases |
| Integration tests | 3d | All above |
| Performance benchmarks | 2d | Integration |

### Phase 7: Polish & Documentation (1 week)

| Task | Estimate | Dependencies |
|------|----------|--------------|
| Error handling review | 2d | All phases |
| Documentation updates | 2d | All phases |
| README updates | 1d | Documentation |

**Total Estimate**: 8-13 weeks

---

## 11. Success Metrics

| Metric | Current | Target |
|--------|---------|--------|
| fdatasync calls per 1000 writes | ~2000 | ~1-10 |
| Max concurrent aggregates | ~10K (limited by handles) | ~100K+ |
| Write latency p99 (1000 agg, 100 writes/s each) | TBD | <10ms |
| Recovery time (1M batches) | TBD | <30s |
| Memory per aggregate | ~varies (file handles) | ~500B + cache |

---

## 12. Open Questions

1. **Compaction scheduling**: Time-based? Space-based? Both?
2. **Index checkpoint frequency**: Balance recovery time vs write amplification
3. **Large aggregate handling**: Separate WAL segments for aggregates exceeding size threshold?
4. **Multi-shard transactions**: Not currently supported, but does new design preclude future support?
5. **Encryption at rest**: Per-record encryption keys? Per-shard?

---

## 13. Alternatives Considered

### 13.1 Batch fdatasync Across Files

Keep per-aggregate files but batch fdatasync calls using `sync_file_range` + final `fdatasync`.

**Rejected**: Still requires metadata sync per aggregate, doesn't solve file handle problem.

### 13.2 Memory-Mapped WAL

Use mmap instead of DMA for WAL access.

**Rejected**: Conflicts with O_DIRECT requirement, unpredictable latency from page faults.

### 13.3 External Index (RocksDB, SQLite)

Use embedded database for index instead of custom format.

**Rejected**: Adds dependency, potential performance overhead, complicates deployment.

### 13.4 Per-Aggregate-Type Files

One file per `(org_id, aggregate_type_id)` instead of per-shard.

**Rejected**: Still potentially thousands of files for organizations with many aggregate types.