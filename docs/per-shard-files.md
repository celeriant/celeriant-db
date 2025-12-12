# Design Document: Shard-Level WAL Migration

## RFC-001: Two Files Per Shard Architecture

**Status:** Draft  
**Author:** [Engineering]  
**Created:** 2025-01-XX  
**Target Release:** v2.0

---

## 1. Executive Summary

Migrate from per-aggregate file storage (2 files × N aggregates) to per-shard file storage (2 files × M shards) to eliminate fsync contention that currently limits write throughput to ~18K req/s under load.

**Expected outcome:** 10-50× improvement in write throughput with fsync enabled.

---

## 2. Motivation

### Current State

```
Shard 0/
└── aggregates/
    ├── org_1/type_1/agg_1/
    │   ├── metadata.bin      # 256B fixed records
    │   └── event_batches.bin # Variable compressed events
    ├── org_1/type_1/agg_2/
    │   ├── metadata.bin
    │   └── event_batches.bin
    └── ... (thousands more)
```

- 3000 aggregates = 6000 files
- Each aggregate has independent fsync coordination
- Benchmark with fsync: **18,610 req/s**, p99 = 294ms, max = 4990ms
- Benchmark without fsync: **747,728 req/s**, p99 = 313ms

### Root Cause

With N active aggregates, even with per-aggregate fsync batching, we issue up to N fsyncs per batch window. NVMe devices have finite IOPS (~100-500K), and fsync serialization creates head-of-line blocking.

### Target State

```
Shard 0/
├── metadata.bin      # All aggregates' metadata, sequential
└── events.bin        # All aggregates' event data, sequential
```

- 8 shards = 16 files total
- Single fsync per shard covers all writes in batch window
- Compaction handles trimmed data reclamation

---

## 3. Architecture Overview

### 3.1 File Layout

```
┌─────────────────────────────────────────────────────────────────┐
│                     metadata.bin (per shard)                    │
├─────────────────────────────────────────────────────────────────┤
│ [MetadataEntry 0][MetadataEntry 1][MetadataEntry 2]...          │
│                                                                 │
│ Each entry: 256 bytes fixed                                     │
│ - status_byte: u8 (0x00=valid, 0xFF=trimmed)                    │
│ - aggregate_key: AggregateKey (48 bytes)                        │
│ - event_batch_index: u64                                        │
│ - events_offset: u64 (absolute position in events.bin)          │
│ - compressed_size: u64                                          │
│ - ... (existing EventBatchMetadata fields)                      │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                      events.bin (per shard)                     │
├─────────────────────────────────────────────────────────────────┤
│ [EventBatch 0][EventBatch 1][EventBatch 2]...                   │
│                                                                 │
│ Each batch: variable length, compressed                         │
│ Position determined by metadata.events_offset                   │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 Component Hierarchy

```
┌─────────────────────────────────────────────────────────────────┐
│                           Shard                                 │
│  Owns ShardWriter, ShardReader, coordinates fsync               │
├─────────────────────────────────────────────────────────────────┤
│                        ShardWriter                              │
│  - DMA files for metadata.bin + events.bin                      │
│  - Global write queue (all aggregates)                          │
│  - Single fsync with delay coordination                         │
│  - Compaction trigger                                           │
├─────────────────────────────────────────────────────────────────┤
│                        ShardReader                              │
│  - DMA files for reading                                        │
│  - Metadata index: AggregateKey → AggregateIndex                │
│  - read_objects_absolute for event data                         │
├─────────────────────────────────────────────────────────────────┤
│                      AggregateCache                             │
│  LRU cache of AggregateResources (unchanged interface)          │
├─────────────────────────────────────────────────────────────────┤
│                    AggregateResources                           │
│  - Per-aggregate writer cache (in-memory batches)               │
│  - Per-aggregate state (next_batch_index, client_indexes)       │
│  - Delegates I/O to ShardWriter/ShardReader                     │
└─────────────────────────────────────────────────────────────────┘
```

### 3.3 Write Flow

```
WriteRequest arrives
        │
        ▼
┌─────────────────────────────────────────┐
│  AggregateResources.queue_events()      │
│  - Validate (concurrency, idempotency)  │
│  - Serialize + compress events          │
│  - Build metadata entry                 │
│  - Add to aggregate's pending queue     │
│  - Return WriteResponse (optimistic)    │
└─────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────┐
│  ShardWriter.queue_batch()              │
│  - Accept (aggregate_key, metadata,     │
│    compressed_events) from aggregate    │
│  - Add to shard-level pending queue     │
└─────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────┐
│  ShardWriter.sync_with_delay()          │
│  - Coalesce concurrent sync requests    │
│  - Sleep for batch window (e.g. 100µs)  │
│  - Write all pending events to events.bin│
│  - Write all pending metadata           │
│  - Single fdatasync() on events.bin     │
│  - Single fdatasync() on metadata.bin   │
│  - Notify all waiting aggregates        │
└─────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────┐
│  AggregateResources (post-sync)         │
│  - Move pending → writer cache          │
│  - Notify watchers                      │
│  - Update ShardReader index             │
└─────────────────────────────────────────┘
```

### 3.4 Read Flow

```
ReadRequest arrives
        │
        ▼
┌─────────────────────────────────────────┐
│  AggregateResources.maybe_read_cached() │
│  - Check writer's in-memory cache       │
│  - If hit: apply filters, return        │
└─────────────────────────────────────────┘
        │ cache miss
        ▼
┌─────────────────────────────────────────┐
│  ShardReader.get_metadata_range()       │
│  - Lookup aggregate in index            │
│  - Get metadata_file_positions for      │
│    requested batch range                │
│  - Read metadata entries from disk      │
│  - Filter by batch index, timestamp,    │
│    bloom filter, etc.                   │
└─────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────┐
│  ShardReader.read_events()              │
│  - Use metadata.events_offset values    │
│  - read_objects_absolute() for batches  │
│  - Decompress + deserialize             │
│  - Apply event-level filters            │
└─────────────────────────────────────────┘
```

---

## 4. Design Decisions

### 4.1 Metadata Entry Format

**Decision:** Extend existing 256-byte metadata format with aggregate_key and events_offset.

```rust
// New fields added to EventBatchMetadata (or wrapper struct)
pub struct ShardMetadataEntry {
    pub status: u8,                      // 0x00=valid, 0xFF=trimmed
    pub aggregate_key: AggregateKey,     // 48 bytes (org_id, type_id, aggregate_id)
    pub events_offset: u64,              // Absolute position in events.bin
    pub metadata: EventBatchMetadata,    // Existing 200~ bytes
    // Padding to 256 bytes
}
```

**Rationale:** 
- Fixed size enables O(1) position calculation by metadata index
- Status byte allows trim marking without deserializing
- events_offset required since events are interleaved across aggregates

### 4.2 In-Memory Index Structure

**Decision:** On startup, build minimal index; lazy-load full metadata on demand.

```rust
pub struct AggregateIndex {
    pub min_event_batch_index: u64,
    pub max_event_batch_index: u64,
    pub next_event_index: u64,
    pub client_event_indexes: HashMap<u128, u64>,
    pub metadata_positions: Vec<u64>,  // File offsets for each batch's metadata
}

pub struct ShardIndex {
    pub aggregates: HashMap<AggregateKey, AggregateIndex>,
    pub total_metadata_entries: u64,
    pub next_events_offset: u64,
}
```

**Rationale:**
- Startup scans metadata.bin sequentially (fast for NVMe)
- Only stores positions, not full metadata (memory efficient)
- Client indexes needed for idempotency checks
- Full metadata read on-demand during reads

### 4.3 Trim Implementation

**Decision:** Mark metadata entry with sentinel byte; reclaim space via compaction.

```rust
impl ShardWriter {
    pub async fn trim_aggregate(
        &mut self,
        aggregate_key: &AggregateKey,
        keep_from_batch_index: u64,
    ) -> Result<(), WriteError> {
        let index = self.index.aggregates.get_mut(aggregate_key)?;
        
        for batch_idx in index.min_event_batch_index..keep_from_batch_index {
            let metadata_pos = index.metadata_positions[batch_idx - index.min_event_batch_index];
            // Write single byte sentinel at metadata entry start
            self.metadata_file.write_at_aligned(metadata_pos, &[0xFF]).await?;
        }
        
        index.min_event_batch_index = keep_from_batch_index;
        // Note: events.bin space not reclaimed until compaction
        Ok(())
    }
}
```

**Rationale:**
- Single-byte write is atomic and fast
- No need to deserialize metadata to check trim status
- Compaction handles actual space reclamation
- Simple recovery: trimmed entries skipped on startup

### 4.4 Compaction Strategy

**Decision:** Background task creates new files, copies non-trimmed entries, atomic rename.

```rust
impl ShardWriter {
    pub async fn compact(&mut self) -> Result<(), WriteError> {
        // 1. Create temp files
        let temp_metadata = create_dma_file("metadata.bin.tmp").await?;
        let temp_events = create_dma_file("events.bin.tmp").await?;
        
        // 2. Stream through existing files, skip trimmed entries
        let mut new_index = ShardIndex::new();
        let mut events_offset = 0u64;
        
        for metadata_pos in 0..self.index.total_metadata_entries {
            let entry = self.read_metadata_entry(metadata_pos).await?;
            if entry.status == 0xFF {
                continue; // Trimmed
            }
            
            // Read events at old offset
            let events = self.read_events(entry.events_offset, entry.compressed_size).await?;
            
            // Write to new files at new offset
            let new_entry = entry.with_events_offset(events_offset);
            temp_events.write_at(events_offset, &events).await?;
            temp_metadata.append(&new_entry).await?;
            
            // Update new index
            new_index.insert(&entry.aggregate_key, events_offset, ...);
            events_offset += events.len() as u64;
        }
        
        // 3. Sync new files
        temp_events.fdatasync().await?;
        temp_metadata.fdatasync().await?;
        
        // 4. Atomic rename
        temp_metadata.rename("metadata.bin").await?;
        temp_events.rename("events.bin").await?;
        
        // 5. Reopen file handles, update index
        self.reopen_files().await?;
        self.index = new_index;
        
        Ok(())
    }
}
```

**Trigger conditions:**
- Trimmed entries exceed threshold (e.g., 20% of file)
- File size exceeds limit (e.g., 10GB)
- Manual trigger via admin API
- Scheduled (e.g., daily during low-traffic period)

### 4.5 No Prepend Support

**Decision:** Remove prepend_batches functionality entirely.

**Rationale:**
- Prepend was designed for restoring trimmed data from S3
- With shard-level files, prepend would require rewriting entire file
- Alternative: restore to a separate "archive" aggregate or read directly from S3
- Simplifies implementation significantly

**Migration:** Clients using prepend must change to:
1. Read archived data directly from S3/cold storage
2. Use a separate aggregate for restored data
3. Application-level merge of hot + cold data

### 4.6 Cross-Aggregate Transactions

**Decision:** Support via sharding strategy; no distributed transactions.

```rust
// Shard assignment
fn shard_for_aggregate(key: &AggregateKey, num_shards: usize) -> usize {
    // Option A: By aggregate_id (current)
    (key.aggregate_id % num_shards as u128) as usize
    
    // Option B: By org_id (enables cross-aggregate within org)
    (key.org_id % num_shards as u128) as usize
    
    // Option C: By aggregate_type_id (enables cross-type within type)
    (key.aggregate_type_id % num_shards as u128) as usize
}
```

**Rationale:**
- All writes within a shard share the same fsync
- If aggregates A and B are on same shard, write to both is atomic at storage level
- Application can implement saga/choreography patterns for cross-shard
- Sharding strategy is configurable at deployment time

### 4.7 Writer Cache Remains Per-Aggregate

**Decision:** Keep AggregateResources with its writer cache; only I/O moves to shard level.

```rust
pub struct AggregateWriteState {
    // Stays in AggregateResources
    pub data_cache: VecDeque<CacheItem>,
    pub pending_queue: Vec<EventBatchQueueItem>,
    pub client_event_indexes: HashMap<u128, u64>,
    pub next_event_batch_index: u64,
    pub next_event_index: u64,
    
    // Removed - now in ShardWriter
    // pub metadata_dma_file: DmaFile,
    // pub event_batches_dma_file: DmaFile,
}
```

**Rationale:**
- Hot read path (cache hit) unchanged
- Per-aggregate state still needed for validation
- Only sync coordination moves to shard level
- Minimal code changes in LocalAggregate

---

## 5. Code Changes Required

### 5.1 New Files

| File | Purpose |
|------|---------|
| `celeriant_aggregate/src/shard_writer.rs` | Shard-level write coordination, fsync batching |
| `celeriant_aggregate/src/shard_reader.rs` | Shard-level read operations, index management |
| `celeriant_aggregate/src/shard_index.rs` | In-memory index structures, startup recovery |
| `celeriant_aggregate/src/compaction.rs` | Background compaction task |

### 5.2 Modified Files

#### `celeriant_aggregate/src/write_operations/write_operations.rs`

```diff
 pub struct WriteOperationsWithDmaFile {
-    pub metadata_dma_file: Option<DmaFile>,
-    pub event_batches_dma_file: Option<DmaFile>,
-    pub metadata_buffer: Vec<u8>,
-    pub event_batch_buffer: Vec<u8>,
+    // DMA files moved to ShardWriter
     pub data_cache: VecDeque<CacheItem>,
     pub append_event_batch_queue: Vec<EventBatchQueueItem>,
     // ... rest unchanged
 }

-impl WriteOperationsWithDmaFile {
-    async fn sync(&mut self, ...) -> Result<(), WriteError> {
-        // ... direct file writes
-    }
-}

+impl WriteOperationsWithDmaFile {
+    /// Queue events locally, return items for shard-level sync
+    pub fn prepare_for_shard_sync(&mut self) -> Vec<ShardWriteItem> {
+        self.append_event_batch_queue
+            .drain(..)
+            .map(|item| ShardWriteItem {
+                aggregate_key: self.aggregate_key.clone(),
+                metadata_bytes: item.metadata_bytes,
+                compressed_events: item.compressed_event_batch_item,
+                // ... 
+            })
+            .collect()
+    }
+
+    /// Called after shard sync completes
+    pub fn post_sync_update(&mut self, sync_result: &ShardSyncResult) {
+        // Move to cache, update file lengths from sync result
+    }
+}
```

#### `celeriant_aggregate/src/read_operations/read_operations.rs`

```diff
 pub struct ReadOperationsWithDmaFiles {
-    pub metadata_dma_file: Option<DmaFile>,
-    pub event_batches_dma_file: Option<DmaFile>,
+    // Now holds reference to shard reader
+    shard_reader: Rc<ShardReader>,
+    aggregate_key: AggregateKey,
     pub config: AggregateReadConfig,
 }

 impl ReadOperations for ReadOperationsWithDmaFiles {
     async fn read(&self, ...) -> Result<ReadResponse, ReadError> {
-        // Direct file reads
+        // Delegate to shard reader with aggregate_key filter
+        self.shard_reader.read_aggregate(
+            &self.aggregate_key,
+            read_filters,
+            max_bytes,
+        ).await
     }
 }
```

#### `celeriant_aggregate/src/cache/aggregate_resources.rs`

```diff
 pub struct AggregateResources {
     // ... paths removed, now computed from shard
+    shard_writer: Rc<RefCell<ShardWriter>>,
+    shard_reader: Rc<ShardReader>,
     reader: RwLock<Option<ReadOperationsWithDmaFiles>>,
     writer: RwLock<Option<WriteOperationsWithDmaFile>>,
     // ...
 }

-impl AggregateResources {
-    pub async fn sync_with_delay(&self, delay: Option<Duration>, ...) -> SyncResult {
-        // Per-aggregate fsync coordination
-    }
-}

+impl AggregateResources {
+    pub async fn sync_with_delay(&self, delay: Option<Duration>, ...) -> SyncResult {
+        // Prepare items from aggregate writer
+        let items = {
+            let mut writer = self.writer.write().await?;
+            writer.prepare_for_shard_sync()
+        };
+        
+        if items.is_empty() {
+            return Ok(());
+        }
+        
+        // Queue to shard writer and wait for shard-level sync
+        let result = self.shard_writer
+            .borrow_mut()
+            .queue_and_sync(items, delay)
+            .await?;
+        
+        // Update aggregate state post-sync
+        {
+            let mut writer = self.writer.write().await?;
+            writer.post_sync_update(&result);
+        }
+        
+        // Notify watchers
+        self.watched_aggregates.notify(...);
+        
+        Ok(())
+    }
+}
```

#### `celeriant_aggregate/src/cache/aggregate_cache.rs`

```diff
 pub struct AggregateCache {
     aggregates_cache: Rc<RefCell<LruCache<AggregateKey, Rc<AggregateResources>>>>,
+    shard_writer: Rc<RefCell<ShardWriter>>,
+    shard_reader: Rc<ShardReader>,
     // ...
 }

 impl AggregateCache {
+    pub async fn new_with_shard_files(
+        capacity: NonZeroUsize,
+        shard_id: usize,
+        data_root: &str,
+        // ...
+    ) -> Result<Self, ReadError> {
+        let shard_path = format!("{}/shard_{}", data_root, shard_id);
+        
+        // Open or create shard files
+        let (shard_writer, shard_reader, index) = 
+            initialize_shard_files(&shard_path).await?;
+        
+        Ok(Self {
+            aggregates_cache: ...,
+            shard_writer: Rc::new(RefCell::new(shard_writer)),
+            shard_reader: Rc::new(shard_reader),
+            // ...
+        })
+    }

     fn get_aggregate_resources(&self, key: &AggregateKey) -> Rc<AggregateResources> {
         // ... existing LRU logic
         let resources = Rc::new(AggregateResources::new(
             key.clone(),
+            self.shard_writer.clone(),
+            self.shard_reader.clone(),
             // ...
         ));
         // ...
     }
 }
```

#### `celeriant_aggregate/src/local_aggregate.rs`

```diff
 impl LocalAggregateTrait for LocalAggregate {
-    async fn trim_start(&self, request: &TrimStartRequest) -> Result<(), ReadWriteError> {
-        // ... complex file manipulation per aggregate
-    }

+    async fn trim_start(&self, request: &TrimStartRequest) -> Result<(), ReadWriteError> {
+        let shard_writer = self.aggregate_cache.shard_writer.borrow_mut();
+        shard_writer.trim_aggregate(
+            &request.aggregate_key,
+            request.keep_from_event_batch_index,
+        ).await?;
+        
+        // Update aggregate's in-memory state
+        let resources = self.aggregate_cache.get_aggregate_resources(&request.aggregate_key);
+        let mut writer = resources.get_writer_mut(false).await?;
+        writer.minimum_available_event_batch_index = request.keep_from_event_batch_index;
+        writer.data_cache.retain(|c| 
+            c.event_batch_metadata.event_batch_index >= request.keep_from_event_batch_index
+        );
+        
+        Ok(())
+    }

-    async fn prepend_batches(&self, ...) -> Result<(), ReadWriteError> {
-        // ... prepend logic
-    }
+    // REMOVED: prepend_batches no longer supported
 }
```

#### `celeriant_runtimes/src/sharded/shard.rs`

```diff
 impl Shard {
     pub fn new(...) -> Result<Self, GlommioError<()>> {
         // ...
-        let local_aggregate = LocalAggregate::new(...);
+        let local_aggregate = LocalAggregate::new_with_shard(
+            current_shard_id,
+            aggregate_read_config,
+            aggregate_write_config,
+            node_config,
+        ).await?;
         // ...
     }
+
+    pub async fn run(&mut self) {
+        // ... existing setup
+        
+        // Spawn compaction background task
+        spawn_compaction_task(self.shard_data.clone());
+        
+        self.enter_main_loop_until_shutdown().await;
+    }
 }

+fn spawn_compaction_task(shard_data: ShardData) {
+    glommio::spawn_local(async move {
+        loop {
+            glommio::timer::sleep(Duration::from_secs(60)).await;
+            
+            if shard_data.shutdown_requested.get() {
+                break;
+            }
+            
+            let should_compact = {
+                let writer = shard_data.local_aggregates
+                    .aggregate_cache
+                    .shard_writer
+                    .borrow();
+                writer.should_compact()
+            };
+            
+            if should_compact {
+                if let Err(e) = shard_data.local_aggregates
+                    .aggregate_cache
+                    .shard_writer
+                    .borrow_mut()
+                    .compact()
+                    .await
+                {
+                    error!("Compaction failed: {:?}", e);
+                }
+            }
+        }
+    }).detach();
+}
```

### 5.3 Removed/Deprecated

| Item | Reason |
|------|--------|
| `prepend_batches()` | Not supported with shard-level files |
| `PrependBatchesRequest` | Remove from API |
| Per-aggregate file paths in AggregateResources | Replaced by shard paths |
| `create_and_write_only_dma` per aggregate | Only called for shard files now |

---

## 6. Risks and Compromises

### 6.1 Read Performance Impact

| Scenario | Before | After | Mitigation |
|----------|--------|-------|------------|
| Cache hit | O(1) | O(1) | Unchanged |
| Cache miss, single batch | O(1) seek | O(log n) index lookup + O(1) seek | Index is in-memory |
| Cache miss, range scan | Sequential in aggregate file | Non-sequential in shard file | read_objects_absolute handles gaps |
| Metadata scan | 256B × batches for aggregate | 256B × batches for aggregate (filtered) | Index provides positions |

**Assessment:** Read latency increase expected to be <1ms for cache misses. Hot path (cache hit) unchanged.

### 6.2 Startup Time

| Aggregates | Batches/Aggregate | Total Metadata | Scan Time (est.) |
|------------|-------------------|----------------|------------------|
| 1,000 | 100 | 25 MB | ~50ms |
| 10,000 | 100 | 250 MB | ~500ms |
| 100,000 | 100 | 2.5 GB | ~5s |

**Mitigation:** 
- Sequential scan is fast on NVMe (~3GB/s)
- Can parallelize across shards
- Consider checkpoint/snapshot for very large deployments

### 6.3 Compaction Impact

**Risk:** Compaction blocks writes to shard during file rename.

**Mitigation:**
- Use double-buffering: write to new files, atomic rename, reopen
- Rename is ~instantaneous
- Brief window (~1ms) where new writes must wait
- Schedule during low-traffic periods

### 6.4 Memory Usage

| Component | Before | After |
|-----------|--------|-------|
| Per-aggregate file handles | 2 × N aggregates | 2 × M shards |
| Index memory | None | ~100 bytes × total_batches |
| Writer cache | Unchanged | Unchanged |

**Assessment:** Net reduction in file handles; slight increase in index memory.

### 6.5 Loss of Prepend

**Impact:** Cannot restore trimmed data by prepending.

**Workarounds:**
1. **Archive aggregate:** Create `{aggregate_id}_archive` for restored data
2. **Application-level merge:** Read from both hot and archive aggregates
3. **Direct S3 read:** Application reads cold data directly from S3

### 6.6 Failure Modes

| Failure | Impact | Recovery |
|---------|--------|----------|
| Crash during write | Partial metadata entries | Startup scan detects incomplete entries (size mismatch) |
| Crash during compaction | Temp files exist | Delete temp files, retry compaction |
| Corrupt metadata entry | Single batch unreadable | Skip entry, log error, continue |
| Corrupt events | CRC mismatch on read | Return error for that batch |

---

## 7. Migration Strategy

### Phase 1: Feature Flag (Week 1-2)
- Implement ShardWriter/ShardReader behind feature flag
- Both paths coexist
- Test with synthetic load

### Phase 2: Shadow Mode (Week 3)
- Write to both per-aggregate and shard files
- Read from per-aggregate (source of truth)
- Verify shard files match

### Phase 3: Cutover (Week 4)
- New deployments use shard files by default
- Migration tool converts existing per-aggregate → shard files
- Rollback path: re-enable per-aggregate mode

### Phase 4: Cleanup (Week 5+)
- Remove per-aggregate file code
- Remove feature flag
- Delete deprecated API endpoints

---

## 8. Task Breakdown

### Epic: Shard-Level WAL Migration

#### 8.1 Core Infrastructure (P0)

| Task | Estimate | Dependencies |
|------|----------|--------------|
| Define `ShardMetadataEntry` struct with aggregate_key, events_offset | 2h | - |
| Implement `ShardIndex` in-memory structure | 4h | - |
| Implement startup scan: metadata.bin → ShardIndex | 8h | ShardIndex |
| Implement `ShardWriter::new()` with DMA file setup | 4h | - |
| Implement `ShardWriter::queue_batch()` | 4h | ShardWriter::new |
| Implement `ShardWriter::sync_with_delay()` (single fsync coordination) | 8h | queue_batch |
| Implement `ShardReader::new()` with index reference | 4h | ShardIndex |
| Implement `ShardReader::read_aggregate()` | 8h | ShardReader::new |

**Subtotal: 42h**

#### 8.2 Integration (P0)

| Task | Estimate | Dependencies |
|------|----------|--------------|
| Modify `AggregateCache` to hold ShardWriter/ShardReader | 4h | Core Infrastructure |
| Modify `AggregateResources` to delegate to shard-level I/O | 8h | AggregateCache changes |
| Modify `WriteOperationsWithDmaFile` to remove direct file I/O | 6h | ShardWriter |
| Add `prepare_for_shard_sync()` and `post_sync_update()` | 4h | WriteOperations changes |
| Modify `ReadOperationsWithDmaFiles` to use ShardReader | 6h | ShardReader |
| Update `LocalAggregate::new()` to initialize shard files | 4h | All above |
| Update `Shard::new()` in celeriant_runtimes | 4h | LocalAggregate changes |
| Wire through shard_id from runtime to aggregate layer | 2h | - |

**Subtotal: 38h**

#### 8.3 Trim & Compaction (P1)

| Task | Estimate | Dependencies |
|------|----------|--------------|
| Implement `ShardWriter::trim_aggregate()` (sentinel byte write) | 4h | ShardWriter |
| Update `LocalAggregate::trim_start()` to use shard-level trim | 4h | trim_aggregate |
| Implement `ShardWriter::should_compact()` heuristics | 2h | ShardWriter |
| Implement `ShardWriter::compact()` (full compaction logic) | 12h | ShardWriter |
| Add compaction background task in Shard runtime | 4h | compact() |
| Handle file handle replacement after compaction | 4h | compact() |
| Update ShardReader index after compaction | 4h | compact() |

**Subtotal: 34h**

#### 8.4 Remove Deprecated Features (P1)

| Task | Estimate | Dependencies |
|------|----------|--------------|
| Remove `prepend_batches()` from WriteOperations trait | 2h | - |
| Remove `PrependBatchesRequest` handling in LocalAggregate | 2h | - |
| Remove `PrependBatchesRequest` from celeriant_msg | 2h | - |
| Update API documentation to reflect removal | 2h | - |
| Add migration guide for prepend users | 4h | - |

**Subtotal: 12h**

#### 8.5 Testing (P0)

| Task | Estimate | Dependencies |
|------|----------|--------------|
| Unit tests for ShardIndex construction and lookup | 4h | ShardIndex |
| Unit tests for ShardWriter queue and sync | 6h | ShardWriter |
| Unit tests for ShardReader read paths | 6h | ShardReader |
| Unit tests for trim with sentinel byte | 4h | trim_aggregate |
| Unit tests for compaction correctness | 8h | compact() |
| Integration tests: write → read round-trip | 6h | Integration complete |
| Integration tests: concurrent writes from multiple aggregates | 6h | Integration complete |
| Integration tests: trim → compaction → read | 6h | Compaction complete |
| Integration tests: crash recovery simulation | 8h | All above |
| Benchmark: compare throughput before/after | 4h | Integration complete |
| Stress test: 3000 aggregates, 4000 connections | 4h | Benchmark |

**Subtotal: 62h**

#### 8.6 Migration Tooling (P2)

| Task | Estimate | Dependencies |
|------|----------|--------------|
| Implement migration tool: per-aggregate → shard files | 12h | Core Infrastructure |
| Implement verification: compare old vs new file contents | 6h | Migration tool |
| Add feature flag for runtime selection | 4h | - |
| Shadow write mode: write to both, verify match | 8h | Feature flag |
| Rollback procedure documentation | 4h | - |

**Subtotal: 34h**

#### 8.7 Documentation & Cleanup (P2)

| Task | Estimate | Dependencies |
|------|----------|--------------|
| Update celeriant_aggregate README | 4h | All P0/P1 complete |
| Update celeriant_disk README if needed | 2h | - |
| Architecture diagram updates | 2h | - |
| Inline code documentation | 4h | - |
| Remove dead code paths | 4h | Migration complete |
| Performance tuning guide | 4h | Benchmarks complete |

**Subtotal: 20h**

---

### Summary

| Category | Estimate |
|----------|----------|
| Core Infrastructure | 42h |
| Integration | 38h |
| Trim & Compaction | 34h |
| Remove Deprecated | 12h |
| Testing | 62h |
| Migration Tooling | 34h |
| Documentation | 20h |
| **Total** | **242h** |

**Recommended team allocation:** 2 engineers, 4-5 weeks

---

### Milestone Schedule

```
Week 1: Core Infrastructure
├── ShardMetadataEntry, ShardIndex
├── Startup scan implementation
├── ShardWriter basic implementation
└── Unit tests for core structs

Week 2: Core Infrastructure + Integration Start
├── ShardWriter sync_with_delay
├── ShardReader implementation
├── Begin AggregateCache integration
└── Unit tests continue

Week 3: Integration Complete
├── AggregateResources delegation
├── WriteOperations/ReadOperations modifications
├── LocalAggregate + Shard runtime changes
└── Integration tests

Week 4: Trim, Compaction, Testing
├── Trim implementation
├── Compaction implementation
├── Remove prepend
├── Stress testing + benchmarks
└── Bug fixes

Week 5: Migration + Polish
├── Migration tooling
├── Shadow mode testing
├── Documentation
├── Performance tuning
└── Release prep
```

---

## 9. Success Criteria

| Metric | Current | Target |
|--------|---------|--------|
| Write throughput (fsync enabled) | 18,610 req/s | >200,000 req/s |
| Write latency p99 (fsync enabled) | 294ms | <50ms |
| Write latency max (fsync enabled) | 4,990ms | <500ms |
| Startup time (10K aggregates) | N/A | <2s |
| Compaction impact (write pause) | N/A | <10ms |
| Memory overhead per batch | 0 | <100 bytes |

---

## 10. Open Questions

1. **Compaction scheduling:** Time-based (e.g., 2 AM daily) vs. threshold-based (e.g., 20% trimmed)? 
   - *Recommendation:* Threshold-based with configurable minimum interval

2. **Index persistence:** Should we checkpoint the index to avoid full scan on startup?
   - *Recommendation:* Defer to v2.1; scan is fast enough for initial release

3. **Shard rebalancing:** If we add shards, how do we redistribute data?
   - *Recommendation:* Out of scope; requires separate design for shard splitting

4. **Multi-tenant isolation:** Should different orgs have separate shard files?
   - *Recommendation:* No; sharding by org_id achieves logical isolation

---

## 11. Appendix: New Struct Definitions

### ShardMetadataEntry

```rust
/// On-disk metadata entry format (256 bytes fixed)
#[repr(C)]
pub struct ShardMetadataEntry {
    /// 0x00 = valid, 0xFF = trimmed/pending compaction
    pub status: u8,
    
    /// Aggregate identification
    pub org_id: u128,
    pub aggregate_type_id: u128,
    pub aggregate_id: u128,
    
    /// Batch identification
    pub event_batch_index: u64,
    
    /// Location in events.bin
    pub events_offset: u64,
    pub compressed_size: u64,
    pub uncompressed_size: u64,
    
    /// Event range
    pub min_event_index: u64,
    pub max_event_index: u64,
    pub event_count: u32,
    
    /// Timestamps
    pub server_timestamp_ms: u64,
    pub min_event_timestamp: u64,
    pub max_event_timestamp: u64,
    
    /// Client tracking
    pub client_id: u128,
    pub user_id_present: u8,
    pub user_id: u128,
    pub min_client_event_index: u64,
    pub max_client_event_index: u64,
    
    /// Integrity & filtering
    pub events_crc: u32,
    pub compression_type: u8,
    pub event_types_data: EventTypesData,  // 33 bytes
    
    /// Padding to 256 bytes
    pub _reserved: [u8; PADDING_SIZE],
}

pub const SHARD_METADATA_ENTRY_SIZE: usize = 256;
```

### ShardIndex

```rust
/// In-memory index built from metadata.bin on startup
pub struct ShardIndex {
    /// Per-aggregate index data
    pub aggregates: HashMap<AggregateKey, AggregateIndex>,
    
    /// Global shard state
    pub metadata_entry_count: u64,
    pub next_events_offset: u64,
    pub trimmed_entry_count: u64,
}

/// Per-aggregate tracking within shard
pub struct AggregateIndex {
    /// Batch range
    pub min_event_batch_index: u64,
    pub max_event_batch_index: u64,
    
    /// Event range (for validation)
    pub next_event_index: u64,
    
    /// Client idempotency
    pub client_event_indexes: HashMap<u128, u64>,
    
    /// Positions in metadata.bin for each batch
    /// Index: batch_index - min_event_batch_index
    /// Value: byte offset in metadata.bin
    pub metadata_positions: Vec<u64>,
}
```

### ShardWriter

```rust
pub struct ShardWriter {
    /// Shard identification
    shard_id: usize,
    shard_path: PathBuf,
    
    /// DMA file handles
    metadata_file: DmaFile,
    events_file: DmaFile,
    
    /// Alignment buffers (carry-over from previous writes)
    metadata_buffer: Vec<u8>,
    events_buffer: Vec<u8>,
    
    /// Current file positions
    metadata_file_len: u64,
    events_file_len: u64,
    
    /// In-memory index (shared with ShardReader)
    index: Rc<RefCell<ShardIndex>>,
    
    /// Pending writes awaiting sync
    pending_writes: Vec<ShardWriteItem>,
    
    /// Fsync coordination
    sync_event: RefCell<Option<Rc<LocalEvent<SyncResult>>>>,
    
    /// Compaction state
    trimmed_bytes: u64,
    last_compaction: Instant,
}

pub struct ShardWriteItem {
    pub aggregate_key: AggregateKey,
    pub metadata_entry: ShardMetadataEntry,
    pub compressed_events: Vec<u8>,
    pub notify: Option<Rc<LocalEvent<SyncResult>>>,
}
```

### ShardReader

```rust
pub struct ShardReader {
    /// DMA file handles (may be dup'd from writer)
    metadata_file: DmaFile,
    events_file: DmaFile,
    
    /// Shared index with writer
    index: Rc<RefCell<ShardIndex>>,
    
    /// Read configuration
    config: AggregateReadConfig,
}
```

---

## 12. Appendix: Key Algorithm - Startup Recovery

```rust
impl ShardIndex {
    pub async fn recover_from_files(
        metadata_file: &DmaFile,
        events_file: &DmaFile,
    ) -> Result<Self, ReadError> {
        let metadata_len = metadata_file.file_size().await?;
        let events_len = events_file.file_size().await?;
        
        // Handle misaligned file (crash during write)
        let aligned_metadata_len = (metadata_len / SHARD_METADATA_ENTRY_SIZE as u64) 
            * SHARD_METADATA_ENTRY_SIZE as u64;
        
        let mut index = ShardIndex {
            aggregates: HashMap::new(),
            metadata_entry_count: 0,
            next_events_offset: 0,
            trimmed_entry_count: 0,
        };
        
        let mut max_events_offset: u64 = 0;
        
        // Sequential scan through metadata
        read_fixed_records_visit_const::<SHARD_METADATA_ENTRY_SIZE, ReadError>(
            metadata_file,
            aligned_metadata_len,
            0,
            None,
            1 << 20, // 1MB chunks
            |entry_bytes| {
                // Check status byte first (fast path for trimmed)
                if entry_bytes[0] == 0xFF {
                    index.trimmed_entry_count += 1;
                    index.metadata_entry_count += 1;
                    return Ok(());
                }
                
                // Deserialize full entry
                let entry: ShardMetadataEntry = deserialize(entry_bytes)?;
                let aggregate_key = AggregateKey::new(
                    entry.org_id,
                    entry.aggregate_type_id,
                    entry.aggregate_id,
                );
                
                // Track max events offset for integrity check
                let entry_end = entry.events_offset + entry.compressed_size;
                if entry_end > max_events_offset {
                    max_events_offset = entry_end;
                }
                
                // Update aggregate index
                let agg_index = index.aggregates
                    .entry(aggregate_key)
                    .or_insert_with(|| AggregateIndex {
                        min_event_batch_index: entry.event_batch_index,
                        max_event_batch_index: entry.event_batch_index,
                        next_event_index: entry.max_event_index + 1,
                        client_event_indexes: HashMap::new(),
                        metadata_positions: Vec::new(),
                    });
                
                // Update batch range
                if entry.event_batch_index < agg_index.min_event_batch_index {
                    agg_index.min_event_batch_index = entry.event_batch_index;
                }
                if entry.event_batch_index > agg_index.max_event_batch_index {
                    agg_index.max_event_batch_index = entry.event_batch_index;
                    agg_index.next_event_index = entry.max_event_index + 1;
                }
                
                // Update client idempotency
                agg_index.client_event_indexes
                    .entry(entry.client_id)
                    .and_modify(|v| {
                        if entry.max_client_event_index > *v {
                            *v = entry.max_client_event_index;
                        }
                    })
                    .or_insert(entry.max_client_event_index);
                
                // Store metadata position
                let metadata_pos = index.metadata_entry_count * SHARD_METADATA_ENTRY_SIZE as u64;
                // Ensure positions vec is sized correctly
                let relative_idx = (entry.event_batch_index - agg_index.min_event_batch_index) as usize;
                if agg_index.metadata_positions.len() <= relative_idx {
                    agg_index.metadata_positions.resize(relative_idx + 1, u64::MAX);
                }
                agg_index.metadata_positions[relative_idx] = metadata_pos;
                
                index.metadata_entry_count += 1;
                Ok(())
            },
        ).await?;
        
        // Validate events file covers all referenced offsets
        if max_events_offset > events_len {
            // Crash occurred after metadata write but before events
            // Truncate metadata to last valid entry
            // (Implementation detail: scan backwards to find last valid)
            return Err(ReadError::CorruptMetadata { 
                file_pos_metadata: aligned_metadata_len 
            });
        }
        
        index.next_events_offset = max_events_offset;
        
        Ok(index)
    }
}
```
