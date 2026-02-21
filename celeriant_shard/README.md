# celeriant_shard

Shard-level write-ahead log orchestrator. Coordinates validation, queue management, durability, replication, S3 catchup, caching, and read filtering for a single shard.

`ShardWal<R: ReplicationClient, D: S3Downloader>` is the central type. It is generic over its replication and S3 dependencies so that the runtime crate can inject real implementations and tests can inject stubs.

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                         ShardWal<R: ReplicationClient, D: S3Downloader>          │
├──────────────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────────┐  ┌──────────────────┐  ┌───────────────────────────────┐  │
│  │  ShardMemCache   │  │ LogSegmentsCache │  │      AggregateWatchers        │  │
│  │  (positions,     │  │  (DMA file I/O)  │  │   (watch subscribers)         │  │
│  │   queues, cache) │  │                  │  │                               │  │
│  └────────┬─────────┘  └────────┬─────────┘  └───────────────────────────────┘  │
│           │                     │                                               │
│           ▼                     ▼                                               │
│  ┌────────────────────────────────────────────────────────────────────────────┐  │
│  │              Coordinator<ShardFsyncError>  (fsync batching)                │  │
│  │          leader/follower coalescing with delay, two-phase capture          │  │
│  └────────────────────────────────┬───────────────────────────────────────────┘  │
│                                   │  on leader: commit to disk                  │
│                                   ▼                                              │
│  ┌────────────────────────────────────────────────────────────────────────────┐  │
│  │             Coordinator<ReplicationError>  (replication batching)          │  │
│  │       leader replicates to follower TCP or S3 fallback, with rollback      │  │
│  └────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                  │
│  ┌─────────────────────┐  ┌──────────────────────┐  ┌─────────────────────────┐  │
│  │  LoadingCoordinator │  │   BloomFilterCache   │  │     TimestampConfig     │  │
│  │  (thundering herd)  │  │   (reusable filter)  │  │   (precision/epoch)     │  │
│  └─────────────────────┘  └──────────────────────┘  └─────────────────────────┘  │
│                                                                                  │
│  ┌──────────────────────┐  ┌─────────────────────────────────────────────────┐   │
│  │  R: ReplicationClient│  │               D: S3Downloader                   │   │
│  │  (TCP or stub)       │  │            (S3 catchup for followers)           │   │
│  └──────────────────────┘  └─────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────────────┘
```

## Module Structure

| Module | Purpose |
|--------|---------|
| `shard_wal.rs` | Main orchestrator: `ShardWal`, `process_request`, all public operations |
| `shard_wal_sync.rs` | Fsync capture/commit/rollback, disk write layout, hash chain |
| `shard_wal_replicate.rs` | Replication: capture snapshot, replicate to follower/S3, rollback |
| `shard_wal_s3_catchup.rs` | S3 catchup loop: list, download, apply, fsync, delete |
| `collect_from_disk.rs` | Batch disk I/O for reading datablocks grouped by log file |
| `in_memory_filtering.rs` | Metablock-level and event-level filter application |
| `amortisation/coordinator.rs` | Two-phase fsync/replication coordinator |
| `amortisation/local_event.rs` | Single-threaded async event for multi-listener broadcasting |
| `bloom/bloom_filter_cache.rs` | Reusable bloom filter to avoid allocations |
| `loading_coordinator.rs` | Serializes concurrent disk loads per key (thundering herd) |
| `watch_event_collector.rs` | Collects watch events, merges ranges, broadcasts after commit |
| `replication_client.rs` | `ReplicationClient` trait, `FollowerConnection<S>`, `StubReplicationClient` |
| `s3_uploader.rs` | `S3Uploader` trait (implemented in `celeriant_runtimes`) |
| `s3_downloader.rs` | `S3Downloader` trait, `StubS3Downloader` |
| `internal_shard_config.rs` | All shard configuration parameters |
| `timestamp_config.rs` | Configurable precision (ms/μs/ns) and epoch offset |

## Key Types

| Type | Purpose |
|------|---------|
| `ShardWal<R, D>` | Main orchestrator; generic over replication and S3 traits |
| `Coordinator<E>` | Fsync/replication batching with two-phase capture |
| `CaptureResult<T, E>` | `Captured(T)` / `Failed(E)` / `NoCaptureRaceButOk` for two-phase sync |
| `SyncResult<E>` | `Result<(), E>` type alias used throughout the coordinator |
| `LocalEvent<T>` | Single-threaded async event; notifies all waiting `LocalEventListener<T>`s |
| `LocalEventListener<T>` | `Future` impl; polled by followers waiting for sync result |
| `BloomFilterCache` | Reusable bloom filter to avoid allocations on each write |
| `LoadingCoordinator` | Serializes concurrent loads per key (thundering herd prevention) |
| `TimestampConfig` | Configurable precision (ms/μs/ns) and epoch offset |
| `InternalShardConfig` | All shard configuration parameters |
| `WatchEventCollector` | Collects create/write/delete/trim events; broadcasts in order |
| `ReplicationClient` | Trait: replicate to follower TCP, replicate to S3, heartbeat, kick |
| `FollowerConnection<S: S3Uploader>` | Concrete TCP replication with split locks; manages reconnect |
| `StubReplicationClient` | Test/dev stub with simulated delays |
| `S3Uploader` | Trait for uploading fallback batches to S3 (injected from runtimes) |
| `S3Downloader` | Trait for listing/downloading/deleting S3 fallback batches |
| `StubS3Downloader` | Test/dev stub; always returns empty list |
| `S3ObjectRef` | `{ path: String, size: u64 }` — S3 object reference from listing |
| `S3CatchupResult` | `{ batches_applied, bytes_downloaded, rounds, fully_caught_up }` |
| `ReplicationCapturedData` | Snapshot taken between fsync and replication commit |
| `ReplicationDetails` | `ReplicatedToFollower` / `ReplicatedToS3(err)` — outcome enum |

## Key Functions

| Function | Purpose |
|----------|---------|
| `ShardWal::open` | Open or create shard WAL from config |
| `ShardWal::process_request` | Route `Request` enum to appropriate handler |
| `ShardWal::write` | Append events to aggregates |
| `ShardWal::read` | Read event batches with filtering |
| `ShardWal::delete` | Soft-delete aggregates |
| `ShardWal::trim_start` | Remove old event batches |
| `ShardWal::exists` | Check aggregate existence, returns `AggregateDetailsResponse` |
| `ShardWal::list_orgs/aggregate_types/aggregates` | Discovery with pagination |
| `ShardWal::enter_s3_catchup` | Entry point for follower S3 catchup; transitions node status |
| `ShardWal::close` | Flush and close shard |
| `Coordinator::request_sync` | Batched sync with delay (single-phase) |
| `Coordinator::request_sync_two_phase` | Two-phase: capture → clear orchestrator → commit |
| `Coordinator::acquire_rollback_lock` | Block new fsyncs during rollback |
| `capture_fsync_snapshot` | Phase-1 of fsync: take `SyncPositionsSnapshot` from memcache |
| `commit_fsync_with_rollback` | Phase-2 of fsync: write/sync disk, commit caches, or rollback |
| `capture_replication_snapshot` | Phase-1 of replication: take `ReplicationCapturedData` |
| `commit_replication_with_rollback` | Phase-2: paginated TCP or S3 replication with rollback |
| `catchup_from_s3` | S3 catchup loop: list → download → apply → fsync → delete |
| `apply_external_batch` | Validate WAL continuity and queue replicated entries |

## Write Flow

```
Client Write Request
        │
        ▼
┌───────────────────────────────────────┐
│  Phase 1: Validation (can fail)       │
│  • Empty events check                 │
│  • Zero event type check              │
│  • Node status / lease check          │
│  • Aggregate exists / allow_create    │
│  • Client idempotency check           │
│  • Optimistic concurrency check       │
│  • Build metablock + datablock        │
└───────────────────┬───────────────────┘
                    │ all validations pass
                    ▼
┌───────────────────────────────────────┐
│  Phase 2: Queue (cannot fail)         │
│  • Append to pending_append_queue     │
│  • Update queue positions             │
└───────────────────┬───────────────────┘
                    │
                    ▼
┌───────────────────────────────────────────────────────────────────┐
│  Phase 3: Fsync (Coordinator::request_sync_two_phase)             │
│  • Capture: take SyncPositionsSnapshot (while event still set)    │
│  • Clear orchestrator (followers now wait for this batch result)  │
│  • Write datablocks (growing down from EOF)                       │
│  • Write metablocks (growing up from header)                      │
│  • Update bloom filter, write dual headers                        │
│  • fdatasync()                                                    │
│  • Commit or rollback caches                                      │
│  • If leader: push to pending_replication queue (not yet visible) │
│  • If follower/standalone: advance read position, broadcast watch │
└───────────────────┬───────────────────────────────────────────────┘
                    │                     (leader path continues)
                    ▼
┌───────────────────────────────────────────────────────────────────┐
│  Phase 4: Replication (Coordinator::request_sync_two_phase)       │
│  • Capture: take pending_replication snapshot                     │
│  • Paginate batches across TCP (max_request_size chunks)          │
│  • If follower rejects (WalIndexMismatch):                        │
│      → Fetch older entries from local disk (catchup)              │
│      → If too far behind: fallback to S3                          │
│  • If follower offline / queue pressure > max_catchup_gap_bytes:  │
│      → Upload to S3 in max_s3_fallback_batch_bytes chunks         │
│      → Kick follower when done                                    │
│  • Commit: advance read positions, cache recent writes, broadcast │
│  • Rollback on failure: rewrite dual headers, fdatasync           │
└───────────────────┬───────────────────────────────────────────────┘
                    │
                    ▼
             Client ACK
```

### Validation Errors

| Check | Error | Purpose |
|-------|-------|---------|
| Empty events | `EmptyEventsList` | At least one event required |
| Event type = 0 | `ZeroEventType` | Reserved sentinel value |
| Node not authorized | `ShardCannotAcceptWrites` | Returns leader address for client redirect |
| Aggregate missing | `AggregateNotExists` | Unless `allow_create = true` |
| Client idempotency | `ClientIdempotencyViolation` | Reject duplicate `client_event_index` |
| OCC | `OptimisticConcurrencyViolation` | Expected batch index mismatch |
| Deleted aggregate | `AggregateRecreateNotAllowed` | Unless `allow_recreate = true` |

## Read Flow

```
Client Read Request
        │
        ▼
┌───────────────────────────────────────┐
│  1. Ensure aggregate cached           │
│     (LoadingCoordinator serializes)   │
└───────────────────┬───────────────────┘
                    │
                    ▼
┌───────────────────────────────────────┐
│  2. Collect metablocks (size-bounded) │
│     • Check recent write cache first  │
│     • Scan disk backwards if needed   │
│     • Apply metablock-level filters   │
│     • Evict newest when over budget   │
└───────────────────┬───────────────────┘
                    │
                    ▼
┌───────────────────────────────────────┐
│  3. Fetch datablocks                  │
│     • From cache: already have data   │
│     • Inline: deserialize immediately │
│     • Block: batch I/O per log file   │
└───────────────────┬───────────────────┘
                    │
                    ▼
┌───────────────────────────────────────┐
│  4. Apply event-level filters         │
│     • Event type whitelist            │
│     • Event index range               │
│     • Event timestamp range           │
│     • Client event index range        │
└───────────────────┬───────────────────┘
                    │
                    ▼
             ReadResponse
```

## Replication Architecture

### Normal Path: TCP Replication

```
Leader fsync completes
        │
        ▼ (pending_replication queue populated)
Replication coordinator batches writers
        │
        ▼
capture_replication_snapshot()
  → take pending queue from memcache
  → detect follower_falling_behind flag
        │
        ▼ (paginate across TCP, max_request_size chunks)
replicate_to_follower(batch)
  ├── Ok → drain batch, continue
  └── Rejected(WalIndexMismatch) → fetch_catchup_entries from disk
        ├── entries within max_catchup_gap_bytes → prepend + retry
        └── too far behind → fallback to S3
        │
        ▼ (all batches sent)
commit_replication()
  → advance read positions (data now visible)
  → cache_recent_write
  → broadcast watch events
```

### Fallback Path: S3 Replication

```
Condition: follower offline, behind, or workset > max_catchup_gap_bytes
        │
        ▼ (paginate in max_s3_fallback_batch_bytes chunks)
replicate_to_s3(batch)
  → serialize as FallbackBatch
  → upload to S3 path: cluster/fallback/shard_NNN/batch_START_END.bin
  └── If upload fails → rollback_replicate()
        │
        ▼ (after all chunks uploaded)
send_kick()
  → signal follower to run catchup
```

### Rollback Path

```
replicate_to_s3 fails (follower AND S3 both down)
        │
        ▼
acquire_rollback_lock()  → drains in-flight fsyncs, blocks new ones
        │
        ▼
execute_replication_rollback()  → clear in-memory caches
        │
        ▼ (for each affected log segment file)
rewrite dual headers to read position
fdatasync()
read datablocks carry-over bytes
        │
        ▼
Error propagated to all waiting writers
```

### Two-Phase Coordinator Pattern

Both fsync and replication use `request_sync_two_phase` to avoid a race where the orchestrator is cleared before the snapshot is taken:

```
Writer becomes leader
        │
        ├── fast path: sync_gate free AND no followers → try_write gate immediately
        └── slow path: sleep(delay), then wait for gate
        │
        ▼
capture_fn() called   ← snapshot taken BEFORE orchestrator cleared
        │
        ▼
orchestrator cleared  ← subsequent writers start a new batch
        │
        ▼
sync_fn(captured) called
        │
        ▼
event.notify(result)  ← all followers receive same result
```

## S3 Catchup Flow (Follower)

```
enter_s3_catchup()
  → transition node_status to FollowerCatchingUp
        │
        ▼
catchup_from_s3() loop (up to s3_download_max_rounds rounds):
  1. list_objects(shard prefix)
  2. parse and sort FallbackBatchRef by start_wal_index
  3. validate no WAL index gaps between consecutive batches
  4. for each batch in order:
     a. download(path)
     b. skip already-applied entries (partial overlap)
     c. apply_external_batch()
        → validate wal_index continuity + tip hash
        → queue entries via add_to_pending_queue
     d. sync_applied_batch()
        → coordinator captures + commits fsync (standalone mode)
     e. delete(path)  → remove from S3 once applied
  5. if round applied 0 batches → fully_caught_up = true, break
```

### S3 Path Format

```
cluster/fallback/shard_NNN/batch_SSSSSSSSS_EEEEEEEEE.bin
                 ^^^^^^^^^  ^^^^^^^^^^^^^^^^^^^^^^^^
                 zero-padded 3-digit shard ID
                            start wal_index  end wal_index (9-digit, zero-padded)
```

## Fsync Amortisation

The `Coordinator` batches multiple writers to amortise fsync cost:

```
Writer 1 ─┐
Writer 2 ─┼──► Coordinator ──► delay ──► Leader calls sync_fn()
Writer 3 ─┘                                     │
    ▲                                           │
    └───────── all receive same result ─────────┘
```

| Mode | Behavior |
|------|----------|
| Durable | Wait for fsync, batched by `fsync_delay` (typically 5-10ms) |
| Non-durable | Spawn fsync task, return immediately to client |
| Force immediate | Skip delay (after previous fsync failure) |

## In-Memory Filtering

### Metablock-Level (skip disk I/O)

| Filter | Check Against |
|--------|---------------|
| `from/to_event_batch_index` | `event_batch_index` bounds |
| `min/max_server_timestamp` | `server_timestamp` range |
| `include/exclude_client_id` | `client_id` match |
| `include/exclude_user_id` | `user_id` match |
| `min/max_client_event_index` | Overlaps with batch range |
| `min/max_event_timestamp` | Overlaps with batch range |
| `min/max_event_index` | Overlaps with batch range |
| `include_event_types` | Direct array or bloom filter |

### Event-Level (after deserialize)

Applied to individual events within kept batches for final filtering.

## Watch Integration

`WatchEventCollector` accumulates events during commit and broadcasts them after all caches are updated. Write events for the same aggregate are merged into a single range notification.

| Method | Behavior |
|--------|----------|
| `add_write_event(batch)` | Insert or extend `from..to_event_batch_index` range |
| `add_create_event(key)` | Deduplicated per aggregate |
| `add_delete_event(key)` | Deduplicated per aggregate |
| `add_trim_event(key, idx)` | First trim wins (or_insert) |
| `broadcast_all(watchers)` | Fires in order: Create → Write → Delete → Trim |

When is each event fired:

| Event Type | Trigger | Path |
|------------|---------|------|
| `Create` | `event_batch_index == FIRST_EVENT_BATCH_INDEX` | On fsync commit (follower/standalone) or replication commit (leader) |
| `Write` | Event batch appended | Same as above |
| `Delete` | Soft delete committed | Same as above |
| `TrimStart` | Trim operation committed | Same as above |

## List Operations

Reverse WAL scanning with pagination for discovery:

```rust
// List all orgs in shard
list_orgs(ListOrgsRequest { cursor: None, .. })

// List aggregate types filtered by org
list_aggregate_types(ListAggregateTypesRequest { org_id: Some(123), .. })

// List aggregates with full metadata
list_aggregates(ListAggregatesRequest { org_id: Some(123), aggregate_type_id: Some(456), .. })
```

Features:
- Time-bounded scans (`list_max_duration`)
- LRU deduplication within page
- WAL index position caching for fast cursor resumption (`list_wal_index_cache_bytes`)
- Returns metadata: batch counts, index ranges, timestamps, sizes

## Error Types

### Operation Errors

| Error | Returned By |
|-------|-------------|
| `ShardWriteError` | `write` — validation failures, fsync, replication |
| `ShardReadError` | `read` — not found, size limits, I/O |
| `ShardFsyncError` | `commit_fsync_with_rollback` — I/O, space, corruption |
| `ShardDeleteError` | `delete` — not exists, OCC, fsync, replication |
| `ShardTrimError` | `trim_start` — not exists, out of range, fsync, replication |
| `ShardAggregateDetailsError` | `exists` (was `ShardExistsError`) — not found, cache error |
| `ShardListingError` | `list_*` — disk scan errors |
| `ShardError` | `process_request` — wraps all of the above |

### Replication Errors

| Error | Purpose |
|-------|---------|
| `ReplicationError` | Top-level: rollback in progress, rollback failed, S3 error, catchup failure |
| `ReplicationRollbackFailure` | Lock timeout, file unavailable, header write error, fdatasync failure |
| `ReplicateToFollowerError` | Network error, rejected, server error, too far behind, lock timeout |
| `ReplicateToS3Error` | Not configured, unavailable, put failed, serialization failed |
| `SendHeartbeatError` | Connection failure, unexpected response, lock timeout |
| `FollowerReplicationWriteError` | Follower-side: fsync error, serialization, WAL index gap |
| `FetchCatchupEntriesError` | Follower too far behind, disk read error |

### S3 Catchup Errors

| Error | Purpose |
|-------|---------|
| `S3CatchupError` | List/get/delete failed, deserialization failed, WAL index gap, apply failed, fsync failed |
| `ApplyBatchError` | WAL index mismatch, tip hash mismatch, batch gap, missing datablock, serialization |

## Configuration

| Field | Purpose |
|-------|---------|
| `node_id` | Identifies this node in the cluster |
| `shard_id` | This shard's ID |
| `fsync_delay` | Amortisation batch window |
| `replication_delay` | Replication batch window |
| `max_response_size` | Size bound for read responses |
| `max_request_size` | Max TCP request size (limits replication page size) |
| `read_max_chunk_size` | Disk read chunk size |
| `shard_log_preallocate_bytes` | Log file size |
| `max_open_files` | LRU cache for log files |
| `recent_write_cache_bytes` | Hot write cache size |
| `aggregate_snapshots_cache_bytes` | Position cache size |
| `aggregate_client_snapshots_cache_bytes` | Client idempotency index cache size |
| `list_page_size` | Results per list page |
| `list_max_duration` | Max time for list scan |
| `list_wal_index_cache_bytes` | WAL position cache for fast list cursor resumption |
| `pending_replication_high_water_bytes` | Queue pressure threshold → S3 fallback |
| `max_catchup_gap_bytes` | Workset size threshold → S3 fallback instead of TCP catchup |
| `max_s3_fallback_batch_bytes` | Max bytes per S3 fallback upload chunk |
| `s3_download_max_rounds` | Max catchup rounds per `enter_s3_catchup` call |
| `max_cluster_time_drift_ms` | Max tolerated clock skew between leader and follower |
| `timestamp_config` | Precision and epoch settings |

## Timestamp Configuration

```rust
pub struct TimestampConfig {
    pub precision: TimestampPrecision,  // Milliseconds, Microseconds, Nanoseconds
    pub epoch_offset_secs: i64,         // Custom epoch offset from Unix epoch
}

let config = TimestampConfig {
    precision: TimestampPrecision::Microseconds,
    epoch_offset_secs: 1704067200,  // Custom epoch: 2024-01-01
};
let timestamp = config.now();  // Microseconds since custom epoch
```

## Loading Coordinators

Prevent thundering herd when multiple async tasks request the same data:

```rust
// Only one task loads; others wait
let guard = self.aggregate_loading.acquire(&aggregate_key);
let _ = write_with_timeout(&guard, "context").await?;

// Check again (another task may have loaded while we waited)
if already_loaded { return Ok(()); }

// Perform expensive load...
```

Two coordinators:
- `aggregate_loading` — Aggregate snapshot loading from disk
- `aggregate_client_loading` — Client idempotency index loading

## Hash Chain

Every metablock includes `previous_tip_hash`, forming a blake3 chain over the WAL. The chain excludes `datablock_position` (a node-local offset that legitimately differs between leader and follower). This allows followers to verify integrity without requiring identical on-disk layout.

```
tip_hash[n] = blake3(tip_hash[n-1] || metablock_bytes_excluding_datablock_position)
```

`apply_external_batch` validates both `wal_index` continuity and `previous_tip_hash` before queuing replicated entries.

## Thread Safety

`ShardWal` is **not thread-safe**. Designed for single-threaded async execution per shard (thread-per-core architecture). Uses:

- `Rc<RefCell<_>>` for interior mutability
- `glommio::sync::RwLock` for async coordination within single thread
- `Cell` for lock-free flags

`FollowerConnection<S>` uses split `RwLock`s for the replication TCP connection and the heartbeat TCP connection so that a slow heartbeat cannot block an ongoing replication and vice versa.

## Dependencies

| Crate | Purpose |
|-------|---------|
| `celeriant_memcache` | In-memory state (queues, positions, cache) — see its README for dual-cache design |
| `celeriant_rotating_log` | Direct I/O log file management |
| `celeriant_wal` | Metablock/datablock types, hash chain constants |
| `celeriant_wire` | Serialization (versioned blocks, CRC) |
| `celeriant_watch` | Watch subscription system |
| `celeriant_msg` | Request/response types |
| `celeriant_disk` | DMA read utilities, rwlock timeout helpers |
| `celeriant_distributed` | `NodeStatus`, S3 path helpers, heartbeat utilities |
| `celeriant_client_glommio` | TCP client for follower replication |
| `glommio` | Async runtime |
| `fastbloom` | Bloom filter implementation |
| `lru` | LRU cache for bounded collections |
| `blake3` | Hash chain computation |
