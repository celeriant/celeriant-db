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

## Invariants

- Client ACK is withheld until local `fdatasync()` succeeds AND replication succeeds (TCP or S3 fallback). Both must complete before the response is sent.
- OCC validation runs before client idempotency checks. A concurrent writer with a stale read receives `OccConflict`, not `ClientIdempotencyViolation`.
- S3 replication uploads a single file per batch. Splitting into sub-batches is prohibited, it creates WAL sequence gaps on the consumer.
- Pending replication entries are never silently dropped. If rollback fails, entries are requeued for the next replication cycle.
- The rollback lock (`sync_gate` write-lock) blocks all concurrent writes. Any in-flight fsync completes before the lock is granted.
- Rollback is durable: dual headers are rewritten and `fdatasync()` completes before the lock is released.
- The coordinator's Phase 1 (capture snapshot) runs before Phase 2 (clear queue), preventing a race where a new leader finds an empty queue.
- A kick is always attempted after S3 fallback replication succeeds, regardless of TCP reachability state.
- On promotion, the new leader uploads a "promotion batch" to S3 covering the last TCP-replicated batch.
- S3 catchup handles mid-batch resume: entries at or before the current WAL sequence are skipped.
- Hash computation excludes `datablock_position` so leader and follower produce identical hashes despite different on-disk layouts.
- `RefCell` borrows must NEVER be held across `.await` points. Snapshot into owned data, drop borrow, await, re-borrow to commit.

## Key Types

| Type | Purpose |
|------|---------|
| `ShardWal<R, D>` | Main orchestrator; generic over replication and S3 traits |
| `Coordinator<E>` | Fsync/replication batching with two-phase capture |
| `CaptureResult<T, E>` | `Captured(T)` / `Failed(E)` / `NoCaptureRaceButOk` for two-phase sync |
| `LocalEvent<T>` | Single-threaded async event; notifies all waiting `LocalEventListener<T>`s |
| `BloomFilterCache` | Reusable bloom filter to avoid allocations on each write |
| `LoadingCoordinator` | Serializes concurrent loads per key (thundering herd prevention) |
| `WatchEventCollector` | Collects create/write/delete/trim events; broadcasts in order |
| `ReplicationClient` | Trait: replicate to follower TCP, replicate to S3, heartbeat, kick |
| `FollowerConnection<S: S3Uploader>` | Concrete TCP replication with split locks; manages reconnect |
| `ReplicationCapturedData` | Snapshot taken between fsync and replication commit |
| `ReplicationDetails` | `ReplicatedToFollower` / `ReplicatedToS3(err)` outcome enum |
| `S3CatchupResult` | `{ batches_applied, bytes_downloaded, rounds, fully_caught_up }` |

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
│  • OCC check                          │
│  • Client idempotency check           │
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
│  • If follower rejects (WalSeqMismatch):                        │
│      → Fetch older entries from local disk (catchup)              │
│      → If too far behind: fallback to S3                          │
│  • If follower offline / queue pressure > high water mark:         │
│      → Upload single file to S3                                   │
│      → Kick follower (best-effort)                                │
│  • Commit: advance read positions, cache recent writes, broadcast │
│  • Rollback on failure: rewrite dual headers, fdatasync           │
└───────────────────┬───────────────────────────────────────────────┘
                    │
                    ▼
             Client ACK
```

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
  └── Rejected(WalSeqMismatch) → fetch_catchup_entries from disk
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
        ▼ (single file per batch, never split)
replicate_to_s3(batch)
  → serialize as FallbackBatch
  → upload to S3 (see S3 Path Format below)
  └── If upload fails → rollback_or_requeue()
        │
        ▼
send_kick()
  → signal follower to run catchup (best-effort, regardless of TCP reachability)
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
  2. parse and sort FallbackBatchRef by start_wal_seq
  3. validate no WAL sequence gaps between consecutive batches
  4. for each batch in order:
     a. download(path)
     b. skip already-applied entries (partial overlap)
     c. apply_external_batch()
        → validate wal_seq continuity + tip hash
        → queue entries via add_to_pending_queue
     d. sync_applied_batch()
        → coordinator captures + commits fsync (standalone mode)
     e. delete(path)  → remove from S3 once applied
  5. if round applied 0 batches → fully_caught_up = true, break
```

### S3 Path Format

```
cluster/fallback/shard_{shard_id:03}/batch_{start:09}_{end:09}_{node_uuid}.bin
```

Example: `cluster/fallback/shard_002/batch_000000005_000000010_00000000-0000-0000-0000-000000000000.bin`

Zero-padded so lexicographic ordering = temporal ordering. The `node_uuid` suffix identifies which node uploaded the batch (followers skip batches they uploaded themselves).

## Fsync Amortisation

The `Coordinator` batches multiple writers to amortise fsync cost:

```
Writer 1 ─┐
Writer 2 ─┼──► Coordinator ──► delay ──► Leader calls sync_fn()
Writer 3 ─┘                                     │
    ▲                                           │
    └───────── all receive same result ─────────┘
```


## Watch Integration

`WatchEventCollector` accumulates events during commit and broadcasts them after all caches are updated. Write events for the same aggregate are merged into a single range notification. Broadcast order: Create → Write → Delete → Trim.

Watch events fire after the write is durably replicated (leader) or after fsync (non-leader). Never before.

## Hash Chain

Every metablock includes `previous_tip_hash`, forming a blake3 chain over the WAL. The chain excludes `datablock_position` (a node-local offset that legitimately differs between leader and follower). This allows followers to verify integrity without requiring identical on-disk layout.

```
tip_hash[n] = blake3(tip_hash[n-1] || metablock_bytes_excluding_datablock_position)
```

`apply_external_batch` validates both `wal_seq` continuity and `previous_tip_hash` before queuing replicated entries.
