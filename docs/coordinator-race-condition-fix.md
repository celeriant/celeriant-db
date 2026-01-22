# Coordinator Race Condition: Analysis and Fix

## Executive Summary

The `Coordinator` used for fsync and replication batching has a race condition where a leader can "steal" items from a subsequent leader's batch, causing the subsequent leader to find an empty queue and error out—even though all items were successfully synced.

---

## Background: How the Coordinator Works

The `Coordinator` implements **delayed sync with writer coalescing**:

1. Multiple writers call `request_sync()` concurrently
2. The first writer becomes the **leader**, others become **followers**
3. The leader sleeps for a configurable delay (e.g., 5ms) to accumulate more followers
4. The leader executes the sync function (fsync to disk, replicate to followers)
5. All followers receive the leader's result

This batching amortizes the cost of expensive operations (fsync, network replication) across many writers.

```
Writers:     A ──────────────────────────────────► Ok
             B ────────────────────────────────► Ok
             C ──────────────────────────────► Ok
                    │                    │
                    │   batch together   │
                    ▼                    ▼
             ┌─────────────┐      ┌───────────┐
             │    Sleep    │ ───► │   Fsync   │
             │  (5ms delay)│      │  (batch)  │
             └─────────────┘      └───────────┘
```

---

## The Problem: Race Between Leaders

### Current Code Flow

```rust
// coordinator.rs - request_sync() leader path
Acquired::Leader(event) => {
    glommio::timer::sleep(delay).await;           // 1. Sleep for delay

    if let Ok(mut guard) = write_with_timeout(&self.lock_orchestrator, ...).await {
        guard.take();                              // 2. Clear orchestrator ← PROBLEM
    }

    let _sync_guard = write_with_timeout(&self.sync_gate, ...).await.ok();
    let result = sync_fn().await;                  // 3. sync_fn takes snapshot internally
    drop(_sync_guard);

    event.notify(result.clone());                  // 4. Notify followers
    return result;
}
```

The issue: **Step 2 (clear orchestrator) happens BEFORE Step 3 (take snapshot)**

This creates a window where:
- A new leader can be elected (sees empty orchestrator)
- But the old leader hasn't taken its snapshot yet
- The old leader's snapshot captures items intended for the new leader

### Race Condition Timeline

```
Time    Leader A                    Writer C                    Queue           Orchestrator
────    ────────                    ────────                    ─────           ────────────
T1      adds item                                               [A]
T2      becomes leader                                          [A]             A's event
T3      sleeps...                                               [A]             A's event
T4                                  adds item                   [A, C]          A's event
T5      wakes up                                                [A, C]          A's event
T6      clears orchestrator ─────────────────────────────────── [A, C] ──────── None ◄── WINDOW OPENS
T7                                  request_sync()              [A, C]
T8                                  sees None, becomes leader   [A, C]          C's event
T9                                  sleeps...                   [A, C]          C's event
T10     acquires sync_gate                                      [A, C]          C's event
T11     takes snapshot ──────────────────────────────────────── [] ◄─────────── C's event
        (captures A AND C's items!)
T12     syncs [A, C] to disk                                    []              C's event
T13     notifies A's followers                                  []              C's event
T14     returns Ok                                              []              C's event
T15                                 wakes up                    []              C's event
T16                                 clears orchestrator         []              None
T17                                 acquires sync_gate          []              None
T18                                 takes snapshot ─────────────────────────── None
                                    QUEUE IS EMPTY! ◄── ERROR
```

### The Paradox

- C's item **was successfully synced** (by Leader A at T12)
- But C **returns an error** because it found an empty queue
- C's followers (if any) also receive the error
- From the application's perspective: **writes that succeeded appear to fail**

---

## Visual Diagram of the Race

```
                    ┌─────────────────────────────────────────────────────────────┐
                    │                    RACE WINDOW                               │
                    │     (between clear orchestrator and take snapshot)           │
                    └─────────────────────────────────────────────────────────────┘
                                              │
                                              ▼
    Leader A        ═══════╤════════════╤═════════════════╤══════════════╤═════════
                           │            │                 │              │
                         sleep    clear orch.      take snapshot      notify
                                        │                 │
                                        │    ┌───────────┐│
                                        │    │ C's item  ││
                                        │    │ captured! ││
                                        │    └───────────┘│
                                        │                 │
    Writer C        ────────────────────┼─────────────────┼──────────────────────
                           │            │                 │              │
                       add item    become leader        sleep        find empty!
                                   (sees None)                         ERROR
                                        │
                                        └── New leader elected before
                                            old leader takes snapshot
```

---

## The Fix: Two-Phase Sync Function

### Core Insight

The problem is that **"taking the snapshot"** and **"clearing the orchestrator"** happen in the wrong order. We need:

1. Take snapshot (capture exactly which items are in this batch)
2. Clear orchestrator (allow new leaders for NEW items only)
3. Execute sync with captured items

But currently, the coordinator doesn't control when the snapshot is taken—that happens inside `sync_fn()`.

### Solution: Split sync_fn into capture + commit

Instead of one closure, the coordinator accepts two:

```rust
pub async fn request_sync_two_phase<C, S, CapturedData, Fut1, Fut2>(
    &self,
    delay: Option<Duration>,
    capture_fn: C,    // Phase 1: Take snapshot, return captured data
    sync_fn: S,       // Phase 2: Process captured data
) -> SyncResult<E>
where
    C: FnOnce() -> Fut1,
    Fut1: Future<Output = Result<CapturedData, E>>,
    S: FnOnce(CapturedData) -> Fut2,
    Fut2: Future<Output = SyncResult<E>>,
```

### New Code Flow

```rust
Acquired::Leader(event) => {
    glommio::timer::sleep(delay).await;           // 1. Sleep for delay

    let _sync_guard = write_with_timeout(&self.sync_gate, ...).await.ok();

    let captured = capture_fn().await;             // 2. Take snapshot FIRST

    if let Ok(mut guard) = write_with_timeout(&self.lock_orchestrator, ...).await {
        guard.take();                              // 3. THEN clear orchestrator
    }

    let result = match captured {
        Ok(data) => sync_fn(data).await,           // 4. Sync with captured data
        Err(e) => Err(e),
    };

    drop(_sync_guard);
    event.notify(result.clone());                  // 5. Notify followers
    return result;
}
```

### Fixed Timeline

```
Time    Leader A                    Writer C                    Queue           Orchestrator
────    ────────                    ────────                    ─────           ────────────
T1      adds item                                               [A]
T2      becomes leader                                          [A]             A's event
T3      sleeps...                                               [A]             A's event
T4                                  adds item                   [A, C]          A's event
T5      wakes up                                                [A, C]          A's event
T6      acquires sync_gate                                      [A, C]          A's event
T7      capture_fn() ────────────────────────────────────────── [] ◄─────────── A's event
        (takes snapshot: [A, C])
T8      clears orchestrator ─────────────────────────────────── [] ──────────── None
T9                                  request_sync()              []
T10                                 sees None, becomes leader   []              C's event
T11                                 sleeps...                   []              C's event
T12     sync_fn([A, C])                                         []              C's event
T13     syncs to disk                                           []              C's event
T14     notifies A's followers                                  []              C's event
        (C was follower! Gets Ok)
T15     returns Ok                                              []              C's event
T16                                 wakes up                    []              C's event
T17                                 acquires sync_gate          []              C's event
T18                                 capture_fn() ───────────────────────────── C's event
                                    returns Ok(empty) or
                                    short-circuits ◄── NO ERROR, nothing to do
```

### Why This Works

1. **Snapshot happens while orchestrator still has the event**
   - Any writer that arrives sees the event and becomes a follower
   - Their items go into the queue and get captured in the snapshot

2. **Orchestrator cleared only after snapshot**
   - New leaders can only emerge for items added AFTER the snapshot
   - No items can be "stolen" from the new leader's batch

3. **Followers are correctly associated with batches**
   - If you became a follower, your item was in the queue when snapshot was taken
   - If you became a leader, you're handling genuinely new items

---

## Implementation Details

### Changes to Coordinator

```rust
// coordinator.rs

/// Result of a capture operation
pub enum CaptureResult<T, E: Clone> {
    /// Data was captured, proceed with sync
    Captured(T),
    /// Nothing to capture, previous sync handled our items
    NothingToCapture,
    /// Capture failed (e.g., rollback occurred)
    Failed(E),
}

impl<E: Clone> Coordinator<E> {
    /// Two-phase sync: capture then commit
    pub async fn request_sync_two_phase<C, S, T, Fut1, Fut2>(
        &self,
        delay: Option<Duration>,
        capture_fn: C,
        sync_fn: S,
    ) -> SyncResult<E>
    where
        C: FnOnce() -> Fut1,
        Fut1: std::future::Future<Output = CaptureResult<T, E>>,
        S: FnOnce(T) -> Fut2,
        Fut2: std::future::Future<Output = SyncResult<E>>,
    {
        let delay = match delay {
            Some(d) if d.as_micros() > 0 => d,
            _ => Duration::from_millis(0),
        };

        loop {
            let acquired = { /* ... same as before ... */ };

            match acquired {
                Acquired::Leader(event) => {
                    glommio::timer::sleep(delay).await;

                    // Acquire sync gate BEFORE capture
                    let _sync_guard = write_with_timeout(&self.sync_gate, "sync_gate").await.ok();

                    // Phase 1: Capture snapshot
                    let captured = capture_fn().await;

                    // NOW clear orchestrator - new leaders can start for new items
                    if let Ok(mut guard) = write_with_timeout(
                        &self.lock_orchestrator,
                        "clear_orchestrator"
                    ).await {
                        guard.take();
                    }

                    // Phase 2: Process captured data
                    let result = match captured {
                        CaptureResult::Captured(data) => sync_fn(data).await,
                        CaptureResult::NothingToCapture => Ok(()),
                        CaptureResult::Failed(e) => Err(e),
                    };

                    drop(_sync_guard);
                    event.notify(result.clone());
                    return result;
                }
                Acquired::Follower(event) => return event.listen().await,
                Acquired::Retry => continue,
            }
        }
    }
}
```

### Changes to Fsync Path

```rust
// shard_wal_sync.rs

/// Capture phase: take snapshot from memcache
fn capture_sync_snapshot(
    shard_mem_cache: &Rc<RefCell<ShardMemCache>>,
) -> CaptureResult<(u64, SyncPositionsSnapshot), ShardFsyncError> {
    let mut cache = shard_mem_cache.borrow_mut();

    if cache.pending_append_queue_is_empty() {
        // Check if this is due to rollback or just race condition
        if cache.had_fsync_rollback() {
            return CaptureResult::Failed(ShardFsyncError::RollbackInvalidatedWrites);
        }
        // Empty queue, but no rollback = previous sync got our items
        return CaptureResult::NothingToCapture;
    }

    let required_disk_space = cache.buffer_size_total();
    let snapshot = cache.take_sync_positions_snapshot();

    CaptureResult::Captured((required_disk_space, snapshot))
}

/// Commit phase: write captured data to disk
async fn commit_sync(
    log_segments_cache: Rc<LogSegmentsCache>,
    shard_mem_cache: Rc<RefCell<ShardMemCache>>,
    watched_aggregates: Rc<AggregateWatchers>,
    cluster_role: ClusterRole,
    captured: (u64, SyncPositionsSnapshot),
) -> Result<(), ShardFsyncError> {
    let (required_disk_space, mut sync_positions_snapshot) = captured;

    log_segments_cache.rotate_to_next_log(required_disk_space).await?;
    let active_log_segment = log_segments_cache.active();

    match sync(active_log_segment.clone(), &mut sync_positions_snapshot).await {
        Ok(metadata) => {
            commit_sync_to_cache(/* ... */);
            Ok(())
        }
        Err(e) => {
            rollback_sync(shard_mem_cache, &log_segments_cache);
            Err(e)
        }
    }
}
```

### Changes to shard_wal.rs

```rust
// shard_wal.rs

async fn sync_durable(&self) -> Result<(), ShardFsyncError> {
    let log_cache = self.log_segments_cache.clone();
    let mem_cache = self.shard_mem_cache.clone();
    let watchers = self.watched_aggregates.clone();
    let role = self.cluster_role.clone();

    self.fsync_coordinator
        .request_sync_two_phase(
            Some(self.config.fsync_delay),
            // Capture phase
            move || {
                let mc = mem_cache.clone();
                async move { capture_sync_snapshot(&mc) }
            },
            // Commit phase
            move |captured| {
                commit_sync(log_cache, mem_cache, watchers, role.get(), captured)
            },
        )
        .await
}
```

---

## Verification: All Scenarios Handled

### Scenario 1: Normal Operation (No Race)

```
A adds item → A becomes leader → A sleeps → A captures [A] → A clears orch → A syncs → A returns Ok
```
✅ Works as before

### Scenario 2: Multiple Followers

```
A adds → A leader → A sleeps → B adds, follower → C adds, follower →
A captures [A,B,C] → A clears → A syncs → A,B,C all get Ok
```
✅ All followers correctly batched

### Scenario 3: The Race Condition (FIXED)

```
A adds → A leader → A sleeps → C adds → A captures [A,C] → A clears →
C becomes leader → C sleeps → A syncs [A,C] → A notifies (C was follower!) →
C wakes → C captures (empty) → C returns Ok (nothing to do)
```
✅ C correctly identified as having nothing to do

### Scenario 4: Rollback Scenario

```
A adds → A leader → A sleeps → A captures [A] → A clears →
A sync FAILS → A rollback (sets flag, clears queue) →
B adds → B leader → B captures → sees rollback flag → returns Error
```
✅ Rollback correctly propagates error

### Scenario 5: Items Added After Capture

```
A adds → A leader → A sleeps → A captures [A] → A clears →
D adds (after capture) → D becomes leader →
A syncs [A] → A returns Ok →
D captures [D] → D syncs [D] → D returns Ok
```
✅ New items correctly handled by new leader

---

## Migration Path

1. Add `request_sync_two_phase` to `Coordinator` (new method, doesn't break existing)
2. Add `CaptureResult` enum
3. Refactor `sync_with_rollback` into `capture_sync_snapshot` + `commit_sync`
4. Update `shard_wal.rs` to use two-phase API
5. Apply same pattern to `replicate_with_rollback`
6. Optionally deprecate single-phase `request_sync`

---

## Testing Strategy

1. **Unit test the race window**: Spawn leader, inject delay before capture, spawn second leader, verify no error
2. **Stress test**: High concurrency writes with short delays, verify no spurious errors
3. **Rollback test**: Inject sync failure, verify subsequent leader gets proper error
4. **Benchmark**: Verify batching efficiency unchanged

---

## Summary

| Aspect | Before | After |
|--------|--------|-------|
| Orchestrator cleared | Before snapshot | After snapshot |
| Race window | Between clear and snapshot | Eliminated |
| Empty queue handling | Always error | Distinguish race vs rollback |
| API | Single `sync_fn` | Two-phase `capture_fn` + `sync_fn` |
| Follower association | Can be wrong | Always correct |
