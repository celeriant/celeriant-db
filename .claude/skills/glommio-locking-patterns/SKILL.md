---
name: glommio-locking-patterns
description: Locking patterns for Glommio's single-threaded async executors. Use when working with shard read/write paths, replication, or any async code needing synchronization. Critical for avoiding deadlocks.
---

# Glommio Locking Patterns

## The Rule

Glommio is single-threaded per executor. No thread contention, but async tasks interleave at `.await` points. The one rule that matters:

**Never hold a `RefCell` borrow across an `.await` boundary.** Another task will try to borrow while you're suspended. Deadlock under load.

## RefCell vs RwLock

`RefCell` when all operations on the data are synchronous. No `.await` while borrowed.

Glommio `RwLock` when you need to hold across async operations (file I/O, network). More overhead, but safe across `.await`.

If you're unsure, ask: will I `.await` while holding this? Yes = RwLock. No = RefCell.

## Snapshot Before Async

The core pattern for working with RefCell-wrapped state:

1. Borrow, snapshot into owned data, drop borrow (sync)
2. Do async work with the snapshot (no borrow held)
3. Re-borrow, commit or rollback (sync)

This is how the fsync path works. `take_sync_positions_snapshot()` clones queue positions and swaps out the pending queue, then the borrow drops. Async disk I/O happens with no borrow held. Then commit or rollback re-borrows briefly.

## RwLock Timeouts

All RwLock acquisitions use `read_with_timeout` / `write_with_timeout` wrappers with a 1-second deadline. Timeout returns `PotentialDeadlock` error with a location string instead of blocking forever. This catches bugs during development and prevents stuck tasks under high load.

Always use these wrappers. Always pass a descriptive location string.

See `celeriant_rotating_log/src/rwlock_timeout.rs`.

## Loading Coordinator (Thundering Herd)

When multiple async tasks want the same cold aggregate from disk, only one does the I/O. Others wait on a per-key RwLock, then double-check the cache. Without the double-check after acquiring the lock, you get duplicate I/O.

See `celeriant_shard/src/loading_coordinator.rs`.

## Amortisation Coordinator (Fsync Batching)

Multiple concurrent writers coalesce into one fsync. First caller becomes leader (sleeps for configurable delay to accumulate followers), then performs the sync and broadcasts the result via `LocalEvent`. Followers never call the sync function. At most one sync executes at a time.

Key subtlety: the snapshot is captured before the queue is cleared. Reversing this loses writes that arrive between clear and snapshot.

See `celeriant_shard/src/amortisation/coordinator.rs`.

## Semaphores (Concurrency Limiting)

Glommio `Semaphore` limits how many async tasks can do expensive work concurrently:

- `list_semaphore`: bounds concurrent list operations. Lists allocate unbudgeted per-request memory, so unlimited concurrency can exhaust RAM.
- `cache_load_semaphore`: bounds concurrent cache-miss disk scans. Without this, cold start read amplification saturates NVMe reads and starves the fsync write path.

Both configured via `list_max_concurrent` and `read_max_concurrent` in shard config.

## Sync Gate (Rollback Serialisation)

The `sync_gate: RwLock<()>` in the amortisation coordinator serialises fsync execution and supports rollback. Normal fsyncs acquire a read lock (concurrent reads OK, one-at-a-time via the coordinator). Rollback acquires the write lock, which drains all in-flight fsyncs and blocks new ones until rollback completes.
