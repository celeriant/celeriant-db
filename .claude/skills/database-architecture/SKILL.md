---
name: database-architecture
description: Core architecture invariants for Celeriant. Memory bounds, WAL durability, write pipeline, storage layout, and tracing rules. Use when implementing features, reviewing code, or understanding why patterns exist.
---

# Database Architecture

## Memory: Bounded Everything

Celeriant supports millions of aggregates per shard. No data structure grows proportional to cardinality.

All long-lived caches use `LruCache` with byte-based capacity derived from a per-shard memory budget. Total memory = `detected_memory * CELERIANT_MEMORY_CONSUMPTION_PERCENT / 100` (default 80%, respects cgroup limits). Divided equally across shards, then split into fixed ratios: recent_write 71.5%, aggregate_snapshots 9%, client_snapshots 9%, schema_cache 9%, WAL index 1.5%.

Scan pollution prevention: entries from WAL scans insert at low priority, only into spare capacity, immediately demoted to LRU tail. A list operation can't flush the hot working set.

Intentionally unbounded exceptions: `pending_append_queue` and `aggregate_queue_positions` are transient in-flight state that drains every fsync cycle (milliseconds). `pending_replication_batches` is bounded indirectly by `pending_replication_high_water_bytes` triggering S3 fallback.

Periodically shrink collections that grew temporarily - `shrink_to_fit()` when capacity > 2x length.

## Write Pipeline

**queue -> fsync (amortised) -> replication (amortised) -> ACK**

Multiple concurrent writers coalesce into a single fsync, then a single replication round. Under low load, the fast path executes immediately. Under high load, delay-based batching kicks in. One fsync and one TCP round-trip can serve hundreds of writers.

The fsync coordinator's two phases: capture snapshot first, then clear queue. This ordering prevents a race where a new leader finds an empty queue.

## Durability

Never acknowledge a write before it's durable. The disk write order within a single fsync: datablocks first (offsets known), metablocks (referencing those offsets), dual headers, then `fdatasync()`. In-memory state updates only after fsync succeeds.

Both primary header (offset 0) and backup header (end of file) are written on every fsync. If the primary is corrupt on recovery, the backup is used. CRC32C validates before deserialisation.

All I/O uses Direct I/O (`O_DIRECT` via glommio `DmaFile`). Bypasses the kernel page cache entirely. Buffered I/O is vulnerable to silent data loss on fsync failure. DMA writes aligned to 4096 bytes, with carry-over buffers for unaligned datablock positions.

Replication is synchronous. Client gets an ACK only after both leader and follower have fsynced. If replication fails (follower and S3 both down), rollback fires: revert write cursor to read cursor, clear all un-replicated in-memory state, rewrite headers at rolled-back positions, fsync. Durable before any new writes.

## Storage Layout

File layout: `[Header 512KB] [Metablocks growing down ->] [Free space] [<- Datablocks growing up] [Header 512KB]`.

Metablocks are fixed 1024 bytes. Datablocks grow backwards from the bottom. File rotates when they meet. Log segments are preallocated at creation (minimum 1.5MB).

Small events (<= 512 bytes serialised) live inline in the metablock itself, avoiding a separate disk seek. Auto-selected transparently.

Each segment carries a 256KB bloom filter (10 hashes, <1% FP at 200k aggregates). The reverse WAL scanner skips entire segments with a single bloom check. This is how unlimited cardinality works without unlimited index memory.

The `small-metablock` compile-time feature halves metablock size (1024 -> 512 bytes) and inline threshold (512 -> 128 bytes). Affects the on-disk format and replication wire format only. Client request/response protocol is unaffected. Both cluster nodes must be compiled with the same setting.

## Read Visibility

Two separate LRU caches: write snapshots (updated after fsync, used for OCC/idempotency) and read snapshots (updated after replication on leader, used by reads). On leader, writes are invisible to readers until replication completes.

Recent write cache entries carry WAL index. Reads filter by `visible_wal_index`, excluding un-replicated data. The hot cache can hold speculative data without leaking it.

Read operations are never rejected based on node status. A fenced or catching-up node serves stale reads silently.

## OCC and Idempotency

OCC checks use the write-ahead snapshot, not the read snapshot. A write fsynced but not yet replicated still triggers OCC conflict.

Check ordering matters: OCC first, then idempotency. If OCC fails, client retries with fresh state. If OCC passes but idempotency fails, the exact write already landed (crash recovery scenario, treat as success). Reversing this order creates false-positive "already landed" results from concurrent writers that use the same client event index source.

## Tracing

No `info!` or `debug!` in hot paths. This generates gigabytes of logs and degrades performance.

- `error!`: unrecoverable failures, data integrity issues (fsync failure, corruption)
- `warn!`: recoverable issues (client disconnect, timeout, retry)
- `info!`: startup, shutdown, configuration, rare events
- `trace!`: per-request detail, disabled by default

Use structured fields (`aggregate_key = ?key, error = %err, "Write failed"`) not string interpolation.
