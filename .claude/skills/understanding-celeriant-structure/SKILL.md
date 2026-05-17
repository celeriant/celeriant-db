---
name: understanding-celeriant-structure
description: Celeriant codebase architecture, crate responsibilities, and how they fit together. Use when navigating the codebase, understanding what a crate does, or figuring out where to make changes.
---

# Celeriant Project Structure

Fast, distributed write-ahead log for event sourcing. Thread-per-core (glommio/io_uring). Two-node cluster with S3 for coordination and backup replication.

## Layers (top to bottom)

1. **Binary**: `celeriant` (server), `celeriant_cli` (CLI/TUI)
2. **Runtime**: `celeriant_runtimes` (glommio orchestration, shard routing, inter-shard channels), `celeriant_sidecar` (tokio bridge for S3)
3. **Shard**: `celeriant_shard` (WAL orchestrator, fsync batching, validation), `celeriant_watch` (subscriptions)
4. **Storage**: `celeriant_memcache` (caches, queues), `celeriant_rotating_log` (log files, bloom filters), `celeriant_disk` (DMA I/O)
5. **Core**: `celeriant_wal` (data types, no I/O), `celeriant_wire` (serialization, CRC, framing), `celeriant_msg` (request/response types)
6. **Infra**: `celeriant_crypto` (RSA, API key hashing, PKI), `celeriant_ktls` (kernel TLS offload), `celeriant_distributed` (replication, leader election, S3 catchup)
7. **Clients**: `celeriant_client_tokio` (application clients), `celeriant_client_glommio` (server-to-server replication)
8. **Examples**: `celeriant_demo`, `celeriant_reference` (reference implementations mirroring .NET client examples)

## Thread-Per-Core

One glommio executor per CPU core. Each shard owns aggregates via routing (`aggregate_id % num_shards` by default, configurable to `org_id` or `aggregate_type_id`). No locks on the hot path. `Rc<RefCell<_>>` for per-shard state. The only `Arc` types crossing shard boundaries are immutable-after-creation or atomic booleans.

Shard 0 handles all cluster coordination: lease management, heartbeats, kick processing.

S3/HTTP runs in a separate tokio sidecar runtime. io_uring and tokio are incompatible in the same thread.

## Write Path

1. Validation (OCC, idempotency, allow_create, schema validation)
2. Queue to `pending_append_queue`
3. Fsync batching via Coordinator (amortises across concurrent writers)
4. Replication (sync to follower or S3 fallback)
5. Commit positions, broadcast watch events, ACK to client

## Read Path

1. Check recent write cache (filtered by `visible_wal_seq`)
2. Reverse scan via `ReverseMetablockScanner`
3. Bloom filter skips entire log segments
4. Metablock-level filters (timestamps, batch indices, event type bloom)
5. Fetch datablocks (inline if <= 512 bytes, otherwise from disk)
6. Event-level filters

## File Layout

```
log_N.wal (preallocated, up to 1GB)
[Header 512KB] [Metablocks 1024B each, growing down ->] [Free space] [<- Datablocks variable, growing up] [Header 512KB]
```

Rotates when metablocks and datablocks meet. Dual headers for crash recovery. File does not grow. Can shrink via compaction.

## Where to Find Things

Every crate has a `README.md`. Some also have `ARCHITECTURE.md` for deeper design details (`celeriant_wire`, `celeriant_wal`, `celeriant_crypto`, `celeriant_client_tokio`, `celeriant_msg`). System-wide invariants are in `docs/invariants.md`.
