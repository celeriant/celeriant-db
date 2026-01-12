---
name: understanding-celeriant-structure
description: Provides quick overview of Celeriant codebase architecture, crate dependencies, and responsibilities. Use when asked about project structure, what a crate does, how crates relate, or where to find specific functionality. Also use when needing to understand the codebase before making changes.
---

# Celeriant Project Structure

Celeriant is a fast, distributed write-ahead log for event sourcing using thread-per-core architecture (glommio/io_uring).

## Layers (top to bottom)

1. **Binary**: celeriant (main server), celeriant_cli (CLI/TUI)
2. **Runtime**: celeriant_runtimes (glommio orchestration, routing), celeriant_sidecar (tokio bridge, S3)
3. **Shard**: celeriant_shard (WAL orchestrator, fsync batching), celeriant_watch (subscriptions)
4. **Storage**: celeriant_memcache (cache, queues), celeriant_rotating_log (log files), celeriant_disk (DMA I/O)
5. **Core**: celeriant_wal (data types), celeriant_wire (serialization), celeriant_msg (protocol messages)
6. **Clients**: celeriant_client_tokio, celeriant_client_glommio
7. **Other**: celeriant_crypto (RSA), celeriant_distributed (replication WIP), celeriant_integration_tests

## Crate Summaries

| Crate | One-liner |
|-------|-----------|
| **celeriant** | Main server binary: CLI args, config |
| **celeriant_runtimes** | Thread-per-core orchestration, spawns glommio executors and sidecar, shard routing, inter-shard channels, tokio sidecar bridge |
| **celeriant_shard** | Shard WAL orchestrator: validation, fsync batching, read filtering, watch integration |
| **celeriant_memcache** | In-memory layer: pending write queues, aggregate and client metadata lru caches, recent write cache |
| **celeriant_rotating_log** | Log segment management: DMA files, rotation, LRU cache, bloom filters, crash recovery |
| **celeriant_disk** | Low-level DMA I/O: alignment handling, chunked reads, gap skipping |
| **celeriant_wal** | WAL data structures: Metablock, Datablock, AggregateKey, indexes (no I/O) |
| **celeriant_wire** | Serialization/framing: bincode, msgpack, compression, CRC, wire headers |
| **celeriant_msg** | Request/response types: Read, Write, Watch, Delete, List operations |
| **celeriant_watch** | Watch subscription system: filtering, batching, backpressure, real-time notifications |
| **celeriant_sidecar** | S3 object store abstraction: conditional puts, batch deletes, tokio runtime |
| **celeriant_crypto** | RSA key generation and signing utilities |
| **celeriant_client_tokio** | Async Rust client using tokio runtime |
| **celeriant_client_glommio** | Async Rust client using glommio runtime (for server-to-server) |
| **celeriant_cli** | CLI and TUI for interacting with Celeriant |
| **celeriant_integration_tests** | Test binaries: throughput, chaos, watch, connection handling |
| **celeriant_distributed** | Replication and lease logic (work in progress) |
| **celeriant_embedded** | Placeholder for embedded/in-process mode |

## Key Architectural Concepts

### Thread-Per-Core
One glommio executor per CPU core. Each shard owns aggregates via routing (OrgId, AggregateTypeId, or AggregateId modulo num_shards). No locks on hot path.

### Write Path
1. Validation (OCC, idempotency, allow_create)
2. Queue to pending_append_queue
3. Fsync batching via Coordinator (amortizes across concurrent writers)
4. Commit positions, broadcast watch events

### Read Path
1. Check recent write cache
2. Scan disk backwards via ReverseMetablockScanner
3. Apply metablock-level filters (bloom, timestamps, batch indices)
4. Fetch datablocks (inline or from disk)
5. Apply event-level filters

### File Layout
```
log_N.wal (1GB preallocated)
├── Header (1MB) at offset 0
├── Metablocks (512B each) growing forward
├── Free space
├── Datablocks (variable) growing backward
└── Header (1MB) at EOF-1MB (backup for crash recovery)
```

File size does not get larger. File can shrink as old events are trimmed and WAL is compacted.

## Crate READMEs (Progressive Detail)

For implementation details beyond this overview, read the specific crate README:

| Topic | README | Key Content |
|-------|--------|-------------|
| Write/read flows | [celeriant_shard](celeriant_shard/README.md) | Fsync batching, filtering logic |
| Data structures | [celeriant_wal](celeriant_wal/README.md) | Index semantics, metablock/datablock |
| Log files | [celeriant_rotating_log](celeriant_rotating_log/README.md) | Dual headers, bloom, LRU caching |
| In-memory layer | [celeriant_memcache](celeriant_memcache/README.md) | Two-phase commit, eviction |
| Subscriptions | [celeriant_watch](celeriant_watch/README.md) | Backpressure, event merging |
| Routing | [celeriant_runtimes](celeriant_runtimes/README.md) | Connection redirect, sidecar bridge |
| Wire protocol | [celeriant_wire](celeriant_wire/README.md) | Compression, CRC placement |
| Messages | [celeriant_msg](celeriant_msg/README.md) | Request/response types |
| DMA I/O | [celeriant_disk](celeriant_disk/README.md) | Alignment, gap skipping |
| S3 storage | [celeriant_sidecar](celeriant_sidecar/README.md) | Conditional puts, error mapping |
| CLI/TUI | [celeriant_cli](celeriant_cli/README.md) | Commands, shortcuts |
| Tests | [celeriant_integration_tests](celeriant_integration_tests/README.md) | Test binaries |
