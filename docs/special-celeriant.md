# What Makes Celeriant Special

Celeriant is a distributed, append-only event store built for [event sourcing](https://www.martinfowler.com/eaaDev/EventSourcing.html) and the write side of [CQRS](https://www.martinfowler.com/bliki/CQRS.html). Written in Rust, thread-per-core, Direct I/O, io_uring. Two-node cluster with S3 for coordination and backup replication. 400k writes/sec on a single node.

Below are the engineering decisions and design patterns that set it apart.

---

## 1. S3 Conditional Writes as a Consensus Protocol

No Raft. No Paxos. No etcd or ZooKeeper. Celeriant uses S3 conditional writes for leader election. A single CAS-protected object in S3 provides mutual exclusion — if two nodes race for the lease, only one wins.

What makes this more than "just using S3" is the full state machine built around it. Nodes transition through Leader, Follower, FollowerCatchingUp, BootCatchup, Fenced, and Standalone states. When a leader's TTL expires, it self-fences automatically — no external signal needed for a node to know it's lost leadership. Standalone bypasses election entirely for single-node deployments — no S3 dependency, no replication, just local durability.

S3 also serves as a backup replication target. If the follower is unreachable, the leader replicates to S3 instead. Writes are never acknowledged until they're on two storage systems. S3 is both the coordination plane and the safety net.

Election handles the edge cases: fresh cluster races, lease renewal via CAS with an incremented lease index, expired lease races, follower-to-leader transitions. Membership registration retries up to 5 times, with S3 round-trip latency providing natural backoff.

## 2. kTLS — Kernel-Level TLS with io_uring

Most databases that use io_uring don't bother with TLS. The ones that do wrap the stream in a userspace TLS layer, which defeats io_uring's zero-copy advantages. Celeriant takes a third option.

The TLS 1.3 handshake happens in userspace via rustls's unbuffered API. Once the handshake completes, Celeriant extracts the session secrets and hands them to the Linux kernel via `setsockopt(SOL_TLS)`. From that point on, the raw TCP stream is used directly — the kernel handles all encryption and decryption transparently. io_uring reads and writes pass through kernel TLS without Celeriant touching a single byte of ciphertext.

Supports AES-128-GCM, AES-256-GCM, and ChaCha20-Poly1305. Session tickets are disabled because they desync sequence counters between kTLS endpoints. There's also a subtle race condition handled during the handshake — application data can arrive before kTLS is installed, so trailing bytes from the handshake buffer are captured and prepended to the first kTLS read.

## 3. Metablocks Growing Up, Datablocks Growing Down

The WAL file layout is unusual. Fixed-size metablocks (1024 bytes) grow from the top of the file after the header. Variable-size datablocks grow backwards from the bottom. The file rotates when they meet.

This gives:
- **Constant-time metablock scanning** — every metablock is at a predictable offset
- **No fragmentation** — no free-space management within a file
- **Natural separation** — metablock scans never touch datablock regions
- **Simple math** — free space is just the gap between the two cursors

Fixed metablock sizes mean the scanner reads them with zero copies, no carry buffer, and chunk boundaries always aligned to record boundaries. Pure pointer arithmetic on the hot path.

## 4. Two-Phase Amortised Fsync + Replication

Multiple concurrent writers coalesce into a single fsync, then a single replication round. The pipeline is: **queue -> fsync (amortised) -> replication (amortised) -> ACK**. Each stage batches independently.

The two-phase design solves a subtle race condition: the snapshot of pending writes is captured *before* the queue is cleared, so no writes are lost between snapshot and clear.

Under low load, the fast path executes immediately with zero delay — single-writer latency stays minimal. Under high load, delay-based batching kicks in and accumulates requests. This is a key reason for 350k writes/sec. One fsync and one TCP round-trip can serve hundreds of concurrent writers.

## 5. Replication Rollback with Physical WAL Repair

When replication fails (both follower and S3 are down), the system executes a full rollback:

1. Drain in-flight fsyncs and block new ones
2. Clear write snapshots, pending replication queue, and client snapshots
3. For each affected log segment: rewrite both headers to the read position and fdatasync

Data that was fsynced to disk but not replicated is logically erased by reverting the header positions. This maintains the invariant that acknowledged writes are always on two storage systems.

Most databases would either accept single-node durability or refuse the write upfront. Celeriant commits optimistically, then rolls back if replication fails.

## 6. Dual Cursor Architecture

Every log segment has two independent cursors — write and read. Data becomes visible to the write path (for OCC checks) immediately after fsync, but only visible to readers after successful replication.

A reader never sees data that might be rolled back. If replication fails, the write cursor resets back to the read cursor and in-memory caches are cleared. Two separate rollback flags distinguish "empty queue because idle" from "empty queue because rollback cleared it", preventing the system from incorrectly treating post-failure states as normal.

On a freshly rotated log file, the read cursor starts empty until the first replication completes. If a read comes in before that, it transparently falls back to the previous file's cursor.

## 7. DMA Carry Buffer for Reverse-Growing Datablocks

Datablocks grow downward from the bottom of the WAL file. DMA writes must be aligned to 4096-byte boundaries. These two things don't naturally agree — a variable-size datablock can land at any position.

When a datablock's position doesn't align, the DMA write must include bytes from the previous write that fall within the same alignment boundary. Celeriant saves these partial-boundary bytes between fsyncs and re-includes them in the next write's DMA buffer. Both front carry-over (rounding the new position down) and end carry-over (rounding the previous position up) are tracked.

Most databases avoid this problem by padding everything to alignment boundaries (wasting space) or by not using Direct I/O. Celeriant preserves space efficiency while satisfying DMA alignment constraints.

## 8. Bloom Filters for Reverse WAL Scanning

Each log segment (up to 1GB) carries a 256KB bloom filter of all aggregate keys written to it. At 200K aggregates per segment, the false positive rate is under 1%.

When scanning backwards to find an aggregate's data, entire log segments are skipped with a single bloom filter check. This is the key to handling unlimited aggregate cardinality. A database with 10 million aggregates doesn't need 10 million index entries in memory. Cold aggregates fall back to reverse WAL scanning, and bloom filters prune most segments — making the scan effectively logarithmic.

There are actually two completely different bloom filter designs serving different purposes:

| Filter | Size | Purpose |
|--------|------|---------|
| Aggregate bloom | 256KB per segment | Skip entire log segments during aggregate lookups |
| Event type bloom | 32 bytes per batch | Filter batches by event type within a segment |

The scanner only considers bloom filters from replicated data, so unreplicated writes can never leak into read results.

## 9. WAL Divergence Detection and Recovery

When a hash mismatch is detected during S3 catchup, Celeriant doesn't just fail. It finds the divergence point and recovers.

- **Fast path**: uses the already-downloaded batch's overlap with local data to find the common ancestor — no additional I/O needed
- **Fallback**: backward-scans local metablocks to find the common ancestor when the batch doesn't overlap local data

Once the divergence point is found, the system safely truncates divergent entries — clearing caches, rewinding cursors, rewriting headers, and fsyncing for durability. The follower then replays from the common ancestor forward.

## 10. OCC-Before-Idempotency Check Ordering

When both optimistic concurrency control and idempotency checks would fail on the same write, the order matters. Celeriant checks OCC first.

If a client's read was stale, they get an OCC violation — retry with fresh state. If OCC passes but the client's event index was already used, they get an idempotency violation — their exact write already landed (crash recovery), treat as success.

If idempotency were checked first, a concurrent writer's event could look like your duplicate. The client would falsely conclude "my write already landed" and silently drop its operation. The check ordering eliminates an entire class of exactly-once bugs. Timeout retries hold the client event index constant (to detect already-landed writes), while OCC retries re-derive it (to avoid false positives).

## 11. Small Events Live Inside Metablocks

If a serialised event batch fits in 512 bytes or less, it's stored *inside* the metablock itself. This eliminates an entire disk seek for small events. Event sourcing workloads often have lots of small domain events, so a large fraction of reads can be served from the metablock scan alone — never touching the datablock region.

The system auto-selects inline vs block storage transparently. Compression still applies to inline data. The 512-byte threshold is configurable at compile time down to 128 bytes.

## 12. Blake3 Hash Chain

Every metablock includes the hash of its predecessor, forming a tamper-evident chain. But the datablock position is explicitly excluded from the hash, because that's a node-local offset that legitimately differs between leader and follower.

This means the follower can verify WAL integrity via the hash chain even though its physical on-disk layout differs from the leader's. Before accepting replicated data, the follower validates both WAL sequence continuity and the previous tip hash. You get an immutable audit log for free.

The hash chain carries over across log file rotations — the new file's header picks up the WAL sequence and tip hash from the previous file, maintaining continuity.

## 13. Anti-Scan-Pollution in the Cache

List operations and discovery scans can be devastating to a cache. One big scan evicts the entire hot working set.

Celeriant uses priority-based cache insertion. Entries from WAL scans are inserted at low priority — only when there's spare capacity, and immediately demoted to the LRU eviction position. Targeted reads and writes get standard MRU insertion. A single list operation can't flush the cache.

## 14. TCP Stream Migration Between Shards

In a thread-per-core architecture, TCP streams are bound to a specific executor. When a client connects and its first request routes to a different shard, the stream needs to move.

The stream is unbound from the current executor, sent through the intrashard mesh channel, and rebound on the target shard's executor. If the mesh channel is full, the client gets a SERVER_BUSY error instead of blocking — this prevents channel saturation from cascading into WAL write stalls on the target shard.

## 15. Visibility Filtering in the Read Cache

Each cached write carries the WAL sequence that produced it. When reading from cache, the reader supplies the highest WAL sequence that's safe to serve — the replication frontier. Writes beyond that boundary are silently excluded.

The hot cache can hold speculative pre-replication data without ever leaking it to readers. The visibility boundary is a runtime parameter, not a cache eviction concern.

## 16. Event Type Filtering with Dual Encoding

The same 32-byte storage is used for two different purposes. If an event batch has 4 or fewer unique event types, they're stored as an exact match array. If more, the same bytes become a bloom filter. Exact filtering for the common case, probabilistic filtering for the rare case, with zero size overhead either way.

## 17. Zero-Copy Metablock Field Access

Metablock fields are read directly from raw byte slices at hardcoded offsets — no deserialisation needed. The reverse WAL scanner can check aggregate key membership, read aggregate versions, and apply filters without ever allocating. On the hot scan path, it's pure pointer arithmetic.

All metablocks define precise offset and wire size constants for fixed-size layout guarantees. Compile-time size assertions verify everything fits.

## 18. Thread-Per-Core with io_uring, Bridged to Tokio

One [Glommio](https://github.com/DataDog/glommio) executor per CPU core. Each shard owns its aggregates. No locks on the hot path. Inspired by [ScyllaDB](https://www.scylladb.com/) and [TigerBeetle](https://tigerbeetle.com/).

A separate Tokio runtime (the "sidecar") handles operations that don't work with io_uring — primarily S3 HTTP clients. The bridge uses bounded channels with two QoS lanes, so lease renewals can't get stuck behind large batch uploads:

| Lane | Operations | Purpose |
|------|------------|---------|
| Control | Lease CAS, membership | Leader election, coordination |
| Data | Batch upload/download | S3 fallback replication |

Instead of trying to make everything work in io_uring (which would mean writing an HTTP client from scratch), the sidecar is a pragmatic escape hatch that keeps the fast path pure.

## 19. Server-Side Schema Validation

Events aren't opaque blobs. Celeriant validates event structure at write time against registered schemas. Supports JSON Schema, Apache Avro, and Protocol Buffers (via compiled FileDescriptorSet).

Schemas are registered in the WAL as metablocks. Compiled validators are cached in an LRU to avoid recompilation. Encrypted events skip validation entirely — the server can't validate what it can't read. This is a clean separation: encryption and validation are orthogonal concerns.

## 20. Client Identity from Public Key Hash

Client identity is derived deterministically from the SHA-256 hash of the client's public key. Same keypair, same identity, on any server. No central identity registry needed.

Authentication is nonce-based — the nonce is the epoch timestamp, signed with the client's private key. Nonce validation enforces a 2-minute expiration window with 60-second clock skew tolerance, bounding replay attacks. Stateless and simple.

## 21. Dual Headers for Crash Recovery

Every log file has its header written at both the start and end of the file. On every fsync, both copies are updated. On recovery, if the primary header is corrupted from a torn write, the backup is used. CRC32C validates integrity before deserialisation.

Combined with Direct I/O (bypassing the kernel page cache), fsync failures surface immediately rather than being silently swallowed. Buffered I/O through the page cache is vulnerable to [silent data loss on fsync failure](https://lwn.net/Articles/752063/). Celeriant sidesteps this entirely.

## 22. Thundering Herd Prevention

When multiple async tasks on the same shard all want the same cold aggregate loaded from disk, only one task does the I/O. The others wait, then recheck the cache — another task may have already loaded it.

Without this, 100 concurrent reads for the same cold aggregate would each independently scan the WAL. The same generic coordinator is reused for both aggregate snapshot loading and client idempotency index loading — same pattern, different key types.

## 23. Pre-Computed Hashes on Composite Keys

Composite keys (aggregate key, aggregate+client key, aggregate+type key) store a pre-computed hash at construction time. The hash is *not* serialised — it's recomputed on decode. Every HashMap and LRU lookup using these keys pays zero hashing cost, while the wire format stays compact.

## 24. O(1) Batch Lookup via Tracked Starting Index

Recent writes for an aggregate are stored in a ring buffer with a tracked starting aggregate version. Since aggregate versions are monotonic with no gaps, any batch can be looked up in O(1) by subtracting the starting index. No HashMap, no search.

## 25. Separate Connections for Replication and Heartbeat

The follower connection uses separate locks for the replication TCP connection and the heartbeat TCP connection. A slow heartbeat can't block ongoing replication, and vice versa. This eliminates a subtle problem where heartbeat timeouts could cascade into replication failures.

---

## The Meta-Patterns

**Speculate aggressively, roll back safely.** Celeriant writes to disk before replication confirms, caches data before it's visible, batches operations across concurrent writers. But every speculative action has a matching rollback path. The dual cursor model, the rollback lock, the visibility filter, and the WAL truncation logic all exist to make speculation safe.

**Bounded everything.** Every cache is LRU-bounded. The replication queue has a high water mark. Bloom filters are fixed-size. Metablocks are fixed-size. Log files are pre-allocated. No data structure grows proportional to aggregate cardinality. The one intentional exception — the pending append queue — drains every fsync (milliseconds), so it's acceptable.

**Compile-time configurability where it matters.** Block sizes, minibatch thresholds, and protocol versions are compile-time constants. A feature flag allows halving metablock and inline sizes for different density/performance trade-offs without runtime cost.
