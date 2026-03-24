# Celeriant Database Invariants

Rules the system enforces. Breaking any of these is a bug. Provide this to LLMs working on Celeriant to prevent violations.

---

## Durability

- Client ACK is withheld until local `fdatasync()` succeeds AND replication succeeds (TCP or S3 fallback). Both must complete before the response is sent.
- All I/O uses Direct I/O (`O_DIRECT` via glommio `DmaFile`). The OS page cache is never used for WAL files.
- Disk write order within a single fsync: datablocks first (so offsets are known), then metablocks (referencing those offsets), then dual headers, then `fdatasync()`.
- Both the primary header (offset 0) and backup header (offset `file_len - 512KB`) are written on every fsync. If the primary is corrupt on open, the backup is used.
- DMA writes are aligned to 4096 bytes. Metablock batches are padded up to 3072 bytes to the next alignment boundary.
- Replication is a no-op on non-leader nodes (standalone and follower return immediately from `replicate_durable()`).

## WAL Ordering and Integrity

- WAL entries are globally contiguous within a shard. Each new entry receives exactly `current_wal_index + 1`. Gaps are fatal.
- Every metablock carries `previous_tip_hash`, forming a Blake3 hash chain over the entire WAL history.
- Hash computation intentionally excludes `datablock_position` so leader and follower produce identical hashes despite different on-disk layouts.
- Log segment rotation carries `wal_index` and `tip_hash` from the old file's write cursor into the new file's header. Hash chain and WAL sequence are unbroken across file boundaries.
- A batch whose items have non-contiguous WAL indices is rejected with `BatchWalIndexGap`.

## Read Visibility

- On the leader, writes are invisible to readers until replication completes. After fsync, data sits in `pending_replication_batches`. The read cursor advances only in `commit_replication`.
- On standalone/follower, writes become readable immediately after fsync.
- Two separate LRU caches exist: `aggregate_write_snapshots` (updated after fsync, used for OCC/idempotency) and `aggregate_read_snapshots` (updated after replication on leader, used by reads).
- The recent-write cache filters by `visible_wal_index`. Entries with `wal_index > visible_wal_index` are excluded from reads.
- The reverse metablock scanner uses the `read` cursor exclusively. Segments not yet replicated are invisible to scans.
- Read operations are never rejected based on node status. A fenced or catching-up node serves stale reads silently.
- There is no cross-shard transactional consistency. Multi-shard listings are sequential per-shard with no cross-shard snapshot isolation.

## Optimistic Concurrency and Idempotency

- OCC checks use the write-ahead snapshot (not the read snapshot). A concurrent write that has been fsynced but not yet replicated still triggers an OCC conflict.
- Client idempotency checks use write-ahead state. A duplicate write is rejected even if the original is not yet visible to readers.
- A write is rejected with `ClientIdempotencyViolation` if any event's `client_event_index <= max stored client_event_index` for that `(aggregate_key, client_id)`.

## Leader Election

- Leader is determined by S3 CAS on a single `cluster/lease.json` object. No Raft, no quorum.
- `lease_index` is strictly monotonically increasing and never reused. A fresh cluster starts at `lease_index = 1`.
- A node seeing a valid (non-expired) lease from another node becomes follower unconditionally — no CAS attempt.
- A lease supersedes another if and only if `lease_index > our_lease_index AND leader_node_id != our_node_id`.
- Membership is a fixed 2-slot array in S3. A third node cannot join.
- Only `Leader` and `Standalone` nodes can accept writes. All other states reject with a leader address hint.
- Timing is asymmetric: the leader renews every `heartbeat_interval` (500ms), but the follower's TTL extends in `heartbeat_lease_duration` chunks (1500ms). The leader fences at `lease_expires_at_ms - max_clock_drift_ms`, which always fires before the follower's TTL expires.
- While TCP heartbeats succeed, the leader skips S3 lease renewal entirely. S3 is only checked when heartbeat fails AND either no peer is known or `lease_time_remaining <= s3_lease_duration / 2`.
- When the follower is unreachable, S3 renewal backs off proportional to `s3_lease_duration`, not `heartbeat_interval`. Peer discovery uses exponential backoff capped at `s3_lease_duration / 2`.
- A follower waits for its full TTL to expire before challenging for leadership via S3 CAS.
- A newly elected leader runs S3 catchup before serving writes, as a safety check against race conditions with the previous leader.

## Heartbeat and Fencing

- Heartbeats flow leader-to-follower only, handled exclusively by shard 0.
- A leader fences itself early: when `now > lease_expires_at_ms - max_clock_drift_ms`.
- A follower rejects a heartbeat and fences all local shards immediately if clock drift exceeds `max_clock_drift_ms`.
- Heartbeat success extends the follower's TTL: `new_expiry = max(current_expiry, leader_timestamp_ms + heartbeat_lease_duration)`. TTL is never reduced by a heartbeat.
- `FollowerCatchingUp` and `BootCatchup` states are TTL-exempt — they never decay to `Fenced`.

## Kick Follower

- A kick is sent after S3 fallback replication succeeds, not before. If TCP is unreachable, the kick cannot be delivered.
- Kick triggers when S3 fallback is used because: follower is offline, workset exceeds `max_catchup_gap_bytes`, or pending queue exceeds `pending_replication_high_water_bytes`.
- `kick_sent` flag ensures at most one kick per S3 fallback cycle.
- Kick is always routed to shard 0. Shard 0 broadcasts the state change to all local shards.
- On the follower: `Follower → FollowerCatchingUp`. If already catching up, the kick is acknowledged but is a no-op.
- A non-follower node rejects a kick with `acknowledged: false`.
- `FollowerCatchingUp` is TTL-exempt and cannot transition directly to `Leader`. It must catch up and return to `Follower` first.
- While catching up, writes are rejected with `WRITE_NOT_LEADER`. Reads serve stale data silently.

## Replication Protocol

- TCP replication is the primary path. S3 fallback triggers when: the follower is offline, the workset exceeds `max_catchup_gap_bytes`, or the pending queue exceeds `pending_replication_high_water_bytes`.
- A follower rejects a TCP batch if: (a) `lease_index < leader_lease_index` (stale leader), (b) WAL index is not `current + 1` (gap), or (c) `previous_tip_hash` doesn't match local tip (divergence).
- The leader attempts one extended catchup (prepending missing entries) on WAL mismatch. If the second attempt also fails, it switches to S3 fallback unconditionally.
- Empty replication batches are no-ops on both TCP and S3 paths.

## Rollback

- Rollback fires when both TCP and S3 replication fail. The goal is to revert to the last durably replicated state.
- The rollback lock (`sync_gate` write-lock) blocks all concurrent writes. Any in-flight fsync completes before the lock is granted.
- All un-replicated in-memory state is wiped: write snapshots, client snapshots, pending replication batches, sealed segment summaries, queue positions, pending appends, schema caches.
- Write cursor resets to the read cursor (`write = read.clone()`). If a segment was never replicated (read position is `None`), the write cursor resets to file start.
- Dual headers are rewritten at the rolled-back positions and `fdatasync()` completes before the lock is released. Rollback is durable before any new writes.
- Datablocks carry-over bytes are re-read from the new write position to recalculate metablock padding alignment.
- Rollback flags (`fsync_rollback_occurred`, `replication_rollback_occurred`) are one-time-consumption. The next capture phase reads and resets the flag.
- After rollback, the node accepts new writes immediately. Rollback does not permanently disable the shard.
- WAL divergence rollback (during S3 catchup): truncates both read and write cursors to the common ancestor, clears all caches including read-side, rewrites dual headers, and fsyncs. There is no window where read cursor is ahead of write cursor.

## S3 Catchup (Follower)

- S3 batches are sorted by `start_wal_index` and deduplicated before apply. Duplicate starts keep the batch with the highest `end_wal_index`.
- Contiguous WAL index across consecutive S3 batches is enforced: `batch[i].end + 1 == batch[i+1].start`. Gaps are fatal.
- A node never applies a batch it uploaded itself (filtered by `node_id` in the filename).
- On `TipHashMismatch`, the follower finds the common ancestor via hash chain traversal, truncates its WAL to the divergence point, then re-applies from S3.
- WAL truncation clears all caches, rewinds both cursors, rewrites dual headers, and fsyncs before returning.
- After applying an S3 batch, the batch file is deleted from S3 immediately.
- S3 catchup fsyncs as `Standalone` (read position advances immediately, no replication gate).
- Post-catchup, the node must win an S3 CAS election before accepting writes.

## Sharding and Concurrency

- One glommio `LocalExecutor` per shard, pinned to a CPU core via `CpuSet`. `shard % num_cpus` wraps when shards exceed cores.
- Shard routing: `routing_id % num_shards` where `routing_id` is configurable (`org_id`, `aggregate_type_id`, or `aggregate_id`; default: `aggregate_id`).
- Multi-aggregate writes that span multiple shards are rejected with `IncompatibleFilters`.
- A connection can be redirected between shards on any request via the glommio channel mesh. The `TcpStream` is converted to an executor-agnostic form and re-bound on the target.
- No shared mutable state across shards. All per-shard state uses `Rc<RefCell<_>>`. The only `Arc` types crossing shard boundaries are immutable-after-creation or atomic booleans.
- `RefCell` borrows must NEVER be held across `.await` points. Snapshot into owned data, drop borrow, await, re-borrow to commit.
- All `RwLock` acquisitions use 1-second timeout wrappers. Timeout returns `PotentialDeadlock` error rather than blocking indefinitely.
- The fsync `Coordinator` batches concurrent writers: first caller becomes leader (sleeps `fsync_delay`), subsequent callers become followers receiving the same result. At most one fsync executes at a time, enforced by `sync_gate`.
- The coordinator's Phase 1 (capture snapshot) runs before Phase 2 (clear queue), preventing a race where a new leader finds an empty queue.
- Shard 0 handles all cluster coordination: lease management, heartbeats, kick processing.
- All S3/HTTP work runs in a separate tokio sidecar runtime. io_uring and tokio are incompatible in the same thread.

## Memory Management

- Total memory is bounded by `detected_memory * CELERIANT_MEMORY_CONSUMPTION_PERCENT / 100` (default 80%, range 1-95%). Respects cgroup limits.
- Per-shard budget = total / num_shards. Five categories consume exactly 100% via fixed ratios: recent_write 71.5%, aggregate_snapshots 9%, client_snapshots 9%, schema_cache 9%, WAL index 1.5%.
- LRU capacity is derived from byte budget divided by per-entry size estimate — bounded by memory, not cardinality.
- Recent write cache is bounded by bytes with FIFO eviction before each insert.
- `aggregate_queue_positions` and `pending_append_queue` are intentionally unbounded (transient in-flight state that drains every fsync cycle).
- `pending_replication_batches` is intentionally unbounded; bounded indirectly by `pending_replication_high_water_bytes` triggering S3 fallback.
- Low-priority LRU inserts (scan-driven) only populate spare capacity and immediately demote to LRU tail, preventing scan pollution.

## Storage Layout

- File layout: `[Header 512KB] [Metablocks →] [Free space] [← Datablocks] [Header 512KB]`.
- Log segments are preallocated at creation. Minimum valid size: 1.5MB (two 512KB headers + one usable block).
- Rotation is caller-driven (checked before each fsync). A batch that cannot fit in a fresh segment is rejected with `BatchesTooLarge`.
- After rotation, the new file's read cursor is `None` until the first successful replication.
- Each segment carries a 256KB bloom filter (10 hashes, <1% FP rate for 200k aggregates). Bloom is persisted in the header and used by the reverse scanner.
- Sealed segments produce a sidecar summary file. On a leader, the sidecar is deferred until the segment is fully replicated.
- Listing scans newest-to-oldest, bounded by `list_max_duration` and `list_page_size`. A `deleted_barrier` prevents re-appearance of deleted aggregates from older segments.

## Disk Serialization

- All on-disk structures use bincode with fixed-width integer encoding and little-endian byte order. No varints.
- Every on-disk block is prefixed with 8 bytes: `[CRC32C (4B LE)] [Version (4B LE)]`. CRC covers everything after the CRC field (version + payload).
- Version is checked after CRC validation — this distinguishes corruption from format incompatibility. Unknown versions are rejected with `UnsupportedVersion`.
- All current on-disk versions are `1`: metablock, datablock, shard log header, S3 fallback batch, segment summary.
- Metablocks are fixed-size (1024 bytes). Unused trailing bytes are zero-padded.
- Datablocks use dual storage: inline (up to 512 bytes, stored within the metablock) or external (written to end of file, growing backward). External datablocks carry their own CRC32C.
- `AggregateKey` serializes as 3 contiguous u128 LE values (48 bytes). The in-memory `hash` field is never serialized.
- Enum discriminants are 4-byte u32 (fixed-int encoding). `Option<T>` is 1-byte discriminant + T.

## Wire Format

- Two protocol versions: V2 (bincode, fixed-int, little-endian) and V3 (MessagePack). V0, V1, V4+ are rejected with `UnsupportedProtocol`.
- Every message has a 17-byte frame header: `[version u32 LE] [message_type u32 LE] [compressed_length u32 LE] [uncompressed_length u32 LE] [compression_type u8]`.
- Both `compressed_length` and `uncompressed_length` are validated against `max_size_bytes` before any allocation or decompression. Prevents decompression bombs.
- Default max request: 16 MiB. Default max response: 64 MiB.
- Protocol version is set on the first message (Identify or first ClientRequest). All subsequent messages use that version. No renegotiation.
- Compression types: None (0), Zstd (1), Snappy (2), Brotli (3), Gzip (4).
- Small messages (header + body <= 1024 bytes, no compression) use a stack-allocated buffer. Larger messages allocate on the heap.
- TCP_NODELAY is always set. No Nagle.

## TLS and Security

- TLS 1.3 is the only permitted version. No fallback.
- Two separate CAs: client CA (trusted on port 10000) and intracluster CA (trusted on port 10001). A client cert cannot authenticate to the replication port and vice versa.
- Replication port always requires mTLS (`ClientAuthMode::Require`, hardcoded). Client port mTLS is configurable (default: `Require`).
- kTLS (kernel TLS offload) is used for all connections. Session tickets are disabled (would desync kernel TLS sequence counters). kTLS support is verified at startup.
- TLS handshake has a 10-second timeout and 128KB buffer cap.
- All certs use ECDSA P-256. CA certs have `pathLen:0`. Node certs carry both `serverAuth` and `clientAuth` EKU. Client certs carry only `clientAuth`.
- API keys are stored as SHA-256 hashes only; raw keys are never stored server-side. Comparison is constant-time.
- Four API key slots: two ReadWrite, two ReadOnly. `ReadOnly` blocks write/delete/trim/schema operations.
- API keys require TLS unless `--insecure-allow-plaintext-auth` is explicitly set (server exits at startup otherwise).
- Client identity is `SHA-256(DER public key bytes)[0..16]` as little-endian u128. Validated per-connection at `Identify` time.
- Nonces expire after 2 minutes with 60-second forward clock-skew tolerance. Signing uses RSA-2048 PKCS1v15-SHA256.

## Watch Subscriptions

- Watch subscriptions are shard-local. No cross-shard fan-out at the server level.
- Watch events fire after the write is durably replicated (leader) or after fsync (non-leader). Never before.
- Each client has a bounded channel of 10,000 pending events (`MAX_PENDING_EVENTS`). This is a hard cap.
- The write path uses non-blocking `try_send()`. If the channel is full, the client is immediately removed from the watcher list. No backpressure propagates to the writer.
- Broadcast filtering (operation type, org, aggregate type, aggregate ID) runs before `try_send()`. The write hot path never blocks on watch consumers.
- Clients cannot request a watch latency exceeding `max_requested_latency` (default 100ms). Rejected with `WatchLatencyTooHigh`.
- Watch subscriptions are not included in the per-shard memory budget. Bounded per-client (10K events) but unbounded in total subscriber count.

## State Machine Transitions

- `FollowerCatchingUp` cannot transition directly to `Leader`.
- `KickFollower` only transitions `Follower→FollowerCatchingUp`. If already catching up, it's a no-op (idempotent).

## Server Startup

These checks run before the server accepts connections. Fatal checks abort the process.

- Direct I/O is verified by attempting an unaligned write with `O_DIRECT`. If it succeeds, the filesystem is silently falling back to buffered I/O — fatal. `EINVAL` confirms DIO is enforced.
- `adjtimex()` checks the kernel clock is NTP-disciplined. Warning only — does not abort.
- Four immutable config parameters are persisted to `server_meta.toml` on first startup and must never change: `num_shards`, `timestamp_precision`, `timestamp_epoch_offset_secs`, `routing_rule`. Mismatch is fatal.
- If `compaction_temp_dir` is configured, it must be on the same filesystem as `data_root` (validated via `st_dev`). Cross-device `rename(2)` is not atomic. Fatal.
- Both client and replication ports are probed via TCP connect to `127.0.0.1`. Glommio uses `SO_REUSEPORT` so `bind()` alone cannot detect a running instance. Fatal.
- If TLS is enabled, kTLS kernel support is verified via `setsockopt(SOL_TCP, TCP_ULP, "tls")`. Fatal if missing.
- If `api_keys.toml` exists, each hash must be exactly 64 hex characters. API keys without TLS are fatal unless `--insecure-allow-plaintext-auth` is set.
- Filesystem metadata warmup walks all `shard_*` directories, stats and opens every `.wal` file to preload XFS inode and extent metadata. No data is read.
