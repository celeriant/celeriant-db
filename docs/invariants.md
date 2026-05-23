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
- `last_self_acked_wal_seq` is fsynced synchronously (header-only write coalesced via the shared `sync_gate`) on every bump in `commit_replication`, before Ok is returned to the client. Without this fsync, an in-memory bump can be lost on shutdown or crash, leaving the on-disk header lagging the in-memory truth.

## WAL Ordering and Integrity

- WAL entries are globally contiguous within a shard. Each new entry receives exactly `current_wal_seq + 1`. Gaps are fatal.
- Every metablock carries `previous_tip_hash`, forming a Blake3 hash chain over the entire WAL history.
- Hash computation intentionally excludes `datablock_position` so leader and follower produce identical hashes despite different on-disk layouts.
- Log segment rotation carries `wal_seq` and `tip_hash` from the old file's write cursor into the new file's header. Hash chain and WAL sequence are unbroken across file boundaries.
- A batch whose items have non-contiguous WAL indices is rejected with `BatchWalSeqGap`.
- `read.wal_seq <= write.wal_seq` is a hard invariant. The cull-before-catchup path drains stale `pending_replication` PCDs because committing one whose `log_metadata.write` snapshot captured a pre-cull position would advance `read` past the (now-lower) `write` and trip the ack-barrier downstream.
- The read cursor is persisted in the segment log header as a separate field from the write cursor (zero sentinel maps to `None`). A crash with `read < write` restores the gap on reload; it does NOT collapse to `read = write`.

## Read Visibility

- On the leader, writes are invisible to readers until replication completes. After fsync, data sits in `pending_replication_batches`. The read cursor advances only in `commit_replication`.
- On standalone/follower, writes become readable immediately after fsync.
- Two separate LRU caches exist: `aggregate_write_snapshots` (updated after fsync, used for OCC/idempotency) and `aggregate_read_snapshots` (updated after replication on leader, used by reads).
- The recent-write cache filters by `visible_wal_seq`. Entries with `wal_seq > visible_wal_seq` are excluded from reads.
- The reverse metablock scanner uses the `read` cursor exclusively. Segments not yet replicated are invisible to scans.
- Read operations are never rejected based on node status. A fenced or catching-up node serves stale reads silently.
- There is no cross-shard transactional consistency. Multi-shard listings are sequential per-shard with no cross-shard snapshot isolation.

## Optimistic Concurrency and Idempotency

- OCC validation runs before client idempotency checks. A concurrent writer with a stale read receives `OccConflict`, not `ClientIdempotencyViolation`.
- OCC checks use the write-ahead snapshot (not the read snapshot). A concurrent write that has been fsynced but not yet replicated still triggers an OCC conflict.
- Client idempotency checks use write-ahead state. A duplicate write is rejected even if the original is not yet visible to readers.
- A write is rejected with `ClientIdempotencyViolation` if any event's `client_seq <= max stored client_seq` for that `(aggregate_key, client_id)`.

## Leader Election

- Leader is determined by S3 CAS on a single `cluster/lease.json` object. No Raft, no quorum.
- `lease_epoch` is strictly monotonically increasing and never reused. A fresh cluster starts at `lease_epoch = 1`. Same-leader S3 renewals refresh expiry without bumping the index; the index is a fencing token for cross-node handovers, not a renewal counter.
- A node seeing a valid (non-expired) lease from another node becomes follower unconditionally - no CAS attempt.
- A lease supersedes another if and only if `lease_epoch > our_lease_epoch AND leader_node_id != our_node_id`.
- Membership is a fixed 2-slot array in S3. A third node cannot join.
- Only `Leader` and `Standalone` nodes can accept writes. All other states reject with a leader address hint.
- Timing is asymmetric: the leader renews at `heartbeat_interval`, the follower's TTL extends in `heartbeat_lease_duration` chunks. The leader fences at `lease_expires_at_ms - max_clock_drift_ms`, which must always fire before the follower's TTL expires.
- While TCP heartbeats succeed, the leader skips S3 lease renewal entirely. S3 is only checked when heartbeat fails AND either no peer is known or `lease_time_remaining <= s3_lease_duration / 2`.
- When the follower is unreachable, S3 renewal backs off proportional to `s3_lease_duration`, not `heartbeat_interval`. Peer discovery uses exponential backoff capped at `s3_lease_duration / 2`.
- A follower waits for its full TTL to expire before challenging for leadership via S3 CAS. This applies to ALL paths that could result in promotion, including the post-`FollowerCatchingUp` CAS attempt; heartbeat-derived TTL is the "current leader is alive" signal, and a fresh heartbeat must override an apparently-expired S3 lease. (Happy-ops S3 lease silently expires while heartbeats keep extending the leader's TTL; S3 expiry alone is not sufficient evidence to promote.)
- A node booting with no heartbeat TTL history (`lease_expires_at_ms == 0` after catchup) waits up to `min(heartbeat_lease_duration, 5s)` before challenging via S3 CAS, but only if `lease.bin` already exists in S3 and is held by another node. Wait exits early on incoming heartbeat. Fresh-cluster boot (no `lease.bin`) and self-renewal skip the wait. The wait gives an active leader's heartbeat time to claim the booting node; a stale `lease.bin` cannot be used as evidence of leader death because lease.bin is intentionally not renewed while heartbeats succeed.
- A newly elected leader runs S3 catchup before serving writes. Catchup also runs whenever the observed `lease_epoch` is higher than the previous one we held; same-leader renewals leave it unchanged, so any increase means another node held the lease in between (e.g. 6 to 7) and may have uploaded S3 fallback batches.
- On promotion, the new leader uploads a "promotion batch" to S3 covering the last TCP-replicated batch. This closes the gap where the old leader rolled back a batch the follower kept.
- Cull-before-catchup: any transition out of Leader (became_leader OR became_follower_from_leader_or_fenced) rewinds local `write` to `read` and drains stale PCDs from `pending_replication` BEFORE S3 catchup runs. Required because `catchup_from_s3` starts the apply filter at `next_wal_seq = write + 1`; without the cull, peer batches in the `[read+1, write]` gap are skipped and the node later overwrites positions the peer already self-acked.
- Cull and catchup are decoupled: cull is local-only and always fires on a Leader→non-Leader transition; catchup (S3 list+download+apply) only fires when the lease actually changed hands or we became a new leader. A bare self-fence with no lease change SKIPS catchup so the original leader can re-acquire its own lease when S3 reachability returns.
- The post-publish lease check in `commit_replication` reads `node_status.get().is_leader()` (LOCAL atomic + must_fence). This closes the in-process leg of the dual-ack race but does NOT consult S3. Local `lease_expires_at_ms` is HB-extended via `compute_new_ttl` and CAN drift above the S3-confirmed expiry; cross-node dual-ack remains an open architectural gap (see `dual_ack_split_brain` integration test).
- `last_self_acked_wal_seq` only advances after the leader returns Ok to a client. It is the source of truth for "this node acked this wal_seq" and the only signal in the truncate refusal barrier; `last_received_replication_wal_seq` and `read.wal_seq` are intentionally NOT in the barrier because they bump on receive/apply paths that don't reflect what bytes are on disk.

## Heartbeat and Fencing

- Heartbeats flow leader-to-follower only, handled exclusively by shard 0.
- A leader fences itself early: when `now > lease_expires_at_ms - max_clock_drift_ms`.
- A follower rejects a heartbeat and fences all local shards immediately if clock drift exceeds `max_clock_drift_ms`.
- Heartbeat success extends BOTH the leader's and the follower's local TTL: `new_expiry = max(current_expiry, leader_timestamp_ms + heartbeat_lease_duration)`. **TTL is never reduced by a heartbeat.** The longer of the current TTL and the proposed extension wins. This is critical because the initial S3 CAS sets local TTL to `now + s3_lease_duration` (e.g. 30s); heartbeats arrive every `heartbeat_interval` proposing `leader_timestamp_ms + heartbeat_lease_duration` (e.g. 1.5s). Without max-merge, each heartbeat would shrink the TTL by ~28s, defeating the whole "TTL only ever extends" invariant.
- TTL is **leader-controlled and pre-computed**: the leader stamps `leader_timestamp_ms` BEFORE sending the heartbeat (or before writing the S3 lease), and both sides use that pre-op timestamp to compute the new TTL. Recomputing TTL post-network-op (using `now()` after the heartbeat returns) bakes the network round-trip into the lease window; wasting potentially seconds of effective TTL and creating asymmetry between leader and follower views.
- `FollowerCatchingUp` and `BootCatchup` states are TTL-exempt - they never decay to `Fenced`.
- Heartbeat `Ack` carries `follower_can_accept_tcp_replication`. The flag is `true` only in plain `Follower` state; `false` in `FollowerCatchingUp`. The leader uses it to gate TCP replication; during follower catchup commits route straight to S3 fallback without paying the TCP-reject round-trip.

## Kick Follower

- A kick is attempted after S3 fallback replication succeeds, gated by `try_acquire_kick` (skipped if a previous kick is still in-flight). Delivery is best-effort.
- Kick triggers when S3 fallback is used because: follower is offline, workset exceeds `max_catchup_gap_bytes`, or pending queue exceeds `pending_replication_high_water_bytes`.
- The commit path does not await `send_kick`. The kick is spawned fire-and-forget via `try_acquire_kick` / `release_kick`; at most one in-flight kick task per shard. Commits never pay `internode_request_timeout` waiting on a slow/dead follower.
- Kick is always routed to shard 0. Shard 0 broadcasts the state change to all local shards.
- On the follower: `Follower → FollowerCatchingUp`. If already catching up, the kick is acknowledged but is a no-op.
- A non-follower node rejects a kick with `acknowledged: false`.
- `FollowerCatchingUp` is TTL-exempt and cannot transition directly to `Leader`. It must catch up and return to `Follower` first.
- While catching up, writes are rejected with `WRITE_NOT_LEADER`. Reads serve stale data silently.

## Replication Protocol

- In a healthy cluster where both nodes are reachable and clocks are within drift tolerance, S3 is never touched. No lease renewal, no fallback replication, no S3 reads. All coordination flows over TCP heartbeats, all data over TCP replication. Chaos baseline enforces this via `NoS3Fallbacks`, `NoRollbacks`, `NoHeartbeatFailures`.
- TCP replication is the primary path. S3 fallback triggers when: the follower is offline, the workset exceeds `max_catchup_gap_bytes`, or the pending queue exceeds `pending_replication_high_water_bytes`.
- S3 replication uploads a single file per batch. Splitting into sub-batches is prohibited - it creates WAL sequence gaps on the consumer.
- S3 uploads are semaphore-limited to prevent network saturation. When replication backpressure exceeds `pending_replication_high_water_bytes`, clients receive `ServerBusy`.
- Pending replication entries are never silently dropped. If rollback fails, entries are requeued for the next replication cycle.
- A follower rejects a TCP batch if: (a) `lease_epoch < leader_lease_epoch` (stale leader), (b) WAL sequence is not `current + 1` (gap), or (c) `previous_tip_hash` doesn't match local tip (divergence).
- The leader attempts one extended catchup (prepending missing entries) on WAL mismatch. If the second attempt also fails, it switches to S3 fallback unconditionally.
- Empty replication batches are no-ops on both TCP and S3 paths.

## S3 Object Lifecycle

- A node never deletes an S3 object it uploaded. Deletion is only performed by the other node, once it has progressed past the object.

## Rollback

- Rollback fires when both TCP and S3 replication fail. The goal is to revert to the last durably replicated state.
- The rollback lock (`sync_gate` write-lock) blocks all concurrent writes. Any in-flight fsync completes before the lock is granted.
- All un-replicated in-memory state is wiped: write snapshots, client snapshots, pending replication batches, sealed segment summaries, queue positions, pending appends, schema caches.
- Write cursor resets to the read cursor (`write = read.clone()`). If a segment was never replicated (read position is `None`), the write cursor resets to file start.
- Dual headers are rewritten at the rolled-back positions and `fdatasync()` completes before the lock is released. Rollback is durable before any new writes.
- Datablocks carry-over bytes are re-read from the new write position to recalculate metablock padding alignment.
- Rollback flags (`fsync_rollback_occurred`, `replication_rollback_occurred`) are one-time-consumption. The next capture phase reads and resets the flag.
- After rollback, writes are rejected with `ReplicationBackpressure` for `replication_rollback_cooldown` (default 500ms). Gives the pending queue time to drain via TCP/S3 before accepting new load; prevents the rollback → rewrite → rollback storm that produces overlapping S3 batch generations. Rollback does not permanently disable the shard; writes resume after the cooldown.
- The leader re-checks `is_leader()` on every loop iteration in `commit_replication` and once more before the final `commit_replication`. If the lease expired mid-pipeline (slow S3 upload, retry backoff), rolls back local WAL and returns `LeaderFenced` instead of committing with an expired lease.
- WAL divergence rollback (during S3 catchup): truncates both read and write cursors to the common ancestor, clears all caches including read-side, rewrites dual headers, and fsyncs. There is no window where read cursor is ahead of write cursor.

## S3 Catchup (Follower)

- Among S3 batches covering overlapping wal_seq ranges, the authoritative chain is selected by `(lease_epoch, upload_sequence)` lexicographically. `lease_epoch` is globally monotonic via the S3 CAS election protocol and is the primary comparator: a batch from a higher-lease leader always supersedes one from a lower-lease leader regardless of `upload_sequence`. `upload_sequence` is per-process and serves only as a within-lease tiebreaker; it resets to zero on each leader handover, so it is not meaningful across leaders.
- A node never applies a batch it uploaded itself.
- A follower doesn't need to be caught-up to the WAL tip of the leader, just close enough to re-join as follower using TCP. It can catchup 'offline' via S3, and when close enough to the leader's WAL tip, it can rejoin.
- Leader will always ensure S3 contains the data required to catch up. If there is no active TCP follower, the leader never ACKs to clients until it has replicated the WAL data to S3.
- S3 files will not have gaps. There may be divergent paths though, due to leaders being overwhelmed and rolling back writes. Leader -> S3 involves a network hop and it may or may not be delivered, and S3 ACK may or may not return to the leader.
- WAL truncation fires only when a common ancestor has been verified via local-metablock hash match. The follower never truncates without an ancestor; unresolved divergence is surfaced as an error.
- Truncation is durable before writes resume: caches cleared, both cursors rewound, dual headers rewritten, and `fdatasync()` completes before the rollback lock is released.
- Truncation never leaves orphan segments ahead of the active one. When the ancestor lies in a sealed segment, the active segment and any intermediate sealed segments are discarded from disk before the target segment's headers are rewritten.
- S3 batches are not deleted during catchup. They remain available for divergence resolution until the follower returns to `Follower` state.
- Never truncate unless a common hash tip is found on both local disk and in an s3 replicated file
- Never truncate unless S3 has gap-free contiguous coverage from the ancestor's wal_seq up through the current write position. Truncating into a range S3 cannot re-supply would destroy entries that cannot be reconstructed.
- S3 catchup fsyncs as `Standalone`: read position advances with write position, no replication gate.
- Post-catchup, the node must win an S3 CAS election before accepting writes.

## Sharding and Concurrency

- One glommio `LocalExecutor` per shard, pinned to a CPU core via `CpuSet`.
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
- Per-shard budget = total / num_shards. Every category has a fixed percentage of the per-shard budget that sums to exactly 100%.
- LRU capacity is derived from byte budget divided by per-entry size estimate - bounded by memory, not cardinality.
- Recent write cache is bounded by bytes with FIFO eviction before each insert.
- `aggregate_queue_positions` and `pending_append_queue` are intentionally unbounded (transient in-flight state that drains every fsync cycle).
- `pending_replication_batches` is intentionally unbounded; bounded indirectly by `pending_replication_high_water_bytes` triggering S3 fallback.
- Low-priority LRU inserts (scan-driven) only populate spare capacity and immediately demote to LRU tail. Scans must not evict hot entries.

## Storage Layout

- File layout: `[Header 512KB] [Metablocks →] [Free space] [← Datablocks] [Header 512KB]`.
- Log segments are preallocated at creation. Minimum valid size: 1.5MB (two 512KB headers + one usable block).
- Rotation is caller-driven (checked before each fsync). A batch that cannot fit in a fresh segment is rejected with `BatchesTooLarge`.
- After rotation, the new file's read cursor is `None` until the first successful replication.
- A log segment is safe to auto-delete as an orphan iff BOTH the front and rear `HEADER_BLOCK_SIZE_BYTES` regions are all zero (or the file is shorter than `HEADER_BLOCK_SIZE_BYTES * 2`, including 0-byte partial-create remnants). Either header non-zero is treated as possible live data and is fatal.
- A pre-existing file at the rotation target is handled defensively: if it is a zero-dual-header orphan it is deleted and rotation proceeds; otherwise rotation fails with `RotationTargetUnsafe` (no overwrite).
- ENOSPC during rotation returns `OpenOrCreateError::OutOfSpace` (typed) and fires the `celeriant_rotation_out_of_space_total` counter plus a loud trace. The shard stays alive; writes that need rotation fail until disk space is recovered. Reads keep working. The 0-byte file left by a failed `create_file_dma` is removed before the error returns so startup orphan recovery can proceed. Prior behaviour was a `panic!` which crash-looped the shard; the typed error avoids the loop while preserving the loud-alarm property.
- Each segment carries a 256KB bloom filter (10 hashes, <1% FP rate for 200k aggregates). Bloom is persisted in the header and used by the reverse scanner.
- Sealed segments produce a separate sidecar `.summary` file, never embedded in the WAL. Summary data is node-dependent (rotation timing differs between leader and follower) and would break hash chain integrity if serialised into the WAL. On a leader, the sidecar is deferred until the segment is fully replicated.
- Listing scans newest-to-oldest, bounded by `list_max_duration` and `list_page_size`. A `deleted_barrier` prevents re-appearance of deleted aggregates from older segments.
- Storage files can be copied to a new node. There are no node-specific entries in storage files and they are safe to backup and copy to any node in the cluster.

## Disk Serialization

- All on-disk structures use bincode with fixed-width integer encoding and little-endian byte order. No varints.
- Every on-disk block is prefixed with 8 bytes: `[CRC32C (4B LE)] [Version (4B LE)]`. CRC covers everything after the CRC field (version + payload).
- Version is checked after CRC validation - this distinguishes corruption from format incompatibility. Unknown versions are rejected with `UnsupportedVersion`.
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
- Messages that fit in a fixed-size buffer (header + body <= 1024 bytes, no compression) must be stack-allocated. Heap allocation for small messages is prohibited.
- TCP_NODELAY is always set. Nagle buffering is never permitted.

## TLS and Security

- TLS 1.3 is the only permitted version. No fallback.
- Two separate CAs: client CA (trusted on port 10000) and intracluster CA (trusted on port 10001). A client cert cannot authenticate to the replication port and vice versa.
- Replication port always requires mTLS (`ClientAuthMode::Require`, hardcoded). Client port mTLS is configurable (default: `Require`).
- kTLS (kernel TLS offload) is used for all connections. Session tickets are prohibited - they desync kernel TLS sequence counters. kTLS support is verified at startup.
- TLS handshake has a 10-second timeout and 128KB buffer cap.
- All certs use ECDSA P-256. CA certs have `pathLen:0`. Node certs carry both `serverAuth` and `clientAuth` EKU. Client certs carry only `clientAuth`.
- API keys are stored as SHA-256 hashes only; raw keys are never stored server-side. Comparison is constant-time.
- Four API key slots: two ReadWrite, two ReadOnly. `ReadOnly` blocks write/delete/trim/schema operations.
- API keys and client identity require TLS unless `--insecure-allow-plaintext-auth` is explicitly set (server exits at startup otherwise). Without TLS, signed nonces are vulnerable to replay attacks within the 2-minute acceptance window.
- Client identity is `SHA-256(DER public key bytes)[0..16]` as little-endian u128. Validated per-connection at `Identify` time.
- Nonces expire after 2 minutes with 60-second forward clock-skew tolerance. Signing uses RSA-2048 PKCS1v15-SHA256.

## Watch Subscriptions

- Watch subscriptions are shard-local. No cross-shard fan-out at the server level.
- Watch events fire after the write is durably replicated (leader) or after fsync (non-leader). Never before.
- Each client has a bounded channel of 10,000 pending events (`MAX_PENDING_EVENTS`). This is a hard cap.
- The write path uses non-blocking `try_send()`. If the channel is full, the client is immediately removed from the watcher list. No backpressure propagates to the writer.
- Broadcast filtering (operation type, org, aggregate type, aggregate ID) runs before `try_send()`. The write hot path never blocks on watch consumers.
- Clients cannot request a watch latency exceeding `max_requested_latency` (default 2000ms). Rejected with `WatchLatencyTooHigh`.
- Watch subscriptions are not included in the per-shard memory budget. Bounded per-client (10K events) but unbounded in total subscriber count.

## State Machine Transitions

- `FollowerCatchingUp` cannot transition directly to `Leader`.
- `KickFollower` only transitions `Follower→FollowerCatchingUp`. If already catching up, it's a no-op (idempotent).

## Server Startup

These checks run before the server accepts connections. Fatal checks abort the process.

- Direct I/O is verified by attempting an unaligned write with `O_DIRECT`. If it succeeds, the filesystem is silently falling back to buffered I/O - fatal. `EINVAL` confirms DIO is enforced.
- `adjtimex()` checks the kernel clock is NTP-disciplined. Warning only - does not abort.
- Five immutable config parameters are persisted to `server_meta.toml` on first startup and must never change: `num_shards`, `timestamp_precision`, `timestamp_epoch_offset_secs`, `routing_rule`, `reserve_coordinator_shard`. Mismatch is fatal.
- If `compaction_temp_dir` is configured, it must be on the same filesystem as `data_root` (validated via `st_dev`). Cross-device `rename(2)` is not atomic. Fatal.
- Both client and replication ports must be probed via TCP connect before binding. `bind()` alone cannot detect a running instance because glommio uses `SO_REUSEPORT`. Fatal on conflict.
- If TLS is enabled, kTLS kernel support is verified via `setsockopt(SOL_TCP, TCP_ULP, "tls")`. Fatal if missing.
- If `api_keys.toml` exists, each hash must be exactly 64 hex characters. API keys without TLS are fatal unless `--insecure-allow-plaintext-auth` is set.
- Filesystem metadata must be warm before serving requests. Startup walks all `shard_*` directories, stats and opens every `.wal` file to preload XFS inode and extent metadata into OS page cache. No data is read - only metadata. Cold metadata causes latency spikes on first access.
