# S3-Based Distributed Replication Design

## Why S3, Not Raft

Raft is conceptually simple but implementation is treacherous: pre-vote protocol, configuration changes, log compaction, membership changes, leader lease optimization. Production implementations are 10,000+ lines. The edge cases multiply: partial network partitions, clock skew, disk full, slow followers, crash during snapshot install.

Raft also requires 2f+1 nodes to tolerate f failures. We want exactly 2 data-bearing nodes.

S3 provides strong read-after-write consistency and conditional writes (`If-Match`/`If-None-Match`). These primitives are sufficient for lease-based coordination without a full consensus protocol.

---

## Architecture

```
┌─────────────────┐         ┌─────────────────┐
│     Leader      │◄───────►│    Follower     │
│   (Node A)      │  sync   │    (Node B)     │
│                 │  repl   │                 │
└────────┬────────┘         └────────┬────────┘
         │                           │
         │    ┌─────────────────┐    │
         └───►│       S3        │◄───┘
              │  (Coordination) │
              │                 │
              │  - Lease file   │
              │  - Membership   │
              │  - Fallback     │
              │    replication  │
              └─────────────────┘
```

**Leader**: Accepts writes, replicates to follower, owns the lease.
**Follower**: Receives replicated writes, promotes if lease expires.
**S3**: Coordination state and fallback replication target.

---

## Core Design Principles

1. **Single fsync per batch** — No Raft-style double-commit. Flow: fsync → replicate → rollback on failure → ack
2. **Heartbeat-based leadership** — Leader doesn't touch S3 while heartbeat maintained with follower
3. **Shard 0 owns leadership** — All cluster coordination flows through shard 0 via intrashard messaging
4. **Separate replication coordinator** — Don't combine with fsync to preserve throughput/latency
5. **Hash chaining for divergence detection** — Each WAL entry includes hash of previous entry
6. **Dynamic lease duration** — Adapts based on heartbeat latency/jitter, targeting 1-2 seconds when stable
7. **S3 only on promotion** — Normal operation never touches S3; only used when follower promotes

---

## Data Structures

### Lease (`cluster/lease.bin`)

Contains: leader node ID, lease index (fencing token, monotonically increasing), acquired/expires timestamps, leader address.

### Membership (`cluster/membership.bin`)

Contains: version, leader info, follower info, s3_fallback_pending flag (indicates new leader must pull S3 fallback data before accepting writes).

Each node membership includes: node ID, address, replication port, last seen timestamp, state (Active/Joining/Leaving/Unreachable).

### Hash Chain

Each WAL entry hash = blake3(previous_hash || wal_index || content). Chain starts from genesis (all zeros). Divergence detected when follower hash at index N ≠ leader hash → truncate and resync.

### S3 Fallback

Path format: `cluster/fallback/shard_{shard_id:02}/batch_{s3_index:09}.bin`

Zero-padded monotonic index ensures lexicographic ordering = temporal ordering.

---

## Durability Semantics

### Write Visibility

**Critical**: Readers must only see data after successful replication, not just after leader fsync.

LogSegmentFileMetadata tracks two positions:
- **write_position** — Where next write goes (updated after fsync)
- **visible_position** — What readers see (updated after successful replication)

Operations:
- Advance write positions after fsync
- Advance visible positions after successful replication
- Rollback write positions when replication fails (both follower and S3)

Requires changes to how we structure data in celeriant_memcache and celeriant_rotating_log.

### Follower Acknowledgment

Follower fsyncs before acknowledging. Same durability guarantee as leader.

---

## Write Flow (Leader)

```
Client Write
     │
     ▼
┌─────────────────────────────────────────────────────┐
│ Phase 1: Validation + Queue                         │
│   • Validate OCC, idempotency                       │
│   • Build metablock + datablock                     │
│   • Compute entry hash (chain to previous)          │
│   • Add to pending queue                            │
└─────────────────────┬───────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────┐
│ Phase 2: Fsync                                      │
│   • Coalesce pending writes                         │
│   • Single fdatasync                                │
│   • Update write positions (NOT visible)            │
└─────────────────────┬───────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────┐
│ Phase 3: Replication                                │
│                                                     │
│   ┌─────────────────┬─────────────────────┐         │
│   │  Follower OK    │   Follower Timeout  │         │
│   │  (fsynced)      │   or Unreachable    │         │
│   └────────┬────────┴─────────┬───────────┘         │
│            │                  │                     │
│            ▼                  ▼                     │
│   ┌────────────────┐  ┌────────────────────┐        │
│   │ Advance        │  │ S3 Fallback Write  │        │
│   │ visible pos,   │  └─────────┬──────────┘        │
│   │ ACK to client  │            │                   │
│   └────────────────┘  ┌─────────┴──────────┐        │
│                       │  S3 OK  │  S3 Down │        │
│                       └────┬────┴────┬─────┘        │
│                            │         │              │
│                            ▼         ▼              │
│                   ┌──────────┐  ┌──────────────┐    │
│                   │ Advance  │  │ Rollback,    │    │
│                   │ visible, │  │ reject write │    │
│                   │ ACK      │  └──────────────┘    │
│                   └──────────┘                      │
└─────────────────────────────────────────────────────┘
```

**Critical Rule**: If both follower AND S3 are unreachable, leader rejects writes.

---

## Catch-up Flow (Follower)

Followers can fall behind, or crash. If they are falling behind, they need to remove themselves from the cluster. After a crash + restart, they need to catch-up the shard WALs or S3 data before rejoining the cluster. Joining too early will kill cluster write latency, causing timeouts.

Leaders need to tolerate followers that are slightly behind. A shard on a leader needs to know which WAL entries to send to the follower, so it keeps track of this in memory. It shouldn't check each write, we only want a single network round-trip per replication batch.

If a follower gets too far behind, the leader should kick it out of the cluster.

## Failure Modes

| # | Scenario | Behavior | Safe |
|---|----------|----------|------|
| 1 | Leader crash mid-fsync | No ack sent, not committed | ✅ |
| 2 | Leader crash post-fsync, pre-replicate | Data on leader only, no ack. Truncates on restart as follower | ✅ |
| 3 | Leader crash post-replicate, pre-ack | Data on both nodes, idempotency handles retry | ✅ |
| 4 | Follower crash during receive | Leader uses S3 fallback | ✅ |
| 5 | Follower crash after fsync | Durable on both | ✅ |
| 6 | Network partition (L↔F) | Leader uses S3 fallback, follower waits for lease expiry | ✅ |
| 7 | Network partition (L↔S3) | Leader continues with follower only | ✅ |
| 8 | Network partition (F↔S3) | Follower cannot promote if leader fails | ⚠️ |
| 9 | Both follower AND S3 down | Leader rejects writes | ✅ |
| 10 | Clock skew > threshold | Immediate step-down | ✅ |
| 11 | Stale lease on write | Follower rejects (fencing) | ✅ |
| 12 | Hash chain divergence | Follower truncates and re-syncs | ✅ |

---

## Invariants

### Safety

1. **Single Writer** — Only leader accepts writes. Enforced via `lease_index` fencing token.
2. **Durability Before Ack** — Leader: fsync → replicate → ack. Follower: fsync before ack.
3. **Visibility Ordering** — `write_position ≥ visible_position` always. Readers only see replicated data.
4. **Hash Chain Integrity** — Divergence detected, truncate and resync.
5. **Dual Failure Rejection** — Both follower + S3 unreachable → reject writes.
6. **Clock Drift Bounds** — |drift| ≤ 500ms. Violation → immediate step-down.
7. **Lease Expiry Safety** — Leader stops writes before lease expires. Follower only promotes after S3 confirms expiry.
8. **Fencing Token Monotonicity** — `lease_index` only increases. New leader = previous + 1.

### Operational

9. **One Connection Per Shard** — Persistent TCP, shard 0 owns coordination.
10. **S3 Access Minimization** — S3 only on: startup, promotion, fallback writes, expiry check.
11. **Single Fsync Per Batch** — Normal operation. Rollback requires second fsync.
12. **Join Only When Caught Up** — Nodes must be near end of WAL before joining cluster.

---

## Key Decisions

| Question | Decision |
|----------|----------|
| Follower ack semantics | Fsync before ack (same as leader) |
| WAL index continuity | Hash chaining detects divergence; follower truncates |
| S3 fallback ordering | Monotonic zero-padded index in key |
| Lease duration | Dynamic: 1-30 seconds based on RTT + jitter + drift |
| S3 usage frequency | Only on promotion; heartbeat maintains leadership |
| Read visibility | Separate write/visible positions |
| Both follower + S3 down | Leader rejects writes |
| Client failover | Clients know both addresses, switch on NotLeader or timeout |

---

## What's Not Specified

Implementation details left to engineering judgment:

- Exact message wire formats
- Batch size limits and tuning
- Reconnection backoff parameters
- Catch-up streaming implementation
- Metrics naming
- Configuration defaults
- Crate boundaries

---

## Future Work (Out of Scope)

- **Backpressure** — Memory growth under high write load
- **Multi-Region** — Async replication, region-aware routing
- **More Than Two Nodes** — Would require quorum-based writes, likely Raft
