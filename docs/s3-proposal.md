# S3-Based Distributed Replication Design Document

## Overview

This document describes a distributed replication system for Celeriant that uses Amazon S3's conditional write semantics as the coordination mechanism, replacing traditional consensus protocols like Raft or Paxos.

The core insight: S3 provides strong read-after-write consistency and conditional writes (`If-Match`/`If-None-Match` ETags). These primitives are sufficient to build a correct lease-based coordination system without implementing a full consensus protocol.

---

## Why Not Raft or Paxos?

### Complexity

Raft is conceptually simple but implementation is treacherous:

- **Pre-vote protocol** - Required to prevent disruptive elections from partitioned nodes
- **Configuration changes** - Joint consensus or single-server changes, both have subtle bugs
- **Log compaction** - Snapshotting interacts with replication in non-obvious ways
- **Membership protocol** - Adding/removing nodes safely is harder than the core protocol
- **Leader lease optimization** - Required for read performance, but breaks linearizability if clocks drift

Production Raft implementations (etcd/raft, raft-rs) are 10,000+ lines of carefully audited code. Integrating them requires understanding subtle invariants around state machine application, log persistence ordering, and snapshot coordination.

### Failure Mode Edge Cases

Raft papers and blog posts undercount the edge cases:

| Scenario | Problem |
|----------|---------|
| Partial network partition | Split-brain between leader and different follower subsets |
| Clock skew > election timeout | Leader believes it has lease, follower starts election |
| Disk full on leader | Committed entries can't be persisted, unclear how to recover |
| Slow disk on one follower | Blocks commit if that follower is in quorum |
| Leader crash during snapshot install | Follower state unclear |

Each requires specific handling. The interaction matrix grows combinatorially.

### Three-Node Minimum

Raft/Paxos require 2f+1 nodes to tolerate f failures:

- **3 nodes** - Tolerates 1 failure, but 2 failures = unavailable
- **5 nodes** - Tolerates 2 failures, but 3 failures = unavailable

For Celeriant's use case (high-throughput event store), we want:
- Exactly 2 data-bearing nodes (cost efficiency)
- Tolerance for 1 node failure
- Fallback to durable storage when replication unavailable

This maps poorly onto Raft's model. You'd need a third "witness" node that participates in elections but doesn't store data—adding operational complexity for marginal benefit.

---

## How S3 Enables Correctness

### S3 Consistency Model (Post-2020)

Amazon S3 now provides **strong read-after-write consistency**:

> After a successful write of a new object or an overwrite of an existing object, any subsequent read request immediately receives the latest version of the object.

This is not eventual consistency. A PUT followed by a GET returns the PUT's data. This property is essential for our lease mechanism.

### Conditional Writes

S3 supports conditional operations via ETags:

```
PUT /bucket/key
If-None-Match: *           # Create only if key doesn't exist
If-Match: "abc123"         # Update only if ETag matches
```

These provide compare-and-swap (CAS) semantics:

```python
# Pseudo-code for atomic lease acquisition
def try_acquire_lease(node_id, expire_time):
    try:
        s3.put_object(
            Key="cluster/lease",
            Body=json.dumps({"leader": node_id, "expires": expire_time}),
            IfNoneMatch="*"  # Only succeed if no lease exists
        )
        return True
    except PreconditionFailed:
        return False
```

### Why This Is Sufficient

For leader election, we need:

1. **Mutual exclusion** - At most one leader at a time
2. **Liveness** - If leader fails, a new leader can be elected
3. **Fencing** - Old leaders can't corrupt data after losing leadership

S3 conditional writes provide #1 (CAS semantics). Time-bounded leases provide #2 (leases expire). Monotonic lease indexes (fencing tokens) provide #3.

---

## System Architecture

### Two-Node Model

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

**Leader**: Accepts client writes, replicates to follower, owns the lease.

**Follower**: Receives replicated writes, ready to become leader if lease expires.

**S3**: Stores coordination state (lease, membership) and acts as fallback replication target.

### Lease Structure

```rust
struct Lease {
    leader_node_id: u128,
    lease_index: u64,           // Monotonically increasing fencing token
    acquired_at: u64,           // Unix timestamp (ms)
    expires_at: u64,            // Unix timestamp (ms)
    leader_address: String,     // For client discovery
}
```

Stored at: `s3://{bucket}/{subfolder}/cluster/lease.json`

The `lease_index` is critical: it's a fencing token that monotonically increases with each leadership change. All writes include the current `lease_index`, and followers reject writes with stale indexes.

### Membership Structure

```rust
struct ClusterMembership {
    members: Vec<NodeMembership>,
    version: u64,
}

struct NodeMembership {
    node_id: u128,
    address: String,
    last_seen: u64,             // Unix timestamp
    state: NodeState,           // Leader, Follower, Joining, Leaving
}
```

Stored at: `s3://{bucket}/{subfolder}/cluster/membership.json`

---

## Protocol Description

### Leader Election

```
┌─────────────────────────────────────────────────────────────┐
│                    Leader Election Flow                     │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  1. Node starts, reads current lease from S3                │
│                                                             │
│  2. If lease exists and not expired:                        │
│     - If we are the leader: resume leadership               │
│     - If we are not leader: become follower                 │
│                                                             │
│  3. If lease missing or expired:                            │
│     - Attempt CAS write with new lease (If-None-Match       │
│       or If-Match with current ETag)                        │
│     - If success: we are leader                             │
│     - If fail: someone else won, become follower            │
│                                                             │
│  4. Leader periodically renews lease before expiry          │
│     - Uses If-Match to ensure we still own it               │
│     - If renewal fails: step down immediately               │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Clock Requirements

The system requires **loosely synchronized clocks**. Specifically:

- Clock skew between nodes must be less than `lease_duration / 2`
- Recommended: Use NTP with `lease_duration = 30 seconds`
- Maximum safe skew: ~10-15 seconds

If clocks are badly skewed, two nodes could both believe they hold the lease. The fencing token (`lease_index`) prevents data corruption—a node with a stale lease index will have its writes rejected by followers and by S3.

### Write Path

```
Client Write Request
        │
        ▼
┌───────────────────┐
│ 1. Lease Check    │ ◄── Cached locally, validated periodically
│    (am I leader?) │
└────────┬──────────┘
         │
         │ No ──► Return redirect to current leader
         │
         ▼ Yes
┌───────────────────┐
│ 2. Local Append   │ ◄── Append to local WAL with lease_index
│    + fsync        │
└────────┬──────────┘
         │
         ▼
┌───────────────────────────────────────────────┐
│ 3. Replicate to Follower                      │
│                                               │
│    ┌─────────────┐      ┌─────────────┐       │
│    │  Success    │      │  Timeout/   │       │
│    │             │      │  Failure    │       │
│    └──────┬──────┘      └──────┬──────┘       │
│           │                    │              │
│           ▼                    ▼              │
│    ┌─────────────┐      ┌─────────────┐       │
│    │    ACK      │      │ Replicate   │       │
│    │  to client  │      │   to S3     │       │
│    └─────────────┘      └──────┬──────┘       │
│                                │              │
│                                ▼              │
│                         ┌─────────────┐       │
│                         │    ACK      │       │
│                         │  to client  │       │
│                         └─────────────┘       │
└───────────────────────────────────────────────┘
```

### Replication Message

```rust
struct ReplicationRequest {
    lease_index: u64,           // Fencing token - reject if stale
    shard_id: u32,
    log_id: u64,
    metablock_position: u64,    // Where this batch goes
    datablock_position: u64,
    metablock_bytes: Vec<u8>,
    datablock_bytes: Option<Vec<u8>>,
}

struct ReplicationResponse {
    success: bool,
    error: Option<ReplicationError>,
}

enum ReplicationError {
    StaleLeaseIndex { current: u64, received: u64 },
    ShardMismatch,
    IoError(String),
}
```

### Follower Promotion

When a follower detects the lease has expired:

```
┌─────────────────────────────────────────────────────────────┐
│                   Follower Promotion Flow                   │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  1. Detect lease expiry (poll S3, check timestamp)          │
│                                                             │
│  2. Wait for safety buffer (clock_skew_allowance)           │
│                                                             │
│  3. Attempt to acquire lease with CAS:                      │
│     - new lease_index = old lease_index + 1                 │
│     - If-Match: current ETag                                │
│                                                             │
│  4. If successful:                                          │
│     a. Pull any missing data from S3 (fallback replicas)    │
│     b. Begin accepting client writes                        │
│     c. Mark old leader as follower in membership            │
│                                                             │
│  5. If failed (another node won):                           │
│     - Remain follower to new leader                         │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### S3 Fallback Replication

When the follower is unavailable:

```rust
// Data is written to S3 organized by shard and position
// s3://{bucket}/{subfolder}/data/shard_{id}/log_{log_id}/batch_{position}.bin

struct S3ReplicationBatch {
    lease_index: u64,
    metablock_bytes: Vec<u8>,
    datablock_bytes: Option<Vec<u8>>,
}
```

A new leader must:
1. List all S3 fallback data for each shard
2. Apply any batches with `lease_index` >= their local state
3. Delete applied fallback data (or mark as applied)

---

## Failure Scenarios

### Leader Crashes

1. Follower detects lease expiry (no renewal)
2. Follower waits safety buffer
3. Follower acquires lease, increments `lease_index`
4. Follower pulls any S3 fallback data
5. Follower becomes leader, accepts writes

**Recovery time**: `lease_duration + safety_buffer` ≈ 35-40 seconds

### Follower Crashes

1. Leader continues accepting writes
2. Replication times out, leader falls back to S3
3. Writes are durably stored (local + S3)
4. When follower recovers, it catches up from leader
5. Leader resumes replicating to follower

**Impact**: Higher write latency (S3 round-trip), higher cost

### Network Partition (Leader ↔ Follower)

1. Leader can't reach follower, falls back to S3
2. Follower can't reach leader, waits for lease expiry
3. If follower can reach S3 and lease expires: promotion attempt
4. Fencing token prevents split-brain data corruption

**Risk**: If leader and follower are both partitioned from each other but can both reach S3, the follower will take over after lease expiry. The old leader's in-flight writes (not yet S3-replicated) could be lost. Mitigation: leader steps down if it can't renew lease OR replicate for `lease_duration / 2`.

### S3 Outage

1. Leader can still replicate to follower—writes continue
2. Lease renewal fails—after `lease_duration`, leadership uncertain
3. If S3 remains down: cluster continues with degraded coordination
4. Follower promotion is blocked (can't acquire lease)

**Mitigation**: Cache lease locally, allow writes to continue with follower replication. Accept risk of brief split-brain on S3 recovery. Leader should step down if S3 unreachable for extended period AND follower unreachable.

---

## Client Protocol

### Leader Discovery

Clients discover the leader through:

1. **S3 lease file** (authoritative, slower)
2. **Redirect response** from non-leader nodes (fast path)

```rust
// Client connection flow
loop {
    let node = pick_node_from_membership();
    match send_write_request(node, request).await {
        Ok(response) => return response,
        Err(NotLeader { leader_address }) => {
            // Fast path: redirect from node
            update_cached_leader(leader_address);
            continue;
        }
        Err(Timeout) => {
            // Slow path: re-read lease from S3
            refresh_leader_from_s3().await;
            continue;
        }
    }
}
```

### Lease Validation Response

When a non-leader receives a write:

```rust
struct NotLeaderResponse {
    correlation_id: Option<u128>,
    current_leader_node_id: u128,
    current_leader_address: String,
    lease_expires_at: u64,
}
```

---

## Implementation Changes

### New Crates

```
celeriant_cluster/
├── src/
│   ├── lib.rs
│   ├── lease.rs              # Lease acquisition, renewal, monitoring
│   ├── membership.rs         # Cluster membership management
│   ├── replication.rs        # Leader→Follower replication protocol
│   ├── s3_fallback.rs        # S3 fallback replication
│   ├── promotion.rs          # Follower→Leader promotion logic
│   └── clock.rs              # Clock skew detection/validation
```

### Modified Crates

#### celeriant_sidecar

Add S3 operations for cluster coordination:

```rust
// New request types
enum Request {
    // ... existing ...
    
    // Lease operations
    LeaseRead,
    LeaseAcquire { lease: Lease, expected_etag: Option<String> },
    LeaseRenew { lease: Lease, expected_etag: String },
    
    // Membership operations  
    MembershipRead,
    MembershipUpdate { membership: ClusterMembership, expected_etag: String },
    
    // Fallback data operations
    FallbackWrite { shard_id: u32, position: u64, data: Bytes },
    FallbackList { shard_id: u32 },
    FallbackRead { shard_id: u32, position: u64 },
    FallbackDelete { shard_id: u32, position: u64 },
}
```

#### celeriant_runtimes

- Add replication listener (TCP) on leader for follower connections
- Add replication sender on follower
- Integrate lease checking into write path
- Add redirect responses for non-leader writes

#### celeriant_filesystem

- Store `lease_index` in metablocks
- Validate `lease_index` on follower before applying replication

#### celeriant/src/server_config.rs

New configuration options:

```rust
// Cluster mode configuration
#[arg(long, env = "CELERIANT_CLUSTER_ENABLED")]
pub cluster_enabled: bool,

#[arg(long, env = "CELERIANT_LEASE_DURATION_MS", default_value = "30000")]
pub lease_duration_ms: u64,

#[arg(long, env = "CELERIANT_LEASE_RENEW_INTERVAL_MS", default_value = "10000")]
pub lease_renew_interval_ms: u64,

#[arg(long, env = "CELERIANT_REPLICATION_TIMEOUT_MS", default_value = "5000")]
pub replication_timeout_ms: u64,

#[arg(long, env = "CELERIANT_REPLICATION_PORT", default_value = "10001")]
pub replication_port: u16,
```

---

## Task Breakdown

### Phase 1: S3 Coordination Layer (2 weeks)

| Task | Description | Estimate |
|------|-------------|----------|
| 1.1 | Define lease and membership data structures | 2h |
| 1.2 | Implement S3 lease read/write in sidecar | 4h |
| 1.3 | Implement lease acquisition with CAS | 4h |
| 1.4 | Implement lease renewal loop | 4h |
| 1.5 | Implement lease expiry detection | 4h |
| 1.6 | Implement membership read/update | 4h |
| 1.7 | Add cluster config options | 2h |
| 1.8 | Unit tests for lease state machine | 8h |
| 1.9 | Integration tests with LocalStack/MinIO | 8h |

### Phase 2: Replication Protocol (2 weeks)

| Task | Description | Estimate |
|------|-------------|----------|
| 2.1 | Define replication message types | 2h |
| 2.2 | Implement replication TCP listener (leader) | 8h |
| 2.3 | Implement replication sender (follower) | 8h |
| 2.4 | Add lease_index to metablocks | 4h |
| 2.5 | Implement lease_index validation on apply | 4h |
| 2.6 | Integrate replication into write path | 8h |
| 2.7 | Implement replication timeout + retry | 4h |
| 2.8 | Unit tests for replication protocol | 8h |
| 2.9 | Integration tests (two-node) | 8h |

### Phase 3: S3 Fallback Replication (1 week)

| Task | Description | Estimate |
|------|-------------|----------|
| 3.1 | Implement S3 fallback write | 4h |
| 3.2 | Implement S3 fallback list/read | 4h |
| 3.3 | Integrate fallback into write path | 4h |
| 3.4 | Implement fallback data cleanup | 4h |
| 3.5 | Tests for fallback scenarios | 8h |

### Phase 4: Leader Promotion (1 week)

| Task | Description | Estimate |
|------|-------------|----------|
| 4.1 | Implement promotion state machine | 8h |
| 4.2 | Implement S3 fallback data recovery | 8h |
| 4.3 | Implement graceful leader stepdown | 4h |
| 4.4 | Tests for promotion scenarios | 8h |

### Phase 5: Client Protocol (1 week)

| Task | Description | Estimate |
|------|-------------|----------|
| 5.1 | Implement NotLeader response type | 2h |
| 5.2 | Add redirect handling in shard | 4h |
| 5.3 | Update client library with leader discovery | 8h |
| 5.4 | Implement client-side S3 lease reading | 4h |
| 5.5 | Client integration tests | 8h |

### Phase 6: Hardening (2 weeks)

| Task | Description | Estimate |
|------|-------------|----------|
| 6.1 | Clock skew detection and warnings | 4h |
| 6.2 | Metrics for replication lag, lease state | 8h |
| 6.3 | Chaos testing (network partitions) | 16h |
| 6.4 | Chaos testing (node crashes) | 16h |
| 6.5 | Performance benchmarking | 8h |
| 6.6 | Documentation | 8h |

---

## Open Questions

1. **Exactly-once delivery to follower**: Should we use sequence numbers or position-based deduplication? Position-based is simpler but requires follower state tracking.

2. **Read consistency**: Can follower serve reads? If yes, what staleness is acceptable? Recommend: follower reads with bounded staleness, or require lease check for linearizable reads.

3. **More than 2 nodes**: Design supports 2 nodes. For N>2, we'd need quorum writes—significantly more complex. Recommend: keep 2-node model, use S3 for additional durability.

4. **Lease duration tuning**: 30 seconds is conservative. Could reduce to 10-15 seconds for faster failover, but increases S3 API costs and clock skew sensitivity.

5. **S3 bucket permissions**: Leader and follower need read/write to lease path. Should we use separate IAM roles? Recommend: single role, audit logging for security.