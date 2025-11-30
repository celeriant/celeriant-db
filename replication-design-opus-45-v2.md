# EventPlaneDB Replication Design Document

## Table of Contents

1. [Introduction](#1-introduction)
2. [High-Level Design](#2-high-level-design)
3. [Detailed Design](#3-detailed-design)
4. [Risk Evaluation](#4-risk-evaluation)
5. [Task Breakdown](#5-task-breakdown)

---

## 1. Introduction

### 1.1 Purpose

This document specifies the replication strategy for EventPlaneDB, enabling high availability and strong durability without implementing traditional distributed consensus protocols like Raft or Paxos.

### 1.2 Design Philosophy

Traditional consensus algorithms are complex, require odd-numbered clusters for quorum, and add significant latency to the write path. Instead, we delegate coordination to **Amazon S3's conditional writes** (optimistic concurrency control), treating S3 as an external, strongly-consistent coordination service.

This approach trades S3 dependency for implementation simplicity while maintaining:
- **Strong durability**: Writes acknowledged only after leader fsync AND replication
- **Per-aggregate leadership**: Different aggregates can have different leaders
- **Two-node clusters**: Viable without the typical quorum restrictions
- **Cost efficiency**: S3 operations only for active aggregates

### 1.3 Why Replicate to ALL Followers (Not Quorum)

A common question is why we require acknowledgment from all followers rather than a quorum (majority). The rationale:

1. **No latency penalty**: Replication occurs **concurrently** via parallel TCP connections. The latency is determined by the slowest follower, not the sum. With same-spec machines in the same availability zone, follower response times are nearly identical.

2. **Kafka precedent**: Kafka's `acks=all` mode (the strongest durability setting) works identically. The alternatives (`acks=0`, `acks=1`) sacrifice durability for latency in scenarios where that trade-off is acceptable.

3. **Follower unavailability is rare**: In a well-operated cluster, follower unavailability is a <0.1% edge case. Optimizing for quorum penalizes the 99.9% normal case with weaker durability guarantees.

4. **Degraded mode handles failures**: When a follower is unavailable, we fall back to S3 (degraded mode) rather than blocking. This provides durability without sacrificing availability.

5. **Simpler reasoning**: "All replicas have the data" is easier to reason about than "a majority have the data, and we need read-repair or anti-entropy to fix the rest."


### 1.4 Terminology

| Term | Definition |
|------|------------|
| **Aggregate** | A unique event stream identified by `(org_id, aggregate_type_id, aggregate_id)` |
| **Leader** | The node holding a valid lease for an aggregate; accepts writes |
| **Follower** | A node that receives replicated data from the leader |
| **Lease** | A time-bound lock stored in S3 granting leadership of an aggregate |
| **Degraded Mode** | State where leader writes to S3 because followers are unavailable |
| **Fencing Token** | Monotonic `lease_index` preventing stale leaders from writing |

---

## 2. High-Level Design

### 2.1 Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           S3 Control Plane                              │
│  ┌────────────────────┐  ┌────────────────────────────────────────────┐ │
│  │ Cluster Membership │  │ Per-Aggregate Leases                       │ │
│  │ /cluster/members   │  │ /leases/{org}/{type}/{agg}/lease.json     │ │
│  └────────────────────┘  └────────────────────────────────────────────┘ │
│                          ┌────────────────────────────────────────────┐ │
│                          │ Degraded Mode Batches (hot path fallback)  │ │
│                          │ /batches/{org}/{type}/{agg}/{index}.bin   │ │
│                          └────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
                                      │
              ┌───────────────────────┼───────────────────────┐
              │                       │                       │
              ▼                       ▼                       ▼
       ┌────────────┐          ┌────────────┐          ┌────────────┐
       │   Node A   │◄────────►│   Node B   │◄────────►│   Node C   │
       │  Leader:   │   TCP    │  Follower  │   TCP    │  Follower  │
       │  Agg 1,3   │  Repl.   │            │  Repl.   │            │
       │  Follower: │          │  Leader:   │          │            │
       │  Agg 2     │          │  Agg 2     │          │            │
       └────────────┘          └────────────┘          └────────────┘
              │
              ▼
        ┌──────────┐
        │ Clients  │
        └──────────┘
```

### 2.2 Core Invariants

1. **Single Leader**: At most one node holds a valid lease for any aggregate at any time
2. **Durability Before Acknowledgment**: Client receives success only after:
   - Leader has fsync'd locally, AND
   - All active followers have acknowledged, OR all unavailable followers have their batch in S3
3. **Fencing**: All writes carry `lease_index`; followers reject stale epochs
4. **Clock Discipline**: Nodes refuse leadership if clock skew exceeds safety margin

### 2.3 Write Path Summary

```
┌─────────────────┐
│ Client Request  │
└────────┬────────┘
         ▼
┌─────────────────┐     ┌─────────────────────┐
│ Leader Check    │────►│ Not Leader?         │
│ (Lease Valid?)  │     │ Return leader hint  │
└────────┬────────┘     └─────────────────────┘
         │ Valid
         ▼
┌─────────────────┐
│ Local Append    │
│ (in-memory WAL) │
└────────┬────────┘
         ▼
┌─────────────────────────────────────────────┐
│ Parallel Replication to Followers (TCP)     │
│                                             │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐     │
│  │Follower1│  │Follower2│  │Follower3│     │
│  └────┬────┘  └────┬────┘  └────┬────┘     │
│       │            │            │          │
│      ACK          ACK         TIMEOUT      │
└───────┬────────────┬────────────┬──────────┘
        │            │            │
        ▼            ▼            ▼
┌─────────────────────────────────────────────┐
│ All ACK'd?                                  │
│                                             │
│  YES ──► Local fsync ──► ACK to Client      │
│                                             │
│  NO  ──► Write failed batches to S3         │
│          (degraded mode) ──► Local fsync    │
│          ──► ACK to Client (degraded flag)  │
└─────────────────────────────────────────────┘
```

### 2.4 Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Per-aggregate leadership** | Distributes load; different aggregates can have different leaders |
| **S3 as coordination layer** | Avoids implementing Paxos/Raft; leverages S3's strong consistency |
| **Lease-based with dormancy** | Saves S3 costs; inactive aggregates don't renew leases |
| **Synchronous replication to all** | Maximum durability; concurrent TCP means no latency penalty vs quorum |
| **S3 degraded fallback** | Maintains durability when followers unreachable |
| **Two-node support** | S3 provides the "third vote"; availability trades for latency in failure |

---

## 3. Detailed Design

### 3.1 Node Identity

Each node requires a persistent, unique identifier.

**Storage**: `{data_root}/node_id`

**Format**: 32 hexadecimal characters representing a `u128`

**Generation**: Random `u128` on first startup using cryptographically secure RNG

**Usage**:
- Lease acquisition and validation
- Event batch metadata attribution
- Cluster membership registration
- Replication message authentication

### 3.2 Cluster Membership

**S3 Path**: `s3://{bucket}/{cluster}/membership.json`

**Schema**:
```json
{
  "version": 42,
  "members": [
    {
      "node_id": "a1b2c3d4...",
      "client_address": "10.0.1.5:10000",
      "replication_address": "10.0.1.5:10001",
      "is_active": true,
      "joined_at": 1700000000000,
      "last_heartbeat": 1700000060000
    }
  ]
}
```

**Semantics**:
- `version`: Monotonic; enables conditional updates
- `is_active`: Nodes can be marked inactive for maintenance
- `last_heartbeat`: Nodes update periodically; stale nodes considered dead

**Operations**:
- Nodes poll membership every 30 seconds and cache locally
- Updates via conditional write (`If-Match` on ETag)
- Leader uses membership to determine replication targets

### 3.3 Per-Aggregate Leases

#### 3.3.1 Lease Structure

**S3 Path**: `s3://{bucket}/{cluster}/leases/{org_id}/{type_id}/{agg_id}/lease.json`

**Schema**:
```json
{
  "lease_index": 105,
  "node_id": "a1b2c3d4...",
  "lease_expiry": 1700000030000,
  "event_batch_index": 4500,
  "requested_by_client": "client-uuid"
}
```

| Field | Purpose |
|-------|---------|
| `lease_index` | Fencing token; monotonically increasing; prevents zombie writes |
| `node_id` | Current lease holder |
| `lease_expiry` | Absolute timestamp (ms) when lease expires |
| `event_batch_index` | Last committed batch index; helps catch-up |
| `requested_by_client` | Debugging/tracing: which client triggered leadership |

#### 3.3.2 Lease Acquisition

**Trigger Conditions**:
1. Node receives write for an aggregate it doesn't lead
2. Current lease is expired or near expiry
3. Node is recovering and needs to determine state

**Algorithm**:
```
1. READ current lease from S3
2. IF lease exists AND not expired AND holder ≠ self:
     RETURN NotLeader(current_leader_hint)
3. IF local state behind lease.event_batch_index:
     CATCH UP from S3 or peers before proceeding
4. CONSTRUCT new lease:
     lease_index = old_lease_index + 1 (or 1 if none)
     node_id = self
     lease_expiry = now + LEASE_DURATION
     event_batch_index = local committed index
5. CONDITIONAL PUT with If-Match on ETag
6. IF success: Become leader with new lease_index
7. IF 412 conflict: Re-read and retry (or return NotLeader)
```

#### 3.3.3 Lease Renewal

**Active Aggregates**: If writes received within the last period, renew before `expiry - RENEWAL_THRESHOLD`.

**Dormant Aggregates**: If no writes for `IDLE_THRESHOLD`, stop renewing. Let lease expire. Next write triggers re-acquisition.

**Renewal Algorithm**:
```
1. READ current lease
2. VERIFY node_id == self AND not yet expired
3. CONDITIONAL PUT with:
     lease_index = old_lease_index + 1
     lease_expiry = now + LEASE_DURATION
     (preserve event_batch_index)
4. IF conflict: Lost leadership; step down immediately
```

#### 3.3.4 Lease Release

On graceful shutdown:
```
For each held lease:
  1. SET lease_expiry = now (immediate expiry)
  2. CONDITIONAL PUT
```

Enables faster failover during rolling deployments.

#### 3.3.5 Lease Constants

| Constant | Default | Purpose |
|----------|---------|---------|
| `LEASE_DURATION` | 30s | Time before lease expires |
| `RENEWAL_THRESHOLD` | 10s | Renew when this much time remains |
| `IDLE_THRESHOLD` | 60s | Stop renewing if no writes |
| `MAX_CLOCK_SKEW` | 2s | Safety margin for clock drift |

### 3.4 Time Synchronization

#### 3.4.1 Clock Requirements

Time-based leases require bounded clock skew across all nodes.

**Synchronization**: All nodes must use NTP (AWS Time Sync Service recommended)

**Validation**:
```
On startup and every 60s:
  1. Probe all known peers for their current time
  2. Calculate maximum observed drift
  3. IF drift > MAX_CLOCK_SKEW:
       Log warning, enter degraded replication mode
  4. IF drift > 2 × MAX_CLOCK_SKEW:
       Refuse to acquire new leases
       Continue as follower only
```

#### 3.4.2 Lease Safety Windows

To prevent split-brain from clock skew:

- **Lease validity check**: `now < lease_expiry - MAX_CLOCK_SKEW`
- **New acquisition wait**: `now > lease_expiry + MAX_CLOCK_SKEW`
- **Write acceptance**: Refuse if `lease_expiry - now < 2 × MAX_WRITE_TIME`

### 3.5 Replication Protocol

#### 3.5.1 Transport

- **Protocol**: TCP with binary framing (glommio-compatible)
- **Port**: Dedicated replication port separate from client port
- **Connections**: Persistent mesh between all node pairs

**Wire Format**:
```
┌──────────┬──────────┬────────────┬─────────────────┐
│ Version  │ Msg Type │ Length     │ Payload         │
│ 4 bytes  │ 4 bytes  │ 4 bytes    │ Variable        │
└──────────┴──────────┴────────────┴─────────────────┘
```

#### 3.5.2 Message Types

| Type | Direction | Purpose |
|------|-----------|---------|
| `ReplicateBatch` | Leader → Follower | Send batch with metadata |
| `ReplicateBatchAck` | Follower → Leader | Acknowledge receipt + fsync |
| `ReplicateBatchNack` | Follower → Leader | Missing batches; request backfill |
| `CatchUpRequest` | Follower → Leader | Request batches from index X |
| `CatchUpResponse` | Leader → Follower | Stream of requested batches |
| `TimeSync` | Bidirectional | Clock drift detection probe |

#### 3.5.3 ReplicateBatch Message

```rust
struct ReplicateBatch {
    org_id: u128,
    aggregate_type_id: u128,
    aggregate_id: u128,
    lease_index: u64,
    from_event_batch_index: u64,
    batches: Vec<EventBatchItem>,
}
```

#### 3.5.4 Follower Processing

```
1. VALIDATE lease_index ≥ expected (reject stale leaders)
2. CHECK event_batch_index is contiguous with local state
3. IF gap detected:
     RETURN Nack(expected_index)
4. WRITE batch to local files
5. FSYNC
6. RETURN Ack
```

#### 3.5.5 Gap Recovery

When follower detects missing batches:
```
Follower state: batches 1-100
Leader sends:   batch 105

Follower: Nack(expected=101)

Leader responds with:
  ReplicateBatch { batches: [101, 102, 103, 104, 105] }
```

If leader cannot provide (trimmed), follower falls back to S3 catch-up.

### 3.6 Write Path (Detailed)

#### 3.6.1 Write Batching (Per-Thread)

Batching is critical for achieving target throughput (300k+ writes/sec). Batching occurs at multiple levels:
Each glommio executor thread batches writes before replication:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Thread-Local Write Batcher                   │
│                                                                 │
│  Incoming writes accumulate in thread-local queue               │
│  Batch triggers:                                                │
│    1. Time threshold reached (e.g., 1ms)                        │
│    2. Batch size threshold reached (e.g., 64KB)                 │
│    3. Explicit flush requested (durable_write_with_delay_us=0)  │
│                                                                 │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐                            │
│  │ Agg A   │ │ Agg B   │ │ Agg A   │  ← Writes from clients     │
│  │ Write 1 │ │ Write 1 │ │ Write 2 │                            │
│  └────┬────┘ └────┬────┘ └────┬────┘                            │
│       └───────────┴───────────┘                                 │
│                   │                                             │
│                   ▼                                             │
│  ┌─────────────────────────────────────────┐                    │
│  │ Batched Replication Message             │                    │
│  │ [AggA:W1, AggB:W1, AggA:W2]             │                    │
│  └─────────────────────────────────────────┘                    │
└─────────────────────────────────────────────────────────────────┘
```

A single TCP write to a follower can contain multiple event batches from different aggregates:

```rust
struct BatchedReplicateMessage {
    /// Multiple batches, potentially from different aggregates
    batches: Vec<ReplicateBatchEntry>,
}

struct ReplicateBatchEntry {
    org_id: u128,
    aggregate_type_id: u128,
    aggregate_id: u128,
    lease_index: u64,
    event_batch_index: u64,
    compressed_batch: Vec<u8>,
}
```
Cross-aggregate batching affects transaction semantics:

| Scenario | Behavior |
|----------|----------|
| All entries succeed | Single ACK for entire batch |
| Some entries fail (stale lease) | Per-entry NACK; leader handles individually |
| Follower unreachable | Entire batch fails; all entries go to S3 |

**Important**: Cross-aggregate batching is an optimization, not a transaction boundary. Each aggregate's write is independently durable once acknowledged. There is no cross-aggregate atomicity guarantee.

This is the primary throughput multiplier: we're bounded by NVMe fsync latency (~100μs), not write count. With 1ms batching windows, we can batch ~1000 writes per fsync.


#### 3.6.2 Leader Write Sequence

```
1. RECEIVE write request from client
2. VALIDATE lease:
   - If no lease or expired: Attempt acquisition
   - If acquisition fails: Return NotLeader(hint)
   - If lease expiry too close: Attempt renewal first
3. VALIDATE write (optimistic concurrency, idempotency)
4. ASSIGN event_batch_index, event indexes
5. CREATE EventBatchItem with metadata (node_id, lease_index)
6. SERIALIZE and compress
7. APPEND to local WAL (in-memory queue)
8. PREPARE rollback state:
   - pre_write_file_len_metadata
   - pre_write_file_len_event_batch
   - prior_client_event_index
9. REPLICATE to all active followers in parallel:
   - Send ReplicateBatch
   - Collect responses with timeout
   - Handle Nacks (send missing batches)
10. EVALUATE responses:
    - All ACK: Proceed to step 11
    - Any unreachable/timeout: Degraded mode (step 12)
11. NORMAL COMMIT:
    - Local fsync
    - Clear rollback state
    - Return success to client
12. DEGRADED COMMIT:
    - Write batch to S3 for failed followers
    - Local fsync
    - Mark aggregate as degraded
    - Return success with degraded flag
13. ON FAILURE before commit:
    - Execute rollback (truncate files)
    - Return error to client
```

#### 3.6.3 Rollback State

```rust
struct PendingWrite {
    event_batch_index: u64,
    pre_write_file_len_metadata: u64,
    pre_write_file_len_event_batch: u64,
    prior_client_event_indexes: HashMap<u128, u64>,
}
```

**Rollback Procedure**:
1. Truncate metadata file to `pre_write_file_len_metadata`
2. Truncate event batch file to `pre_write_file_len_event_batch`
3. Restore `next_event_batch_index`
4. Restore `client_event_indexes`
5. Clear pending batch from memory

### 3.7 Degraded Mode

When followers are unreachable, batches must still be durable for later catch-up.

#### 3.7.1 S3 Batch Storage

**Path**: `s3://{bucket}/{cluster}/batches/{org_id}/{type_id}/{agg_id}/{batch_index}.bin`

**Content**: Serialized + compressed `EventBatchItem` with metadata

**Write Procedure**:
```
1. After local fsync succeeds
2. PUT to S3 (no conditional write needed; batch_index is unique)
3. Record in local degraded mode state
```

#### 3.7.2 S3 Cost Amortization

To minimize S3 costs during degraded mode, we amortize multiple event batches into single S3 objects:

**Amortized Path**: `s3://{bucket}/{cluster}/batches/{org_id}/{type_id}/{agg_id}/range_{from_index}_{to_index}.bin`

**Batching Strategy**:
```
┌─────────────────────────────────────────────────────────────────┐
│                  S3 Write Amortization                          │
│                                                                 │
│  Event batches accumulate in memory during degraded mode        │
│  S3 flush triggers:                                             │
│    1. Time threshold (configurable, e.g., 1500ms)               │
│    2. Size threshold (e.g., 1MB accumulated)                    │
│    3. Graceful shutdown                                         │
│                                                                 │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐              │
│  │Batch 101│ │Batch 102│ │Batch 103│ │Batch 104│              │
│  └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘              │
│       └───────────┴───────────┴───────────┘                    │
│                         │                                       │
│                         ▼                                       │
│  ┌─────────────────────────────────────────────────┐           │
│  │ S3 Object: range_101_104.bin                    │           │
│  │ Contains: [Batch101, Batch102, Batch103, Batch104]          │
│  └─────────────────────────────────────────────────┘           │
└─────────────────────────────────────────────────────────────────┘
```

**Configuration**:
```rust
pub struct DegradedModeConfig {
    /// How often to flush accumulated batches to S3
    pub s3_flush_interval_ms: u64,       // default: 1500
    
    /// Maximum bytes to accumulate before forced S3 flush
    pub s3_flush_size_bytes: usize,      // default: 1_048_576 (1MB)
    
    /// Maximum batches to accumulate before forced S3 flush
    pub s3_flush_max_batches: usize,     // default: 1000
}
```

**Cost Analysis**:
- Without amortization: 10k writes/sec × $0.000005/PUT = $4.32/day
- With 1500ms amortization: ~667 PUTs/sec × $0.000005/PUT = $0.29/day
- **93% cost reduction**

**Trade-off**: Higher `s3_flush_interval_ms` reduces cost but increases potential data loss window if leader crashes during degraded mode before S3 flush.


#### 3.7.3 Exiting Degraded Mode

```
1. All followers are reachable AND caught up
2. All pending S3 batches have been replicated
3. Clear degraded mode flag
4. Resume TCP-only replication
5. Optionally: Delete S3 batch objects (or use lifecycle policy)
```

#### 3.7.4 S3 Batch Retention and Cleanup

S3 batches are temporary and should be cleaned up once all followers have caught up:

**Cleanup Strategy (Leader-Driven)**:
```
1. Leader tracks: min_replicated_index per follower
2. When ALL followers have replicated past batch X:
   a. Batch X is safe to delete from S3
   b. Leader issues DELETE to S3
3. Cleanup is batched (delete multiple objects per request)
4. Lifecycle policy as backup: auto-delete objects older than 7 days
```

**Leader Tracking State**:
```rust
struct DegradedModeState {
    /// S3 batches pending cleanup, keyed by aggregate
    pending_s3_batches: HashMap<AggregateKey, Vec<S3BatchRange>>,
    
    /// Minimum replicated index per follower per aggregate
    follower_progress: HashMap<(NodeId, AggregateKey), u64>,
}

struct S3BatchRange {
    from_index: u64,
    to_index: u64,
    s3_key: String,
    uploaded_at: u64,
}
```

**Safety**: If leader crashes before cleanup, S3 lifecycle policy ensures eventual cleanup. Followers can handle missing S3 batches gracefully if they've already replicated the data directly.


### 3.8 Follower Behavior

#### 3.8.1 Write Rejection

Followers reject client writes with leader information:
```rust
EventPlaneDBError::NotLeader {
    leader_node_id: Some(leader_id),
    leader_address: Some("10.0.1.5:10000"),
}
```

Client SDK should automatically retry to the indicated leader.

#### 3.8.2 Read Behavior

**Recommended**: Followers serve reads from local state
- Lower latency
- Eventually consistent (replication lag)
- Simpler implementation

**Optional**: `require_leader` flag forwards reads to current leader for strong consistency.

#### 3.8.3 Leadership Takeover

When follower detects expired lease:
```
1. WAIT until lease_expiry + MAX_CLOCK_SKEW (safety window)
2. ATTEMPT lease acquisition
3. IF successful:
   a. CHECK for S3 degraded batches
   b. APPLY any missing batches
   c. VERIFY local state matches lease.event_batch_index
   d. BEGIN accepting writes
4. IF failed: Another node won; remain follower
```

### 3.9 Metadata Extensions

#### 3.9.1 EventBatchMetadata Changes

Add provenance tracking:
```rust
pub struct EventBatchMetadata {
    // ... existing fields ...
    
    /// Node ID that wrote this batch
    pub writer_node_id: u128,
    
    /// Lease index at time of write (fencing token)
    pub lease_index: u64,
}
```

#### 3.9.2 Backward Compatibility

- New fields default to 0 during deserialization of old data
- Old readers ignore unknown fields
- Wire format version already exists for migration

### 3.10 Error Handling Extensions

```rust
pub enum EventPlaneDBError {
    // ... existing variants ...
    
    NotLeader {
        leader_node_id: Option<u128>,
        leader_address: Option<String>,
    },
    LeaseExpired,
    ReplicationFailed,
    ClockSkewExceeded {
        local_time: u64,
        peer_time: u64,
        skew_ms: u64,
    },
}
```

### 3.11 Configuration

```rust
pub struct ReplicationConfig {
    /// Enable replication (false = single-node mode)
    pub enabled: bool,
    
    /// S3 bucket for control plane
    pub s3_bucket: String,
    pub s3_region: String,
    pub s3_cluster_prefix: String,
    
    /// Lease timing
    pub lease_duration_ms: u64,        // default: 30000
    pub lease_renewal_threshold_ms: u64, // default: 10000
    pub lease_idle_threshold_ms: u64,   // default: 60000
    
    /// Clock safety
    pub max_clock_skew_ms: u64,        // default: 2000
    pub clock_check_interval_ms: u64,  // default: 60000
    
    /// Replication
    pub replication_port: u16,
    pub replication_timeout_ms: u64,   // default: 100
    pub replication_connect_timeout_ms: u64, // default: 5000
    
    /// Degraded mode
    pub enable_degraded_mode: bool,    // default: true
    pub degraded_mode_hysteresis_ms: u64, // default: 30000
}
```

### 3.12 Backpressure and Flow Control

#### 3.12.1 The Problem

Without backpressure, a fast leader can overwhelm slow followers:
- Replication queue grows unbounded
- Memory exhaustion on leader
- Followers fall further behind, creating a negative feedback loop

#### 3.12.2 Design Assumption: Homogeneous Hardware

The primary mitigation is operational: all nodes in a cluster run identical hardware specifications. This ensures:
- Similar write throughput capacity
- Similar fsync latency
- Similar network bandwidth

**Recommendation**: Document minimum hardware requirements and enforce homogeneity in deployment tooling.

#### 3.12.3 Replication Queue Limits

Despite homogeneous hardware, transient slowdowns can occur. We implement bounded queues:

```rust
pub struct ReplicationConfig {
    // ... existing fields ...
    
    /// Maximum pending replication messages per follower
    pub max_pending_replication_messages: usize,  // default: 1000
    
    /// Maximum pending bytes per follower
    pub max_pending_replication_bytes: usize,     // default: 64MB
    
    /// Action when queue is full
    pub queue_full_policy: QueueFullPolicy,       // default: Block
}

pub enum QueueFullPolicy {
    /// Block new writes until queue drains (preserves ordering)
    Block,
    
    /// Enter degraded mode for affected aggregates (maintains throughput)
    DegradeToS3,
    
    /// Reject writes with backpressure error (client retries)
    RejectWithBackpressure,
}
```

#### 3.12.4 Client-Side Backpressure

When the server is overloaded, clients receive explicit backpressure signals:

```rust
pub enum EventPlaneDBError {
    // ... existing variants ...
    
    /// Server is experiencing backpressure, client should retry with backoff
    Backpressure {
        retry_after_ms: u64,
    },
}
```

Client SDK should implement exponential backoff:
```rust
async fn write_with_backoff(&mut self, request: WriteRequest) -> Result<WriteResult, Error> {
    let mut backoff_ms = 10;
    let max_backoff_ms = 5000;
    let max_retries = 10;
    
    for attempt in 0..max_retries {
        match self.send_request(&request).await {
            Ok(result) => return Ok(result),
            Err(ClientError::Backpressure { retry_after_ms }) => {
                let wait_ms = retry_after_ms.max(backoff_ms);
                sleep(Duration::from_millis(wait_ms)).await;
                backoff_ms = (backoff_ms * 2).min(max_backoff_ms);
            }
            Err(e) => return Err(e),
        }
    }
    
    Err(Error::MaxRetriesExceeded)
}


---

## 4. Risk Evaluation

### 4.1 Critical Risks

#### 4.1.1 Split Brain from Clock Skew

| Aspect | Details |
|--------|---------|
| **Scenario** | Two nodes believe they hold valid leases due to clock drift exceeding `MAX_CLOCK_SKEW` |
| **Likelihood** | Low (modern cloud VMs have good NTP sync) |
| **Consequence** | **CRITICAL**: Both nodes accept writes; data divergence |
| **Mitigations** | 1. Continuous clock drift monitoring via peer probes<br>2. Refuse writes if drift > threshold<br>3. Use AWS Time Sync Service<br>4. `lease_index` fencing token rejects stale writes on followers<br>5. Safety windows: `expiry - MAX_CLOCK_SKEW` for validity<br>6. Startup clock validation before joining cluster<br>7. **Peer time probe runs every 10s, not just 60s**<br>8. **Immediate lease release on detected skew > threshold** |
| **Detection** | Follower rejects `ReplicateBatch` with lower `lease_index` |
| **Recovery** | Compare `lease_index` and `event_batch_index` across nodes; higher wins |

**Enhanced Clock Skew Handling**:

Clock skew can occur as gradual drift OR sudden steps (VM migration, NTP correction, leap seconds). We handle both:

```rust
enum ClockSkewAction {
    /// Drift within acceptable range, continue normally
    Normal,
    
    /// Drift detected but within warning threshold
    /// Log warning, increase probe frequency
    Warning { skew_ms: u64 },
    
    /// Drift exceeds safety threshold
    /// Release all leases, enter follower-only mode
    Critical { skew_ms: u64 },
    
    /// Drift exceeds maximum safe operation threshold
    /// Refuse all operations, require manual intervention
    Unsafe { skew_ms: u64 },
}

impl ClockSync {
    fn evaluate_skew(&self, observed_skew_ms: u64) -> ClockSkewAction {
        match observed_skew_ms {
            s if s <= MAX_CLOCK_SKEW / 2 => ClockSkewAction::Normal,
            s if s <= MAX_CLOCK_SKEW => ClockSkewAction::Warning { skew_ms: s },
            s if s <= MAX_CLOCK_SKEW * 2 => ClockSkewAction::Critical { skew_ms: s },
            s => ClockSkewAction::Unsafe { skew_ms: s },
        }
    }
}
```

**Leap Second Handling**:
- AWS Time Sync Service uses leap smearing (spreads adjustment over hours)
- If using other NTP sources, monitor for step adjustments
- Treat any backward time jump > 100ms as potential leap second, enter Critical mode temporarily

#### 4.1.2 S3 Unavailability

| Aspect | Details |
|--------|---------|
| **Scenario** | S3 API outage or network partition to S3 |
| **Likelihood** | Very Low (S3 99.99% availability) |
| **Consequence** | **HIGH**: No new lease acquisitions; no degraded mode writes |
| **Mitigations** | 1. Existing leaders continue until lease expires<br>2. Cache lease state locally with TTL<br>3. Exponential backoff on S3 retries<br>4. Alert on S3 errors<br>5. Consider multi-region S3 for control plane |
| **Detection** | S3 API errors logged and metriced |
| **Recovery** | Automatic when S3 recovers; leaders re-acquire if expired |

#### 4.1.3 Rollback Failure

| Aspect | Details |
|--------|---------|
| **Scenario** | Rollback of speculative write fails (disk error, crash mid-rollback) |
| **Likelihood** | Very Low |
| **Consequence** | **CRITICAL**: Local state diverges from acknowledged state |
| **Mitigations** | 1. Rollback is file truncation (atomic on ext4/xfs)<br>2. Startup integrity check: validate last batch CRC<br>3. If inconsistent, recover from followers or S3<br>4. Idempotency allows safe replay<br>5. **Pending write state persisted before write attempt**<br>6. **Crash recovery replays rollback if needed** |
| **Detection** | CRC mismatch on startup scan |
| **Recovery** | Truncate to last valid batch; catch up from cluster |

**Rollback Atomicity and Crash Recovery**:

The concern is that rollback involves multiple operations:
1. Truncate metadata file
2. Truncate event batch file
3. Restore in-memory state

If we crash mid-rollback, files could be inconsistent. We handle this:

**Approach: Pending Write Marker**

Before starting a write, we record intent. On startup, we check for incomplete writes and complete the rollback.

```rust
/// Written to {aggregate_dir}/pending_write.bin before write attempt
/// Deleted after successful commit OR successful rollback
#[derive(Encode, Decode)]
struct PendingWriteMarker {
    /// Event batch index being written
    event_batch_index: u64,
    
    /// File lengths BEFORE this write (rollback targets)
    pre_write_metadata_len: u64,
    pre_write_event_batch_len: u64,
    
    /// Timestamp for debugging
    started_at: u64,
}
```

**Write Sequence with Crash Safety**:
```
1. CREATE pending_write.bin with rollback info
2. FSYNC pending_write.bin
3. APPEND to event_batches.bin
4. APPEND to metadata.bin  
5. FSYNC both files
6. REPLICATE to followers
7. On SUCCESS:
   a. DELETE pending_write.bin
   b. ACK to client
8. On FAILURE:
   a. TRUNCATE event_batches.bin to pre_write_event_batch_len
   b. TRUNCATE metadata.bin to pre_write_metadata_len
   c. DELETE pending_write.bin
   d. NACK to client
```

**Startup Recovery**:
```rust
async fn recover_pending_writes(&mut self) -> Result<(), Error> {
    let pending_path = format!("{}/pending_write.bin", self.aggregate_dir);
    
    if !Path::new(&pending_path).exists() {
        return Ok(()); // No pending write, clean state
    }
    
    let marker: PendingWriteMarker = read_and_decode(&pending_path)?;
    
    log::warn!(
        "Found pending write marker for batch {}, rolling back",
        marker.event_batch_index
    );
    
    // Complete the rollback
    self.metadata_file.truncate(marker.pre_write_metadata_len).await?;
    self.event_batch_file.truncate(marker.pre_write_event_batch_len).await?;
    
    // Fsync to ensure truncation is durable
    self.metadata_file.fdatasync().await?;
    self.event_batch_file.fdatasync().await?;
    
    // Remove marker
    std::fs::remove_file(&pending_path)?;
    
    log::info!("Rollback completed for batch {}", marker.event_batch_index);
    Ok(())
}
```

**Additional CRC Validation on Startup**:

Even with pending write markers, we validate the last batch on startup:

```rust
async fn validate_last_batch(&self) -> Result<(), Error> {
    let last_metadata = self.read_last_metadata()?;
    let last_batch_bytes = self.read_batch_at(last_metadata.position)?;
    
    let actual_crc = crc32c::crc32c(&last_batch_bytes);
    if actual_crc != last_metadata.events_crc {
        log::error!(
            "CRC mismatch on last batch: expected {}, got {}",
            last_metadata.events_crc, actual_crc
        );
        
        // Truncate to previous batch
        self.truncate_last_batch().await?;
        
        // Mark aggregate as needing catch-up
        self.needs_catchup = true;
    }
    
    Ok(())
}
```


### 4.2 High Risks

#### 4.2.1 Replication Timeout After Local Write

| Aspect | Details |
|--------|---------|
| **Scenario** | Leader wrote locally but follower times out before ACK |
| **Likelihood** | Medium (network issues, follower overload) |
| **Consequence** | **HIGH**: Must choose between rollback (data loss) or degraded mode (latency) |
| **Mitigations** | 1. Degraded mode: Write to S3, then acknowledge<br>2. Aggressive timeouts (100ms default)<br>3. Monitor replication latency percentiles<br>4. Follower health checks in membership<br>5. Automatic follower removal after sustained failures |
| **Detection** | Replication timeout metrics spike |
| **Recovery** | Degraded mode preserves durability; follower catches up from S3 |

#### 4.2.2 Network Partition Between Nodes

| Aspect | Details |
|--------|---------|
| **Scenario** | Nodes can reach S3 but not each other |
| **Likelihood** | Low |
| **Consequence** | **HIGH**: Leader acquires lease but can't replicate |
| **Mitigations** | 1. Degraded mode with S3 batch storage<br>2. Detect via failed replication<br>3. Configurable policy: require N followers before accepting writes<br>4. Monitor partition duration<br>5. **S3 conditional writes prevent dual leadership for same aggregate** |
| **Detection** | Persistent replication failures to all peers |
| **Recovery** | Automatic when partition heals; catch-up from S3 |

**Why Partitions Don't Cause Dual Leadership**:

In a network partition where both nodes can reach S3:

```
┌─────────┐         ┌─────────┐
│ Node A  │    X    │ Node B  │
│ Leader  │    X    │ Wants   │
│ Agg 1   │         │ Agg 1   │
└────┬────┘         └────┬────┘
     │                   │
     └───────┬───────────┘
             │
        ┌────┴────┐
        │   S3    │
        └─────────┘
```

**Scenario Analysis**:

1. **Node A holds valid lease for Agg 1**:
   - Node B cannot acquire: lease hasn't expired
   - Node B's conditional write would fail (ETag mismatch)

2. **Node A's lease expires during partition**:
   - Node A detects expiry, stops accepting writes
   - Node B reads expired lease from S3
   - Node B waits `MAX_CLOCK_SKEW` after expiry
   - Node B acquires lease via conditional write
   - Node A's renewal attempt fails (ETag changed)
   - Node A becomes follower

3. **Both nodes try to acquire simultaneously (no existing lease)**:
   - Both read "no lease" from S3
   - Both attempt conditional write with `If-None-Match: *`
   - S3 guarantees exactly one succeeds (strong consistency)
   - Loser gets 412 Precondition Failed
   - Loser backs off and becomes follower

**The key insight**: S3's conditional write semantics provide mutual exclusion. Two nodes cannot both succeed in acquiring the same lease.


#### 4.2.3 Zombie Leader

| Aspect | Details |
|--------|---------|
| **Scenario** | Former leader continues writing after losing lease (network delay, GC pause) |
| **Likelihood** | Medium |
| **Consequence** | **HIGH**: Conflicting writes from old and new leaders |
| **Mitigations** | 1. `lease_index` fencing: followers reject lower epochs<br>2. Leader validates lease before client ACK<br>3. S3 conditional writes prevent stale renewals<br>4. New leader waits `MAX_CLOCK_SKEW` before accepting writes<br>5. **Leader re-validates lease AFTER replication, before ACK**<br>6. **Leader stops accepting writes when lease_expiry - now < WRITE_DEADLINE_BUFFER** |
| **Detection** | Follower Nack with "stale lease_index" |
| **Recovery** | Zombie's writes are rejected; new leader's writes succeed |

**Lease Renewal Race Prevention**:

The renewal race scenario requires precise timing to prevent:

```
Dangerous scenario (PREVENTED):
T=0:    Leader checks lease, sees 10s remaining
T=0.1:  Leader accepts write W1
T=9.5:  Lease expires (leader didn't renew in time)
T=10:   Follower becomes new leader
T=10.5: Original leader's replication of W1 arrives at follower
        → REJECTED: lease_index is stale
```

**Prevention Mechanisms**:

1. **WRITE_DEADLINE_BUFFER**: Leader refuses new writes when `lease_expiry - now < WRITE_DEADLINE_BUFFER`
   ```rust
   const WRITE_DEADLINE_BUFFER_MS: u64 = 5000; // 5 seconds before expiry
   
   fn can_accept_write(&self) -> bool {
       let now = current_time_ms();
       self.lease_expiry > now + WRITE_DEADLINE_BUFFER_MS
   }
   ```

2. **Post-Replication Lease Validation**: Before ACKing to client, leader re-checks lease validity
   ```rust
   async fn complete_write(&mut self) -> Result<WriteResult, Error> {
       // ... replication completed ...
       
       // Re-validate lease before ACK
       if !self.is_lease_still_valid() {
           self.rollback_write();
           return Err(Error::LeaseExpiredDuringWrite);
       }
       
       // Safe to ACK
       Ok(result)
   }
   ```

3. **Follower Acquisition Delay**: New leader waits `MAX_CLOCK_SKEW` after lease expiry before accepting writes
   ```rust
   async fn acquire_lease(&mut self) -> Result<(), Error> {
       let current_lease = self.read_lease_from_s3().await?;
       
       // Must wait for safety window after expiry
       let safe_acquisition_time = current_lease.expiry + MAX_CLOCK_SKEW_MS;
       if current_time_ms() < safe_acquisition_time {
           sleep_until(safe_acquisition_time).await;
       }
       
       // Now safe to acquire
       self.write_new_lease().await
   }
   ```

**Timing Budget**:
```
LEASE_DURATION = 30s
RENEWAL_THRESHOLD = 10s (renew when 10s remaining)
WRITE_DEADLINE_BUFFER = 5s (stop writes when 5s remaining)
MAX_CLOCK_SKEW = 2s

Timeline for normal operation:
T=0:   Lease acquired (expires T=30)
T=20:  Renewal triggered (10s remaining)
T=20.1: Renewal succeeds, new expiry T=50
...

Timeline if renewal fails:
T=0:   Lease acquired (expires T=30)
T=20:  Renewal triggered
T=20.5: Renewal fails (S3 issue)
T=21:  Retry renewal
T=25:  WRITE_DEADLINE_BUFFER reached, stop accepting writes
T=25-30: Leader completes in-flight writes, refuses new ones
T=30:  Lease expires
T=32:  Other nodes can safely acquire (T=30 + MAX_CLOCK_SKEW)
```

#### 4.2.4 Lease Expiry Mid-Write

| Aspect | Details |
|--------|---------|
| **Scenario** | Leader's lease expires while processing a write |
| **Likelihood** | Low (lease >> write time) |
| **Consequence** | **HIGH**: Another node may become leader; duplicate batch risk |
| **Mitigations** | 1. Check lease at write start AND before client ACK<br>2. Refuse writes if `lease_expiry - now < 2 × MAX_WRITE_TIME`<br>3. `(event_batch_index, lease_index)` pair prevents duplicate application |
| **Detection** | Lease validation failure mid-write |
| **Recovery** | Rollback local write; client receives error and retries to new leader |

### 4.3 Medium Risks

#### 4.3.1 Follower Falls Far Behind

| Aspect | Details |
|--------|---------|
| **Scenario** | Follower offline for extended period; catch-up queue is huge |
| **Likelihood** | Medium (maintenance, hardware failure) |
| **Consequence** | **MEDIUM**: Writes may be delayed waiting for slow follower |
| **Mitigations** | 1. Timeout threshold before entering degraded mode<br>2. Admin ability to mark follower inactive<br>3. Parallel batch transfer during catch-up<br>4. Snapshot-based recovery for very large gaps<br>5. Consider "write without waiting for this follower" policy |
| **Detection** | Replication lag metric per follower |
| **Recovery** | Follower catches up from S3; rejoins replication when current |

#### 4.3.2 Two-Node Cluster Single Failure

| Aspect | Details |
|--------|---------|
| **Scenario** | In 2-node cluster, one node fails completely |
| **Likelihood** | Medium |
| **Consequence** | **MEDIUM**: Writes enter degraded mode (S3 hot path); higher latency |
| **Mitigations** | 1. Document limitation clearly<br>2. Recommend 3-node for production<br>3. Degraded mode maintains durability via S3<br>4. Alert on sustained single-node operation |
| **Detection** | Membership shows only one active node |
| **Recovery** | Automatic when second node returns; manual intervention may be needed for data alignment |

#### 4.3.3 S3 Conditional Write Race

| Aspect | Details |
|--------|---------|
| **Scenario** | Multiple nodes attempt lease acquisition simultaneously |
| **Likelihood** | Medium (expected during failover) |
| **Consequence** | **LOW**: Expected behavior; one wins, others become followers |
| **Mitigations** | 1. Jitter on lease check timing<br>2. Exponential backoff on 412 conflicts<br>3. Clear leader hint in error response |
| **Detection** | Lease acquisition conflict metrics |
| **Recovery** | Normal operation; losing node becomes follower |

### 4.4 Low Risks

#### 4.4.1 S3 Cost Explosion

| Aspect | Details |
|--------|---------|
| **Scenario** | Many active aggregates renewing leases; prolonged degraded mode |
| **Likelihood** | Low (dormancy mitigates) |
| **Consequence** | **LOW**: Increased operational cost |
| **Mitigations** | 1. Dormant mode: stop renewal for idle aggregates<br>2. Batch lease renewals where possible<br>3. Monitor S3 request costs<br>4. Lifecycle policy for old degraded batches |
| **Detection** | AWS billing alerts; S3 request metrics |
| **Recovery** | Tune idle thresholds; investigate sustained degraded mode causes |

#### 4.4.2 Membership File Corruption

| Aspect | Details |
|--------|---------|
| **Scenario** | Membership JSON becomes invalid or inconsistent |
| **Likelihood** | Very Low |
| **Consequence** | **MEDIUM**: Nodes may not know replication targets |
| **Mitigations** | 1. Version field enables conflict detection<br>2. Local cache as fallback<br>3. Validation on read<br>4. Admin tooling for manual repair |
| **Detection** | JSON parse errors; version conflicts |
| **Recovery** | Restore from backup or reconstruct from running nodes |

#### 4.4.3 Node ID Collision

| Aspect | Details |
|--------|---------|
| **Scenario** | Two nodes generate same `u128` ID |
| **Likelihood** | Astronomically Low (2^-128 probability) |
| **Consequence** | **HIGH**: Lease confusion; replication misdirection |
| **Mitigations** | 1. Cryptographically secure RNG<br>2. Validate uniqueness on cluster join<br>3. Reject startup if collision detected |
| **Detection** | Membership registration conflict |
| **Recovery** | Regenerate node_id on affected node; restart |

### 4.5 Risk Summary Matrix

| Risk | Likelihood | Consequence | Severity | Primary Mitigation | Status |
|------|------------|-------------|----------|-------------------|--------|
| Split Brain (Clock Skew) | Low | Critical | **Critical** | Clock monitoring + fencing tokens | ✅ Mitigated |
| Lease Renewal Race | Medium | High | **High** | WRITE_DEADLINE_BUFFER + post-replication validation | ✅ Mitigated |
| S3 Unavailability | Very Low | High | **High** | Local cache + graceful degradation | ✅ Mitigated |
| Rollback Failure | Very Low | Critical | **High** | Pending write marker + startup recovery | ✅ Mitigated |
| Replication Timeout | Medium | High | **High** | Degraded mode + S3 fallback | ✅ Mitigated |
| Network Partition | Low | High | **High** | S3 conditional writes + degraded mode | ✅ Mitigated |
| Zombie Leader | Medium | High | **High** | Fencing tokens + lease validation | ✅ Mitigated |
| Lease Expiry Mid-Write | Low | High | **Medium** | WRITE_DEADLINE_BUFFER + safety margins | ✅ Mitigated |
| Follower Far Behind | Medium | Medium | **Medium** | Timeout + catch-up from S3 | ✅ Mitigated |
| Two-Node Failure | Medium | Medium | **Medium** | Degraded mode + documentation | ✅ Accepted |
| Lease Race | Medium | Low | **Low** | Expected behavior + jitter | ✅ Accepted |
| S3 Cost Explosion | Low | Low | **Low** | Dormancy + amortization + monitoring | ✅ Mitigated |
| Client Retry Loop | Medium | Medium | **Medium** | Retry limits + circuit breaker | ✅ Mitigated |
| Backpressure/Memory | Low | High | **Medium** | Queue limits + same-spec hardware | ⚠️ Needs monitoring |
| Membership Corruption | Very Low | Medium | **Low** | Validation + local cache | ✅ Mitigated |
| Node ID Collision | Astronomically Low | High | **Low** | Secure RNG + validation | ✅ Mitigated |

---

## 5. Task Breakdown

### 5.1 Phase 1: Foundations (2-3 weeks)

#### 5.1.1 Node Identity
- [ ] Create `NodeIdentity` struct with `u128` ID
- [ ] Implement file-based persistence (`{data_root}/node_id`)
- [ ] Generate on first startup using secure RNG
- [ ] Load and validate on subsequent startups
- [ ] Expose node_id through server configuration
- [ ] Unit tests for generation, persistence, reload

**Files**: `eventplanedb_core/src/replication/node_identity.rs`

#### 5.1.2 Configuration
- [ ] Create `ReplicationConfig` struct with all parameters
- [ ] Create `BatchingConfig` struct
- [ ] Create `DegradedModeConfig` struct
- [ ] Add CLI arguments to `EventPlaneDBConfig`:
  - `--replication-enabled`
  - `--s3-bucket`, `--s3-region`, `--s3-cluster-prefix`
  - `--lease-duration-ms`, `--lease-renewal-threshold-ms`
  - `--max-clock-skew-ms`
  - `--replication-port`, `--replication-timeout-ms`
  - `--batch-timeout-ms`, `--batch-size-bytes`
  - `--s3-flush-interval-ms`
- [ ] Environment variable support for S3 credentials
- [ ] Configuration validation (timeouts, port ranges)
- [ ] Unit tests for parsing and validation

**Files**: `eventplanedb_server/src/config.rs`

#### 5.1.3 S3 Client Integration
- [ ] Add `aws-sdk-s3` dependency (async, compatible with glommio)
- [ ] Create `S3ControlPlane` abstraction trait
- [ ] Implement conditional GET with ETag caching
- [ ] Implement conditional PUT with If-Match/If-None-Match
- [ ] Implement batch DELETE for cleanup
- [ ] Error handling with categorization (retriable vs permanent)
- [ ] Exponential backoff retry logic
- [ ] Integration tests with LocalStack

**Files**: `eventplanedb_core/src/replication/s3_control_plane.rs`

#### 5.1.4 Metadata Extensions
- [ ] Add `writer_node_id: u128` to `EventBatchMetadata`
- [ ] Add `lease_index: u64` to `EventBatchMetadata`
- [ ] Update serialization with backward compatibility (default 0)
- [ ] Update `METADATA_BATCH_SIZE_BYTES` constant if needed
- [ ] Migration tests for reading old format data

**Files**: `eventplanedb_structures/src/event_batch_metadata.rs`

#### 5.1.5 Pending Write Marker (Crash Safety)
- [ ] Create `PendingWriteMarker` struct
- [ ] Implement write marker creation before writes
- [ ] Implement marker deletion on commit/rollback
- [ ] Implement startup recovery scan
- [ ] Unit tests for crash recovery scenarios

**Files**: `eventplanedb_core/src/write_operations/pending_write.rs`

### 5.2 Phase 2: Cluster Membership (1-2 weeks)

#### 5.2.1 Membership Data Structures
- [ ] Create `ClusterMember` struct (node_id, addresses, is_active, timestamps)
- [ ] Create `ClusterMembership` struct (version, members list)
- [ ] Implement JSON serialization/deserialization
- [ ] Unit tests for struct operations

**Files**: `eventplanedb_core/src/replication/membership.rs`

#### 5.2.2 Membership Manager
- [ ] Create `MembershipManager` component
- [ ] Implement initial read from S3 on startup
- [ ] Implement local caching with configurable TTL
- [ ] Implement periodic refresh (every 30s)
- [ ] Implement heartbeat update for self
- [ ] Implement conditional write for membership changes
- [ ] Handle stale node detection (heartbeat > threshold)
- [ ] Integration tests with mock S3

**Files**: `eventplanedb_core/src/replication/membership_manager.rs`

#### 5.2.3 Node Registration
- [ ] Register self on startup
- [ ] Deregister on graceful shutdown
- [ ] Handle registration conflicts (duplicate node_id)

### 5.3 Phase 3: Lease Management (2-3 weeks)

#### 5.3.1 Lease Data Structures
- [ ] Create `LeaseFile` struct matching JSON schema
- [ ] Create `LeaseState` enum: `NoLease`, `Leader(LeaseFile)`, `Follower(LeaseFile)`
- [ ] Implement serialization/deserialization
- [ ] Unit tests for lease struct

**Files**: `eventplanedb_core/src/replication/lease.rs`

#### 5.3.2 Lease Operations
- [ ] Implement `acquire_lease(aggregate_key)` with conditional write
- [ ] Implement `renew_lease()` with conditional write
- [ ] Implement `release_lease()` for graceful shutdown
- [ ] Implement `read_lease()` with local caching
- [ ] Handle S3 412 conflict responses
- [ ] Handle clock skew safety windows
- [ ] Unit tests for all operations

**Files**: `eventplanedb_core/src/replication/lease_ops.rs`

#### 5.3.3 Per-Aggregate Lease Manager
- [ ] Create `AggregateLeaseManager` per aggregate
- [ ] Track current lease state
- [ ] Background task for proactive renewal
- [ ] Implement dormancy detection (no recent writes)
- [ ] Implement automatic renewal scheduling
- [ ] Graceful shutdown lease release
- [ ] Integration with `AggregateResources`

**Files**: `eventplanedb_core/src/replication/lease_manager.rs`

#### 5.3.4 Lease Cache
- [ ] LRU cache for lease states across aggregates
- [ ] Invalidation on lease changes
- [ ] Refresh on cache miss

### 5.4 Phase 4: Time Synchronization (1 week)

#### 5.4.1 Clock Verification
- [ ] Create `ClockSync` component
- [ ] Implement peer time probe protocol
- [ ] Calculate and track maximum observed drift
- [ ] Startup validation: refuse to start if skew > 2×MAX_CLOCK_SKEW
- [ ] Runtime monitoring: log warnings if skew > MAX_CLOCK_SKEW
- [ ] Integration with lease manager: refuse new leases if unsafe

**Files**: `eventplanedb_core/src/replication/clock_sync.rs`

#### 5.4.2 Time Sync Messages
- [ ] Add `TimeSync` request/response to replication protocol
- [ ] Include local timestamp in probe
- [ ] Calculate round-trip adjusted drift

### 5.5 Phase 5: Replication Protocol (3-4 weeks)

#### 5.5.1 Wire Protocol Definition
- [ ] Define `ReplicationMessageType` enum
- [ ] Define `ReplicateBatch` message struct
- [ ] Define `ReplicateBatchAck` / `ReplicateBatchNack` structs
- [ ] Define `CatchUpRequest` / `CatchUpResponse` structs
- [ ] Implement binary serialization with version header
- [ ] Unit tests for all message types

**Files**: `eventplanedb_core/src/replication/protocol.rs`

#### 5.5.2 Replication Server (Follower Side)
- [ ] Create TCP listener on replication port (glommio)
- [ ] Accept connections and authenticate via node_id
- [ ] Dispatch by message type
- [ ] Handle `ReplicateBatch`: validate, write, fsync, ack
- [ ] Handle `CatchUpRequest`: read batches, stream response
- [ ] Handle `TimeSync`: respond with local time
- [ ] Connection management and cleanup

**Files**: `eventplanedb_core/src/replication/replication_server.rs`

#### 5.5.3 Replication Client (Leader Side)
- [ ] Create `ReplicationClient` for outbound connections
- [ ] Connection pooling per peer node
- [ ] Persistent connections with reconnection logic
- [ ] Send `ReplicateBatch` with timeout
- [ ] Handle `Nack`: send missing batches
- [ ] Parallel sends to multiple followers
- [ ] Collect and aggregate responses

**Files**: `eventplanedb_core/src/replication/replication_client.rs`

#### 5.5.4 Catch-Up Protocol
- [ ] Follower: detect gaps in `event_batch_index`
- [ ] Follower: send `CatchUpRequest` to leader
- [ ] Leader: stream `CatchUpResponse` with batches
- [ ] Handle large catch-up sets (chunking)
- [ ] Timeout and retry logic

### 5.6 Phase 6: Write Path Integration (2-3 weeks)

#### 5.6.1 Leader Write Coordination
- [ ] Create `ReplicatedWriteCoordinator` struct
- [ ] Add lease validation before write
- [ ] Add lease validation before client acknowledgment
- [ ] Implement parallel follower replication
- [ ] Implement response collection with timeout
- [ ] Route to degraded mode on failures
- [ ] Integrate with existing `sync_with_rollback`

**Files**: `eventplanedb_core/src/replication/write_coordinator.rs`

#### 5.6.2 Enhanced Rollback
- [ ] Extend `PendingWrite` struct for replication state
- [ ] Implement rollback on replication + S3 failure
- [ ] Ensure atomicity of rollback operations
- [ ] Unit tests for rollback scenarios

#### 5.6.3 Follower Write Rejection
- [ ] Add `NotLeader` error variant to `EventPlaneDBError`
- [ ] Include `leader_node_id` and `leader_address` in error
- [ ] Update `process_request.rs` to check leadership before writes
- [ ] Return appropriate error with redirect hint

**Files**: 
- `eventplanedb_structures/src/eventplanedb_error.rs`
- `eventplanedb_core/src/process_request.rs`

#### 5.6.4 Metadata Attribution
- [ ] Populate `writer_node_id` on write
- [ ] Populate `lease_index` on write
- [ ] Update `WriteOptions` to include replication context

### 5.7 Phase 7: Degraded Mode (2-3 weeks)

#### 5.7.1 S3 Batch Storage
- [ ] Define S3 path structure: `/batches/{org}/{type}/{agg}/{index}.bin`
- [ ] Implement batch PUT to S3
- [ ] Implement batch GET from S3
- [ ] Implement batch LIST for catch-up discovery
- [ ] Handle serialization/compression consistency

**Files**: `eventplanedb_core/src/replication/s3_batch_storage.rs`

#### 5.7.2 Degraded Mode State Tracking
- [ ] Per-aggregate degraded mode flag
- [ ] Track which followers are behind
- [ ] Track which batches are in S3
- [ ] Transition logic: enter on follower failure

**Files**: `eventplanedb_core/src/replication/degraded_mode.rs`

#### 5.7.3 Catch-Up from S3
- [ ] Detect gaps on leader takeover
- [ ] List and download missing batches from S3
- [ ] Validate batch integrity (CRC)
- [ ] Apply batches locally
- [ ] Exit degraded mode when all followers caught up

#### 5.7.4 Hysteresis
- [ ] Don't immediately exit degraded mode on single successful replication
- [ ] Configurable stability period before exiting
- [ ] Prevent flapping between modes

### 5.8 Phase 8: Client SDK Updates (2-3 weeks)

#### 5.8.1 Error Handling

- [ ] Handle `NotLeader` error in client
- [ ] Parse leader hint from error response
- [ ] Implement automatic retry to new leader
- [ ] Configurable retry limits and backoff
- [ ] Handle `Backpressure` error with exponential backoff
- [ ] Handle `LeaseExpired` error (retry to any node)
- [ ] Handle `ClockSkewExceeded` error (wait and retry)

**Files**: `eventplanedb_client/src/lib.rs`

#### 5.8.2 Connection Management

- [ ] Support connecting to multiple nodes
- [ ] Automatic leader discovery on `NotLeader`
- [ ] Connection pooling for cluster
- [ ] Health checking and connection cycling
- [ ] Configurable timeouts per operation type

#### 5.8.3 Retry Logic

The client must handle various failure modes intelligently:

```rust
pub struct RetryConfig {
    /// Maximum retries for transient errors
    pub max_retries: u32,                    // default: 5
    
    /// Initial backoff duration
    pub initial_backoff_ms: u64,             // default: 10
    
    /// Maximum backoff duration
    pub max_backoff_ms: u64,                 // default: 5000
    
    /// Backoff multiplier
    pub backoff_multiplier: f64,             // default: 2.0
    
    /// Jitter factor (0.0 to 1.0)
    pub jitter_factor: f64,                  // default: 0.1
    
    /// Maximum time to spend retrying
    pub total_timeout_ms: u64,               // default: 30000
}
```

**Retry Decision Matrix**:

| Error | Retry? | Action |
|-------|--------|--------|
| `NotLeader` | Yes | Immediate retry to hinted leader |
| `NotLeader` (no hint) | Yes | Retry to random node with backoff |
| `Backpressure` | Yes | Retry after `retry_after_ms` or backoff |
| `LeaseExpired` | Yes | Retry to any node (leadership changing) |
| `ClockSkewExceeded` | Yes | Retry after 1s (wait for stabilization) |
| `OptimisticConcurrencyViolation` | No | Return to caller (application logic) |
| `ClientIdempotencyViolation` | No | Return to caller (duplicate write) |
| `NetworkError` | Yes | Retry to different node with backoff |
| `Timeout` | Yes | Retry to same or different node |
| `Internal` | Yes (limited) | Retry with backoff, fail after 2 attempts |

#### 5.8.4 Leader Tracking Cache

To avoid unnecessary redirects, client caches known leaders:

```rust
struct LeaderCache {
    /// Known leader per aggregate
    leaders: HashMap<AggregateKey, CachedLeader>,
    
    /// TTL for cache entries
    ttl_ms: u64,  // default: 30000 (matches lease duration)
}

struct CachedLeader {
    node_id: u128,
    address: String,
    cached_at: u64,
}

impl LeaderCache {
    fn get_leader(&self, key: &AggregateKey) -> Option<&CachedLeader> {
        self.leaders.get(key).filter(|l| !self.is_expired(l))
    }
    
    fn update_leader(&mut self, key: AggregateKey, node_id: u128, address: String) {
        self.leaders.insert(key, CachedLeader {
            node_id,
            address,
            cached_at: current_time_ms(),
        });
    }
    
    fn invalidate(&mut self, key: &AggregateKey) {
        self.leaders.remove(key);
    }
}
```

#### 5.8.5 Circuit Breaker

Prevent cascading failures when a node is consistently failing:

```rust
struct CircuitBreaker {
    /// Failure count per node
    failures: HashMap<String, NodeFailures>,
    
    /// Threshold to open circuit
    failure_threshold: u32,        // default: 5
    
    /// Time window for failure counting
    failure_window_ms: u64,        // default: 10000
    
    /// Time to wait before half-open
    recovery_timeout_ms: u64,      // default: 30000
}

enum CircuitState {
    Closed,      // Normal operation
    Open,        // Failing, don't send requests
    HalfOpen,    // Testing if recovered
}
```

#### 5.8.6 Request Routing

Smart routing to minimize redirects:

```rust
impl EventPlaneDBClient {
    async fn route_request(&mut self, request: &Request) -> Result<Response, Error> {
        let aggregate_key = request.aggregate_key();
        
        // 1. Check leader cache
        if let Some(leader) = self.leader_cache.get_leader(&aggregate_key) {
            if self.circuit_breaker.is_closed(&leader.address) {
                match self.send_to_node(&leader.address, request).await {
                    Ok(response) => return Ok(response),
                    Err(ClientError::NotLeader { leader_hint, .. }) => {
                        self.leader_cache.invalidate(&aggregate_key);
                        if let Some(hint) = leader_hint {
                            self.leader_cache.update_leader(aggregate_key.clone(), hint.node_id, hint.address.clone());
                        }
                        // Fall through to retry
                    }
                    Err(e) => {
                        self.circuit_breaker.record_failure(&leader.address);
                        // Fall through to retry
                    }
                }
            }
        }
        
        // 2. Try other nodes with retry logic
        self.retry_with_discovery(request).await
    }
}
```


### 5.9 Phase 9: Testing & Hardening (3-4 weeks)

#### 5.9.1 Unit Tests
- [ ] Lease acquisition, renewal, release
- [ ] Replication protocol message serialization
- [ ] Rollback scenarios
- [ ] Clock skew handling
- [ ] Degraded mode state transitions
- [ ] Metadata backward compatibility

#### 5.9.2 Integration Tests
- [ ] Multi-node cluster startup and shutdown
- [ ] Leader election under contention
- [ ] Failover: kill leader, verify new election
- [ ] Follower failure and catch-up
- [ ] Degraded mode end-to-end
- [ ] Network partition simulation (using network namespaces)
- [ ] S3 failure simulation

#### 5.9.3 Performance Tests
- [ ] Write throughput with replication (target: 300k+ writes/sec)
- [ ] Write latency percentiles (p50, p99, p999; target: <10ms p99)
- [ ] Replication lag measurement
- [ ] S3 operation counts per write
- [ ] Memory usage under sustained load

#### 5.9.4 Chaos Tests
- [ ] Random node kills during writes
- [ ] Network delay injection (tc qdisc)
- [ ] Clock skew simulation (libfaketime)
- [ ] S3 failure injection
- [ ] Disk failure simulation
- [ ] Combined failure scenarios

### 5.10 Phase 10: Operational Readiness (1-2 weeks)

#### 5.10.1 Observability
- [ ] Metrics: lease acquisitions, renewals, expirations per aggregate
- [ ] Metrics: replication latency histogram per follower
- [ ] Metrics: degraded mode duration per aggregate
- [ ] Metrics: S3 operations count and latency
- [ ] Metrics: clock skew gauge
- [ ] Structured logging for all replication events
- [ ] Trace IDs for request correlation across nodes

#### 5.10.2 Admin CLI
- [ ] Command: view current lease state for aggregate
- [ ] Command: force lease release
- [ ] Command: view cluster membership
- [ ] Command: mark node active/inactive
- [ ] Command: view degraded mode status
- [ ] Command: trigger manual catch-up

#### 5.10.3 Health Checks
- [ ] HTTP endpoint: `/health/replication`
- [ ] Include: lease count, degraded aggregate count, clock skew
- [ ] Kubernetes readiness/liveness probe support

#### 5.10.4 Documentation
- [ ] Operations runbook
- [ ] Configuration reference
- [ ] Troubleshooting guide
- [ ] Architecture diagrams
- [ ] Failure recovery procedures
- [ ] Capacity planning guidelines

### 5.11 Implementation Timeline

| Phase | Duration | Dependencies | Deliverables |
|-------|----------|--------------|--------------|
| 1. Foundations | 2-3 weeks | None | Node ID, Config, S3 client, Metadata |
| 2. Membership | 1-2 weeks | Phase 1 | Cluster membership management |
| 3. Leases | 2-3 weeks | Phase 1, 2 | Per-aggregate leadership |
| 4. Time Sync | 1 week | Phase 1 | Clock skew detection |
| 5. Replication Protocol | 3-4 weeks | Phase 1, 3 | TCP replication |
| 6. Write Path | 2-3 weeks | Phase 3, 5 | Replicated writes |
| 7. Degraded Mode | 2-3 weeks | Phase 5, 6 | S3 fallback |
| 8. Client SDK | 1 week | Phase 6 | Leader redirection |
| 9. Testing | 3-4 weeks | All above | Comprehensive test suite |
| 10. Operations | 1-2 weeks | Phase 9 | Production readiness |

**Total Estimated Duration**: 18-26 weeks

### 5.12 Milestone Checkpoints

#### Milestone 1: Single-Node with Replication Scaffolding (Week 4)
- Node identity working
- S3 client integrated
- Metadata extended
- Configuration complete
- No behavioral changes yet

#### Milestone 2: Leader Election Working (Week 8)
- Lease acquisition/renewal functional
- Membership tracking active
- Clock sync operational
- Can elect leader for aggregate
- Writes still single-node

#### Milestone 3: Basic Replication (Week 14)
- TCP replication protocol working
- Leader replicates to followers
- Followers acknowledge
- No degraded mode yet
- Manual failover only

#### Milestone 4: Full Replication with Failover (Week 20)
- Degraded mode working
- Automatic failover on leader failure
- Catch-up from S3
- Client redirection working

#### Milestone 5: Production Ready (Week 26)
- All tests passing
- Performance targets met
- Observability complete
- Documentation complete
- Chaos testing passed

---

## 6. Performance Expectations

### 6.1 Target Metrics

| Metric | Target | Conditions |
|--------|--------|------------|
| Write throughput | 300k+ writes/sec | Per cluster, normal mode, batched |
| Write latency p50 | < 2ms | Normal mode, same-AZ |
| Write latency p99 | < 10ms | Normal mode, same-AZ |
| Write latency p99 | < 200ms | Degraded mode (S3 path) |
| Replication lag | < 5ms | Normal mode, same-AZ |
| Failover time | < 35s | Lease expiry + safety window |

### 6.2 Performance Model

#### 6.2.1 Throughput Bound by Fsync

The primary throughput limiter is NVMe fsync latency:

```
NVMe fsync latency: ~100μs
Theoretical max fsyncs/sec: 10,000
With 1ms batching window: ~30 writes per batch average
Theoretical throughput: 10,000 × 30 = 300,000 writes/sec
```

**Key insight**: We're bounded by fsync frequency, not write count. Batching amortizes the fsync cost across many writes.

#### 6.2.2 Latency Breakdown (Normal Mode)

```
Client request received:          0.0ms
Parse and validate:               0.1ms
Queue in write batcher:           0.0ms
Wait for batch window:            0.5ms (avg, 1ms max)
Local write + fsync:              0.2ms
Parallel replication (3 nodes):   1.5ms (network RTT + follower fsync)
Post-replication lease check:     0.1ms
Response to client:               0.1ms
─────────────────────────────────────────
Total:                            2.5ms typical
```

#### 6.2.3 Latency Breakdown (Degraded Mode)

```
Client request received:          0.0ms
Parse and validate:               0.1ms
Local write + fsync:              0.2ms
Replication timeout:              100ms (configured)
S3 PUT (amortized wait):          0-1500ms (depends on flush interval)
Response to client:               0.1ms
─────────────────────────────────────────
Total:                            100-1600ms (p99 ~200ms with batching)
```

#### 6.2.4 Throughput Scaling

| Cluster Size | Throughput | Notes |
|--------------|------------|-------|
| 1 node | 300k/sec | No replication overhead |
| 2 nodes | 280k/sec | Replication adds ~7% overhead |
| 3 nodes | 270k/sec | Parallel replication, minimal additional overhead |
| 5 nodes | 250k/sec | More followers to wait for |

**Note**: These are cluster-wide throughputs. Individual aggregate throughput is bounded by single-leader processing.

### 6.3 Capacity Planning

#### 6.3.1 Memory Requirements

| Component | Memory Usage | Scaling Factor |
|-----------|--------------|----------------|
| Write cache per aggregate | 128MB default | × active aggregates |
| Replication queue per follower | 64MB max | × (nodes - 1) |
| S3 batch buffer (degraded) | 1MB per aggregate | × degraded aggregates |
| Lease cache | ~1KB per aggregate | × total aggregates |

**Example**: 1000 active aggregates, 3-node cluster
- Write cache: 1000 × 128MB = 128GB (distributed across nodes)
- Replication queues: 2 × 64MB = 128MB per node
- Total per node: ~43GB write cache + 128MB replication

#### 6.3.2 S3 Request Budget

| Operation | Normal Mode | Degraded Mode |
|-----------|-------------|---------------|
| Lease renewal | 1 PUT per 20s per active aggregate | Same |
| Batch storage | 0 | 1 PUT per 1.5s per degraded aggregate |
| Catch-up reads | Rare | 1 LIST + N GETs per recovery |

**Cost estimate** (1000 active aggregates, 0.1% in degraded mode):
- Lease renewals: 1000 × 3/min × $0.000005 = $0.015/min = $21.60/day
- Degraded writes: 1 × 40/min × $0.000005 = $0.0002/min = $0.29/day
- **Total: ~$22/day** for S3 operations

### 6.4 Failure Mode Performance Impact

| Failure | Throughput Impact | Latency Impact | Duration |
|---------|-------------------|----------------|----------|
| Single follower slow | None (parallel) | +1-5ms p99 | Until resolved |
| Single follower down | None (degraded mode) | +100-200ms | Until recovery |
| Network partition | None (degraded mode) | +100-200ms | Until healed |
| Leader crash | Writes blocked | N/A | 32-35s (lease + safety) |
| S3 degraded | Degraded mode blocked | Writes queue | Until S3 recovers |


## Appendix A: Design Alternatives Considered

### A.1 Raft/Paxos
**Rejected because**: Implementation complexity, odd-cluster requirement, latency overhead for consensus rounds.

### A.2 Quorum Writes (2 of 3)
**Rejected because**: Design goal is strong durability to ALL replicas or S3. Quorum allows data loss if minority fails before replication.

### A.3 Synchronous S3 on Every Write
**Rejected because**: Adds ~50-100ms latency to every write. S3 only used for control plane and degraded mode.

### A.4 External Coordination Service (etcd/ZooKeeper)
**Rejected because**: Adds operational complexity. S3 is already highly available and supports conditional writes.

### A.5 Leader Per Node (Not Per Aggregate)
**Rejected because**: Single point of failure for all aggregates on that node. Per-aggregate distributes risk.

---

## Appendix B: Comparison with Similar Systems

| System | Consensus | Replication | Failover | S3 Usage |
|--------|-----------|-------------|----------|----------|
| **EventPlaneDB** | S3 leases | Sync to all | Automatic | Control plane + degraded |
| **Kafka** | ZooKeeper/KRaft | ISR quorum | Automatic | Tiered storage (optional) |
| **AutoMQ** | Controller | Sync + S3 | Automatic | Hot path fallback |
| **CockroachDB** | Raft | Quorum | Automatic | Backup only |
| **TiKV** | Raft | Quorum | Automatic | Backup only |

---

## Appendix C: Glossary

| Term | Definition |
|------|------------|
| **Aggregate** | Unique event stream: `(org_id, aggregate_type_id, aggregate_id)` |
| **Conditional Write** | S3 PUT with `If-Match` or `If-None-Match` header |
| **Degraded Mode** | State where S3 is used for replication due to follower unavailability |
| **Dormant** | Aggregate with no recent writes; lease allowed to expire |
| **ETag** | S3 object version identifier for conditional operations |
| **Fencing Token** | Monotonic `lease_index` preventing stale writes |
| **Follower** | Node receiving replicated data; rejects direct writes |
| **Leader** | Node holding valid lease; accepts writes for aggregate |
| **Lease** | Time-bound lock granting leadership; stored in S3 |
| **Rollback** | Reverting speculative local write on replication failure |
| **Split Brain** | Multiple nodes believing they are leader simultaneously |
| **Zombie Leader** | Former leader continuing to write after losing lease |

## Appendix D: Edge Case Analysis

This appendix documents edge cases identified during design review and their mitigations.

### D.1 Timing and Clock Edge Cases

#### D.1.1 Lease Renewal Race

**Scenario**: Leader starts renewal, but by the time it reaches S3, lease has expired and another node acquired it.

**Mitigation**:
1. `WRITE_DEADLINE_BUFFER` (5s) prevents accepting writes close to expiry
2. Post-replication lease validation catches late failures
3. `lease_index` fencing ensures followers reject stale leader's writes

**Residual Risk**: None. Either the write completes before expiry (valid) or the lease check fails and write is rolled back.

#### D.1.2 Clock Step Adjustment

**Scenario**: NTP corrects clock with a step adjustment (e.g., +3 seconds) instead of gradual slew.

**Mitigation**:
1. Peer time probing detects sudden changes
2. On detection, node enters Critical mode and releases leases
3. Node waits for clock stabilization before resuming leadership

**Residual Risk**: Brief window (probe interval) where step might not be detected. Mitigated by `lease_index` fencing.

#### D.1.3 Leap Second

**Scenario**: Leap second causes clock to repeat or skip a second.

**Mitigation**:
1. AWS Time Sync uses leap smearing (no step change)
2. For other NTP sources, treat backward jumps as Critical skew
3. `MAX_CLOCK_SKEW` provides buffer for small adjustments

**Residual Risk**: Minimal with AWS Time Sync. Document requirement to use smearing NTP.

### D.2 Replication Edge Cases

#### D.2.1 Follower Receives Out-of-Order Batches

**Scenario**: Network reordering causes batch 102 to arrive before batch 101.

**Mitigation**:
1. Follower tracks expected `event_batch_index`
2. Out-of-order batch triggers NACK with expected index
3. Leader resends from expected index

**Residual Risk**: None. Protocol handles reordering.

#### D.2.2 Partial Batch Write on Follower

**Scenario**: Follower crashes mid-write of a replicated batch.

**Mitigation**:
1. Same pending write marker mechanism as leader
2. On startup, follower recovers by truncating partial write
3. Follower requests catch-up from leader

**Residual Risk**: None. Same crash recovery as leader.

#### D.2.3 All Followers Timeout Simultaneously

**Scenario**: Network blip causes all followers to timeout together.

**Mitigation**:
1. All aggregates enter degraded mode
2. Batches written to S3 with amortization
3. When followers recover, catch-up from S3
4. Degraded mode hysteresis prevents flapping

**Residual Risk**: Temporary latency spike. Acceptable for rare event.

### D.3 S3 Edge Cases

#### D.3.1 S3 Conditional Write Race

**Scenario**: Two nodes attempt to acquire the same lease simultaneously.

**Mitigation**:
1. S3 provides strong consistency for conditional writes
2. Exactly one write succeeds (first to complete)
3. Loser receives 412 Precondition Failed
4. Loser backs off with jitter and becomes follower

**Residual Risk**: None. S3 provides mutual exclusion.

#### D.3.2 S3 Eventual Consistency on LIST

**Scenario**: Recently written S3 object doesn't appear in LIST results during catch-up.

**Mitigation**:
1. S3 provides strong read-after-write consistency (since Dec 2020)
2. LIST operations are also strongly consistent
3. No special handling needed

**Residual Risk**: None. S3 strong consistency eliminates this concern.

#### D.3.3 S3 Throttling During Degraded Mode

**Scenario**: High write rate in degraded mode triggers S3 throttling (503 Slow Down).

**Mitigation**:
1. S3 batch amortization reduces request rate
2. Exponential backoff on 503 responses
3. S3 prefix partitioning for high-throughput scenarios
4. Alert on sustained throttling

**Residual Risk**: Writes queue in memory during throttling. Bounded by `max_pending_s3_batches`.

### D.4 Leadership Edge Cases

#### D.4.1 Infinite Leader Redirect Loop

**Scenario**: Client bounces between nodes, each claiming the other is leader.

**Mitigation**:
1. Client tracks redirect count per request
2. After N redirects (default: 3), try random node
3. After M total attempts (default: 10), fail request
4. Circuit breaker prevents repeated failures to same node

**Residual Risk**: Request fails if leadership is genuinely unstable. Client receives clear error.

#### D.4.2 Leader Elected But Can't Reach Followers

**Scenario**: Node wins lease but discovers all followers are unreachable.

**Mitigation**:
1. Enters degraded mode immediately
2. Writes go to S3 with amortization
3. Monitors for follower recovery
4. Optional: configurable policy to reject writes without N reachable followers

**Residual Risk**: Latency increase. Acceptable as this is the designed fallback.

#### D.4.3 Ghost Leader After Network Heal

**Scenario**: After partition heals, old leader's writes arrive at followers.

**Mitigation**:
1. `lease_index` is strictly increasing
2. Followers reject writes with old `lease_index`
3. Old leader receives NACKs and discovers it lost leadership
4. Old leader becomes follower and catches up

**Residual Risk**: None. Fencing prevents ghost writes.

### D.5 Data Integrity Edge Cases

#### D.5.1 Bit Rot / Silent Corruption

**Scenario**: Storage media corruption silently modifies data.

**Mitigation**:
1. CRC32c on every batch, validated on read
2. Corruption detected immediately on access
3. Recovery from followers or S3

**Residual Risk**: Unaccessed data could have undetected corruption. Consider background scrubbing in future version.

#### D.5.2 Torn Write

**Scenario**: Power failure during write causes partial data on disk.

**Mitigation**:
1. Pending write marker enables rollback detection
2. CRC validation catches partial writes
3. Startup recovery truncates to last valid batch

**Residual Risk**: None. Handled by crash recovery.

#### D.5.3 File System Full

**Scenario**: Disk fills up during write operation.

**Mitigation**:
1. Write fails with IO error
2. Pending write marker triggers rollback on next startup
3. No partial/corrupt state left
4. Monitor disk usage, alert at 80%

**Residual Risk**: Writes fail until space freed. Operational concern, not data integrity issue.

### D.6 Operational Edge Cases

#### D.6.1 Rolling Deployment

**Scenario**: Nodes restarted one by one during deployment.

**Impact**:
1. Restarting node's leases expire (30s)
2. Other nodes can acquire after safety window (32s)
3. During transition, some aggregates in degraded mode
4. After restart, node catches up from S3/peers

**Recommendations**:
1. Graceful shutdown releases leases early
2. Deploy during low-traffic periods
3. Monitor degraded mode percentage during deploy

#### D.6.2 All Nodes Restart Simultaneously

**Scenario**: Complete cluster restart (e.g., data center power event).

**Impact**:
1. All in-flight writes lost (not yet ACKed)
2. All nodes start fresh, race for leases
3. S3 degraded batches recovered

**Recommendations**:
1. Avoid simultaneous restart in production
2. If unavoidable, S3 contains durable state
3. Monitor for data consistency after restart

#### D.6.3 S3 Bucket Deleted/Inaccessible

**Scenario**: S3 bucket accidentally deleted or permissions revoked.

**Impact**:
1. Lease operations fail
2. Existing leaders continue until lease expires
3. No new leaders can be elected
4. Degraded mode fails

**Mitigation**:
1. S3 bucket versioning + MFA delete
2. IAM policy limits delete permissions
3. Alert on S3 access failures
4. Runbook for manual recovery

**Residual Risk**: Catastrophic but preventable with proper IAM/bucket configuration.

---

## Appendix E: Open Questions for Implementation

1. **Cross-aggregate batching transaction semantics**: If a batched replication message contains writes for aggregates A and B, and A succeeds but B fails (stale lease), what is the client experience? Currently: both writes are independent, A succeeds, B retries.

2. **S3 batch compaction**: Should we merge small range files into larger ones during quiet periods? Trade-off: reduced S3 object count vs. implementation complexity.

3. **Follower read consistency**: Should followers serve reads during catch-up? Current design: yes, with potentially stale data. Alternative: reject reads until caught up.

4. **Lease duration tuning**: 30s default balances failover time vs. S3 request rate. Should this be per-aggregate configurable for different SLA requirements?

5. **Multi-region considerations**: Design assumes single S3 region. For multi-region deployment, need separate design for cross-region replication (out of scope).
