```markdown replication-design.md
# EventPlaneDB Replication Design Document

## Table of Contents
1. [High-Level Design](#high-level-design)
2. [Detailed Design](#detailed-design)
3. [Risk Evaluation](#risk-evaluation)
4. [Task Breakdown](#task-breakdown)

---

## High-Level Design

### Overview

This document describes a replication strategy for EventPlaneDB that provides strong durability guarantees without implementing a full distributed consensus protocol like Raft or Paxos. Instead, leadership coordination is delegated to Amazon S3 using its conditional write (optimistic concurrency control) capabilities.

### Design Goals

1. **Strong Durability**: Writes are acknowledged only after local fsync AND replication to all followers
2. **Cost Efficiency**: Minimize S3 operations by only performing work on active aggregates
3. **Simplicity**: Avoid the complexity of implementing Raft/Paxos
4. **Performance**: Maintain 300k+ writes/second throughput with <10ms latency on happy path
5. **Availability**: Support graceful degradation when followers are unavailable
6. **Minimal Cluster Size**: Enable two-node clusters (unusual for consensus protocols)

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                         S3 Control Plane                            │
│  ┌──────────────────┐  ┌──────────────────────────────────────────┐ │
│  │ Cluster Members  │  │ Lease Files (per aggregate)              │ │
│  │ membership.json  │  │ /leases/{org}/{type}/{agg}/lease.json   │ │
│  └──────────────────┘  └──────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
                                    │
           ┌────────────────────────┼────────────────────────┐
           │                        │                        │
           ▼                        ▼                        ▼
    ┌─────────────┐          ┌─────────────┐          ┌─────────────┐
    │   Node A    │◄────────►│   Node B    │◄────────►│   Node C    │
    │  (Leader    │   TCP    │ (Follower)  │   TCP    │ (Follower)  │
    │  for Agg X) │ Repl.    │             │ Repl.    │             │
    └─────────────┘          └─────────────┘          └─────────────┘
           │
           ▼
    ┌─────────────┐
    │   Clients   │
    └─────────────┘
```

### Key Concepts

| Concept | Description |
|---------|-------------|
| **Aggregate-Level Leadership** | Each aggregate has independent leadership; Node A can lead aggregate X while Node B leads aggregate Y |
| **Lease-Based Leadership** | Leaders hold time-bound leases; they must renew before expiry to maintain leadership |
| **S3 as Coordination Layer** | S3 conditional writes provide atomic compare-and-swap for lease acquisition |
| **Synchronous Replication** | All followers must acknowledge before client acknowledgment |
| **Degraded Mode** | When followers unavailable, batches written to S3 for later catch-up |

### Write Path Summary

```
Client Write Request
        │
        ▼
┌───────────────────┐
│ 1. Validate Lease │ ─── Expired? ──► Reject with leader hint
└───────────────────┘
        │ Valid
        ▼
┌───────────────────┐
│ 2. Local Write    │
│    + fsync        │
└───────────────────┘
        │
        ▼
┌───────────────────┐
│ 3. Replicate to   │ ─── All Failed? ──► Write to S3 (degraded)
│    All Followers  │                            │
└───────────────────┘                            │
        │ All ACK                                │
        ▼                                        ▼
┌───────────────────┐                   ┌───────────────────┐
│ 4. ACK to Client  │                   │ 4. ACK to Client  │
└───────────────────┘                   │    (degraded)     │
                                        └───────────────────┘
```

---

## Detailed Design

### 2.1 Node Identity

Each node requires a persistent unique identifier.

**Generation**: On first startup, generate a random `u128` node ID. Store in `{data_root}/node_id`.

**Usage**:
- Included in lease acquisition requests
- Written to each event batch metadata for provenance
- Used in cluster membership registration

**File Format** (`{data_root}/node_id`):
```
{node_id as 32 hex chars}
```

### 2.2 Cluster Membership

A single S3 object maintains the authoritative cluster membership list.

**S3 Path**: `s3://{bucket}/cluster/membership.json`

**Schema**:
```json
{
  "version": 1,
  "members": [
    {
      "id": "u128 as hex string",
      "address": "ip:port",
      "replication_address": "ip:port",
      "is_active": true,
      "joined_at": 1234567890000,
      "last_heartbeat": 1234567890000
    }
  ]
}
```

**Operations**:
- Nodes read membership on startup and cache locally
- Nodes refresh membership periodically (every 30s) or on replication failure
- Membership changes require S3 conditional write on `version` field
- Administrative tooling manages member add/remove

**Consistency**: Membership is eventually consistent. Brief inconsistency is acceptable as it only affects which followers receive replication (all-or-nothing semantics handle partial views).

### 2.3 Lease Management

#### 2.3.1 Lease File Structure

**S3 Path**: `s3://{bucket}/leases/{org_id}/{aggregate_type_id}/{aggregate_id}/lease.json`

**Schema**:
```json
{
  "lease_index": 1,
  "node_id": "u128 as hex string",
  "lease_expiry": 1234567890000,
  "event_batch_index": 42,
  "requested_by_client": "u128 as hex string"
}
```

| Field | Purpose |
|-------|---------|
| `lease_index` | Monotonically increasing; enables conditional writes |
| `node_id` | Current lease holder |
| `lease_expiry` | Absolute timestamp (millis) when lease expires |
| `event_batch_index` | Last known batch index; helps new leaders catch up |
| `requested_by_client` | Client that triggered leadership contention (debugging/routing) |

#### 2.3.2 Lease Acquisition

A node attempts to acquire a lease when:
1. It receives a write request for an aggregate it doesn't lead
2. The current lease is near expiry (proactive takeover)
3. It's recovering and needs to determine leadership

**Acquisition Algorithm**:
```
1. Read current lease from S3 (may not exist)
2. If lease exists AND not expired AND holder != self:
   - Return error with current leader info
3. Prepare new lease:
   - lease_index = (current.lease_index or 0) + 1
   - node_id = self
   - lease_expiry = now + LEASE_DURATION
   - event_batch_index = current.event_batch_index or local knowledge
   - requested_by_client = triggering client
4. Conditional PUT with If-Match on ETag (or If-None-Match for new)
5. If success: We are leader
6. If conflict: Re-read and retry or return current leader
```

**Constants**:
- `LEASE_DURATION`: 30 seconds
- `LEASE_RENEWAL_THRESHOLD`: 10 seconds before expiry
- `MAX_CLOCK_SKEW`: 2 seconds (safety margin)

#### 2.3.3 Lease Renewal

Leaders proactively renew leases for active aggregates.

**Renewal Conditions**:
- Aggregate has received writes within the last renewal period
- Lease expiry is within `LEASE_RENEWAL_THRESHOLD`

**Renewal Algorithm**:
```
1. Read current lease
2. Verify we still hold it (node_id == self)
3. Conditional PUT with incremented lease_index and new expiry
4. If conflict: Another node took over; become follower
```

**Dormancy**: If no writes received for an aggregate within `IDLE_THRESHOLD` (e.g., 60s), stop renewing. Let lease expire naturally to save S3 costs.

#### 2.3.4 Lease Release

On graceful shutdown, leaders should release leases:

```
For each held lease:
  1. Read current lease
  2. If still leader, set lease_expiry = now (immediate expiry)
  3. Conditional PUT
```

This allows faster failover during rolling deployments.

### 2.4 Time Synchronization

Time-based leases require reasonable clock synchronization.

**Requirements**:
- All nodes must have clocks within `MAX_CLOCK_SKEW` (2s) of each other
- Clocks should be synchronized via NTP

**Verification**:
Nodes periodically verify clock alignment:
```
1. On startup and every 60s, request time from all peers
2. Calculate max observed drift
3. If drift > MAX_CLOCK_SKEW: Log warning, enter degraded mode
4. If drift > 2 * MAX_CLOCK_SKEW: Refuse to acquire new leases
```

**Safety Margins**:
- Lease expiry checks include `MAX_CLOCK_SKEW` buffer
- A lease is considered "valid" until `expiry - MAX_CLOCK_SKEW`
- New lease requests won't succeed until `expiry + MAX_CLOCK_SKEW`

### 2.5 Replication Protocol

#### 2.5.1 Transport

- **Protocol**: TCP with custom binary framing (glommio-compatible)
- **Port**: Dedicated replication port (separate from client port)
- **Connection**: Persistent connections between all node pairs

**Wire Format**:
```
┌─────────┬──────────┬────────────┬─────────────────┐
│ Version │ Msg Type │ Length     │ Payload         │
│ 4 bytes │ 4 bytes  │ 4 bytes    │ Variable        │
└─────────┴──────────┴────────────┴─────────────────┘
```

#### 2.5.2 Message Types

| Type | Direction | Purpose |
|------|-----------|---------|
| `ReplicateBatch` | Leader → Follower | Send batch for replication |
| `ReplicateBatchAck` | Follower → Leader | Acknowledge batch received and synced |
| `ReplicateBatchNack` | Follower → Leader | Request missing prior batches |
| `CatchUpRequest` | Follower → Leader | Request batches from index X |
| `CatchUpResponse` | Leader → Follower | Batches for catch-up |
| `TimeSync` | Bidirectional | Clock synchronization probe |

#### 2.5.3 ReplicateBatch Message

```json
{
  "aggregate_key": { "org_id": 1, "aggregate_type_id": 2, "aggregate_id": 3 },
  "lease_index": 5,
  "node_id": "leader node id",
  "event_batch": { /* serialized EventBatchItem */ },
  "event_batch_metadata": { /* serialized EventBatchMetadata */ }
}
```

**Follower Processing**:
1. Validate lease_index matches expected (leader hasn't changed)
2. Check event_batch_index is contiguous with local state
3. If gap detected: Return `Nack` with expected index
4. Write batch locally + fsync
5. Return `Ack`

#### 2.5.4 Handling Missing Batches

When a follower detects a gap:

```
Follower has: batch 1-10
Leader sends: batch 13

Follower returns Nack { expected_batch_index: 11 }

Leader sends: 
  - ReplicateBatch { batch 11 }
  - ReplicateBatch { batch 12 }
  - ReplicateBatch { batch 13 }
```

**Maximum Payload**: Batches are sent individually. If a single batch exceeds max message size, it's chunked (using existing compression).

### 2.6 Write Path (Detailed)

#### 2.6.1 Leader Write Sequence

```
1. Receive write request from client
2. Check lease validity:
   a. If no lease or expired: Attempt acquisition
   b. If acquisition fails: Return error with leader hint
3. Validate write (optimistic concurrency, idempotency)
4. Assign event indexes, create EventBatchItem
5. Serialize and compress batch
6. Write to local WAL + fsync
7. Prepare rollback state
8. Replicate to all active followers in parallel:
   a. Send ReplicateBatch
   b. Handle Nacks (send missing batches)
   c. Collect Acks
9. If all followers Ack:
   a. Clear rollback state
   b. Return success to client
10. If any follower unreachable/timeout:
    a. Write batch to S3 (degraded path)
    b. Mark aggregate in degraded mode
    c. Return success to client (with degraded flag)
11. On any failure before step 9:
    a. Execute rollback
    b. Return error to client
```

#### 2.6.2 Rollback Mechanism

The existing `sync_with_rollback` provides the foundation. Extended for replication:

**Rollback State** (per pending write):
```rust
struct PendingWrite {
    event_batch_index: u64,
    pre_write_file_len_metadata: u64,
    pre_write_file_len_event_batch: u64,
    client_id: u128,
    prior_client_event_index: Option<u64>,
}
```

**Rollback Procedure**:
1. Truncate metadata file to `pre_write_file_len_metadata`
2. Truncate event batch file to `pre_write_file_len_event_batch`
3. Restore `next_event_batch_index`
4. Restore `client_event_indexes` entry
5. Clear in-memory cache entries for this batch

### 2.7 Degraded Mode (S3 Hot Path)

When followers are unreachable, batches must be persisted to S3 for durability.

#### 2.7.1 S3 Batch Storage

**Path**: `s3://{bucket}/batches/{org_id}/{aggregate_type_id}/{aggregate_id}/{batch_index}.bin`

**Content**: Serialized and compressed `EventBatchItem` + `EventBatchMetadata`

**Write Procedure**:
1. After successful local fsync
2. PUT to S3 (no conditional write needed; batch_index is unique)
3. Record degraded mode entry locally

#### 2.7.2 Follower Catch-Up from S3

When a follower becomes leader or reconnects:

```
1. Check local event_batch_index
2. Read lease file for latest known batch_index
3. If gap exists:
   a. List S3 objects in batch path
   b. Download and apply missing batches
   c. Apply locally + fsync
4. Resume normal operation
```

#### 2.7.3 Exiting Degraded Mode

```
1. All followers are reachable
2. All followers have caught up (local batch_index >= S3 batch_index)
3. Clear degraded mode flag
4. Optionally: Delete S3 batch objects (or let lifecycle policy handle)
```

### 2.8 Follower Behavior

#### 2.8.1 Rejecting Client Writes

Followers must reject direct writes:

```rust
WriteResponse {
    error: Some(EventPlaneDBError::not_leader(current_leader_address)),
    leader_hint: Some(leader_node_address),
}
```

The client SDK should automatically retry to the indicated leader.

#### 2.8.2 Handling Reads

**Option A (Recommended)**: Followers serve reads from local state
- Lower latency
- Eventually consistent (replication lag)
- Simpler implementation

**Option B**: Forward reads to leader
- Strongly consistent
- Higher latency
- More complex

**Recommendation**: Option A with optional `require_leader` flag for clients needing strong consistency.

#### 2.8.3 Leadership Takeover

When a follower detects leader failure (lease expired):

```
1. Wait until lease_expiry + MAX_CLOCK_SKEW
2. Attempt lease acquisition
3. If successful:
   a. Check for S3 degraded batches
   b. Apply any missing batches
   c. Begin accepting writes
4. If failed: Another node became leader; remain follower
```

### 2.9 Metadata Extensions

#### 2.9.1 EventBatchMetadata Changes

Add to existing structure:

```rust
pub struct EventBatchMetadata {
    // ... existing fields ...
    
    /// Node ID that wrote this batch
    pub writer_node_id: u128,
    
    /// Lease index at time of write
    pub lease_index: u64,
}
```

#### 2.9.2 Backward Compatibility

- New fields have default values (0) for deserialization of old data
- Old readers ignore new fields
- Version field in metadata format already exists

### 2.10 Configuration

New configuration parameters:

```rust
pub struct ReplicationConfig {
    /// S3 bucket for control plane
    pub s3_bucket: String,
    
    /// S3 region (must support conditional writes)
    pub s3_region: String,
    
    /// Duration of lease before expiry (default: 30s)
    pub lease_duration_ms: u64,
    
    /// Time before expiry to renew (default: 10s)
    pub lease_renewal_threshold_ms: u64,
    
    /// Maximum allowed clock skew (default: 2s)
    pub max_clock_skew_ms: u64,
    
    /// Replication port
    pub replication_port: u16,
    
    /// Timeout for replication to single follower
    pub replication_timeout_ms: u64,
    
    /// Enable degraded mode (write to S3 on follower failure)
    pub enable_degraded_mode: bool,
}
```

---

## Risk Evaluation

### 3.1 Clock Skew Exceeds Safety Margin

| Aspect | Details |
|--------|---------|
| **Scenario** | Node clocks drift beyond MAX_CLOCK_SKEW due to NTP failure or VM migration |
| **Likelihood** | Low (modern cloud VMs have good clock sync) |
| **Consequence** | **CRITICAL**: Two nodes believe they are leader; split-brain writes |
| **Mitigations** | 1. Continuous clock drift monitoring with alerts<br>2. Refuse writes if drift detected above threshold<br>3. Use AWS Time Sync Service or equivalent<br>4. Lease includes clock reading; recipients validate<br>5. Consider hybrid logical clocks for ordering |

### 3.2 S3 Unavailability

| Aspect | Details |
|--------|---------|
| **Scenario** | S3 service outage or network partition to S3 |
| **Likelihood** | Very Low (S3 has 99.99% availability) |
| **Consequence** | **HIGH**: No new lease acquisitions; existing leaders continue until expiry |
| **Mitigations** | 1. Existing leaders continue operating with local state<br>2. Alert on S3 failures<br>3. Consider multi-region S3 for control plane<br>4. Cache lease state locally with TTL |

### 3.3 S3 Conditional Write Race

| Aspect | Details |
|--------|---------|
| **Scenario** | Two nodes attempt lease acquisition simultaneously |
| **Likelihood** | Medium (expected during failover) |
| **Consequence** | **LOW**: One wins, one loses; this is expected behavior |
| **Mitigations** | 1. Retry with backoff on conflict<br>2. Jitter on lease renewal timing<br>3. Clear error handling and client redirection |

### 3.4 Replication Timeout During Write

| Aspect | Details |
|--------|---------|
| **Scenario** | Leader fsync'd locally but follower times out during replication |
| **Likelihood** | Medium (network issues, follower overload) |
| **Consequence** | **MEDIUM**: Must choose between rollback or degraded mode |
| **Mitigations** | 1. Degraded mode: Write to S3, acknowledge to client<br>2. Tune replication timeout appropriately<br>3. Monitor replication latency<br>4. Consider quorum writes (2 of 3) for larger clusters |

### 3.5 Rollback Failure

| Aspect | Details |
|--------|---------|
| **Scenario** | Rollback of local write fails (disk error, process crash) |
| **Likelihood** | Low |
| **Consequence** | **CRITICAL**: Inconsistent state between local storage and what clients see |
| **Mitigations** | 1. Rollback is truncation (atomic on most filesystems)<br>2. On startup, validate last batch integrity<br>3. If inconsistency detected, recover from followers/S3<br>4. Idempotency allows safe replay |

### 3.6 Follower Falls Far Behind

| Aspect | Details |
|--------|---------|
| **Scenario** | Follower offline for extended period; catch-up takes too long |
| **Likelihood** | Medium (maintenance, failures) |
| **Consequence** | **MEDIUM**: Writes blocked waiting for slow follower |
| **Mitigations** | 1. Configurable catch-up timeout before degraded mode<br>2. Admin ability to remove follower from active set<br>3. Parallel batch transfer during catch-up<br>4. Snapshot-based recovery for large gaps |

### 3.7 Network Partition Between Nodes

| Aspect | Details |
|--------|---------|
| **Scenario** | Nodes can reach S3 but not each other |
| **Likelihood** | Low |
| **Consequence** | **HIGH**: Leader can acquire/renew lease but can't replicate |
| **Mitigations** | 1. Degraded mode with S3 batch storage<br>2. Detect partition via failed replication<br>3. Consider requiring replication to at least one follower<br>4. Health checks between nodes |

### 3.8 Lease Expiry During Write

| Aspect | Details |
|--------|---------|
| **Scenario** | Leader's lease expires mid-write (slow write, slow renewal) |
| **Likelihood** | Low (lease duration >> write time) |
| **Consequence** | **HIGH**: Another node may become leader; potential duplicate writes |
| **Mitigations** | 1. Check lease validity at write start AND before client ack<br>2. Refuse writes if lease expiry < 2 * MAX_WRITE_TIME<br>3. Batch index + lease_index prevents duplicate application |

### 3.9 Two-Node Cluster Limitations

| Aspect | Details |
|--------|---------|
| **Scenario** | In 2-node cluster, one node failure means no redundancy |
| **Likelihood** | Medium |
| **Consequence** | **MEDIUM**: Writes require degraded mode or blocking |
| **Mitigations** | 1. Document limitation: 2-node cluster sacrifices availability<br>2. Recommend 3-node minimum for production<br>3. Degraded mode maintains durability via S3 |

### 3.10 S3 Conditional Write Eventual Consistency

| Aspect | Details |
|--------|---------|
| **Scenario** | S3 strong consistency violations (historically an issue, now resolved) |
| **Likelihood** | Very Low (S3 provides strong consistency since Dec 2020) |
| **Consequence** | **CRITICAL**: Stale lease reads could cause split-brain |
| **Mitigations** | 1. S3 strong read-after-write consistency is now guaranteed<br>2. Monitor AWS announcements for any changes<br>3. Lease index provides additional protection |

---

## Task Breakdown

### Phase 1: Foundation (2-3 weeks)

#### 1.1 Node Identity
- [ ] Create `NodeIdentity` struct with `u128` ID
- [ ] Implement file-based persistence in data directory
- [ ] Generate on first startup, load on subsequent startups
- [ ] Unit tests for generation and persistence

#### 1.2 Configuration
- [ ] Add `ReplicationConfig` struct
- [ ] Extend CLI arguments in `EventPlaneDBConfig`
- [ ] Add configuration validation (e.g., timeouts, ports)
- [ ] Environment variable support for S3 credentials

#### 1.3 S3 Client Integration
- [ ] Add S3 client dependency (aws-sdk-s3 with async support)
- [ ] Create `S3ControlPlane` abstraction
- [ ] Implement conditional PUT/GET with ETag handling
- [ ] Error handling and retry logic
- [ ] Integration tests with LocalStack or MinIO

### Phase 2: Lease Management (2-3 weeks)

#### 2.1 Lease Data Structures
- [ ] Create `LeaseFile` struct matching JSON schema
- [ ] Implement serialization/deserialization
- [ ] Create `LeaseState` enum (NoLease, Leader, Follower)
- [ ] Unit tests for lease struct

#### 2.2 Lease Operations
- [ ] Implement `acquire_lease()` with conditional write
- [ ] Implement `renew_lease()`
- [ ] Implement `release_lease()`
- [ ] Implement `read_lease()` with caching
- [ ] Handle S3 conflict responses

#### 2.3 Lease Manager
- [ ] Create `LeaseManager` per-aggregate component
- [ ] Background task for proactive renewal
- [ ] Dormancy detection (no recent writes)
- [ ] Integration with write path
- [ ] Graceful shutdown lease release

### Phase 3: Cluster Membership (1-2 weeks)

#### 3.1 Membership Data Structures
- [ ] Create `ClusterMembership` struct
- [ ] Create `ClusterMember` struct
- [ ] Serialization/deserialization

#### 3.2 Membership Operations
- [ ] Read membership from S3
- [ ] Local caching with TTL refresh
- [ ] Admin API for member add/remove (future)
- [ ] Startup registration

### Phase 4: Replication Protocol (3-4 weeks)

#### 4.1 Wire Protocol
- [ ] Define message types enum
- [ ] Implement `ReplicateBatch` message
- [ ] Implement `ReplicateBatchAck` / `Nack`
- [ ] Implement `CatchUpRequest` / `Response`
- [ ] Implement `TimeSync` message
- [ ] Unit tests for serialization

#### 4.2 Replication Client (glommio)
- [ ] Create `ReplicationClient` for outbound connections
- [ ] Connection pooling per peer
- [ ] Send with timeout and retry
- [ ] Handle Nack (send missing batches)
- [ ] Parallel sends to multiple followers

#### 4.3 Replication Server (glommio)
- [ ] Create replication listener on dedicated port
- [ ] Accept and dispatch by message type
- [ ] Process `ReplicateBatch` (validate, write, ack)
- [ ] Process `CatchUpRequest` (read and send batches)
- [ ] Process `TimeSync` (respond with local time)

### Phase 5: Write Path Integration (2-3 weeks)

#### 5.1 Leader Write Path
- [ ] Add lease validation before write
- [ ] Extend `sync_with_rollback` for replication rollback
- [ ] Parallel replication to followers
- [ ] Collect acks with timeout
- [ ] Return with leader hint on lease failure

#### 5.2 Metadata Extensions
- [ ] Add `writer_node_id` to `EventBatchMetadata`
- [ ] Add `lease_index` to `EventBatchMetadata`
- [ ] Update wire format version
- [ ] Backward-compatible deserialization

#### 5.3 Follower Write Rejection
- [ ] Add `NotLeader` error type
- [ ] Include leader hint in response
- [ ] Client SDK update to follow redirects

### Phase 6: Degraded Mode (2 weeks)

#### 6.1 S3 Batch Storage
- [ ] Define S3 path structure for batches
- [ ] Implement batch PUT to S3
- [ ] Implement batch GET from S3
- [ ] Implement batch LIST for catch-up

#### 6.2 Degraded Mode Tracking
- [ ] Per-aggregate degraded mode flag
- [ ] Transition to degraded on follower failure
- [ ] Track which batches are in S3

#### 6.3 Catch-Up from S3
- [ ] Detect gaps on leader takeover
- [ ] Download and apply missing batches
- [ ] Exit degraded mode when caught up

### Phase 7: Time Synchronization (1 week)

#### 7.1 Clock Verification
- [ ] Periodic time probe to all peers
- [ ] Calculate and track max drift
- [ ] Warning logs on drift threshold
- [ ] Refuse new leases on excessive drift

### Phase 8: Testing & Hardening (2-3 weeks)

#### 8.1 Unit Tests
- [ ] Lease acquisition/renewal/release
- [ ] Replication protocol messages
- [ ] Rollback scenarios
- [ ] Clock skew handling

#### 8.2 Integration Tests
- [ ] Multi-node cluster setup
- [ ] Leader election under contention
- [ ] Failover scenarios
- [ ] Network partition simulation
- [ ] Degraded mode end-to-end

#### 8.3 Performance Tests
- [ ] Write throughput with replication
- [ ] Latency percentiles (p50, p99, p999)
- [ ] S3 operation counts
- [ ] Memory usage under load

#### 8.4 Chaos Testing
- [ ] Random node kills
- [ ] Network delay injection
- [ ] Clock skew simulation
- [ ] S3 failure injection

### Phase 9: Operational Readiness (1-2 weeks)

#### 9.1 Observability
- [ ] Metrics: lease acquisitions, renewals, expirations
- [ ] Metrics: replication latency, batch counts
- [ ] Metrics: degraded mode duration
- [ ] Structured logging for replication events

#### 9.2 Admin Tools
- [ ] CLI command to view lease state
- [ ] CLI command to force lease release
- [ ] CLI command to view cluster membership
- [ ] Health check endpoint

#### 9.3 Documentation
- [ ] Operations runbook
- [ ] Configuration reference
- [ ] Troubleshooting guide
- [ ] Architecture diagrams