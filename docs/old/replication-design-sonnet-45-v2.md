# EventPlaneDB Replication Design: Final Specification

## 1. Overview

This document specifies the design for adding high-availability replication to EventPlaneDB using a single-leader, multi-follower architecture at the aggregate level.

### 1.1 Goals

- **High Availability**: Continue serving reads/writes during individual node failures
- **Strong Durability**: Writes acknowledged only after persisted to leader + all available followers (or S3)
- **High Performance**: Maintain 300k+ writes/sec and <10ms latency in normal operation
- **Cost Efficiency**: Minimize S3 costs through dormant lease management
- **Operational Simplicity**: Avoid complex distributed consensus (Raft/Paxos)
- **Two-Node Support**: Enable viable two-node cluster deployments

### 1.2 Non-Goals

- Multi-region replication (single S3 region required for conditional writes)
- Byzantine fault tolerance (nodes are trusted)
- Read replicas (all nodes can serve reads from local cache)
- Automatic network partition resolution (manual intervention may be required)

### 1.3 Design Principles

1. **S3 as Control Plane**: Leverage S3 conditional writes for leader election instead of implementing distributed consensus
2. **Per-Aggregate Leadership**: Each aggregate has independent leadership, enabling natural load distribution
3. **Degraded Mode Gracefully**: Fall back to S3 when followers unavailable, maintaining availability over latency
4. **Proactive Clock Management**: Detect and prevent clock skew issues before they cause split-brain
5. **Audit Trail**: Track which node/lease wrote each batch for debugging and recovery

## 2. Architecture

### 2.1 Topology

```
                    S3 Control Plane
        ┌─────────────────────────────────────┐
        │  • Lease Files (per aggregate)      │
        │  • Cluster Membership               │
        │  • Catch-up Batches (degraded mode) │
        └─────────────────────────────────────┘
                    ▲          ▲
                    │          │
         ┌──────────┘          └──────────┐
         │                                 │
    ┌────▼─────┐  TCP Replication    ┌────▼─────┐
    │  Node A  │◄──────────────────── │  Node B  │
    │ (Leader) │                      │(Follower)│
    │ Agg 1,3  │────────────────────► │ Agg 1,3  │
    └──────────┘                      └──────────┘
         │                                 │
         │            TCP Replication      │
         └──────────────┬──────────────────┘
                        ▼
                   ┌──────────┐
                   │  Node C  │
                   │(Follower)│
                   │ Agg 1,3  │
                   └──────────┘
```

### 2.2 Node Identity

Each node generates and persists a unique `node_id: u128` on first startup:

**File**: `{data_root}/node_id.json`
```json
{
  "node_id": "0x1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d",
  "created_at": 1234567890000,
  "hostname": "eventplanedb-prod-1"
}
```

**Generation**: Cryptographically secure random UUID on first boot, validated for uniqueness on cluster join.

## 3. S3 Control Plane

### 3.1 Lease Structure

**Path**: `s3://{bucket}/clusters/{cluster_id}/leases/{org_id}/{type_id}/{agg_id}/lease.json`

```json
{
  "lease_index": 42,
  "node_id": "0x1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d",
  "lease_expiry": 1734567890000,
  "event_batch_index": 1500,
  "last_replicated_batch_index": 1500,
  "leader_address": "10.0.1.10:10000"
}
```

**Fields:**
- `lease_index`: Monotonically increasing u64 fencing token (prevents stale leaders)
- `node_id`: Current leader's unique identifier
- `lease_expiry`: Unix timestamp (ms) when lease expires
- `event_batch_index`: Latest batch written by this leader
- `last_replicated_batch_index`: Latest batch successfully replicated to all followers or S3
- `leader_address`: IP:port for client redirection

**Lease Parameters:**
- **Duration**: 30 seconds (configurable)
- **Renewal Window**: Leader renews when <10s remain
- **Safety Margin**: 5 seconds (account for clock skew + network latency)

### 3.2 Lease Acquisition

```
1. Read current lease from S3
2. IF lease expired (now > lease_expiry + safety_margin):
     IF local state behind lease.event_batch_index:
       Catch up from S3 first
     Attempt conditional write with:
       - lease_index = old_lease_index + 1
       - Condition: ETag matches OR lease_index matches
3. IF conditional write succeeds:
     → Become leader
   ELSE:
     → Become follower, retry from step 1
```

### 3.3 Lease Renewal

**Active Leader** (receiving writes):
- Renew proactively every 10 seconds (before expiry - safety_margin)
- Conditional write: increment `lease_index`, update `event_batch_index`
- On failure: Step down immediately, stop accepting writes

**Dormant Leader** (no recent writes):
- Stop renewal if no writes for >5 seconds
- Allow lease to expire naturally
- Next write triggers re-acquisition

### 3.4 Cluster Membership

**Path**: `s3://{bucket}/clusters/{cluster_id}/membership.json`

```json
{
  "version": 123,
  "members": [
    {
      "node_id": "0x1a2b3c4d...",
      "address": "10.0.1.10:10000",
      "is_active": true,
      "last_heartbeat": 1734567890000
    }
  ],
  "checksum": "sha256_of_members"
}
```

**Updates**: Nodes update via conditional write (If-Match: version), increment version on each change.

**Heartbeat**: Nodes update `last_heartbeat` every 10s. Nodes with heartbeat >60s old are considered dead.

## 4. Write Path

### 4.1 Happy Path (All Followers Available)

```
Client → Leader:
1. Leader validates lease (time + lease_index)
2. Leader assigns batch/event indices
3. Leader sends ReplicateBatch to all followers (parallel)
4. Leader waits for ACKs (timeout: 100ms)
5. IF all ACKs received:
     Leader writes locally + fsync
     Leader responds SUCCESS to client
   ELSE:
     → Enter degraded mode
```

**Key Points:**
- Replication happens **before** local fsync
- Client acknowledgment requires **all** replications + local fsync
- Followers validate continuity of `event_batch_index`

### 4.2 Degraded Mode (Follower Unavailable)

```
Leader:
1. Detect follower timeout/error during replication
2. Write batch to S3:
   Path: s3://.../catchup/{org_id}/{type_id}/{agg_id}/batch_{event_batch_index}.bin
3. Write locally + fsync
4. Update lease with new last_replicated_batch_index
5. Respond SUCCESS to client
```

**Cost Mitigation:**
- Use timeout hysteresis (don't immediately return to TCP replication)
- Batch multiple failed replications into single S3 write if possible
- Monitor and alert on prolonged degraded mode

### 4.3 Write Rollback

IF replication fails AND S3 write fails:
```
1. Leader truncates local files to remove speculative write
2. Leader rolls back in-memory state:
   - next_event_index
   - next_event_batch_index
   - client_event_indexes
3. Leader returns WriteError to client
```

## 5. Replication Protocol

### 5.1 Wire Format

**Request**: `ReplicateBatchRequest`
```rust
struct ReplicateBatchRequest {
    correlation_id: Option<u128>,
    org_id: u128,
    aggregate_type_id: u128,
    aggregate_id: u128,
    lease_index: u64,
    from_event_batch_index: u64,
    batches: Vec<EventBatchItem>,
}
```

**Response**: `ReplicateBatchResponse`
```rust
struct ReplicateBatchResponse {
    correlation_id: Option<u128>,
    success: bool,
    error: Option<ReplicationError>,
    current_event_batch_index: u64,
}

enum ReplicationError {
    LeaseIndexMismatch { expected: u64, actual: u64 },
    MissingBatches { from_index: u64, to_index: u64 },
    NotFollower,
    IoError,
}
```

### 5.2 Follower Processing

```
1. Validate lease_index matches expected
2. Check batch continuity (no gaps in event_batch_index)
3. IF gaps detected:
     Return MissingBatches error with range
4. Write batches using prepend_batches()
5. Fsync to disk
6. Return success ACK
```

### 5.3 Catch-Up Process

**Trigger**: Follower detects gap in event_batch_index

**Steps:**
```
1. Follower requests missing batches from leader (TCP)
2. IF leader cannot provide (trimmed):
     Follower downloads from S3:
     s3://.../catchup/{org}/{type}/{agg}/batch_{index}.bin
3. Follower applies batches in order
4. Follower verifies CRC for each batch
5. Resume normal replication
```

## 6. Data Structure Changes

### 6.1 EventBatchMetadata Extensions

```rust
pub struct EventBatchMetadata {
    // ... existing fields ...
    
    /// Node that wrote this batch
    #[serde(with = "serde_u128_base64", rename = "ni")]
    pub node_id: u128,
    
    /// Lease index at time of write (fencing token)
    #[serde(rename = "li")]
    pub lease_index: u64,
}
```

### 6.2 WriteOptions Extensions

```rust
pub struct WriteOptions {
    // ... existing fields ...
    
    /// Current node_id
    pub node_id: u128,
    
    /// Current lease_index for this aggregate
    pub lease_index: u64,
}
```

### 6.3 Error Types

```rust
pub enum EventPlaneDBError {
    // ... existing variants ...
    
    NotLeader {
        leader_node_id: Option<u128>,
        leader_address: Option<String>,
    },
    LeaseExpired,
    ReplicationFailed,
    CatchUpRequired {
        from_event_batch_index: u64,
        to_event_batch_index: u64,
    },
}
```

## 7. Clock Skew Management

### 7.1 Startup Validation

```
1. Node fetches current time from S3 (HEAD request)
2. Compare S3 time with local system time
3. IF |diff| > 5s: REFUSE TO START
4. IF |diff| > 1s: LOG WARNING
5. Periodic check every 60s during operation
```

### 7.2 Safety Margins

- **Lease Renewal**: Renew when <10s remain (lease_duration - 10s)
- **Lease Expiry Check**: Consider expired if `now > lease_expiry + 5s`
- **Replication Timeout**: 100ms default (aggressive to detect failures quickly)

## 8. Risk Evaluation

| Risk | Likelihood | Consequence | Mitigation |
|------|-----------|-------------|------------|
| **Split Brain (Clock Skew)** | Medium | Critical | • Mandatory NTP sync validation at startup<br>• Large safety margins (5s)<br>• Fencing via monotonic `lease_index`<br>• Fail-safe: Refuse to start if skew >5s |
| **S3 Control Plane Outage** | Low | High | • Leader continues until lease expires (grace period)<br>• Exponential backoff + retry for S3 ops<br>• Alert on S3 latency >1s<br>• Document manual recovery procedures |
| **Leader Crash After Local Write** | Low | High | • **Two-phase commit**: Replicate FIRST, then fsync<br>• Rollback mechanism on replication failure<br>• New leader validates continuity on takeover<br>• `last_replicated_batch_index` in lease for recovery |
| **Follower Unavailable** | High | Medium | • Immediate switch to S3 degraded mode<br>• Tight timeout (100ms) to unblock writes<br>• Background catch-up process<br>• Hysteresis before returning to TCP mode |
| **Zombie Leader** | Medium | High | • `lease_index` fencing token prevents stale writes<br>• Followers reject batches with old lease_index<br>• Automatic step-down on lease renewal failure |
| **Network Partition** | Medium | Medium | • Partitioned leader cannot renew → steps down<br>• New leader elected via S3 conditional write<br>• Brief write unavailability during election<br>• Monitoring + alerting on leadership changes |
| **Node ID Collision** | Very Low | High | • Validate uniqueness on cluster join<br>• Reject join if collision detected<br>• Alert on collision<br>• Manual recovery: regenerate node_id |
| **Replication Lag** | Medium | Low | • Leader tracks follower lag via heartbeat<br>• Automatic catch-up from S3<br>• New leader validates caught-up before accepting writes<br>• Configurable lag threshold for alerts |
| **S3 Cost Explosion** | Low | Low | • Dormant mode: Stop renewal if idle >5s<br>• Batch degraded writes when possible<br>• Monitor S3 request rate per aggregate<br>• Alert on sustained degraded mode |
| **Membership File Corruption** | Very Low | High | • Version number + checksum<br>• Local cache as fallback<br>• Monitoring for access errors<br>• Manual recovery tools |

## 9. Implementation Task Breakdown

### Phase 1: Foundations (2-3 weeks)

#### 1.1 Node Identity
- [ ] Create `NodeIdentity` module
- [ ] Generate/load node_id on startup
- [ ] Persist to `{data_root}/node_id.json`
- [ ] Add node_id to server config
- [ ] Unit tests for generation/persistence

**Files**: New `eventplanedb_core/src/node_identity.rs`

#### 1.2 Data Structure Updates
- [ ] Add `node_id: u128` to `EventBatchMetadata`
- [ ] Add `lease_index: u64` to `EventBatchMetadata`
- [ ] Add to `WriteOptions`: `node_id`, `lease_index`
- [ ] Update serialization/deserialization
- [ ] Migration tests for backward compatibility

**Files**: 
- `eventplanedb_structures/src/event_batch_metadata.rs`
- `eventplanedb_core/src/write_operations/write_structures.rs`

#### 1.3 S3 Client Integration
- [ ] Add `aws-sdk-s3` dependency
- [ ] Create `S3ControlPlane` abstraction
- [ ] Implement conditional GET/PUT (If-Match/If-None-Match)
- [ ] Add error handling + retry logic
- [ ] Integration tests with localstack

**Files**: New `eventplanedb_core/src/replication/s3_control_plane.rs`

#### 1.4 Clock Skew Detection
- [ ] Implement S3 time fetch (HEAD request)
- [ ] Add startup validation (fail if skew >5s)
- [ ] Add runtime monitoring (check every 60s)
- [ ] Health check endpoint for clock status
- [ ] Unit tests for skew detection

**Files**: New `eventplanedb_core/src/replication/clock_sync.rs`

### Phase 2: Cluster Management (2 weeks)

#### 2.1 Cluster Membership
- [ ] Create `ClusterMembership` struct
- [ ] Implement membership file read/write with conditional updates
- [ ] Add join/leave operations
- [ ] Implement heartbeat mechanism (every 10s)
- [ ] Add dead node detection (>60s)
- [ ] Periodic cache refresh (every 30s)
- [ ] Integration tests

**Files**: New `eventplanedb_core/src/replication/membership.rs`

#### 2.2 Lease Management
- [ ] Create `AggregateLease` struct
- [ ] Create `LeaseManager` trait
- [ ] Implement lease acquisition with conditional writes
- [ ] Implement lease renewal background task
- [ ] Implement lease validation before writes
- [ ] Add automatic step-down on lease failure
- [ ] Add graceful lease release on shutdown
- [ ] Integration tests

**Files**: 
- New `eventplanedb_core/src/replication/lease.rs`
- New `eventplanedb_core/src/replication/lease_manager.rs`

### Phase 3: Replication Protocol (3-4 weeks)

#### 3.1 Wire Protocol Extensions
- [ ] Add `ReplicateBatchRequest` to `request.rs`
- [ ] Add `ReplicateBatchResponse` to `response.rs`
- [ ] Add `ReplicationError` enum
- [ ] Add `RequestType::ReplicateBatch = 10`
- [ ] Add `ResponseType::ReplicateBatch = 10`
- [ ] Update `read_request`/`write_request`
- [ ] Update `read_response`/`write_response`
- [ ] Serialization tests

**Files**: 
- `eventplanedb_structures/src/request.rs`
- `eventplanedb_structures/src/response.rs`

#### 3.2 Follower Replication Handler
- [ ] Add `ReplicateBatch` handler in `process_request.rs`
- [ ] Implement batch continuity validation
- [ ] Integrate with `prepend_batches()`
- [ ] Add missing batch detection + error response
- [ ] Add lease_index validation
- [ ] Unit + integration tests

**Files**: `eventplanedb_core/src/process_request.rs`

#### 3.3 Leader Replication Logic
- [ ] Create `ReplicationManager` struct
- [ ] Implement parallel follower replication (futures_util::join_all)
- [ ] Add ACK tracking with timeout (100ms)
- [ ] Implement retry logic for missing batches
- [ ] Add replication latency metrics
- [ ] Integration tests with multi-node setup

**Files**: New `eventplanedb_core/src/replication/replication_manager.rs`

#### 3.4 Replication Client
- [ ] Create TCP client for replication (reuse existing infrastructure)
- [ ] Implement `replicate_to_follower()` function
- [ ] Add connection pooling/caching
- [ ] Add error handling + timeouts
- [ ] Unit tests

**Files**: New `eventplanedb_core/src/replication/replication_client.rs`

### Phase 4: Degraded Mode & Catch-Up (2-3 weeks)

#### 4.1 S3 Catch-Up Storage
- [ ] Implement batch upload to S3
  - Path: `catchup/{org}/{type}/{agg}/batch_{index}.bin`
- [ ] Add batch download from S3
- [ ] Implement batch cleanup after successful replication
- [ ] Add error handling + retry
- [ ] Integration tests

**Files**: New `eventplanedb_core/src/replication/catchup_storage.rs`

#### 4.2 Follower Catch-Up
- [ ] Implement catch-up detection (compare event_batch_index)
- [ ] Add S3 batch fetching in background task
- [ ] Integrate with `prepend_batches()`
- [ ] Add progress tracking
- [ ] Add catch-up metrics
- [ ] Integration tests

**Files**: New `eventplanedb_core/src/replication/catchup.rs`

#### 4.3 Write Coordinator (Two-Phase Commit)
- [ ] Create `ReplicatedWriteCoordinator` struct
- [ ] Implement two-phase protocol:
  1. Replicate to followers/S3
  2. Local write + fsync
  3. ACK client
- [ ] Add degraded mode detection
- [ ] Add rollback on partial failure
- [ ] Comprehensive error handling
- [ ] Integration tests for all write scenarios

**Files**: New `eventplanedb_core/src/replication/write_coordinator.rs`

### Phase 5: Integration (2 weeks)

#### 5.1 Server Startup Integration
- [ ] Initialize `NodeIdentity` on startup
- [ ] Start cluster membership heartbeat
- [ ] Start lease management background tasks
- [ ] Add health check endpoints
- [ ] Add replication metrics
- [ ] Integration tests

**Files**: `eventplanedb_server/src/main.rs`

#### 5.2 Process Request Integration
- [ ] Update `handle_write` to check leadership
- [ ] Use `ReplicatedWriteCoordinator` for writes
- [ ] Add client redirection for followers
- [ ] Update error responses with leader info
- [ ] Integration tests

**Files**: `eventplanedb_core/src/process_request.rs`

#### 5.3 AggregateResources Integration
- [ ] Add `LeaseManager` to `AggregateResources`
- [ ] Add `ensure_leader()` method
- [ ] Update `sync_with_delay()` to handle replication
- [ ] Integration tests

**Files**: `eventplanedb_core/src/cache/aggregate_resources.rs`

#### 5.4 Graceful Shutdown
- [ ] Implement lease release on shutdown
- [ ] Add cluster membership leave operation
- [ ] Ensure all replications complete before exit
- [ ] Integration tests

**Files**: `eventplanedb_server/src/main.rs`

### Phase 6: Client Updates (1 week)

#### 6.1 Error Handling & Redirection
- [ ] Add `NotLeader` error variant
- [ ] Update client to handle redirection
- [ ] Add automatic retry to new leader
- [ ] Add connection pooling for multiple nodes
- [ ] Unit + integration tests

**Files**: 
- `eventplanedb_structures/src/eventplanedb_error.rs`
- `eventplanedb_client/src/lib.rs`

### Phase 7: Configuration (1 week)

#### 7.1 Server Configuration
- [ ] Add `ReplicationConfig` struct to `config.rs`
- [ ] Add CLI arguments:
  - `--replication-enabled`
  - `--s3-bucket`, `--s3-region`
  - `--cluster-id`
  - `--lease-duration-secs` (default: 30)
  - `--replication-timeout-ms` (default: 100)
  - `--max-clock-skew-secs` (default: 5)
- [ ] Add configuration validation
- [ ] Add documentation
- [ ] Unit tests

**Files**: `eventplanedb_server/src/config.rs`

### Phase 8: Testing & Documentation (2 weeks)

#### 8.1 Integration Tests
- [ ] Multi-node cluster startup/shutdown
- [ ] Leader election scenarios
- [ ] Failover testing (kill leader)
- [ ] Follower failure + catch-up
- [ ] Degraded mode scenarios
- [ ] Split-brain prevention tests
- [ ] Clock skew edge cases
- [ ] Rolling updates

#### 8.2 Documentation
- [ ] Architecture overview
- [ ] Deployment guide
- [ ] Configuration reference
- [ ] Monitoring & alerting guide
- [ ] Troubleshooting playbook
- [ ] Manual recovery procedures

## 10. Configuration Reference

```yaml
replication:
  enabled: true
  s3_bucket: "eventplanedb-control-plane"
  s3_region: "us-east-1"
  cluster_id: "prod-cluster-1"
  
  # Lease settings
  lease_duration_secs: 30
  lease_safety_margin_secs: 5
  lease_renewal_interval_secs: 10
  
  # Replication settings
  replication_timeout_ms: 100
  max_replication_batch_size_mb: 10
  
  # Clock settings
  max_clock_skew_secs: 5
  clock_check_interval_secs: 60
  
  # Heartbeat settings
  heartbeat_interval_secs: 10
  dead_node_threshold_secs: 60
  
  # Degraded mode settings
  s3_batch_max_size_mb: 10
  degraded_mode_hysteresis_secs: 30
```

## 11. Monitoring & Metrics

### 11.1 Key Metrics

**Leadership:**
- `replication.leadership.changes{aggregate}` - Counter of leadership transitions
- `replication.lease.renewals{aggregate}` - Counter of successful lease renewals
- `replication.lease.failures{aggregate}` - Counter of lease renewal failures
- `replication.lease.time_to_expiry{aggregate}` - Gauge of time until lease expires

**Replication:**
- `replication.batch.replicated{aggregate,node}` - Counter of batches replicated
- `replication.batch.latency{aggregate}` - Histogram of replication latency
- `replication.follower.lag{aggregate,node}` - Gauge of follower lag in batches
- `replication.degraded_mode{aggregate}` - Gauge (0/1) if in degraded mode

**S3:**
- `replication.s3.requests{operation}` - Counter of S3 API calls
- `replication.s3.latency{operation}` - Histogram of S3 operation latency
- `replication.s3.errors{operation}` - Counter of S3 errors

**Clock:**
- `replication.clock.skew_ms` - Gauge of current clock skew
- `replication.clock.checks` - Counter of clock checks performed

### 11.2 Alerts

**Critical:**
- Clock skew >5s
- Leadership lost unexpectedly
- Replication failed for >5 minutes
- S3 control plane errors for >1 minute

**Warning:**
- Follower lag >100 batches
- Degraded mode active for >5 minutes
- Clock skew >1s
- Lease renewal latency >1s

## 12. Operational Procedures

### 12.1 Rolling Update

```
1. Mark node as inactive in membership file
2. Wait for leases to migrate (max 30s)
3. Graceful shutdown node (releases leases)
4. Update + restart node
5. Mark node as active in membership file
6. Wait for it to become follower
7. Repeat for next node
```

### 12.2 Manual Recovery from Split Brain

```
1. Identify conflicting nodes (check node_id + lease_index in logs)
2. Determine authoritative node (highest event_batch_index)
3. Stop ALL nodes
4. Delete S3 lease files
5. On non-authoritative nodes:
   - Truncate files to last_replicated_batch_index from lease
   - OR restore from authoritative node
6. Restart nodes
7. Verify single leader elected
```

### 12.3 S3 Outage Response

```
1. Existing leaders continue operating until lease expiry
2. No new leaders can be elected
3. Monitor lease expiry times
4. IF lease expires during outage:
   - Node enters read-only mode
   - Alert operators
   - Document aggregate status
5. WHEN S3 recovers:
   - Nodes attempt lease acquisition
   - System auto-recovers
```