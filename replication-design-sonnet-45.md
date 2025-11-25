# EventPlaneDB Replication Design

## Overview

This document describes the design for adding replication to EventPlaneDB. The system will support a single-leader, multiple-follower topology at the aggregate level, using S3 as a control plane for leader election and cluster coordination.

## Goals

1. **High Availability**: Continue serving reads/writes even when individual nodes fail
2. **Data Durability**: Ensure writes are replicated before acknowledging to clients
3. **Performance**: Maintain 300k+ writes/second throughput and <10ms latency in normal operation
4. **Simplicity**: Avoid complex distributed consensus protocols while maintaining correctness
5. **Cost Efficiency**: Minimize S3 usage to only active aggregates
6. **Operational Flexibility**: Support two-node clusters and rolling updates

## Non-Goals

1. Multi-region replication (single S3 region required for conditional writes)
2. Byzantine fault tolerance (nodes are trusted)
3. Automatic leader election during network partitions (lease timeout mechanism instead)

## Risk Evaluation

### R1: Clock Skew Between Nodes
- **Scenario**: Nodes have significant time differences, causing overlapping leases or premature lease expiry
- **Likelihood**: Medium (can occur in cloud environments)
- **Consequence**: High (split-brain scenarios, data corruption)
- **Mitigation**:
  - Implement mandatory NTP synchronization check at startup
  - Use configurable safety margins (e.g., 30s minimum between lease renewal and expiry)
  - Add clock skew detection in health checks
  - Fail-safe: Node removes itself from cluster if clock skew > threshold

### R2: S3 Control Plane Unavailability
- **Scenario**: S3 API becomes unavailable or experiences high latency
- **Likelihood**: Low (S3 has high availability SLA)
- **Consequence**: High (no new leaders can be elected, existing leases expire)
- **Mitigation**:
  - Leader continues operating until lease expires (grace period)
  - Aggressive retry logic with exponential backoff for S3 operations
  - Monitoring and alerting on S3 control plane latency
  - Document operational procedures for S3 outages

### R3: Follower Unavailability During Write
- **Scenario**: One or more followers are unavailable when leader attempts replication
- **Likelihood**: Medium (network issues, follower crashes, rolling updates)
- **Consequence**: Medium (increased latency, S3 costs, potential data loss if follower becomes leader)
- **Mitigation**:
  - Implement degraded mode: replicate to S3 when followers unavailable
  - Background catch-up process for followers
  - Maximum batch size limits to prevent excessive S3 usage
  - Configurable timeout for follower replication attempts
  - Health checks to detect follower issues proactively

### R4: Leader Crash After Local Write But Before Replication
- **Scenario**: Leader writes locally and fsyncs, but crashes before replicating to followers/S3
- **Likelihood**: Low (requires precise timing of crash)
- **Consequence**: High (data loss, client received success but data not replicated)
- **Mitigation**:
  - Two-phase write protocol: replicate first, then acknowledge client
  - Implement write-ahead log rollback on startup
  - New leader validates event_batch_index continuity on lease acquisition
  - Add "last_replicated_batch_index" to lease file for recovery

### R5: Split Brain Due to Network Partition
- **Scenario**: Network partition causes two nodes to believe they are leader
- **Likelihood**: Low (S3 conditional writes provide strong consistency)
- **Consequence**: Critical (data corruption, divergent writes)
- **Mitigation**:
  - Rely on S3 conditional writes as single source of truth
  - Nodes validate lease before every write operation
  - Implement fencing tokens (lease_index must increment)
  - Automatic step-down on lease write failure

### R6: Replication Lag Causing Follower to Miss Critical Batches
- **Scenario**: Follower is behind, old leader fails, follower becomes leader missing recent writes
- **Likelihood**: Medium (during high write load or network issues)
- **Consequence**: High (data unavailable until catch-up completes)
- **Mitigation**:
  - Store replication watermarks in lease file
  - New leader validates continuity before accepting writes
  - Automatic catch-up from S3 on leader election
  - Reject client writes until catch-up complete (fail-safe mode)

### R7: Node ID Collision
- **Scenario**: Two nodes generate the same ID, causing identity confusion
- **Likelihood**: Very Low (UUID collision)
- **Consequence**: High (cluster confusion, potential data corruption)
- **Mitigation**:
  - Use cryptographically strong random UUID generation
  - Validate uniqueness on cluster join via membership file
  - Reject join if ID collision detected
  - Document manual recovery procedure

### R8: Cluster Membership File Corruption
- **Scenario**: Membership file in S3 becomes corrupted or inconsistent
- **Likelihood**: Very Low (S3 durability guarantees)
- **Consequence**: High (cluster cannot coordinate, potential split brain)
- **Mitigation**:
  - Versioned membership file with checksums
  - Last-known-good membership cached locally
  - Monitoring for membership file access errors
  - Manual recovery tools and procedures

## High-Level Design

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                         S3 Control Plane                     │
│  ┌─────────────────┐  ┌──────────────────────────────────┐  │
│  │ Membership File │  │  Per-Aggregate Lease Files        │  │
│  │  - Node List    │  │  /leases/org/type/agg/lease.json │  │
│  │  - Addresses    │  │  - lease_index                    │  │
│  │  - Status       │  │  - node_id (leader)               │  │
│  └─────────────────┘  │  - lease_expiry                   │  │
│                       │  - event_batch_index              │  │
│                       │  - last_replicated_batch_index    │  │
│                       └──────────────────────────────────┘  │
│                                                             │
│  ┌────────────────────────────────────────────────────┐    │
│  │  Per-Aggregate Catch-Up Batches (Degraded Mode)   │    │
│  │  /catchup/org/type/agg/batch_{index}.bin          │    │
│  └────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
                           ▲  │
                           │  │ S3 API
                           │  ▼
        ┌──────────────────────────────────────────────┐
        │              EventPlaneDB Cluster             │
        │                                               │
        │  ┌─────────────┐      ┌─────────────┐        │
        │  │   Node 1    │      │   Node 2    │        │
        │  │  (Leader)   │─────▶│ (Follower)  │        │
        │  │             │ TCP  │             │        │
        │  │ Agg: 1,2,5  │◀─────│ Agg: 1,2,5  │        │
        │  └─────────────┘      └─────────────┘        │
        │         │                     ▲               │
        │         │ TCP Replication     │               │
        │         ▼                     │               │
        │  ┌─────────────┐              │               │
        │  │   Node 3    │──────────────┘               │
        │  │ (Follower)  │                              │
        │  │             │                              │
        │  │ Agg: 1,2,5  │                              │
        │  └─────────────┘                              │
        └───────────────────────────────────────────────┘
                           ▲
                           │ Client Requests
                           │
                    ┌──────┴──────┐
                    │   Clients   │
                    └─────────────┘
```

### Core Concepts

**Leadership**: Leadership is per-aggregate, not per-node. A single node may be leader for some aggregates and follower for others. This provides natural load distribution and failure isolation.

**Leases**: Leadership is time-bounded via leases stored in S3. A lease grants exclusive write permission for a specific aggregate until the lease expires. Leases are acquired using S3 conditional writes (If-None-Match or If-Match headers).

**Replication Flow**:
1. Client sends write request to any node
2. Non-leader nodes redirect client to current leader
3. Leader validates lease
4. Leader replicates batch to all followers via TCP
5. If all followers acknowledge: Leader writes locally and fsyncs
6. If any follower fails: Leader writes batch to S3 catch-up storage
7. Leader acknowledges write to client

**Degraded Mode**: When followers are unavailable, leader writes batches to S3 for durability. This ensures followers can catch up even if the leader fails before they recover.

## Detailed Design

### 1. Node Identity

Each node requires a persistent unique identifier stored in its data directory.

**File**: `{data_root}/node_id.json`
```json
{
  "node_id": "0x1a2b3c4d5e6f7g8h9i0j1k2l3m4n5o6p",
  "created_at": 1234567890000,
  "hostname": "eventplanedb-1"
}
```

**Generation**: On first startup, generate a cryptographically secure random u128 ID (16 bytes). Persist to disk immediately.

**Validation**: On startup, if file exists, load the ID. If file doesn't exist, generate new ID. Validate uniqueness when joining cluster via membership file.

### 2. Cluster Membership

**File**: `s3://{bucket}/clusters/{cluster_id}/membership.json`
```json
{
  "version": 123,
  "members": [
    {
      "node_id": "0x1a2b3c4d...",
      "address": "10.0.1.10:10000",
      "is_active": true,
      "last_heartbeat": 1234567890000
    },
    {
      "node_id": "0x2b3c4d5e...",
      "address": "10.0.1.11:10000",
      "is_active": true,
      "last_heartbeat": 1234567890050
    }
  ],
  "checksum": "sha256_hash"
}
```

**Updates**: Nodes update membership using S3 conditional writes (If-Match: version). Increment version on each update.

**Heartbeats**: Nodes update their `last_heartbeat` timestamp every 10 seconds. Nodes with heartbeat > 60 seconds old are considered dead.

**Join Process**:
1. Node reads current membership file
2. Validates its node_id is unique
3. Adds itself to members list
4. Conditional write with expected version
5. If conflict, retry from step 1

### 3. Aggregate Leases

**File**: `s3://{bucket}/clusters/{cluster_id}/leases/{org_id}/{type_id}/{agg_id}/lease.json`
```json
{
  "lease_index": 42,
  "node_id": "0x1a2b3c4d...",
  "lease_expiry": 1234567890000,
  "event_batch_index": 1500,
  "last_replicated_batch_index": 1500,
  "requested_by_client": "0x5f6g7h8i..."
}
```

**Fields**:
- `lease_index`: Monotonically increasing fencing token
- `node_id`: Current leader
- `lease_expiry`: Unix timestamp (milliseconds) when lease expires
- `event_batch_index`: Latest batch written by this leader
- `last_replicated_batch_index`: Latest batch replicated to all followers or S3
- `requested_by_client`: Optional, tracks which client triggered this lease acquisition

**Lease Duration**: 30 seconds (configurable)

**Renewal Window**: Leader renews lease when < 10 seconds remain (configurable safety margin)

**Acquisition**:
1. Read current lease file (if exists)
2. Check if lease is expired (current_time > lease_expiry + safety_margin)
3. If expired: Attempt conditional write with incremented lease_index
4. If no lease exists: Conditional write with If-None-Match: *
5. If conditional write succeeds: Node is leader
6. If conditional write fails: Retry from step 1

**Validation**: Before each write operation, leader validates:
- Current time < lease_expiry - safety_margin
- In-memory lease_index matches S3 lease_index (refresh if needed)

### 4. Write Path (Normal Operation)

**Sequence**:
```
Client                Leader              Follower 1         Follower 2          S3
  │                     │                     │                 │                │
  │ WriteRequest        │                     │                 │                │
  ├────────────────────▶│                     │                 │                │
  │                     │ Validate Lease      │                 │                │
  │                     ├────────────────────────────────────────────────────────▶│
  │                     │◀────────────────────────────────────────────────────────┤
  │                     │                     │                 │                │
  │                     │ ReplicateBatch      │                 │                │
  │                     ├────────────────────▶│                 │                │
  │                     │ ReplicateBatch      │                 │                │
  │                     ├─────────────────────────────────────▶│                │
  │                     │                     │                 │                │
  │                     │      BatchAck       │                 │                │
  │                     │◀────────────────────┤                 │                │
  │                     │      BatchAck       │                 │                │
  │                     │◀─────────────────────────────────────┤                │
  │                     │                     │                 │                │
  │                     │ queue_events_in_memory()             │                │
  │                     │ sync_with_rollback() (local fsync)   │                │
  │                     │                     │                 │                │
  │     WriteResponse   │                     │                 │                │
  │◀────────────────────┤                     │                 │                │
```

**Steps**:
1. Client sends `WriteRequest` to any node
2. Node checks if it is leader for this aggregate
   - If not leader: Return error with leader node_id/address
3. Leader validates lease (time remaining, lease_index)
4. Leader replicates batch to all active followers via TCP
5. Leader waits for acknowledgments with timeout (default: 100ms)
6. If all acks received:
   - Leader calls `queue_events_in_memory()`
   - Leader calls `sync_with_rollback()`
   - Leader updates `last_replicated_batch_index` in memory
   - Leader responds to client with success
7. If timeout or error from any follower: Enter degraded mode

### 5. Write Path (Degraded Mode)

When one or more followers don't acknowledge within timeout:

**Sequence**:
```
Leader              Follower (down)         S3
  │                     │                    │
  │ ReplicateBatch      │                    │
  ├────────────────────▶│                    │
  │        (timeout)    │                    │
  │                     │                    │
  │ PutObject(batch)    │                    │
  ├────────────────────────────────────────▶│
  │                     │        Success     │
  │◀──────────────────────────────────────────┤
  │                     │                    │
  │ queue_events_in_memory()                │
  │ sync_with_rollback()                    │
  │                     │                    │
  │ Update lease file   │                    │
  │ (last_replicated=N) │                    │
  ├────────────────────────────────────────▶│
  │◀──────────────────────────────────────────┤
```

**S3 Batch File**: `s3://{bucket}/clusters/{cluster_id}/catchup/{org_id}/{type_id}/{agg_id}/batch_{event_batch_index}.bin`

**Steps**:
1. Leader detects follower timeout/error
2. Leader writes batch to S3 catch-up location
3. Leader writes locally and fsyncs
4. Leader updates lease file with new `last_replicated_batch_index`
5. Leader responds to client with success

**Catch-Up Process** (followers):
1. Follower detects it is behind (via heartbeat response from leader)
2. Follower requests missing batches from S3
3. Follower downloads and applies batches in order
4. Follower updates local state
5. Follower resumes normal replication

### 6. Follower Replication Protocol

**Request**: `ReplicateBatchRequest`
```rust
struct ReplicateBatchRequest {
    correlation_id: Option<u128>,
    org_id: u128,
    aggregate_type_id: u128,
    aggregate_id: u128,
    lease_index: u64,
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

**Follower Processing**:
1. Validate lease_index matches follower's known lease
2. Validate batch continuity (no gaps)
3. If gaps detected: Return `MissingBatches` error
4. Apply batches using `prepend_batches()` or queue if current
5. Fsync to disk
6. Return success acknowledgment

**Multi-Batch Replication** (when follower is behind):
Leader retries with additional historical batches up to max message size. May require multiple round trips.

### 7. Lease Management

**Acquisition State Machine**:
```
    [Startup]
        │
        ▼
    [Follower] ◀─────┐
        │            │
        │ (write req │
        │  received) │
        ▼            │
[Check Lease]        │
        │            │
        ├─ expired? ─┤
        │    yes     │
        ▼            │
[Attempt Acquire]    │
        │            │
        ├─ success? ─┤
        │    no      │
        ▼            │
    [Leader] ────────┘
        │      (lease expires
        │       or voluntary
        │       step down)
```

**Leader Responsibilities**:
- Renew lease proactively (before expiry - safety_margin)
- Only renew if aggregate has active writes in last N seconds
- Update `event_batch_index` in lease on each write
- Validate lease before every write operation
- Step down immediately if lease write fails

**Follower Responsibilities**:
- Don't attempt to acquire lease while valid
- Accept replication requests from leader
- Redirect client writes to leader
- Attempt lease acquisition on leader failure or lease expiry

**Lease Release** (graceful shutdown):
```json
{
  "lease_index": 42,
  "node_id": null,
  "lease_expiry": 0,
  "event_batch_index": 1500,
  "last_replicated_batch_index": 1500,
  "requested_by_client": null
}
```

Leader writes null values on shutdown to allow immediate leader election.

### 8. Clock Skew Management

**Startup Validation**:
1. Node fetches current time from S3 via HEAD request
2. Compare S3 time with local system time
3. If diff > max_allowed_skew (default: 5s): Refuse to start
4. Log warning if diff > warning_threshold (default: 1s)

**Runtime Checks**:
- Periodic NTP synchronization validation (every 60s)
- Monitor clock drift in health check endpoints
- Include safety margins in all time-based decisions

**Safety Margins**:
- Lease renewal: Renew when < 10s remaining (configurable)
- Lease expiry check: Current time > lease_expiry + 5s (configurable)
- Replication timeout: 100ms default (configurable)

### 9. Data Structures Updates

**EventBatchMetadata** (new fields):
```rust
pub struct EventBatchMetadata {
    // ... existing fields ...
    
    /// Node ID that wrote this batch (for tracking)
    #[serde(with = "serde_u128_base64", rename = "ni")]
    pub node_id: u128,
    
    /// Lease index at time of write (for fencing)
    #[serde(rename = "li")]
    pub lease_index: u64,
}
```

**WriteOptions** (new fields):
```rust
pub struct WriteOptions {
    // ... existing fields ...
    
    /// Node ID of this writer
    pub node_id: u128,
    
    /// Current lease index for fencing
    pub lease_index: u64,
}
```

### 10. Configuration

**New Server Configuration**:
```rust
pub struct ReplicationConfig {
    /// Enable replication (default: false for backward compatibility)
    pub enabled: bool,
    
    /// S3 bucket for control plane
    pub s3_bucket: String,
    
    /// S3 region
    pub s3_region: String,
    
    /// Cluster ID
    pub cluster_id: String,
    
    /// Lease duration in seconds (default: 30)
    pub lease_duration_secs: u64,
    
    /// Safety margin for lease operations (default: 5s)
    pub lease_safety_margin_secs: u64,
    
    /// Follower replication timeout (default: 100ms)
    pub replication_timeout_ms: u64,
    
    /// Maximum clock skew allowed (default: 5s)
    pub max_clock_skew_secs: u64,
    
    /// Heartbeat interval (default: 10s)
    pub heartbeat_interval_secs: u64,
    
    /// Maximum S3 batch size for catch-up (default: 10MB)
    pub max_catchup_batch_size_bytes: usize,
}
```

## Task Breakdown

### Phase 1: Foundation (3-4 weeks)

#### Task 1.1: Node Identity Management
- [ ] Create `NodeIdentity` struct
- [ ] Implement node ID generation (cryptographically secure u128)
- [ ] Add node_id.json persistence in data directory
- [ ] Add node ID loading on startup
- [ ] Add node ID validation against cluster membership
- [ ] Unit tests for ID generation and persistence
- **Files**: New `eventplanedb_core/src/node_identity.rs`

#### Task 1.2: S3 Client Integration
- [ ] Add aws-sdk-s3 dependency
- [ ] Create `S3ControlPlane` struct
- [ ] Implement conditional write operations
- [ ] Implement conditional read operations
- [ ] Add error handling and retry logic
- [ ] Add integration tests with localstack
- **Files**: New `eventplanedb_core/src/replication/s3_control_plane.rs`

#### Task 1.3: Clock Skew Detection
- [ ] Implement S3 time fetch via HEAD request
- [ ] Add startup clock skew validation
- [ ] Add runtime clock monitoring
- [ ] Add health check endpoint for clock status
- [ ] Unit tests for skew detection logic
- **Files**: New `eventplanedb_core/src/replication/clock_sync.rs`

#### Task 1.4: Cluster Membership
- [ ] Create `ClusterMembership` struct
- [ ] Implement membership file read/write with conditional updates
- [ ] Add join/leave cluster operations
- [ ] Implement heartbeat mechanism
- [ ] Add dead node detection
- [ ] Unit and integration tests
- **Files**: New `eventplanedb_core/src/replication/membership.rs`

### Phase 2: Lease Management (2-3 weeks)

#### Task 2.1: Lease Data Structures
- [ ] Create `AggregateLease` struct
- [ ] Create `LeaseManager` trait
- [ ] Implement lease file serialization/deserialization
- [ ] Add lease validation logic
- [ ] Unit tests for lease operations
- **Files**: New `eventplanedb_core/src/replication/lease.rs`

#### Task 2.2: Leader Election
- [ ] Implement lease acquisition logic with conditional writes
- [ ] Add lease renewal background task
- [ ] Implement lease validation before writes
- [ ] Add automatic step-down on lease failure
- [ ] Add graceful lease release on shutdown
- [ ] Unit and integration tests
- **Files**: `eventplanedb_core/src/replication/leader_election.rs`

#### Task 2.3: Follower Role Management
- [ ] Implement follower state tracking
- [ ] Add leader node discovery
- [ ] Implement client request redirection
- [ ] Add lease expiry monitoring for promotion
- [ ] Unit tests
- **Files**: `eventplanedb_core/src/replication/follower.rs`

### Phase 3: Replication Protocol (3-4 weeks)

#### Task 3.1: Request/Response Structures
- [ ] Add `ReplicateBatchRequest` to request.rs
- [ ] Add `ReplicateBatchResponse` to response.rs
- [ ] Add `ReplicationError` enum
- [ ] Add serialization tests
- [ ] Update wire protocol versioning
- **Files**: `eventplanedb_structures/src/request.rs`, `response.rs`

#### Task 3.2: Follower Replication Handler
- [ ] Implement `ReplicateBatch` request handler in process_request.rs
- [ ] Add batch continuity validation
- [ ] Integrate with existing write operations
- [ ] Add missing batch detection and error response
- [ ] Add lease_index validation
- [ ] Unit and integration tests
- **Files**: `eventplanedb_core/src/process_request.rs`

#### Task 3.3: Leader Replication Logic
- [ ] Create `ReplicationManager` struct
- [ ] Implement parallel follower replication with timeout
- [ ] Add acknowledgment tracking
- [ ] Implement retry logic for missing batches
- [ ] Add metrics for replication latency
- [ ] Integration tests with multiple nodes
- **Files**: New `eventplanedb_core/src/replication/replication_manager.rs`

### Phase 4: Degraded Mode (2 weeks)

#### Task 4.1: S3 Catch-Up Storage
- [ ] Implement batch upload to S3 catch-up location
- [ ] Add S3 batch download logic
- [ ] Implement batch cleanup after successful replication
- [ ] Add error handling and retry logic
- [ ] Integration tests
- **Files**: `eventplanedb_core/src/replication/catchup_storage.rs`

#### Task 4.2: Follower Catch-Up
- [ ] Implement catch-up detection (compare indexes)
- [ ] Add S3 batch fetching in background task
- [ ] Integrate with prepend_batches
- [ ] Add progress tracking
- [ ] Add metrics for catch-up operations
- [ ] Integration tests
- **Files**: `eventplanedb_core/src/replication/catchup.rs`

### Phase 5: Write Path Integration (2-3 weeks)

#### Task 5.1: Metadata Updates
- [ ] Add `node_id` field to `EventBatchMetadata`
- [ ] Add `lease_index` field to `EventBatchMetadata`
- [ ] Update `from_batch_item` to include new fields
- [ ] Add `node_id` and `lease_index` to `WriteOptions`
- [ ] Update serialization/deserialization
- [ ] Migration tests for backward compatibility
- **Files**: `eventplanedb_structures/src/event_batch_metadata.rs`, `write_operations/write_structures.rs`

#### Task 5.2: Write Coordinator
- [ ] Create `ReplicatedWriteCoordinator` struct
- [ ] Implement two-phase write protocol (replicate then local)
- [ ] Add degraded mode detection and S3 fallback
- [ ] Integrate with existing write operations
- [ ] Add rollback on partial failure
- [ ] Add comprehensive error handling
- [ ] Integration tests for all write scenarios
- **Files**: New `eventplanedb_core/src/replication/write_coordinator.rs`

#### Task 5.3: Process Request Updates
- [ ] Update `handle_write` to use `ReplicatedWriteCoordinator`
- [ ] Add leader validation before write
- [ ] Add client redirection for followers
- [ ] Update error responses with leader information
- [ ] Integration tests
- **Files**: `eventplanedb_core/src/process_request.rs`

### Phase 6: Configuration and Server Integration (1-2 weeks)

#### Task 6.1: Configuration
- [ ] Add `ReplicationConfig` struct to config.rs
- [ ] Add CLI arguments for replication options
- [ ] Add configuration validation
- [ ] Add configuration documentation
- [ ] Unit tests
- **Files**: `eventplanedb_server/src/config.rs`

#### Task 6.2: Server Startup Integration
- [ ] Initialize node identity on startup
- [ ] Start cluster membership heartbeat
- [ ] Start lease management background tasks
- [ ] Add health check endpoints for replication status
- [ ] Add monitoring metrics
- [ ] Integration tests
- **Files**: `eventplanedb_server/src/main.rs`

#### Task 6.3: Graceful Shutdown
- [ ] Implement lease release on shutdown
- [ ] Add cluster membership leave operation
- [ ] Ensure all replications complete before shutdown
- [ ] Integration tests
- **Files**: `eventplanedb_server/src/main.rs`

### Phase 7: Client Updates (1 week)

#### Task 7.1: Leader Redirection
- [ ] Add new error type for leader redirection
- [ ] Update client to handle redirection errors
- [ ] Add automatic retry to new leader
- [ ] Add connection pooling for multiple nodes
- [ ] Unit and integration tests
- **Files**: `eventplanedb_client/src/lib.rs`, `eventplanedb_structures/src/eventplanedb_error.rs`

### Phase 8: Testing and Documentation (2-3 weeks)

#### Task 8.1: Integration Tests
- [ ] Multi-node cluster startup/shutdown
- [ ] Leader election scenarios
- [ ] Failover testing (kill leader, validate new leader election)
- [ ] Follower failure and catch-up
- [ ] Degraded mode testing
- [ ] Split-brain prevention testing
- [ ] Clock skew scenarios