```markdown replication-design-final.md
# EventPlaneDB Replication Design: Per‑Aggregate Leader–Follower with S3 Control Plane

This document is the consolidated, final replication design for EventPlaneDB, based on prior drafts and refinements. It describes the architecture, control plane, data plane, failure modes, and implementation phases for adding high‑availability replication to EventPlaneDB.

---

## 1. Goals & Constraints

### 1.1 Goals

1. **High Availability (per aggregate)**  
   - Single **leader** and multiple **followers** per aggregate.  
   - Followers can take over leadership after failures.

2. **Strong Durability & Consistency for Writes**  
   - A client write is acknowledged only after:
     - The leader has persisted the batch locally **and fsync’d**, and  
     - The batch has been replicated to all healthy followers, or  
     - For any unreachable followers, the batch has been durably written to S3.

3. **High Performance on Happy Path**
   - Target: 300k+ writes/second, p99 latency <10 ms.
   - S3 is not in the hot path during healthy operation:
     - Writes are leader local disk + TCP replication to followers.
     - S3 is used primarily for control plane (leases, membership) and degraded modes.

4. **Simplicity Over Full Consensus**
   - No Raft / Paxos implementation.
   - Use **Amazon S3 (or compatible)** object storage with conditional writes as the control plane:
     - Leader election / leasing.
     - Membership discovery and configuration.
     - Catch‑up / degraded replication log.

5. **Cost Awareness**
   - Minimize S3 operations:
     - Per‑aggregate leases only renewed for **active** aggregates.
     - S3 used for replication only in degraded mode.
   - Ability to scale down leases for inactive aggregates (dormant mode).

6. **Operational Flexibility**
   - Single region per cluster (for S3 conditional write guarantees).
   - Support small clusters (including 2‑node clusters).
   - Allow rolling upgrades and controlled maintenance (proactive lease release).

### 1.2 Non‑Goals

- Multi‑region consensus or replication.
- Byzantine fault tolerance (assume nodes are correct and trusted).
- Strong guarantees under arbitrary network partitions; we rely on S3 as a single consistent authority in one region.

---

## 2. Architectural Overview

### 2.1 Topology

- **Per‑Aggregate Leadership**:
  - For each aggregate `A = (org_id, aggregate_type_id, aggregate_id)`:
    - Exactly one **leader** node is responsible for client writes.
    - Zero or more **followers** maintain a replicated copy.
  - A node may be leader for some aggregates and follower for others.

- **Roles**:
  - **Leader**:
    - Accepts client writes for its aggregates.
    - Assigns and maintains `event_batch_index` for the aggregate.
    - Replicates batches to followers via TCP.
    - Writes to S3 if followers are unreachable (degraded mode).
  - **Follower**:
    - Rejects direct client writes for that aggregate.
    - Accepts replication traffic from the current leader.
    - Applies batches in order, ensuring no gaps.
    - Can be promoted to leader by acquiring the lease.

### 2.2 Control Plane vs Data Plane

- **Control Plane (S3)**:
  - Per‑aggregate **lease files** for leader election and fencing.
  - Global **membership file** for cluster topology and addresses.
  - **Degraded‑mode replication log**: batches written to S3 when followers are unavailable.

- **Data Plane (Local + TCP)**:
  - Leader and followers store data locally on disk.
  - Replication over TCP using existing wire format extended with replication messages.
  - Writes and reads are driven by local on‑disk state plus in‑memory caches.

### 2.3 Node Identity

- Each node has a stable `node_id: u128`:
  - Generated once on first startup and persisted in the data root.
  - Used in:
    - Lease files.
    - Cluster membership.
    - Batch metadata.

- Node identity is not cryptographic; collisions are assumed practically impossible with proper generation.  

---

## 3. S3 Control Plane

### 3.1 Cluster Membership

**Object**: `s3://<bucket>/<cluster_root>/_cluster/members.json`

**Purpose**:
- Each node learns:
  - All node IDs and their network addresses.
  - Which nodes are currently active / eligible to lead.

**Structure (conceptual)**:
- A list of members, each with:
  - `id` (node_id: u128).
  - `address` (ip:port).
  - `is_active` flag (e.g., maintenance windows, decommissioning).
  - Optional metadata such as last heartbeat, version, checksum.

**Semantics**:
- Updates are done using conditional writes (version/etag) to avoid lost updates.
- Nodes periodically poll and cache the membership:
  - For replication target discovery.
  - To avoid replicating to inactive or decommissioned nodes.
- Membership is the base source of truth for who can become leader and who is eligible as follower.

### 3.2 Per‑Aggregate Lease Files

**Object**:  
` s3://<bucket>/<cluster_root>/<org_id>/<aggregate_type_id>/<aggregate_id>/lease.json`

**Fields (logical model)**:
- `lease_index`: `u64`  
  Monotonically increasing epoch / fencing token for this aggregate.  
- `node_id`: `u128`  
  The current leader’s node ID.  
- `lease_expiry`: `u64`  
  Unix time (millis) when the lease is considered expired.  
- `event_batch_index`: `u64`  
  The committed batch index at the time of lease acquisition/renewal.  
- `requested_by_client`: `u128` (optional)  
  ID of the client that triggered acquisition (for diagnostics).  

**Optional Extension (for clarity in degraded mode)**:
- `last_replicated_batch_index`: `u64`  
  The highest batch index that has been fully replicated (to followers or S3).

### 3.3 Lease Semantics

- Every write to the lease file is a **conditional write**:
  - If lease does not exist: condition = “object must not exist” or special etag.
  - If lease exists: condition = “etag / version equals last seen” and optionally “lease_index equals X”.
  - This ensures mutual exclusion and provides a fencing token.

- `lease_index`:
  - Incremented with every successful acquisition or renewal.
  - Used as a fencing token to prevent zombie leaders from writing under an old lease.

- `lease_expiry`:
  - Set to `now + lease_duration`.
  - The effective expiry considered by nodes is `lease_expiry - safety_margin` to account for clock skew and network delays.

### 3.4 Lease Acquisition (Becoming Leader)

Conditions under which a node attempts to become leader for an aggregate:

1. It receives a client write for that aggregate and:
   - There is no known valid leader locally, or
   - The known lease is expired or about to expire.

2. An operator / maintenance event suggests rebalancing (optional, future extension).

**Algorithm (per attempt)**:

1. Read lease object from S3:
   - If missing: treat as no lease (`lease_index = 0`, `lease_expiry = 0`).
2. Evaluate:
   - If lease exists and `now < lease_expiry - safety_margin`:
     - Consider the lease valid. Do **not** attempt to steal.
     - Node acts as follower; it should route writes to current leader.
   - If lease is expired or considered unsafe (now past effective expiry):
     - Node may attempt acquisition.
3. Construct candidate lease:
   - `lease_index = old_lease_index + 1` (or `1` if no lease).
   - `node_id = local node_id`.
   - `lease_expiry = now + lease_duration`.
   - `event_batch_index = local highest committed batch index`.
   - `requested_by_client = client_id` (if applicable).
4. Perform S3 conditional write:
   - Condition based on last read ETag/version and `lease_index`.
5. If conditional write succeeds:
   - Node becomes leader with fence token (`lease_index`).
   - Node must ensure it catches up to at least the recorded `event_batch_index` if it is behind.
6. If conditional write fails:
   - Another node is leader; caller becomes follower and rejects direct write requests.

### 3.5 Lease Renewal (Staying Leader)

- Only leaders that are actively receiving writes for the aggregate renew their leases.
- Renewal is similar to acquisition, but with stricter invariants:
  - Node must be the current recorded `node_id` in the lease.
  - New `lease_index = old_lease_index + 1`.
  - `lease_expiry` extended.

- Trigger:
  - Periodically when `now >= lease_expiry - renewal_margin`.
- On renewal failure (conditional write fails):
  - Node assumes it lost leadership (maybe preempted).
  - It stops accepting writes for this aggregate and starts returning “not leader” errors.

### 3.6 Lease Release (Graceful Shutdown / Maintenance)

- When a leader is being shutdown intentionally:
  - It proactively releases leases for aggregates it leads by:
    - Setting `lease_expiry = now`, or
    - Writing a special “no leader” lease record via conditional write (maintaining fencing semantics).
- This enables fast re‑election without waiting for full lease timeout.

---

## 4. Data Model Extensions

### 4.1 Batch & Metadata Fields

To track provenance and epoch:

- **EventBatchMetadata**:
  - `node_id` (`u128`): ID of the node that produced the batch.
  - `lease_index` (`u64`): Lease epoch at the time the batch was written.

These fields:
- Enable validation and debugging of cross‑epoch behavior.
- Help detect and reject stale or invalid writes.
- Allow future reconciliation tooling to understand conflicting histories.

Event payload structures and indexes (`event_batch_index`, `event_index`, etc.) remain as in the existing design.

---

## 5. Write & Replication Path

### 5.1 Happy Path Write (All Followers Healthy)

**Participants**: Client → Node (leader) → Followers

1. **Client → Node**: Client sends a write to any node.
2. **Leader Determination**:
   - Node checks if it is the leader for this aggregate:
     - If not leader:
       - It returns `NotLeader` error including leader hint (node_id/address) from lease/membership.
       - Client retries against leader.
     - If leader:
       - It validates that the current lease is still valid (time and lease_index).
       - If lease is close to expiry, may trigger a renewal asynchronously.
3. **Leader Local Append**:
   - Leader:
     - Accepts the request, determining `event_batch_index` and event indices.
     - Builds an event batch and appends it to its local WAL / event files.
     - The batch is staged in append buffers ready for durable sync.
4. **Replication to Followers (TCP)**:
   - Leader sends a replication message (batches) to each follower that:
     - Is marked `is_active` in membership, and
     - Is configured as a follower for this aggregate.
   - Followers:
     - Validate continuity of `event_batch_index`.
     - Validate `lease_index` is not older than what they already have for the aggregate.
     - Append and fsync (or follow their configured durability policy).
     - Acknowledge success to the leader.
5. **Leader Durability Commit**:
   - Leader performs a local `fdatasync`/`fsync` for the new batches if not already done.
   - Once:
     - Local fsync succeeded, and  
     - All followers have acknowledged,
   - The leader considers the write fully replicated.
6. **Client Acknowledgement**:
   - Leader responds with a successful write result including new `next_event_batch_index`.

### 5.2 Degraded Mode Write (Followers Unavailable)

If any follower cannot be brought up to date during replication:

1. **Detection**:
   - Follower times out, cannot be connected, or reports missing batches that the leader cannot supply from local disk.
2. **S3 Replication Log**:
   - For each failing follower:
     - Leader writes the batch data for this aggregate to S3:
       - Object name pattern:  
         `.../<org_id>/<type_id>/<agg_id>/batches/<event_batch_index>.bin`  
       - Content: serialized, compressed batch + metadata sufficient for replay.
   - Leader may combine multiple batches per object to amortize S3 overhead, as long as ordering and indexing are preserved.
3. **Durability Condition**:
   - Write is considered durable when:
     - Leader has fsync’d locally, and
     - Either:
       - All followers have acknowledged; or
       - All failed followers have their missing batch(es) safely persisted in S3.
4. **Client Ack**:
   - Once durability conditions are satisfied, leader returns success to the client.

The aggregate is now in **degraded mode** regarding any follower that relies on S3 for catch‑up.

### 5.3 Failure Within the Commit Window (Rollback)

If, after appending locally, the leader fails to:

- Successfully replicate to all followers **and**
- Successfully write necessary batches to S3, or
- Successfully fsync locally,

then:

1. The leader **must roll back** the speculative local write:
   - Truncate or revert the local files to the last consistent state.
   - Restore in‑memory indices and client event index maps accordingly.
2. The leader returns an error to the client indicating the write failed and no data was committed.

This guarantees:
- No client‑acknowledged write can fail to be durable according to the configured guarantees.
- Local state does not diverge silently from replicated state in unrecoverable ways.

---

## 6. Follower Behavior & Catch‑Up

### 6.1 Follower Write Handling

- Followers do **not** accept client writes for aggregates where they are not the leader.
- On client write requests:
  - Respond with `NotLeader` including leader info when available.

### 6.2 Replication Reception

Upon receiving replication messages from the leader:

1. Validate:
   - The incoming `lease_index` is >= follower’s last seen lease index for that aggregate.
   - The `event_batch_index` sequence is contiguous with follower’s existing batches.
2. If valid:
   - Append batches to local storage.
   - Fsync as per configured policy.
   - Track highest committed `event_batch_index` and `lease_index`.
   - Return success result to leader.
3. If gaps or conflicts:
   - Return an error specifying the missing range (e.g., `missing_from` index).
   - Leader may attempt to backfill missing batches from its local data; if not possible, follower must catch up from S3.

### 6.3 Follower Catch‑Up from Leader

- When a follower rejoins after downtime or detects it is behind:
  - It queries the leader for batches from its `local_max_batch_index + 1`.
  - Leader:
    - Streams batches (subject to message size limits) until follower catches up or hits trimmed history.
  - If leader no longer has historical batches locally:
    - Leader (or follower) directs catch‑up to S3.

### 6.4 Catch‑Up from S3

Scenarios:

- Follower was down during degraded mode and needs missing batches.
- New leader must close the gap between its local state and the lease’s `event_batch_index`.
- Follower or leader lacks local historical data (due to trimming or data loss).

Process:

1. Node reads lease file:
   - Determines expected committed `event_batch_index` (and optionally `last_replicated_batch_index`).
2. Compare local `max_event_batch_index` with target index from lease:
   - If local < target:
     - Construct a fetch plan for the missing ranges from S3 replication log.
3. Download and apply batches from S3 in order, validating:
   - Checksums (CRC).
   - Correct order (`event_batch_index`).
   - Consistency with `lease_index` and `node_id` (for debugging/fencing).

Node does not accept new writes (as leader) until catch‑up is complete and state is consistent with lease.

---

## 7. Leadership Safety & Time Handling

### 7.1 Clock Skew Management

To prevent split‑brain due to skew:

- Nodes must run with NTP or an equivalent time synchronization mechanism.
- On startup:
  - Node may compare its system time with an authoritative reference (e.g., S3 or another reliable source).
  - If clock drift exceeds a configured maximum:
    - Node refuses to join the cluster or act as leader.
- Runtime:
  - Node maintains an estimate of its max possible clock drift.
  - Lease expiry checks always use an **effective expiry** = `lease_expiry - safety_margin`, where:
    - `safety_margin >= max_drift + network_latency_budget`.

### 7.2 Fencing with `lease_index`

- Every batch is associated with a `lease_index`.
- When a new leader takes over:
  - It uses a higher `lease_index`.
  - Any attempts by a previous (now zombie) leader to write under an outdated `lease_index` are rejected or ignored.
- Followers and new leaders:
  - Track the max `lease_index` they have observed for each aggregate.
  - Reject replication streams with stale `lease_index`.

### 7.3 Handling Network Partitions

In the presence of partitions:

- A node that cannot reach S3 cannot renew its lease:
  - Its lease will eventually expire or be preempted by another node that can contact S3.
  - It must stop accepting writes once it detects lease renewal failure.
- A partitioned node might believe its lease is still valid if it cannot see updates:
  - Fencing via `lease_index` and conditional writes ensures that it cannot gain a new valid lease or write batches that will be accepted as authoritative.

---

## 8. Error Semantics & Client Behavior

### 8.1 New Error Types

The server introduces structured errors for:

- **NotLeader**:
  - Indicates this node is not leader for the aggregate.
  - Optionally includes:
    - `leader_node_id`
    - `leader_address`
- **LeaseAcquisitionFailed**:
  - Node could not become leader (or renew leadership) due to control plane contention or errors.
- **ReplicationFailed**:
  - Persistent inability to replicate to followers or S3 (beyond simple retry windows).

### 8.2 Client‑Side Handling

- On `NotLeader`:
  - Client reads leader address if present and retries the same request to that node.
  - If no leader hint is available, client may:
    - Retry same node with backoff, or
    - Consult an out‑of‑band service or configuration to discover current leader.

- Clients do not need to be aware of leases, replication, or S3; they only need to handle `NotLeader` and other domain errors as usual.

---

## 9. Risk Evaluation

The table below summarizes key risks, severity, and mitigation mechanisms built into the design.

| Risk | Scenario | Impact | Mitigations |
|------|----------|--------|-------------|
| Split Brain (Clock Skew) | Two nodes believe a lease is valid due to skew, both accept writes. | Critical (data divergence) | Mandatory NTP; startup clock skew checks; `lease_expiry` safety margin; `lease_index` fencing; rejecting stale epochs on followers. |
| Zombie Leader | Former leader continues writing after losing lease. | High | S3 conditional writes prevent renewing lease; `lease_index` fencing ensures their writes are not accepted; their replication attempts are rejected as stale. |
| S3 Control Plane Outage | Cannot acquire/renew leases or write catch‑up batches. | Medium–High | Existing leaders continue serving writes until lease expiry; explicit timeouts and backoff; in worst case, cluster may transition to read‑only; operational procedures for S3 outages. |
| Follower Unavailability | One or more followers are down or slow. | Medium | Degraded mode: leader writes to S3 instead of follower; background catch‑up; timeouts preventing long delays; membership flags to mark nodes inactive. |
| Leader Crash Between Local fsync and Replication | Leader fsyncs locally but crashes before replication or S3 persistence; if client was already acked, data loss. | High | Protocol forbids ack before both local fsync and successful replication (followers or S3). If crash occurs before ack, client sees failure and can retry. |
| Stale Follower Becoming Leader | Follower behind in batches acquires lease and starts taking writes. | High | Lease contains `event_batch_index`; on acquisition, node must compare and catch up from S3/peers before serving; if catch‑up fails, it refuses leadership and returns unavailability. |
| Misconfigured Conditional Writes | Lease file updated concurrently by two nodes due to missing/incorrect conditions. | High | All lease operations implemented via a dedicated control plane module with thorough tests; observability/alerts on any invariant violations. |
| Performance Regression via Extra S3 Ops | Excessive S3 usage in hot path; higher latency; lower throughput. | Medium | S3 avoided on happy path; leases renewed infrequently and only on active aggregates; S3 replication only in degraded mode; instrumentation and tuning of lease durations and batch sizes. |
| Membership File Corruption | Membership file invalid or inconsistent. | High | Versioning and checksums; local caching of last‑known‑good; operational tooling for recovery; nodes can operate with stale membership for a time while leadership is governed by leases. |
| Node ID Collision | Two nodes share a node_id by accident. | High | Use secure random ID generation with extremely low collision probability; optional run‑time check against membership; on collision, node refuses to start or join. |

---

## 10. Implementation Phases (High‑Level)

The design will be implemented in incremental phases. This section is intentionally high‑level (no code), to guide planning and sequencing.

### Phase 1: Foundations & Metadata

1. **Node Identity**
   - Generate and persist `node_id: u128` in data root on first startup.
   - Load and expose `node_id` to core components.

2. **Metadata Extensions**
   - Extend event batch metadata to include `node_id` and `lease_index`.
   - Ensure serialization remains backward compatible (versioned wire format).

3. **Configuration Extensions**
   - Add replication and S3‑related configuration options:
     - S3 bucket/region/root prefix.
     - Lease durations and safety margins.
     - Replication and S3 timeouts.

### Phase 2: S3 Control Plane Services

1. **S3 Client Wrapper**
   - Async wrapper around S3 with explicit support for:
     - Conditional reads/writes.
     - Timeouts and retries.
   - Abstract all direct S3 interactions.

2. **Cluster Membership Manager**
   - Periodically fetch and cache `members.json`.
   - Provide APIs to resolve node_id → address and list active followers.

3. **Lease Manager**
   - Per‑aggregate lease state and in‑memory caching.
   - APIs:
     - Acquire lease.
     - Renew lease.
     - Check/refresh lease status for read/write paths.
   - Enforce catch‑up requirement on successful acquisition.

### Phase 3: Replication Protocol

1. **Wire Protocol Messages**
   - Define new replication messages (leader → follower; follower responses).
   - Ensure compatibility with existing framing and compression.

2. **Replication Server Handler**
   - For each node:
     - Accept replication requests on the existing TCP listener or a dedicated port.
     - Validate lease and ordering.
     - Append and fsync local data.
     - Provide acknowledgements and missing range indications.

3. **Replication Client**
   - Leader‑side module for:
     - Sending replication batches to all followers in parallel.
     - Gathering responses and interpreting errors (including gaps).

### Phase 4: Write Pipeline Integration

1. **Leader Check on Write**
   - Before enqueueing new events:
     - Validate leadership via Lease Manager.
     - If not leader, return `NotLeader`.

2. **Two‑Phase Commit Semantics**
   - Enforce ordering of operations:
     - Append locally.
     - Replicate to followers.
     - Fsync locally.
     - Use S3 replication log for any failing followers.
     - Only then ack client.

3. **Rollback Mechanism**
   - Ensure the ability to revert local append state when replication or S3 steps fail and no commit has been made.

### Phase 5: Follower Catch‑Up & Degraded Mode

1. **Follower Catch‑Up via Leader**
   - Follower requests missing batches from leader when it detects gaps.

2. **S3‑Based Catch‑Up**
   - When leader or follower lacks full history:
     - Use S3 replication logs to retrieve missing batches.
   - Ensure data integrity checks (CRC, ordering, lease and node IDs).

3. **Degraded Mode Management**
   - Track which followers are currently served via S3.
   - Introduce hysteresis (don’t immediately switch back from S3 mode on a single successful replication).

### Phase 6: Client‑Side Enhancements & Observability

1. **Client Behavior**
   - Handle `NotLeader` errors with retries to suggested leader.
   - Optionally allow configurable retry limits and backoff strategies.

2. **Metrics and Logging**
   - Capture metrics for:
     - Lease acquisitions/renewals.
     - Replication latency and failure rates.
     - S3 control plane and data plane operations.
   - Rich logging for debugging leader transitions and catch‑up flows.

3. **Operational Tools**
   - Commands or APIs for:
     - Inspecting leases per aggregate.
     - Forcing leadership transfer or revocation.
     - Monitoring clock skew and membership health.

---

This final design centers S3 as a minimal, strongly consistent control plane for per‑aggregate leadership and degraded durability, while preserving EventPlaneDB’s high performance in the common case via local disk and TCP replication.