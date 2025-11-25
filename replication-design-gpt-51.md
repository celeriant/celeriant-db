```md replication-design-2.md
# EventPlaneDB Replication Design (Leader + Followers with S3 Control Plane)

## Goals

- Provide **per-aggregate** replication with a **single leader and multiple followers**.
- Maintain high write throughput (300k+ writes/sec) and low latency (<10 ms) on the **happy path**.
- Avoid implementing a full distributed consensus protocol (e.g. Raft).
- Delegate leader election and catch‑up logic to **S3-based control plane** using **conditional writes** and **object storage**.
- Ensure followers eventually catch up and are able to take over as leaders if the current leader fails.
- Do not acknowledge a write back to clients until:
  - The leader has **fsync’d** locally, and
  - The batch has been **replicated to all followers** (or, in degraded mode, flushed to S3 in a way that any future leader can recover it).

---

## High-Level Design

### 1. Per-Aggregate Leadership

- Each aggregate has **exactly one leader node** at any time, plus zero or more followers.
- Leadership is governed by a **lease file** stored in S3, using **conditional writes** for optimistic concurrency.
- Only the leader accepts client writes for that aggregate. Followers reject client writes and return the current leader reference.

### 2. S3 as Control Plane

S3 is used for:

1. **Lease management** (per-aggregate):
   - A single **lease object** in S3 per aggregate, with:
     - `lease_index: u64` (monotonic, per-aggregate)
     - `node_id: u128` (current leader)
     - `lease_expiry: u64` (millis since epoch)
     - `event_batch_index: u64` (leader’s current committed batch index)
     - `requested_by_client: u128` (client that triggered the lease attempt, for logging/diagnostic only)

2. **Cluster membership** (global):
   - A **membership object** describing all nodes:
     ```json
     {
       "members": [
         {
           "id": "u128",
           "address": "string ip:port",
           "is_active": "bool"
         }
       ]
     }
     ```

3. **Degraded mode persistence**:
   - When a leader cannot replicate to all followers, it must **persist new batches to S3** for that aggregate.
   - Future leaders **replay missing batches from S3** to catch up.

### 3. Replication Model

- Writes are sent by clients to **any node**.
- The node:
  - If it is the **leader** for that aggregate, it proceeds.
  - If it is a **follower**, it rejects the write and responds with an error that includes the current leader (or hints on where to find it).
- The leader’s write flow:
  1. Append batch to local WAL/files.
  2. **fdatasync/fsync** local files.
  3. Replicate the newly committed **event batch(es)** to all followers via TCP.
  4. On success from all followers:
     - Ack success to client.
  5. If one or more followers are unavailable:
     - Enter **degraded mode**:
       - Persist the batch(es) to S3.
       - Ack success to client **only after** S3 write completes successfully.
       - Mark in local metadata that this aggregate is in degraded mode until followers catch up.

### 4. Lease Management

- To become leader for an aggregate:
  - A node attempts a **conditional write** to the lease object in S3.
  - It:
    - Increments `lease_index`.
    - Sets its own `node_id`.
    - Sets `lease_expiry` to `now + lease_duration - safety_margin`.
    - Sets `event_batch_index` to the current local committed index.
    - Uses the current ETag / version / lease_index as the condition to avoid conflicts.
- Other nodes only attempt leadership when:
  - The lease is **near expiry** or **expired**, and
  - They have checked their **clock drift is within a safe bound** relative to peers or some trusted reference.
- The **current leader**:
  - Proactively **renews** the lease before expiry, but only while it continues to receive writes.
  - If idle (no writes) for some configurable interval, it **stops renewing**, allowing the lease to expire.

### 5. Follower Behavior

- Followers:
  - Refuse client writes for the aggregate; respond with:
    - Error code indicating “not leader”.
    - Optionally, the `node_id` / address of the current leader (from S3 membership & lease).
  - Accept **replication RPCs** from the leader:
    - Apply batches in order, verifying continuity of `event_batch_index`.
    - If they detect gaps, they request missing batches from the leader.
    - If the gap is too large or the leader cannot supply, they (or the leader) fallback to **S3 catch‑up**.

### 6. Node Identity

- Each node has a **stable `node_id: u128`** stored in its local data directory, created on first startup:
  - E.g. `node_id.bin` containing a randomly generated 16‑byte UUID-like value.
- This `node_id` is:
  - Reported in **cluster membership**.
  - Used in **lease objects**.
  - Written into **event batch metadata** so that future leaders can reason about which node produced which batch under which lease.

### 7. Event Batch Metadata Extensions

Each event batch (and its metadata) must include:

- `node_id: u128` — node that produced the batch.
- `lease_index: u64` — the leadership epoch under which this batch was written.

These fields ensure:

- Future leaders can detect **stale or conflicting batches**.
- We can correlate persisted S3 batches with a specific leader epoch.

---

## Detailed Design

### 1. S3 Lease Object Format

**Key**: `<cluster_root>/<org_id>/<aggregate_type_id>/<aggregate_id>/lease.json`

**Schema** (logical, stored as JSON or MessagePack in S3):

```json
{
  "lease_index": 1,
  "node_id": "00000000-0000-0000-0000-000000000000", 
  "lease_expiry": 1730000000000,
  "event_batch_index": 42,
  "requested_by_client": "11111111-1111-1111-1111-111111111111"
}
```

Backend requirements:

- All writes to this object:
  - Must be **conditional** (e.g. conditional on ETag/version, or `lease_index`).
  - This ensures only one node successfully acquires/renews leadership at a time.

Lease semantics:

- `lease_index` is **monotonic** for an aggregate.
- `lease_expiry` is based on the node’s current clock, minus a **safety margin** to account for network and clock skew.
- If `now >= lease_expiry - safety_margin`, the node should treat the lease as at risk of being considered expired and should renew proactively.

### 2. Global Membership Object

**Key**: `<cluster_root>/membership.json`

```json
{
  "members": [
    {
      "id": "u128-as-base64-or-hex",
      "address": "127.0.0.1:10000",
      "is_active": true
    }
  ]
}
```

- This is updated by an **operator** or by a cluster management tool (out of scope for this design).
- Nodes periodically read it to:
  - Determine **follower set** for replication.
  - Determine if they are marked as `is_active = false` (maintenance) and if so:
    - Stop competing for leases.
    - Optionally release leases proactively.

### 3. Leader Election Algorithm (Per Aggregate)

**Inputs**:

- `aggregate_key = (org_id, aggregate_type_id, aggregate_id)`.
- `node_id` (local).
- `requested_by_client` (if triggered by a client write).
- Current time `now`, cluster time drift knowledge (see 4).

**Process**:

1. Read the current lease from S3:
   - If none exists, treat as:
     - `lease_index = 0`, `lease_expiry = 0`.
2. Determine if the lease is **expired**:
   - `expired = now > lease_expiry + lease_safety_margin`.
3. If not expired:
   - Do **not** attempt to steal the lease unless:
     - The holder is `is_active = false` in membership, **and**
     - A configurable timeout has passed (to avoid false positives).
4. If **expired** or we are allowed to preempt:
   - Attempt a **conditional write**:
     - New `lease_index = old.lease_index + 1`.
     - `node_id = self.node_id`.
     - `lease_expiry = now + lease_duration - lease_safety_margin`.
     - `event_batch_index = local committed highest batch index`.
     - `requested_by_client = client_id`.
   - Condition:
     - If no lease: “object must not exist” or `lease_index == 0`.
     - If lease exists: `lease_index == old.lease_index` AND version/ETag matches.
5. If conditional write succeeds:
   - We are leader for `lease_duration` from `now`.
   - Begin accepting **write requests** for this aggregate.
6. If conditional write fails:
   - Another node is (or became) leader.
   - We become follower and must not accept writes.

**Renewal**:

- While actively receiving writes for an aggregate:
  - Periodically (e.g. every `lease_duration/3`), attempt a **lease renewal**:
    - Same `node_id`, `lease_index = old_lease_index + 1`.
    - Set new `lease_expiry = now + lease_duration - lease_safety_margin`.
    - Conditionally write based on previous `lease_index` and version.
  - If renewal fails:
    - Stop accepting new writes (graceful draining).
    - Communicate to clients they must retry to the new leader.

### 4. Time & Clock Skew Considerations

- Nodes periodically check **time drift**:
  - Either via:
    - NTP-synced OS time, or
    - A shared time source (e.g. a simple HTTP time service) across nodes.
- Each node maintains an estimate of **max_drift_ms**.
- Safety margins:
  - `lease_safety_margin >= max_drift_ms + network_latency_budget_ms`.
  - When checking expiry, treat `lease_expiry` as:
    - `effective_expiry = lease_expiry - lease_safety_margin`.
- This minimizes the risk of **dual leaders** due to skew.

### 5. Replication Protocol (Leader → Followers)

Transport:

- TCP over existing Glommio networking stack.
- Define a **replication port** or reuse existing one with **separate message types**.

Wire format:

- Reuse existing `EventBatchItem` + `EventBatchMetadata` structures, extended with:
  - `node_id: u128` (writer node).
  - `lease_index: u64` (epoch).
- Define new request/response types for replication, e.g.:

  - `ReplicationRequest::AppendBatches { aggregate_key, from_event_batch_index, batches }`
  - `ReplicationResponse::AppendBatchesResult { highest_applied_batch_index, missing_from }`

Happy path:

1. Leader accepts a client write, creates a batch (or more) and writes it locally.
2. Leader then synchronously (per write) sends `AppendBatches` to all followers for that aggregate:
   - Batches must be **contiguous in `event_batch_index`**.
3. Followers:
   - Verify:
     - `lease_index` is **monotonic** and not behind what they already have for that aggregate.
     - `event_batch_index` is **exactly previous_index + 1`**.
   - If contiguous → append, fsync (depending on durability policy), and respond success.
   - If gaps are detected:
     - Return `missing_from = current_max_index + 1`.
4. Leader:
   - If a follower reports `missing_from`:
     - Attempts to send the required historical batches (from local disk or memory) in subsequent `AppendBatches` calls.
     - If older batches are no longer available locally (due to trim or some error):
       - Mark that follower as needing **catch‑up from S3** (see below).

Degraded mode:

- If **any follower** cannot be brought up-to-date (e.g. offline or far behind):
  - For each new batch, leader:
    1. Writes and fsyncs locally.
    2. Tries replication to available followers.
    3. If one or more cannot be reached or updated:
       - Writes the new batch(es) to S3 as **replication log**.
       - Only then ack success to the client.
- The S3 replication log format can be:
  - One object per batch:
    - `.../replication/<event_batch_index>.bin` (containing compressed `EventBatchItem` plus metadata), or
  - Grouped objects (e.g. ranges of batch indices) to amortize costs.

### 6. S3-Based Catch-Up for Followers & New Leaders

When a node becomes leader or follower and detects gaps:

1. Read the lease object:
   - `event_batch_index` there is the **committed index** at last lease write.
2. Compare local highest `event_batch_index` to `lease.event_batch_index`.
3. If local index < lease index:
   - Attempt to **fetch missing batches from other nodes** (direct replication).
   - If not possible or lag is too large:
     - Fetch missing batches from S3 replication storage for that aggregate.
4. Apply fetched batches locally in order, verifying:
   - CRC, node_id, lease_index ordering.

When a follower re-joins or recovers:

- It:
  - Connects to current leader for each aggregate it hosts (or subscribes to).
  - Asks for batches from its `local_max_batch_index + 1`.
  - Leader:
    - Sends from local disk if available; else from S3.
    - Once follower catches up, it resumes happy-path replication.

### 7. Proactive Lease Release on Shutdown

On node shutdown:

1. Stop accepting new writes.
2. Complete in-flight replication and fsyncs.
3. For each aggregate where this node is leader:
   - Optionally:
     - Write a **lease release** or set `lease_expiry = now` via conditional update if we still own the lease.
4. This allows other nodes to compete quickly without waiting for the full lease duration.

### 8. Local Data Model Changes

Extend `EventBatchMetadata`:

- Add:
  - `pub node_id: u128`
  - `pub lease_index: u64`

Extend `EventBatchItem` if needed (for wire semantics only), or rely fully on metadata.

Write path:

- In `WriteOperationsWithDmaFile::queue_events_in_memory` (and `prepend_batches`):
  - Fill `node_id` from the local node id.
  - Fill `lease_index` from the current in‑memory lease state for that aggregate.

Read path:

- No semantic change; consumers usually don’t need to use node_id or lease_index, but they can be exposed in APIs if useful.

---

## Risk Evaluation

### Risk 1: Dual Leaders Due to Clock Skew

- **Scenario**: Node A thinks its lease expired and stops; Node B sees an expired lease (due to skew) and acquires leadership, while A still believes it has a valid lease and continues writing.
- **Likelihood**: Medium (depends on NTP / clock discipline).
- **Consequence**: High — split-brain, diverging histories for the same aggregate.
- **Mitigation**:
  - Enforce **tight NTP synchronization** and monitor drift.
  - Use **safety margins** in lease expiry comparisons:
    - `effective_expiry = lease_expiry - lease_safety_margin`.
  - Require node to **self-disable** leadership if its local clock drift estimator exceeds a threshold.
  - Consider storing and checking `lease_index` and `node_id` in batch metadata and on catch‑up to detect conflicting epochs and fail fast.

### Risk 2: S3 Outage or Increased Latency

- **Scenario**: S3 control plane is degraded or down.
- **Likelihood**: Low to Medium (regional S3 is highly available, but outages do occur).
- **Consequence**: Medium to High — leader election and degraded-mode replication may stall, writes might be rejected or delayed.
- **Mitigation**:
  - Prefer in-memory and local disk caches for **ongoing leadership state**, avoiding constant S3 reads in the hot path.
  - Only use S3 for:
    - Lease acquisition/renewal at relatively **low frequency**.
    - Degraded-mode replication when needed.
  - Implement **timeouts** and clear error paths:
    - If S3 write for lease/replication fails, reject client writes with an explicit error.
  - Operationally:
    - Run everything in a **single S3 region** with strict monitoring and alerting.

### Risk 3: Data Loss Due to Partial Local Write + Failure Before Replication/S3

- **Scenario**: Leader writes and fsyncs locally, but crashes before replicating to followers and S3. A new leader takes over and cannot see that batch.
- **Likelihood**: Medium.
- **Consequence**: High — acknowledged data loss if the old leader had already acked to client.
- **Mitigation**:
  - **Do not acknowledge** to client until:
    - Local fsync is done, **and**
    - EITHER:
      - All followers have applied and fsync’d, OR
      - We have successfully stored the batch to S3 as replication log.
  - Ensure the ack happens **after** all durability conditions for the configured consistency mode.

### Risk 4: Stale Followers Becoming Leaders Without Catch-Up

- **Scenario**: A follower that missed some batches gets elected leader, but does not catch up from S3 or other nodes first.
- **Likelihood**: Medium without proper guards; Low with correct implementation.
- **Consequence**: High — missing data for that aggregate.
- **Mitigation**:
  - On leadership acquisition:
    - Compare local highest `event_batch_index` with `lease.event_batch_index`.
    - If lower, **must** catch up from S3/peers before serving writes.
  - If catch‑up fails:
    - Refuse leadership or treat aggregate as **temporarily unavailable**.

### Risk 5: S3 Conditional Writes Misconfigured

- **Scenario**: Conditional writes not correctly configured, allowing multiple nodes to overwrite the lease simultaneously.
- **Likelihood**: Medium if implementation is sloppy.
- **Consequence**: High — multiple leaders.
- **Mitigation**:
  - Abstract S3 conditional write logic in a **well-tested library module**.
  - Include **unit and integration tests** that simulate concurrent lease acquisition.
  - Fail hard (panic in early development/beta) if observed invariants are violated.

### Risk 6: Performance Regressions

- **Scenario**: Extra S3 operations or replication overhead reduce throughput or increase latency beyond design targets.
- **Likelihood**: Medium.
- **Consequence**: Medium — system is safe but underperforms.
- **Mitigation**:
  - Keep the **happy path** purely local + follower replication with:
    - Batching of replication requests.
    - Asynchronous replication pipelining where safe.
  - Only rely on S3 in **exceptional/degraded** cases (e.g. follower offline).
  - Introduce metrics and profiling to tune:
    - Lease duration.
    - Replication batch sizes.
    - Write-ahead and async fsync behavior.

---

## Task Breakdown for Implementation

Below is a pragmatic, code-oriented task list that maps to your existing codebase.

### 1. Core Structures & Metadata Changes

1.1 Add `node_id` and `lease_index` to `EventBatchMetadata`:

```rust eventplanedb_structures/src/event_batch_metadata.rs
#[derive(Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize)]
pub struct EventBatchMetadata {
    // ... existing fields ...

    /// ID of node that produced this batch
    #[serde(with = "serde_u128_base64", rename = "nd")]
    pub node_id: u128,

    /// Lease index (epoch) during which this batch was written
    #[serde(rename = "li")]
    pub lease_index: u64,
}
```

- Update `Default` impl and constructor (`from_batch_item`) to take these additional fields.
- Update all call sites (`WriteOperationsWithDmaFile::queue_events_in_memory`, `prepend_batches`) to supply `node_id` and `lease_index`.

1.2 Create a new `NodeIdentity` helper:

- New module: `eventplanedb_core/src/node_identity.rs`:
  - On startup:
    - Look for `data_root/node_id.bin`.
    - If present, read 16 bytes into `u128`.
    - If absent, generate a random `u128` and persist it.
  - Provide `fn node_id(&self) -> u128`.

- Wire it into server startup (`eventplanedb_server/src/main.rs`) and pass `node_id` into `ProcessRequest` / `AggregateCache` so writers can use it.

### 2. Lease Management Module (S3 Control Plane)

2.1 Introduce a new crate or module, e.g. `eventplanedb_control_plane`:

- Provide:
  - `struct Lease { lease_index: u64, node_id: u128, lease_expiry: u64, event_batch_index: u64, requested_by_client: u128 }`.
  - `async fn get_lease(aggregate_key: &AggregateKey) -> Result<Option<Lease>, ControlPlaneError>`.
  - `async fn try_acquire_or_renew_lease(aggregate_key, current: Option<&Lease>, new_lease: &Lease) -> Result<bool, ControlPlaneError>` (returns true on success).
- Implement using S3 SDK with conditional writes (ETag / version ID / `lease_index` in object).

2.2 Add a `ClusterMembership` abstraction:

- `struct ClusterMembership { members: Vec<Member> }`
- `struct Member { id: u128, address: String, is_active: bool }`
- Provide:
  - `async fn fetch_membership() -> Result<ClusterMembership, ControlPlaneError>`.
  - Local caching + TTL to avoid frequent S3 trips.

### 3. Aggregate Leadership State

3.1 Extend `AggregateResources` / `AggregateCache`:

- Add per-aggregate leadership state:
  - `current_lease: Option<Lease>`
  - `is_leader: bool` (or a small state machine: Leader, Follower, Unknown).
- Provide APIs:
  - `async fn ensure_leader(&self, client_id: u128) -> Result<(), EventPlaneDBError>`
    - Reads/refreshes lease from control plane.
    - If not leader or lease about to expire, tries to acquire/renew.
    - On failure, returns a specific error `NotLeader { leader_id, leader_address }`.

3.2 Integrate into `ProcessRequest::handle_write` and `handle_write_batches`:

- Before doing `get_writer_mut` and writing:
  - Call `ensure_leader`.
  - If error `NotLeader`, map to a new `EventPlaneDBError::NotLeader { leader_id, leader_address }`.

### 4. Replication RPCs

4.1 Extend wire protocol (`request.rs` / `response.rs`):

- Add new request types:
  - `RequestType::ReplicateBatches = 10` (for example).
- Define structs:

```rust eventplanedb_structures/src/request.rs
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ReplicateBatchesRequest {
    pub correlation_id: Option<u128>,
    pub org_id: u128,
    pub aggregate_type_id: u128,
    pub aggregate_id: u128,
    pub from_event_batch_index: u64,
    pub batches: Vec<EventBatchItem>, // include metadata via extended structure if needed
}
```

```rust eventplanedb_structures/src/response.rs
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ReplicateBatchesResponse {
    pub correlation_id: Option<u128>,
    pub error: Option<EventPlaneDBError>,
    pub highest_applied_batch_index: u64,
    pub missing_from: Option<u64>,
}
```

- Add them to:
  - `Request` / `Response` enums.
  - `RequestType` / `ResponseType` enums.
  - `read_request` / `write_request` and `read_response` / `write_response`.

4.2 Implement server-side handling in `ProcessRequest`:

- Add a new branch in `ProcessRequest::process`:

```rust eventplanedb_core/src/process_request.rs
Request::ReplicateBatches(req) => {
    // new handler, similar to WriteBatches but only allowed from a leader
}
```

- Implement `handle_replicate_batches`:
  - Validate:
    - `batches` are contiguous in `event_batch_index`.
  - Write them using `WriteOperationsWithDmaFile::prepend_batches` or a new `append_replicated_batches` helper that:
    - Does NOT re-assign indices.
    - Uses provided metadata (node_id, lease_index, etc.).
  - Return the `highest_applied_batch_index` and any `missing_from`.

4.3 Implement a **glommio-based replication client**:

- New module under `eventplanedb_server` or a small crate:
  - Reuse wire format + `write_request`/`read_response`.
  - Provide:
    - `async fn replicate_to_follower(follower_addr, ReplicateBatchesRequest) -> Result<ReplicateBatchesResponse, WireError>`.

### 5. Write Path Integration (Leader)

5.1 Modify `ProcessRequest::handle_write`:

- Steps (per write):
  1. Ensure we are leader (`ensure_leader`).
  2. Call `queue_events_in_memory`.
  3. Call `sync_with_delay` or immediate `sync_with_delay(Duration::ZERO)` (depending on consistency requirements).
  4. After local fsync succeeded:
     - Build `ReplicateBatchesRequest` with the newly appended batch(es).
     - For each follower (from `ClusterMembership`):
       - Call replication client.
  5. If any follower fails or is unreachable:
     - Call S3 replication writer:
       - Serialize event batch(es) and upload to S3 replication log.
     - Only after that returns OK, return success to client.

5.2 Implement **S3 replication writer**:

- In `eventplanedb_control_plane` or a dedicated replication module:
  - `async fn persist_batches_to_s3(aggregate_key, batches: &[EventBatchItem], metadatas: &[EventBatchMetadata]) -> Result<(), ControlPlaneError>`.

### 6. Follower Catch-Up & Degraded Mode

6.1 When a follower restarts or detects it is far behind:

- Implement:

```rust
async fn catch_up_from_leader_or_s3(&self, aggregate_key: &AggregateKey) -> Result<(), EventPlaneDBError>
```

Steps:

1. Read lease & membership to find leader.
2. Ask leader for missing batches via `ReplicateBatchesRequest`.
3. If leader cannot supply all batches (e.g. trimmed):
   - Fetch the remaining from S3 replication log.
4. Apply them with a dedicated append path (no re-indexing).

6.2 On leadership acquisition (`ensure_leader` success):

- As part of success path:
  - Compare local `max_event_batch_index` with `lease.event_batch_index`.
  - If behind, **force catch-up first** as above.
  - Only then allow writes.

### 7. Error Types & Client Feedback

7.1 Extend `EventPlaneDBError`:

- Add something like:

```rust
pub enum EventPlaneDBError {
    // ... existing ...
    NotLeader {
        leader_node_id: Option<u128>,
        leader_address: Option<String>,
    },
    LeaseAcquisitionFailed,
    ReplicationFailed,
    // ...
}
```

7.2 Update client (`eventplanedb_client`) to:

- Surface `NotLeader` explicitly so applications can implement smart retry routing.

### 8. Testing & Validation

8.1 Unit tests:

- Lease acquisitions races (mock S3).
- Metadata serialization/deserialization with new fields.
- ReplicateBatches semantic checks (contiguity, CRC, etc.).

8.2 Integration tests:

- Single leader + one follower with a local mock of S3.
- Scenario tests:
  - Leader failover:
    - Write batches, kill leader, promote follower, ensure no data loss.
  - Degraded S3:
    - Simulate S3 failure; ensure writes fail safely, not silently dropped.
  - Follower catch-up:
    - Follower down for some time, recovers and catches up from S3.

---

This design keeps the hot path local + TCP-based replication, pushing S3 into control-plane operations and degraded situations while providing a clear set of implementation steps that align with the current code organization.