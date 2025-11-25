Here is the refined design document based on your notes.

```markdown replication-design-2.md
# EventPlaneDB Replication Design: S3-Leased Leader-Follower

## 1. High Level Design

The goal is to implement high-availability replication (Leader + Multiple Followers) per aggregate without implementing a complex distributed consensus algorithm like Raft/Paxos. Instead, we utilize Amazon S3 (or compatible object storage) with Conditional Writes (Optimistic Concurrency Control) as the synchronization primitive (Control Plane).

### Architectural Concepts

*   **Topology:** Each aggregate is managed by a single **Leader** and replicated to $N$ **Followers**.
*   **Consistency:** Strong consistency for the client. A write is acknowledged only after it is persisted to the Leader's disk **AND** replicated to all available Followers.
*   **Leasing (Consensus):** Leadership is acquired via an S3 object lock (Lease). Nodes compete to write this file using conditional writes.
*   **Degraded Mode (AutoMQ Style):** If a Follower is unavailable, the Leader replicates the batch to S3 instead. This ensures availability without sacrificing durability.
*   **Cost Efficiency:** Leaders only renew leases while actively processing writes. Inactive aggregates go "dormant," allowing leases to expire to save S3 request costs.

---

## 2. Detailed Design

### 2.1. Identity & Discovery
Every node in the cluster generates a unique `node_id` (u128) at startup, persisted in its data root.
A cluster membership file exists in S3 at a known path:
```json
{
  "members": [
    {
      "id": "123456789...",
      "address": "10.0.0.5:5000",
      "is_active": true
    }
  ]
}
```
Nodes periodically poll this file to know who to replicate to.

### 2.2. The Lease Mechanism
For a specific aggregate (e.g., `org/type/id`), a `lease.json` object controls leadership.

**Lease Structure:**
```json
{
    "lease_index": 105,             // Monotonically increasing u64
    "node_id": "123...abc",         // Current Leader ID
    "lease_expiry": 1715000000000,  // Unix millis
    "event_batch_index": 500,       // The tail index at moment of lease acquisition
    "requested_by_client": "..."    // Traceability
}
```

**Acquisition Logic:**
1. Node reads current `lease.json`.
2. Checks if expired or non-existent.
3. Performs S3 `PutObject` with `If-Match` (ETag) to overwrite with new lease details.
4. If successful, Node is Leader. If 412 Precondition Failed, someone else won.

**Renewal Logic:**
*   **Active:** If receiving writes, renew lease $T$ seconds before expiry.
*   **Dormant:** If no writes, let lease expire. Next write attempt triggers acquisition logic.

### 2.3. Data Structure Updates
To ensure data lineage and debuggability during failovers, `EventBatchItem` and `EventBatchMetadata` must track the node and lease context.

**Fields to add:**
*   `ni` (`node_id`): u128 - The node that processed this batch.
*   `li` (`lease_index`): u64 - The lease generation this batch belongs to.

### 2.4. Write Path (Happy Path)
1.  **Client** sends `WriteRequest` to Node A.
2.  **Node A** checks if it holds a valid, non-expired lease.
    *   *If not leader:* Reject with `NotLeader(LeaderAddress)` error.
    *   *If expired:* Attempt to acquire/renew lease.
3.  **Node A (Leader)** writes batch to local DMA storage (WAL).
4.  **Node A** sends `ReplicateBatchRequest` to all Followers in parallel via TCP (Glommio channel).
5.  **Followers** validate lineage, write to local DMA, and return `Ack`.
6.  **Node A** `fdatasync` local file.
7.  **Node A** returns `Success` to Client.

### 2.5. Write Path (Degraded Mode)
If a Follower does not Ack (timeout/connection refused):
1.  **Leader** uploads the raw batch to S3 (`/org/type/id/batches/{batch_index}.bin`).
2.  **Leader** updates its internal state to mark that Follower as "lagging".
3.  Once uploaded to S3 + Local Disk, the write is considered durable.
4.  **Leader** returns `Success` to Client.

*Note: When the lagging Follower comes back, it detects a gap in `event_batch_index`. It downloads missing batches from S3 before accepting new direct replication streams.*

### 2.6. Failure Handling (Rollback)
If the Leader writes to local disk but fails to replicate to *either* Followers *or* S3:
1.  Leader performs `truncate` on local files to remove the speculative batch.
2.  Leader returns `WriteError` to client.

---

## 3. Risk Evaluation

| Scenario | Likelihood | Consequence | Severity | Mitigation Controls |
| :--- | :--- | :--- | :--- | :--- |
| **Split Brain (Clock Skew)** | Low | High (Data Corruption) | Critical | 1. Nodes check NTP sync on startup.<br>2. Large safety margin (e.g., Lease=10s, Renew=5s).<br>3. Reject writes if `now() > lease_expiry - safety_margin`. |
| **Zombie Leader** | Medium | Medium (Stale Reads/Writes) | High | S3 Conditional Writes are the source of truth. A zombie leader cannot renew its lease or upload to S3 if a new leader has bumped the `lease_index`. |
| **S3 Latency Spikes** | Medium | Low (Higher Write Latency) | Medium | S3 is only in the hot path during Lease Acquisition or Degraded Mode. Happy path is pure TCP/DMA. |
| **Follower Flapping** | Medium | Low (Performance Jitter) | Low | Implement "Lease Hysteresis". Don't immediately switch back from S3-mode to Follower-mode until connection is stable for $X$ seconds. |
| **S3 Cost Explosion** | Low | Low (Financial) | Low | **Dormant Mode**: Strictly enforce that leases are NOT renewed if no writes occur. |

---

## 4. Implementation Task Breakdown

### Phase 1: Foundations & Structures
- [ ] **Dependencies:** Add AWS SDK (Rust) for S3 support.
- [ ] **Struct Update:** Modify `EventBatchItem` and `EventBatchMetadata` to include `node_id` (u128) and `lease_index` (u64).
- [ ] **Config:** Update `EventPlaneDBConfig` to include `node_id` (generated or config), `cluster_members`, and `s3_bucket` settings.
- [ ] **Migration:** Update serialization/deserialization logic to handle versioning for the new fields.

### Phase 2: S3 Control Plane
- [ ] **Lease Manager:** Create `LeaseManager` struct.
    - [ ] Implement `acquire_lease(aggregate_id)`.
    - [ ] Implement `renew_lease()`.
    - [ ] Implement `get_cluster_members()`.
- [ ] **S3 Client Wrapper:** Implement a Glommio-friendly async wrapper around the S3 SDK (since standard SDKs are Tokio-based, may need `glommio::spawn_blocking` or specific compat layer).

### Phase 3: Replication Protocol
- [ ] **Wire Protocol:** Add `ReplicateBatch` message type to `Request` enum.
- [ ] **Wire Protocol:** Add `ReplicateBatchResponse` to `Response` enum.
- [ ] **Replication Client:** Create a persistent TCP client within `ProcessRequest` to maintain connections to peers.

### Phase 4: Write Path Logic Updates
- [ ] **Leader Check:** Update `handle_write` in `ProcessRequest`.
    - [ ] Before writing, check `LeaseManager` status.
    - [ ] Trigger `acquire_lease` if needed.
- [ ] **Replication Loop:**
    - [ ] After `queue_events_in_memory`, iterate through `cluster_members`.
    - [ ] Send `ReplicateBatch`.
    - [ ] `try_join_all` on responses.
- [ ] **Fallback Logic:** Implement S3 upload if TCP replication fails.
- [ ] **Commit/Rollback:** Update `sync_with_rollback` to account for network replication failures.

### Phase 5: Follower Logic
- [ ] **Replication Handler:** Implement handler for `ReplicateBatch`.
    - [ ] Validation: Ensure `lease_index` matches expected or is higher.
    - [ ] Storage: Write to `WriteOperations` (append-only).
- [ ] **Gap Recovery:** On startup or new lease, check local `event_batch_index` vs Leader's index. Download missing files from S3 if gap exists.
```