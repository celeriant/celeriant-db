Of course. I will create a new file, `replication-design-2.md`, with a more structured and actionable version of your design notes. This new document will include a high-level design, a detailed design, a risk evaluation, and a task breakdown for implementation.

Here is the content for the new file:

```markdown replication-design-2.md
# EventPlaneDB Replication Design

This document outlines the design for implementing single-leader, multi-follower replication at the aggregate level in EventPlaneDB. The goal is to provide high availability and data durability without sacrificing the high-performance characteristics of the system.

## 1. High-Level Design

The replication model is based on a per-aggregate leader-follower architecture. For any given aggregate, one node acts as the single leader responsible for handling all write operations, while other nodes act as followers.

Instead of implementing a complex consensus protocol like Raft, this design leverages an S3-compatible object store as a lightweight control plane for two key functions:
1.  **Leader Election**: Nodes compete to acquire a time-based "lease" for an aggregate by performing conditional writes on a lease file in S3. The winner becomes the leader.
2.  **Degraded Mode Durability**: If a leader cannot replicate a write to a follower, it writes the event batch to S3 as a durable commit log. This ensures that no data is lost and that followers (or a new leader) can catch up later.

Client writes are only acknowledged after the leader has successfully written the data locally, fsync'd it, and replicated it to all active followers (or to S3 for any unavailable followers). This provides strong durability guarantees.

The design prioritizes performance in the "happy path" (all nodes are healthy) by keeping S3 out of the hot path for writes. S3 is only accessed for leader election/renewal and during failure scenarios.

## 2. Detailed Design

### 2.1. Lease Management & Leader Election

Leadership is managed via a lease file stored in S3 for each aggregate.

-   **Lease File Path**: `s3://<bucket>/<org_id>/<agg_type_id>/<agg_id>/_lease.json`
-   **Lease File Content**:
    ```json
    {
        "lease_index": "incremented u64",
        "node_id": "u128 unique identifier of the leader node",
        "lease_expiry": "timestamp in millis when the lease expires",
        "event_batch_index": "the last known event batch index at the time of lease",
        "leader_address": "ip:port of the leader node for client redirection"
    }
    ```

**Lease Acquisition:**
1.  A node wishing to become a leader reads the lease file from S3.
2.  If the lease has expired or doesn't exist, the node attempts to write a new lease file for itself.
3.  The write to S3 is **conditional**, based on the ETag of the file it just read. This ensures that only one node can "win" the race to acquire the lease. The `lease_index` is also incremented.
4.  If the conditional write succeeds, the node becomes the leader. If it fails, another node has acquired the lease, and this node becomes a follower.

**Lease Renewal:**
-   An active leader (one receiving writes) will proactively renew its lease well before the `lease_expiry` time. Renewal is also a conditional write to prevent a "stale" leader from renewing a lease that has already been taken over.
-   If a leader is idle, it allows the lease to expire to reduce S3 costs.

### 2.2. Cluster Membership

Node discovery is handled by a single cluster membership file in S3.

-   **File Path**: `s3://<bucket>/_cluster/members.json`
-   **File Content**:
    ```json
    {
        "members": [
            {
                "id": "u128 unique id of node",
                "address": "ip:port of node",
                "is_active": "boolean flag"
            }
        ]
    }
    ```
-   Nodes update their status in this file on startup and graceful shutdown.
-   Each node periodically caches this file to know which peers to replicate data to.

### 2.3. Write Path

**Happy Path (All nodes healthy):**
1.  A client sends a `WriteRequest` to a node.
2.  The node checks its leadership status for the aggregate.
    -   **If Follower**: It rejects the request with a `NotLeader` error, providing the current leader's address for the client to retry.
    -   **If Leader**: It proceeds.
3.  The leader assigns batch/event indices and writes the `EventBatchItem` to its local disk (without fsync).
4.  The leader sends a `ReplicateBatch` request in parallel to all active followers discovered via the membership file.
5.  The leader waits for a success acknowledgment from all followers.
6.  Upon receiving all acks, the leader `fsync`s its local write to disk.
7.  A successful `WriteResponse` is returned to the client.

**Degraded Mode (Follower Unreachable):**
1.  Steps 1-3 are identical to the happy path.
2.  The leader attempts replication, but one or more followers time out or return an error.
3.  For each failed replication, the leader writes the complete, compressed event batch to S3 at a path like: `s3://<bucket>/<org_id>/<agg_type_id>/<agg_id>/batches/<event_batch_index>.bin`.
4.  Once the batch is successfully stored in S3, it is considered replicated.
5.  After all replications are complete (either to follower or S3), the leader `fsync`s its local write and acknowledges the client.

### 2.4. Follower & State Catch-up

-   **Follower Logic**: A follower accepts `ReplicateBatch` requests. It validates that the `event_batch_index` is sequential. If it detects a gap, it returns an error to the leader specifying the missing index, prompting the leader to send the missing batches.
-   **New Leader Logic**: When a node successfully acquires a lease, it must ensure its state is current before accepting writes. It reads the `event_batch_index` from the lease file. If the lease's index is ahead of its local state, it must "catch up" by downloading and applying any missing batches from S3.

### 2.5. Data Structure Changes

-   **`EventBatchMetadata`**: Will be extended to include `leader_node_id: u128` and `lease_index: u64`. This provides an audit trail for which node wrote the data and under which lease term.
-   **New Wire Protocol Messages**:
    -   `Request::ReplicateBatch(ReplicateBatchRequest)`
    -   `Response::ReplicateBatch(ReplicateBatchResponse)`
-   **`EventPlaneDBError`**: New variants will be added, such as `NotLeader { leader_address: String }`.

## 3. Risk Evaluation

| Scenario | Likelihood | Consequence | Mitigation Controls |
| :--- | :--- | :--- | :--- |
| S3 Control Plane Outage | Low | High | Leader election and degraded mode writes will fail, blocking new writes. Existing leaders can continue serving writes until their lease expires. Systems should cache leader information and could enter a read-only mode. |
| Clock Skew Between Nodes | Medium | High | Incorrect lease evaluation could lead to a split-brain scenario where two nodes believe they are the leader. **Mitigation**: Mandate NTP on all cluster nodes. Use a generous safety margin for lease expiry checks. The monotonically increasing `lease_index` acts as a final guard against split-brain writes. |
| Leader Network Partition | Medium | Medium | A leader partitioned from S3 and followers cannot renew its lease and will step down. A new leader will be elected, causing a brief period of write unavailability for affected aggregates. |
| Split Brain | Low | Critical | Two leaders accept writes, causing data divergence and potential data loss. **Mitigation**: The `lease_index` is the primary defense. A node will refuse to become leader if its local state (`event_batch_index`) is behind the index recorded in the lease file, forcing it to catch up first. All writes must include the `lease_index`. |
| "Slow" Follower | High | Low | A single slow follower increases write latency for everyone. **Mitigation**: The leader should have a tight timeout for replication requests. If a follower times out, the leader should immediately switch to degraded mode for that replication and write to S3, unblocking the client write. |
| Write Rollback Failure | Low | Medium | A leader fails to roll back a local write after replication fails. This leads to inconsistent state. **Mitigation**: The rollback mechanism (`trim_end`) must be robust and atomic. The new leader's catch-up logic will eventually reconcile the state, but there is a window of inconsistency. |

## 4. Implementation Task Breakdown

### Phase 1: Foundations & Data Structures

- [ ] **Node Identity**:
    - [ ] Implement logic to generate/load a unique `node_id: u128` on server startup. Store it in a file within the `data_root` directory.
- [ ] **Configuration**:
    - [ ] Add new server configuration options for `node_id`, S3 bucket, region, credentials, and lease duration.
- [ ] **Update Data Structures**:
    - [ ] In `eventplanedb_structures/src/event_batch_metadata.rs`, add `leader_node_id: u128` and `lease_index: u64` to `EventBatchMetadata`.
- [ ] **Update Wire Protocol**:
    - [ ] In `request.rs`, add a `ReplicateBatch(ReplicateBatchRequest)` type. `ReplicateBatchRequest` should contain the key and a `Vec<EventBatchItem>`.
    - [ ] In `response.rs`, add a corresponding `ReplicateBatchResponse`.
    - [ ] In `eventplanedb_error.rs`, add a `NotLeader { leader_address: String }` error.

### Phase 2: S3 Control Plane & Leadership Logic

- [ ] **S3 Client Integration**:
    - [ ] Add an async S3 client library (e.g., `aws-sdk-rust`) as a dependency.
    - [ ] Create a service for interacting with S3, abstracting conditional (`If-Match`) GET/PUT operations.
- [ ] **Cluster Membership Service**:
    - [ ] Implement a service that periodically reads `members.json` from S3 to maintain a cached list of active peers.
    - [ ] Implement logic for a node to register/unregister itself on startup and shutdown.
- [ ] **Lease Manager**:
    - [ ] Create a `LeaseManager` responsible for acquiring, renewing, and checking the leadership lease for an aggregate via S3.
    - [ ] This manager should be accessible from `AggregateResources`.
    - [ ] Implement the "catch-up" logic: on becoming leader, check the lease's `event_batch_index` and sync from S3 if behind.

### Phase 3: Replication & Write Path Integration

- [ ] **Leader: Modify Write Path**:
    - [ ] In `process_request.rs`, guard `handle_write` by checking leadership status via the `LeaseManager`. If not the leader, return the `NotLeader` error.
    - [ ] After local write (pre-fsync), call a new replication service.
    - [ ] The replication service sends `ReplicateBatch` to all followers in parallel.
    - [ ] Implement degraded mode: on follower timeout/failure, write the batch to S3.
    - [ ] Only after all replications succeed (to peer or S3), perform the local fsync and respond to the client.
    - [ ] Implement rollback logic: if replication fails irrecoverably, use `trim_end` to revert the local write.
- [ ] **Follower: Handle Replication Requests**:
    - [ ] In `main.rs` (or `process_request.rs`), add a handler for `ReplicateBatchRequest`.
    - [ ] The handler should perform sequential `event_batch_index` checks. If a gap is found, return an error specifying the needed index.
    - [ ] On valid request, perform a local write and fsync.
- [ ] **Replication Client**:
    - [ ] Create a client (likely reusing `EventPlaneDBClient` logic) to send replication requests to peers.

### Phase 4: Client-Side Enhancements

- [ ] **Client `NotLeader` Handling**:
    - [ ] In `eventplanedb_client`, update `send_request` to handle the `EventPlaneDBError::NotLeader` variant.
    - [ ] When this error is received, the client should transparently retry the request against the new `leader_address` provided in the error.
```