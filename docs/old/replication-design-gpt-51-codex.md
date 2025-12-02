# EventPlaneDB Replication Design

## High-Level Design

### Motivation
EventPlaneDB must provide strong durability guarantees (local fsync + replicated copies) without adopting a heavyweight consensus protocol. Instead, we lean on Amazon S3’s conditional writes as a coordination primitive for leader election, yielding a simpler control plane that still maintains strict write durability and offers acceptable operational tradeoffs.

### Operating Principles
1. **Aggregate-Scoped Leadership:** Each aggregate picks a single leader. Different aggregates can have different leaders simultaneously, distributing load naturally.
2. **Lease-Based Coordination via S3:** Leadership is encoded in per-aggregate lease files written with S3’s optimistic concurrency control. Winning the conditional write grants the lease.
3. **Synchronous Replication:** The leader fsyncs locally and replicates the new batch to all active followers before acknowledging the client. When followers are unavailable we enter a “degraded” path and durably park the batch in S3 so any future leader can recover.
4. **Graceful Degradation:** If a follower is down or a network partition prevents replication, the leader writes batches to S3 and advertises degraded mode to clients; followers later replay S3 batches during catch-up.
5. **Minimal Cluster Size:** The design supports two-node deployments by relying on S3 as the shared truth for leadership. We accept lower availability in this extreme configuration.
6. **Clock Discipline:** Because leases are time-based, each node continuously validates clock skew and refuses leadership if drift exceeds a safety margin.

### Architecture Snapshot

```
┌──────────────────────────────────────────────┐
│ S3 Control Plane                             │
│  • cluster/membership.json                   │
│  • leases/{org}/{type}/{agg}/lease.json      │
│  • batches/{org}/{type}/{agg}/{batch}.bin    │
└──────────────────────────────────────────────┘
            ▲                         ▲
            │ leases + membership     │ degraded batches
            ▼                         ▼
      ┌─────────────┐        ┌─────────────┐
      │ Node A      │◄──────►│ Node B      │◄─────┐
      │ (Leader)    │  TCP   │ (Follower)  │       │
      └─────────────┘        └─────────────┘       │
            │ replication                         │
            ▼                                      │
        Clients                                    │
                                                   │
                    ┌─────────────┐◄───────────────┘
                    │ Node C      │
                    │ (Follower)  │
                    └─────────────┘
```

---

## Detailed Design

### 1. Node Identity
* Generate a `u128` node ID on first startup; persist to `{data_root}/node_id`.
* Embed the ID in leases, batch metadata, replication headers, and membership records to support auditing and debugging.

### 2. S3 Control Plane

#### 2.1 Cluster Membership
* **Path:** `cluster/membership.json`
* Records each node’s ID, client port, replication port, and health flags.
* Nodes pull membership every 30 s (or immediately after replication failures). Admin tooling updates the file via conditional writes on the `version` field.

#### 2.2 Aggregate Leases
* **Path:** `leases/{org}/{type}/{agg}/lease.json`
* Contents:
  ```json
  {
    "lease_index": 37,
    "node_id": "...",
    "lease_expiry": 1700000000000,
    "event_batch_index": 4242,
    "requested_by_client": "..."
  }
  ```
* **Acquisition:** read current lease → if absent or expired → try conditional PUT with incremented `lease_index`. Conflicts imply another leader; return hint.
* **Renewal:** leaders renew before expiry minus `LEASE_RENEWAL_THRESHOLD` only when the aggregate is active. Dormant aggregates let leases lapse to cut S3 costs.
* **Release:** on shutdown, leaders set `lease_expiry = now` via conditional PUT to shorten failover.
* **Safety windows:** leases are considered valid until `expiry - MAX_CLOCK_SKEW`; new leaders wait until `expiry + MAX_CLOCK_SKEW` to avoid double-writes.

### 3. Time Synchronization
* Nodes probe peers every 60 s (using replication connections) to estimate drift.
* If observed skew > `MAX_CLOCK_SKEW`, log warnings and enter degraded replication; if > 2× the bound, refuse new leases until clocks recover.
* Consider AWS Time Sync Service as the default time source in production templates.

### 4. Replication Protocol

#### 4.1 Transport
* Dedicated TCP port per node managed by glommio.
* Persistent connections between every pair (mesh). Each message carries a 4-byte version, message type, length, and body.

#### 4.2 Message Set
| Type | Direction | Notes |
|------|-----------|-------|
| `ReplicateBatch` | Leader → Follower | Carries one batch with metadata summary, lease index, writer node ID. |
| `ReplicateBatchAck` / `Nack` | Follower → Leader | Ack implies batch fsync’d locally. Nack includes expected batch index for gap recovery. |
| `CatchUpRequest/Response` | Bidirectional | Used when followers rejoin and need historical batches. |
| `TimeSync` | Bidirectional | Piggybacks on transport to report wall-clock readings. |

#### 4.3 Follower Processing
1. Validate the advertised lease index matches the one observed during catch-up.
2. Ensure `event_batch_index` is contiguous; otherwise Nack with the missing index.
3. Append to local WAL, fdatasync, then Ack.

#### 4.4 Gap Handling
Leaders keep recent batches in memory until all followers acknowledge. On Nack the leader backfills the requested range (batch-by-batch to respect payload limits).

### 5. Write Path

1. Client hits any node; followers immediately reject with `NotLeader` plus best-known leader hint.
2. Leader validates lease freshness. If expired, it attempts acquisition; on failure returns leader hint.
3. Convert events into an `EventBatchItem`, update metadata (`writer_node_id`, `lease_index`), serialize, and append to local WAL. Keep rollback markers (`pre_write_file_len_*`, `next_event_batch_index`, etc.).
4. Replicate the batch to each follower concurrently, waiting for Ack (or up to `replication_timeout_ms`).
5. **Normal path:** once all followers Ack, rollback markers are cleared, and the client is acknowledged.
6. **Failure path:** if any follower can’t Ack before timeout, enter degraded mode for that aggregate:
   * Upload the batch to `batches/{org}/{type}/{agg}/{batch_index}.bin`.
   * Mark local state so future writes also go through S3 until quorum recovers.
   * Ack the client but include a degraded indicator for observability.
7. If replication *and* S3 upload both fail, execute rollback (truncate files, restore indexes) and return error.

### 6. Degraded Mode
* Leaders persist every new batch to S3 until each follower is reachable and has caught up.
* Followers that were offline pull missing batches from S3 before campaigning for leadership.
* Lifecycle policy removes stale S3 batches after a retention window; the system tracks whether a batch still needs remote replay to avoid premature deletion.

### 7. Follower Responsibilities
* Reject writes with leader hint.
* Serve reads from local storage (eventually consistent) unless clients opt into a `require_leader` flag that forwards the read to the current leader.
* Monitor leases; when current leader’s lease expires plus skew margin, attempt acquisition. Before accepting writes, they must:
  1. Download any outstanding S3 batches.
  2. Confirm replication to other followers is healthy (or declare degraded mode immediately).

### 8. Metadata Changes
Extend `EventBatchMetadata` with:
```rust
pub struct EventBatchMetadata {
    // ...existing fields...
    pub writer_node_id: u128,
    pub lease_index: u64,
}
```
Defaults of zero preserve backward compatibility for older batches.

### 9. Configuration Additions
```rust
pub struct ReplicationConfig {
    pub s3_bucket: String,
    pub s3_region: String,
    pub lease_duration_ms: u64,
    pub lease_renewal_threshold_ms: u64,
    pub max_clock_skew_ms: u64,
    pub replication_port: u16,
    pub replication_timeout_ms: u64,
    pub enable_degraded_mode: bool,
}
```
Environmental overrides supply AWS credentials (or LocalStack endpoints for tests).

### 10. Observability
* Metrics: lease acquisitions/renewals, replication latency per follower, degraded mode duration, S3 round-trips, clock skew.
* Structured logs for lease conflicts, replication Nacks, degraded transitions, S3 failures.
* Admin tooling: dump current leases, force lease release, inspect membership, toggle node activity.

---

## Risk Evaluation

| Scenario | Likelihood | Consequence | Mitigations |
|----------|------------|-------------|-------------|
| Clock skew exceeds safety margin (split-brain writes) | Low | **Critical** | Continuous skew probing; refuse new leases when drift too high; recommend AWS Time Sync; embed writer timestamp and reject stale batches. |
| S3 unavailable (lease ops fail) | Very Low | **High** | Existing leaders keep serving until leases expire; exponential backoff on S3 retries; alerting; optional multi-region bucket replication. |
| Conditional lease race | Medium | **Low** | Expected behavior—retry with jitter; return leader hints to clients; monitor for excessive thrashing. |
| Replication timeout after local fsync | Medium | **Medium** | Degraded mode with S3 hot path; tune timeout; track per-follower latency; allow operators to mark followers inactive. |
| Rollback failure after double-fault | Low | **Critical** | Rollbacks rely on file truncation (atomic on ext4/xfs); integrity check on startup; ability to recover from followers or S3. |
| Follower far behind (blocks writes) | Medium | **Medium** | Time-bounded catch-up; admin command to deactivate lagging follower; snapshot-based seeding for large gaps. |
| Network partition between nodes but not S3 | Low | **High** | Detect replication failure quickly; degraded mode ensures batches land in S3; optional policy requiring at least one follower. |
| Lease expiry mid-write | Low | **High** | Validate lease before client ack; avoid accepting writes when lease residual < 2× worst-case write time; followers reject stale lease_index. |
| Two-node cluster single failure | Medium | **Medium** | Document reduced availability; rely on degraded mode + S3; encourage 3-node deployments in production. |
| S3 strong consistency regression | Very Low | **Critical** | AWS guarantees read-after-write; still track `lease_index` monotonicity and sanity-check in data plane. |

---

## Task Breakdown

### Phase 1 – Foundations
1. **Node Identity:** generator + persistence + plumb through metadata.
2. **ReplicationConfig:** CLI + env parsing + validation.
3. **S3 Integration:** async client wrapper with conditional PUT/GET helpers and retries (using LocalStack for integration tests).

### Phase 2 – Lease Management
1. Data model (`LeaseFile` struct, serde).
2. Lease operations: acquire / renew / release / read with caching.
3. `LeaseManager` per aggregate with background renewal tasks, dormancy logic, graceful shutdown hooks.

### Phase 3 – Cluster Membership
1. Structures for membership file.
2. Periodic refresh + caching; detect changes and update replication peers.
3. (Future) Admin tooling for mutations.

### Phase 4 – Replication Protocol
1. Message enums, serializers, glommio framing.
2. Replication server: accept connections, dispatch handlers, time sync probes.
3. Replication client: connection pooling, send/receive logic, nack handling, catch-up streaming, backpressure.

### Phase 5 – Write Path Integration
1. Lease validation before writes and before ACK.
2. Extend `sync_with_rollback` bookkeeping for replication state.
3. Parallel follower replication with timeout + degraded fallback path.
4. Follower write rejection and client hint propagation.
5. Update metadata with `writer_node_id`/`lease_index`.

### Phase 6 – Degraded Mode
1. S3 batch storage (PUT/LIST/GET helpers).
2. Track per-aggregate degraded status.
3. Catch-up logic for returning followers or new leaders.
4. Lifecycle strategy for deleting obsolete batches once quorum heals.

### Phase 7 – Time Sync + Safety Nets
1. Periodic `TimeSync` messages, skew calculation.
2. Hooks to gate lease acquisition/renewal on health.
3. Configurable alarms and degraded-mode triggers.

### Phase 8 – Testing & Hardening
1. Unit tests for lease logic, metadata changes, degraded mode transitions.
2. Integration tests across multi-node clusters (happy path, failover, partitions, S3 outages).
3. Performance exercises validating throughput/latency targets with replication enabled.
4. Chaos tests: random node kills, network delays, forced clock drift.

### Phase 9 – Operational Readiness
1. Metrics + logs wiring.
2. Admin CLI/HTTP endpoints for leases, membership, degraded aggregates.
3. Runbook + troubleshooting guide + architecture diagram updates.
