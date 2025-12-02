# EventPlaneDB Object Store Integration Design

## Table of Contents
1. [Introduction](#1-introduction)
2. [High-Level Design](#2-high-level-design)
3. [Detailed Design](#3-detailed-design)
4. [Risk Evaluation](#4-risk-evaluation)
5. [Task Breakdown](#5-task-breakdown)
6. [Appendix](#6-appendix)

---

## 1. Introduction

### 1.1 Purpose
Define how EventPlaneDB integrates Apache Arrow’s `object_store` crate—designed for Tokio runtimes—into the existing Glommio-based server without forking upstream. The integration must support high concurrency for both control-plane (leases, membership) and data-plane (degraded-mode batch uploads, tiering) operations.

### 1.2 Design Philosophy
- **Keep upstream intact**: Avoid maintaining a fork of `object_store`.
- **Respect Glommio’s model**: No Tokio code inside shard executors.
- **Treat S3 as critical infra**: High concurrency, strong error isolation, robust backpressure.
- **Reuse replication semantics**: Backpressure modes, degraded-mode behavior, and observability mirror the replication plan.

### 1.3 Goals
- Full `object_store` feature set (AWS, GCP, Azure) available to Glommio shards.
- Support hundreds of concurrent S3 ops without starving lease renewals.
- Seamless future reuse for data tiering, snapshots, and archival pipelines.

### 1.4 Non-Goals
- Porting `object_store` to Glommio.
- Replacing S3 with another control plane.
- Client SDK changes (covered in replication doc).

---

## 2. High-Level Design

### 2.1 Architecture Overview

```
┌────────────────────────────────────────────────────────────────────┐
│                       Tokio Sidecar Runtime                        │
│ ┌───────────────┐    ┌────────────────────┐   ┌──────────────────┐ │
│ │ Worker Thread │... │ ObjectStore Handle │   │ Req/Resp Channel │ │
│ └───────────────┘    └────────────────────┘   └──────────────────┘ │
└────────────────────────────────────────────────────────────────────┘
               ▲                         ▲
               │                         │
┌──────────────┴──────────────┐  ┌───────┴────────────┐
│ Glommio Shard 0 (clients)   │  │ Glommio Shard N    │
│ - Request processing        │  │ - Replication      │
│ - Lease / S3 intents        │  │ - Degraded batches │
└─────────────────────────────┘  └────────────────────┘
```

### 2.2 Core Concepts
1. **Tokio Sidecar**: Dedicated multi-thread Tokio runtime hosting all `object_store` futures.
2. **Channel Gateway**: Shards submit `ObjectStoreOp` requests via bounded async channels; responses arrive via per-request oneshots.
3. **Priority Lanes**: Separate channels for control-plane (leases, membership) and data-plane (batches) to avoid head-of-line blocking.
4. **Backpressure Parity**: Queue limits and policies mirror replication’s `QueueFullPolicy` to keep mental model consistent.
5. **Observability + Fault Containment**: Failure of sidecar is treated as equivalent to S3 outage; metrics and alerts differentiate causes.

### 2.3 Design Benefits
- No direct Tokio dependencies in Glommio code.
- Preserves `object_store` upgrades without merge pain.
- Enables future tiering or snapshot work to reuse same runtime.

---

## 3. Detailed Design

### 3.1 Tokio Sidecar Runtime

| Aspect | Description |
|--------|-------------|
| Threads | `max(2, num_shards / 2)` worker threads (configurable) |
| Lifecycle | Spawned during server bootstrap before shards start |
| Runtime Type | `tokio::runtime::Builder::new_multi_thread().enable_all()` |
| Shared State | `Arc<ObjectStoreRegistry>` storing configured handles (AWS/GCP/Azure/local) |

Sidecar threads never touch Glommio APIs; they react exclusively to channel inputs.

### 3.2 Channel Gateway

#### 3.2.1 Request Envelope
```rust
struct ObjectStoreRequest {
    op_id: u64,
    target: ObjectStoreTarget,      // e.g., ControlPlaneLease, BatchUpload
    payload: ObjectStoreOp,         // Enum over specific operations
    response_tx: oneshot::Sender<Result<ObjectStoreResult, ObjectStoreError>>,
    deadline: Option<Instant>,      // For lease renewals etc.
    qos_class: QoSClass,            // Control, DegradedData, Tiering, etc.
}
```

#### 3.2.2 Priority Lanes
- **Control Lane**: Leases, membership, degraded-mode metadata. Always processed first to honor WRITE_DEADLINE_BUFFER.
- **Data Lane**: Batch uploads/downloads, tiering tasks.
- **Maintenance Lane** (future): Snapshot compaction, lifecycle cleanup.

Each lane is a bounded `async_channel::bounded(N)` queue. When full, Glommio shard decides policy:
- `Block` (await capacity)
- `DegradeToS3` (if already in degraded mode, accumulate data but pause new writes)
- `RejectWithBackpressure` (bubble up to client)

### 3.3 Operation Flow

1. **Glommio task** builds an `ObjectStoreOp` (e.g., `PutLease { key, payload, condition }`).
2. It selects appropriate lane and tries `send`.
3. `send` either succeeds immediately or returns `Full` → apply policy (block/degrade/reject).
4. Sidecar runtime receives request, dispatches to handler using `tokio::spawn`.
5. Handler awaits `object_store` future (PUT/GET/DELETE) with per-op timeout + retry policy.
6. Result sent back through oneshot. Glommio task awaits response and continues.

### 3.4 Control vs Data Ops

| Target | Examples | QoS | Notes |
|--------|----------|-----|-------|
| `ControlPlaneLease` | Acquire, renew, release leases | High | Strict deadlines, minimal retries |
| `ControlPlaneMembership` | Read/update membership file | High | Uses conditional PUT with ETag |
| `MetadataIntents` | Pending-write markers in S3 (future) | High | Same lane as leases if critical |
| `DegradedBatch` | Range PUT/GET/DELETE | Medium | High concurrency; amortized flush |
| `TieredStorage` | Future cold-tier moves | Low | Best-effort, yield to others |

### 3.5 Retry & Timeout Policy

| Operation | Timeout | Retries | Backoff |
|-----------|---------|---------|---------|
| Lease PUT/GET | 250 ms | 5 | Exponential (50→500ms) |
| Membership update | 500 ms | 5 | Exponential |
| Batch PUT (degraded) | 5 s | 3 | Jittered (200→1000ms) |
| Batch GET | 5 s | 3 | Jittered |
| Batch DELETE | 2 s | 5 | Parallel deletes with throttling |

Errors bubble back with categorization (`Retryable`, `Permanent`, `Auth`, `Timeout`). Glommio logic maps these to replication/degraded rules (e.g., enter degraded mode, alert on permanent control-plane errors).

### 3.6 Backpressure Integration
- `ReplicationConfig::max_pending_replication_messages/bytes` parallels new `ObjectStoreConfig::max_pending_ops/bytes`.
- Each lane exports live metrics (queue depth, bytes, wait time).
- When thresholds exceeded, shards trigger same `Backpressure` error variant described in replication design (with `retry_after_ms`).
- Metrics feed dashboards: `object_store_queue_depth{lane="control"}`, `object_store_inflight_ops`, `object_store_error_total{type}`.

### 3.7 Fault Handling

| Fault | Detection | Reaction |
|-------|-----------|----------|
| Sidecar panic | Join handle error, heartbeat failure | Stop accepting writes; mark S3 unavailable; alert |
| Tokio runtime hung | Heartbeat misses, queue builds | Trip circuit breaker, attempt restart (configurable) |
| Credential failure | Permanent auth error | Fail fast, mark cluster unhealthy |
| Individual op timeout | Per-op timer | Retry/backpressure per policy |

Sidecar exposes health info via shared `Arc<Atomic>` counters so Glommio shards can degrade gracefully before queues explode.

### 3.8 Future Tiering Reuse
- Additional QoS class for tiered storage.
- Long-running ops scheduled via same lane infrastructure.
- Allows pausing tiering when control-plane load spikes (priority inheritance).

---

## 4. Risk Evaluation

| Risk | Likelihood | Impact | Mitigation | Status |
|------|------------|--------|------------|--------|
| Sidecar bottleneck under high concurrency | Medium | High (lease timeouts) | Multi-thread runtime, priority lanes, queue metrics | ✅ Mitigated |
| Control ops blocked by degraded uploads | Medium | High | Separate queues + strict QoS | ✅ Mitigated |
| Channel full → shard deadlock | Medium | Medium | Bounded queues + mirrored backpressure policies | ✅ Mitigated |
| Tokio runtime crash | Low | High | Heartbeat + fatal alerts + graceful degradation | ✅ Mitigated |
| Divergent retry semantics vs replication doc | Low | Medium | Shared retry policy tables | ✅ Mitigated |
| Credential misconfig | Medium | Medium | Startup validation, explicit error surface | ✅ Mitigated |
| Latency spikes due to tiering | Medium | Low | Separate QoS lane, preemption | ✅ Mitigated |

---

## 5. Task Breakdown

| Phase | Tasks | Deliverables |
|-------|-------|--------------|
| 1. Runtime Scaffolding (1w) | Build Tokio runtime bootstrap, global registry, graceful shutdown hooks | `object_store_runtime.rs` |
| 2. Channel API (1w) | Define `ObjectStoreOp`, lanes, queue instrumentation | `object_store_gateway.rs` |
| 3. Control Ops (1-2w) | Implement lease/membership ops via gateway, integrate with existing control-plane logic | Updates in `replication/lease_ops.rs`, `membership_manager.rs` |
| 4. Data Ops (1-2w) | Implement degraded batch PUT/GET/DELETE, amortization timers calling gateway | `replication/degraded_mode.rs` |
| 5. Backpressure & Observability (1w) | Metrics, queue-depth alerts, wiring to `Backpressure` error | `observability_deps`, metrics modules |
| 6. Chaos & Load Tests (1-2w) | Simulate stalled sidecar, S3 throttling, high concurrency | Integration test suite |

---

## 6. Appendix

### 6.1 Configuration Additions
```rust
pub struct ObjectStoreRuntimeConfig {
    pub worker_threads: usize,
    pub control_lane_capacity: usize,
    pub data_lane_capacity: usize,
    pub tiering_lane_capacity: usize,
    pub max_inflight_ops: usize,
    pub heartbeat_interval_ms: u64,
}

pub struct ObjectStoreRetryConfig {
    pub lease_timeout_ms: u64,
    pub lease_retry_attempts: u32,
    pub batch_put_timeout_ms: u64,
    pub batch_put_retries: u32,
    pub jitter_factor: f64,
}
```

### 6.2 Metrics
- `object_store_queue_depth{lane}`
- `object_store_inflight{lane}`
- `object_store_errors_total{kind}`
- `object_store_latency_seconds{op}`
- `object_store_heartbeat_seconds_since_last`

### 6.3 Failure Scenarios to Test
1. Sidecar thread panic mid-flight → shards detect within heartbeat interval, enter degraded mode.
2. Control lane saturation while data lane idle → confirm leases still renew.
3. S3 throttling (503 Slow Down) → observe retries/backoff and queue growth.
4. Credential revocation → ensure clear error surfaces and writes halt safely.
