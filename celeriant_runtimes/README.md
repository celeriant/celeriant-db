# celeriant_runtimes

Runtime orchestration for sharded executors, cluster coordination, and sidecar I/O. Manages thread-per-core glommio executors, inter-shard routing, lease-based leadership elections, boot orchestration, and bridges to tokio for external operations.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Glommio Executor Pool                        │
│  ┌─────────────────┐  ┌─────────────────┐    ┌──────────────┐  │
│  │     Shard 0     │  │     Shard 1     │    │   Shard N    │  │
│  │ ClientListener  │  │ ClientListener  │    │ClientListener│  │
│  │ ReplListener    │  │ ReplListener    │    │ ReplListener │  │
│  │ LeaseManager    │  │                 │    │              │  │
│  │ BootOrchestrat. │  │                 │    │              │  │
│  │ ShardWal<R,D>   │  │ ShardWal<R,D>   │    │ ShardWal<R,D>│  │
│  └────────┬────────┘  └────────┬────────┘    └──────┬───────┘  │
│           └───────────────────┼─────────────────────┘          │
│                        Channel Mesh (Full)                      │
│                       IntrashardMessages                        │
└─────────────────────────────┬───────────────────────────────────┘
                              │ flume channels
┌─────────────────────────────┴───────────────────────────────────┐
│                    Tokio Sidecar Runtime                        │
│  ┌────────────────────┐  ┌────────────────────┐                 │
│  │   Control Lane     │  │     Data Lane      │                 │
│  │ (lease, membership)│  │ (batch upload/dl)  │                 │
│  └─────────┬──────────┘  └──────────┬─────────┘                 │
│            └─────────────────────────┘                          │
│                   SidecarStoreTrait                             │
└─────────────────────────────────────────────────────────────────┘
```

## Invariants

- One glommio `LocalExecutor` per shard, pinned to a CPU core via `CpuSet`.
- No shared mutable state across shards. All per-shard state uses `Rc<RefCell<_>>`. The only `Arc` types crossing shard boundaries are immutable-after-creation or atomic booleans.
- `RefCell` borrows must NEVER be held across `.await` points. Snapshot into owned data, drop borrow, await, re-borrow to commit.
- Shard 0 handles all cluster coordination: lease management, heartbeats, kick processing.
- All S3/HTTP work runs in a separate tokio sidecar runtime. io_uring and tokio are incompatible in the same thread.
- Shard routing: `routing_id % num_shards`. Multi-aggregate writes that span multiple shards are rejected with `IncompatibleFilters`.
- A connection can be redirected between shards on any request via the glommio channel mesh.
- A newly elected leader runs S3 catchup before serving writes. Catchup also runs if the `lease_index` gap indicates the lease changed hands during a network partition.

## Key Types

| Type | Purpose |
|------|---------|
| `Shard<R, D, S>` | Per-core executor: dual TCP accept, routing, request processing, cluster coordination |
| `ShardConfig` | Node-level configuration (paths, ports, timeouts, replication, routing) |
| `RoutingRule` | Aggregate-to-shard mapping strategy (OrgId, AggregateTypeId, AggregateId) |
| `IntrashardMessages` | Inter-shard communication (Shutdown, redirect, S3 catchup, status, follower updates) |
| `ConnectionContext<R, D, S>` | Per-shard shared state: config, WAL, sender mesh, lease manager |
| `PortType` | Distinguishes `Client` vs `Replication` TCP listeners |
| `SignalHandler` | SIGINT/SIGTERM handling via atomic flags |
| `SidecarRuntime` | Tokio runtime for external I/O (S3, object storage) |
| `SidecarSenders` | glommio → tokio channel handles with QoS lanes |
| `SidecarConfig` | Thread pool and queue capacity configuration |
| `SidecarLeaseStorage` | Implements `LeaseStore` via sidecar S3 for lease/membership CAS operations |
| `SidecarS3Uploader` | Implements `S3Uploader` via sidecar for WAL batch replication uploads |
| `SidecarS3Downloader` | Implements `S3Downloader` via sidecar for follower S3 catchup |
| `ShardRoutingError` | Routing failures (no key, multiple shards, incompatible filters) |

## Design Decisions

### Thread-per-core model

One glommio executor per CPU core. Each shard owns a subset of aggregates. No locks on hot path. `Rc<Cell<_>>` for flags, `Rc<RefCell<_>>` for state. `Shard` is generic over `R: ReplicationClient`, `D: S3Downloader`, `S: LeaseStore` to allow concrete injection at startup without dynamic dispatch.

### Dual port architecture

Each shard binds two TCP listeners on startup:

| Port | Field | Accepted requests |
|------|-------|-------------------|
| `client_port` | `ShardConfig::client_port` | Reads, writes, deletes, watch, trim, list, heartbeat, kick |
| `replication_port` | `ShardConfig::replication_port` | ReplicationBatch, CatchUp only |

`PortType` is carried through the connection lifecycle and validated per-request via `Request::is_client_port_request()` / `is_replication_port_request()`. Invalid port usage returns error code 400 before processing. Watch requests are only accepted on the client port.

### Shard routing

| Rule | Routes By | Use Case |
|------|-----------|----------|
| `OrgId` | `org_id % num_shards` | Multi-tenant isolation |
| `AggregateTypeId` | `aggregate_type_id % num_shards` | Type locality |
| `AggregateId` | `aggregate_id % num_shards` | Even distribution (default) |

Replication (`ReplicationBatch`, `CatchUp`), list (`ListOrgs`, `ListAggregateTypes`, `ListAggregates`), and `KickFollower` use an explicit `shard_id` field in the request. `KickFollower` always routes to shard 0.

### Connection redirect

Clients connect to any shard. First request is read, routing computed. If wrong shard, `AcceptedTcpStream` + request sent via channel mesh. Target shard binds stream and continues processing.

```rust
IntrashardMessages::ConnectionRedirect {
    accepted_tcp_stream,  // Transferable socket
    request,              // Already-parsed first request
    message_version,
    port_type,            // Client or Replication
}
```

Redirect check runs again on pipelined requests, a session may migrate shards mid-pipeline.

### Multi-aggregate write/delete routing

All aggregates in a write or delete must hash to the same shard:

```rust
for aggregate_key in req.writes.keys() {
    shard_ids.insert(routing_rule.routing_id_for_rule(aggregate_key) % num_shards);
}
if shard_ids.len() > 1 { return Err(IncompatibleFilters(...)); }
```

### Watch routing and latency

Watch filters must match the routing rule. If routing by `OrgId`, an `orgs` filter is required. All filter values must hash to the same shard (`MultipleShardRoutes` otherwise). Watch is only accepted on the client port.

`max_requested_latency` in `ShardConfig` caps the `requested_latency_ms` a client may request. Exceeding this returns error 8001 (`WatchLatencyTooHigh`).

### Cluster coordination (shard 0)

Cluster coordination runs entirely on shard 0's executor. All other shards receive status updates via `IntrashardMessages`. The boot sequence on shard 0:

```
1. Pre-election S3 catchup (all shards in parallel via EnterS3Catchup)
2. register_self() - write node into membership file
3. run_election() - CAS the S3 lease to become leader or follower
4. Post-election S3 catchup (get any writes missed during election)
5. Broadcast ValidatedNodeStatus to all shards
6. Enter role loop: Leader | Follower | FollowerCatchingUp
```

See `celeriant_distributed` README for `LeaseManager`, `ValidatedNodeStatus`, and `LeaseStore` details.

#### Leader steady-state (`run_leader_loop`)

1. Discover follower via `discover_peer()` (with exponential backoff, capped at `initial_lease_duration / 2`)
2. Heartbeat follower every `heartbeat_interval`
3. On heartbeat success: refresh `ValidatedNodeStatus` TTL, broadcast `StatusUpdate` to all shards
4. On heartbeat failure: `renew_s3_lease_and_broadcast()` CAS-promote S3 lease for fresh TTL
5. On leadership loss: update `leader_client_address` and return

#### Follower watchdog (`run_follower_watchdog`)

Sleeps until `ValidatedNodeStatus::expires_at_ms()`. On TTL expiry:
1. Set local status to `Fenced`, broadcast to all shards
2. Race to S3 CAS (`run_election()`)
3. Win → become leader; lose → resume watching new leader's TTL

On `KickFollower` detection (status transitions to `FollowerCatchingUp`): return to role-flip loop, trigger `run_kick_catchup`.

#### Heartbeat validation (`handle_heartbeat`)

Handled directly in the connection handler (not in `ShardWal`) because it requires intrashard broadcast access:
- Rejects if not in any follower state
- Rejects with `ClockDriftTooHigh` and fences all shards if `|leader_ms - follower_ms| > max_cluster_time_drift_ms`
- On success: refreshes `ValidatedNodeStatus` TTL, broadcasts `StatusUpdate` to all shards

### S3 catchup coordination

`EnterS3Catchup` broadcasts to shards 1..N via mesh. Shard 0 runs its own catchup directly. Each shard sends `S3CatchupComplete { shard_id, result }` back to shard 0 via the mesh. Shard 0's `catchup_completion_tx` local channel collects results:

- All OK → proceed
- Any retriable error → sleep 5s, retry entire round
- Any fatal error → shutdown all shards

### Sidecar bridge

Tokio runtime for operations incompatible with io_uring (S3, HTTP clients). Two QoS lanes keep control plane operations from starving behind large data uploads.

| Lane | Operations | Capacity field |
|------|------------|----------------|
| Control | Lease CAS, membership | `control_lane_capacity` |
| Data | Batch uploads/downloads | `data_lane_capacity` |

`SidecarTarget` determines lane selection:

| Target | Lane | Used by |
|--------|------|---------|
| `ControlPlaneLease` | Control | `SidecarLeaseStorage` |
| `ControlPlaneMembership` | Control | `SidecarLeaseStorage` |
| `DataPlaneReplication` | Data | `SidecarS3Uploader`, `SidecarS3Downloader` |

Flume bounded channels with oneshot response pattern:

```rust
let (response_tx, response_rx) = flume::bounded(1);
tx.send_async(SidecarRequest { target, response_tx, .. }).await?;
response_rx.recv_async().await?
```

#### SidecarLeaseStorage

Implements `LeaseStore` from `celeriant_distributed`. Translates lease/membership operations into `SidecarStoreTrait` `ObjectGet`/`ObjectPut` requests over the control lane. Maps `NotFound` → `Ok(None)`, `PreconditionFailed` → `LeaseStoreError::PreconditionFailed`, `AlreadyExists` → `LeaseStoreError::AlreadyExists`. See `celeriant_distributed` README for `LeaseStore` trait details.

#### SidecarS3Uploader / SidecarS3Downloader

Implement `S3Uploader` and `S3Downloader` from `celeriant_shard`. Routed over the data lane. `AlreadyExists` on upload is treated as OK (crash-restart idempotency). See `celeriant_shard` README for trait details.

### Graceful shutdown

Shard 0 polls `SignalHandler` for SIGINT/SIGTERM. On signal:
1. Set local `shutdown_requested` flag
2. Broadcast `Shutdown` to all shards via mesh
3. Each shard stops accepting, finishes active requests, closes WAL

## IntrashardMessages

| Variant | Direction | Purpose |
|---------|-----------|---------|
| `Shutdown` | Shard 0 → all | Graceful shutdown after signal |
| `ConnectionRedirect { accepted_tcp_stream, request, message_version, port_type }` | Any → target | Reroute connection to correct shard |
| `EnterS3Catchup` | Shard 0 → all | Trigger S3 catchup on each shard |
| `S3CatchupComplete { shard_id, result }` | Any → shard 0 | Report catchup outcome to boot orchestrator |
| `StatusUpdate { status: ValidatedNodeStatus }` | Shard 0 → all | Propagate cluster role/TTL changes |
| `UpdateFollower { replication_address }` | Shard 0 → all | Propagate current follower replication address |
| `UpdateLeaderClientAddress { client_address }` | Shard 0 → all | Propagate leader client address for follower redirect |
