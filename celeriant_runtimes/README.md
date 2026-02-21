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

## Key Functions

| Function | Purpose |
|----------|---------|
| `run_executors_and_sidecar` | Main entry: spawn mesh, sidecar, dual listeners, boot orchestration |
| `new_sidecar` | Create sidecar runtime with store implementation |
| `Shard::new` | Initialize shard with config, channels, WAL, optional lease manager |
| `Shard::run` | Main loop: accept connections, handle intrashard messages, run boot orchestrator on shard 0 |
| `determine_shard` | Compute target shard from request type and routing rule |
| `handle_new_connection` | Accept TCP stream, read first request, redirect or process |
| `handle_heartbeat` | Validate clock drift, refresh TTL on all local shards via intrashard broadcast |
| `handle_kick_follower` | Transition to `FollowerCatchingUp`, broadcast to all shards |
| `spawn_boot_orchestrator` | Shard 0 only: S3 catchup → election → leader/follower loop |
| `run_leader_loop` | Discover follower, heartbeat, renew S3 lease on failure |
| `run_follower_watchdog` | Monitor TTL, race to S3 when leader presumed dead |
| `run_kick_catchup` | Coordinated S3 catchup after KickFollower |
| `SidecarSenders::send_async` | Send request to sidecar via QoS lane, await oneshot response |

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

Redirect check runs again on pipelined requests — a session may migrate shards mid-pipeline.

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
2. register_self() — write node into membership file
3. run_election() — CAS the S3 lease to become leader or follower
4. Post-election S3 catchup (get any writes missed during election)
5. Broadcast ValidatedNodeStatus to all shards
6. Enter role loop: Leader | Follower | FollowerCatchingUp
```

See `celeriant_distributed` README for `LeaseManager`, `ValidatedNodeStatus`, and `LeaseStore` details.

#### Leader steady-state (`run_leader_loop`)

1. Discover follower via `discover_peer()` (with exponential backoff, capped at `initial_lease_duration / 2`)
2. Heartbeat follower every `heartbeat_interval`
3. On heartbeat success: refresh `ValidatedNodeStatus` TTL, broadcast `StatusUpdate` to all shards
4. On heartbeat failure: `renew_s3_lease_and_broadcast()` — CAS-promote S3 lease for fresh TTL
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

## ShardConfig fields

| Field | Purpose |
|-------|---------|
| `node_id` | Unique u128 node identifier |
| `num_shards` | Number of glommio executors / shards |
| `replication_config` | Optional `ReplicationConfig`; `None` = standalone mode |
| `advertised_replication_address` | Address registered in membership for inbound replication |
| `listen_address` | Bind IP |
| `client_port` | Client TCP port |
| `replication_port` | Replication TCP port |
| `data_root` | WAL root directory; shard subdirs created automatically |
| `routing_rule` | `OrgId` / `AggregateTypeId` / `AggregateId` |
| `slow_client_timeout` | Read/write timeout per request |
| `max_requested_latency` | Cap on watch `requested_latency_ms` |
| `fsync_delay` | Amortised fsync batch window |
| `replication_delay` | Replication batch accumulation window |
| `internode_connection_timeout` | TCP connect timeout to follower |
| `internode_request_timeout` | Request/response timeout to follower |
| `max_cluster_time_drift_ms` | Clock skew tolerance before fencing |
| `max_catchup_gap_bytes` | Max WAL gap before S3 fallback on catchup |
| `max_s3_fallback_batch_bytes` | Max bytes per S3 fallback batch |
| `s3_download_max_rounds` | Max S3 listing rounds during catchup |
| `pending_replication_high_water_bytes` | Backpressure threshold for replication queue |
| `max_open_files` | LRU open file descriptor limit per shard |
| `read_max_chunk_size` | Max bytes per read response chunk |
| `write_max_chunk_size` | Max bytes per write batch |
| `max_request_size` / `max_response_size` | Wire message size limits |
| `server_compression_algorithm` | Compression for response payloads |
| `shard_log_preallocate_bytes` | WAL log segment pre-allocation size |
| `recent_write_cache_bytes` | LRU write cache per shard |
| `aggregate_client_snapshots_cache_bytes` | LRU client snapshot cache |
| `aggregate_snapshots_cache_bytes` | LRU aggregate snapshot cache |
| `list_wal_index_cache_bytes` | LRU WAL index cache for listing |
| `list_max_duration` | Max time per list scan |
| `list_page_size` | Items per list page |
| `timestamp_config` | Timestamp precision and epoch offset |

## Error Codes

All errors are returned as `GenericError { error_code, error_message }` where `error_message` is a JSON object. Codes are stable cross-language identifiers.

| Range | Domain |
|-------|--------|
| 1xxx | Read |
| 2xxx | Write |
| 3xxx | Trim |
| 4xxx | Delete |
| 5xxx | Listing |
| 6xxx | Replication batch (follower) |
| 7xxx | Aggregate details (exists) |
| 8xxx | Watch |
| 9xxx | Catch-up |

| Code | Name | Cause |
|------|------|-------|
| 1000 | `READ_UNAVAILABLE_BATCH_INDEX` | Requested index below trim point |
| 1001 | `READ_AGGREGATE_NOT_EXISTS` | Aggregate not found |
| 1002/1003 | `READ_CACHE_LOAD_*` | Cache lock timeout or file scan error |
| 1004 | `READ_FETCH_DATABLOCKS` | Datablock read failure |
| 1005 | `READ_FETCH_METABLOCKS` | Metablock read failure |
| 2000 | `WRITE_EMPTY_EVENTS_LIST` | Write with no events |
| 2001 | `WRITE_ZERO_EVENT_TYPE` | Event type ID is 0 |
| 2002 | `WRITE_CLIENT_IDEMPOTENCY_VIOLATION` | Duplicate client_event_index |
| 2003 | `WRITE_OPTIMISTIC_CONCURRENCY_VIOLATION` | OCC check failed |
| 2004 | `WRITE_FAILED_TO_SERIALISE_DATABLOCKS` | Serialization error |
| 2005 | `WRITE_AGGREGATE_NOT_EXISTS` | Create not allowed, aggregate missing |
| 2006 | `WRITE_AGGREGATE_RECREATE_NOT_ALLOWED` | Deleted aggregate, recreate disallowed |
| 2007 | `WRITE_REPLICATION_ERROR` | Replication failure |
| 2008 | `WRITE_FSYNC_ERROR` | fsync failure |
| 2009/2010 | `WRITE_CACHE_*` | Cache errors during write |
| 2011 | `WRITE_CANNOT_ACCEPT_WRITES` | Node not leader; includes `leader_address` hint |
| 3000–3005 | Trim errors | Not exists, cache, replication, fsync, index range, not leader |
| 4000–4006 | Delete errors | Not exists, empty, OCC, cache, replication, fsync, not leader |
| 5000–5002 | Listing errors | Disk read errors for org/type/aggregate listing |
| 6000 | `REPLICATION_BATCH_FSYNC` | Follower fsync failure |
| 6001 | `REPLICATION_BATCH_SERIALISE_DATABLOCKS` | Follower serialization failure |
| 6002 | `REPLICATION_BATCH_WAL_INDEX_GAP` | WAL index gap on follower |
| 7000 | `EXISTS_CACHE_ERROR` | Cache error in aggregate details |
| 7001 | `EXISTS_AGGREGATE_NOT_EXISTS` | Aggregate not found |
| 7002 | `EXISTS_METABLOCK_READ_ERROR` | Metablock read failure |
| 8000 | `WATCH_REQUEST_INVALID` | Invalid watch request |
| 8001 | `WATCH_LATENCY_TOO_HIGH` | `requested_latency_ms` exceeds `max_requested_latency` |
| 8002–8004 | `WATCH_READ_*` | Watch stream IO, serialization, or other error |
| 9000 | `CATCHUP_REQUEST_INVALID` | Invalid catch-up request |

Routing errors (NoRoutingKeyProvided, MultipleShardRoutes, IncompatibleFilters) and port validation errors return HTTP-style 400 with a plain text message, not a structured code.

## Usage

```rust
use celeriant_runtimes::{run_executors_and_sidecar, ShardConfig, SidecarConfig, RoutingRule};

let shard_config = ShardConfig {
    node_id: 1,
    num_shards: 16,
    replication_config: None,          // standalone; Some(ReplicationConfig) for cluster
    advertised_replication_address: None,
    data_root: "/data".into(),
    listen_address: "0.0.0.0".into(),
    client_port: 10000,
    replication_port: 10001,
    routing_rule: RoutingRule::AggregateId,
    slow_client_timeout: Duration::from_secs(30),
    max_requested_latency: Duration::from_millis(500),
    fsync_delay: Duration::from_millis(5),
    // ... other fields
};

let sidecar_config = SidecarConfig {
    worker_threads: 4,
    control_lane_capacity: 1000,
    data_lane_capacity: 10000,
};

run_executors_and_sidecar(
    shard_config,
    sidecar_config,
    1024,        // mesh channel size
    node_id,
    my_store,    // impl SidecarStoreTrait
);
```

## Dependencies

| Crate | Purpose |
|-------|---------|
| `celeriant_shard` | `ShardWal`, `ReplicationClient`, `S3Uploader`, `S3Downloader`, error types |
| `celeriant_distributed` | `LeaseManager`, `LeaseStore`, `ValidatedNodeStatus`, `ReplicationConfig` |
| `celeriant_sidecar` | `SidecarStoreTrait`, request/response types |
| `celeriant_msg` | Request/response wire types |
| `celeriant_wire` | Wire errors, versioned block serialization |
| `celeriant_watch` | `WatchSession`, `WatchOutputType` |
| `celeriant_wal` | `AggregateKey`, `CompressionType`, S3 paths/constants |
| `glommio` | io_uring async runtime, channel mesh |
| `tokio` | Sidecar async runtime |
| `flume` | Cross-runtime bounded channels |
| `signal-hook` | SIGINT/SIGTERM via atomic flags |
