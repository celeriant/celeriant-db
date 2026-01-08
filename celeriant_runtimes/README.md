# celeriant_runtimes

Runtime orchestration for sharded executors and sidecar I/O. Manages thread-per-core glommio executors, inter-shard routing, and bridges to tokio for external operations.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Glommio Executor Pool                        │
│  ┌─────────────┐  ┌─────────────┐       ┌─────────────┐         │
│  │  Shard 0    │  │  Shard 1    │  ...  │  Shard N    │         │
│  │ TcpListener │  │ TcpListener │       │ TcpListener │         │
│  │  ShardWal   │  │  ShardWal   │       │  ShardWal   │         │
│  │SignalHandler│  │             │       │             │         │
│  └──────┬──────┘  └──────┬──────┘       └──────┬──────┘         │
│         └────────────────┼─────────────────────┘                │
│                   Channel Mesh (Full)                           │
│                  IntrashardMessages                             │
└──────────────────────────┬──────────────────────────────────────┘
                           │ flume channels
┌──────────────────────────┴──────────────────────────────────────┐
│                    Tokio Sidecar Runtime                        │
│  ┌────────────────┐  ┌────────────────┐                         │
│  │  Control Lane  │  │   Data Lane    │                         │
│  │(leases, member)│  │(batch uploads) │                         │
│  └───────┬────────┘  └───────┬────────┘                         │
│          └───────────────────┘                                  │
│                 SidecarStoreTrait                               │
└─────────────────────────────────────────────────────────────────┘
```

## Key Types

| Type | Purpose |
|------|---------|
| `Shard` | Per-core executor: TCP accept, routing, request processing |
| `ShardConfig` | Node-level configuration (paths, timeouts, routing) |
| `RoutingRule` | Aggregate-to-shard mapping strategy (OrgId, AggregateTypeId, AggregateId) |
| `IntrashardMessages` | Inter-shard communication (Shutdown, ConnectionRedirect) |
| `SignalHandler` | SIGINT/SIGTERM handling via atomic flags |
| `SidecarRuntime` | Tokio runtime for external I/O (S3, object storage) |
| `SidecarSenders` | glommio → tokio channel handles with QoS lanes |
| `SidecarConfig` | Thread pool and queue capacity configuration |
| `ShardRoutingError` | Routing failures (no key, multiple shards, incompatible filters) |

## Key Functions

| Function | Purpose |
|----------|---------|
| `run_executors_and_sidecar` | Main entry: spawn mesh, sidecar, and executor pool |
| `new_sidecar` | Create sidecar runtime with store implementation |
| `Shard::new` | Initialize shard with config, channels, WAL |
| `Shard::run` | Main loop: accept connections, handle messages |
| `check_for_shard_redirect` | Route request to correct shard or process locally |
| `determine_shard_route` | Compute target shard from request and routing rule |
| `handle_request_and_further_pipelining` | Process request, support HTTP/1.1-style pipelining |
| `SidecarSenders::send_async` | Send request to sidecar, await response |

## Design Decisions

### Thread-per-core model

One glommio executor per CPU core. Each shard owns a subset of aggregates. No locks on hot path. `Rc<Cell<_>>` for flags, `Rc<RefCell<_>>` for state.

### Shard routing

| Rule | Routes By | Use Case |
|------|-----------|----------|
| `OrgId` | `org_id % num_shards` | Multi-tenant isolation |
| `AggregateTypeId` | `aggregate_type_id % num_shards` | Type locality |
| `AggregateId` | `aggregate_id % num_shards` | Even distribution (default) |

### Connection redirect

Clients connect to any shard. First request is read, routing computed. If wrong shard, `AcceptedTcpStream` + request sent via channel mesh. Target shard binds stream and continues processing.

```rust
IntrashardMessages::ConnectionRedirect {
    accepted_tcp_stream,  // Transferable socket
    request,              // Already-parsed first request
    message_version,
}
```

### Multi-aggregate write routing

All aggregates in a write must hash to the same shard:

```rust
for key in write_request.writes.keys() {
    shard_ids.insert(routing_rule.routing_id_for_rule(key) % num_shards);
}
if shard_ids.len() > 1 { return Err(MultipleShardRoutes); }
```

### Watch request routing

Watch filters must match routing rule. If routing by `OrgId`, `orgs` filter required. Ensures watch request routes to exactly one shard.

### Sidecar bridge

Tokio runtime for operations incompatible with io_uring (S3, HTTP clients). Two QoS lanes:

| Lane | Operations | Capacity |
|------|------------|----------|
| Control | Leases, membership | Lower capacity, higher priority |
| Data | Batch uploads/downloads | Higher capacity |

Flume bounded channels with oneshot response pattern:

```rust
let (response_tx, response_rx) = flume::bounded(1);
tx.send_async(SidecarRequest { response_tx, .. }).await?;
response_rx.recv_async().await?
```

### Graceful shutdown

Shard 0 polls `SignalHandler` for SIGINT/SIGTERM. On signal:
1. Set local `shutdown_requested` flag
2. Broadcast `Shutdown` to all shards via mesh
3. Each shard stops accepting, finishes active requests, closes WAL

### Pipelining support

After processing a request, connection reads next request with timeout. Subsequent requests may route to different shards—redirect check runs again.

### Nagle disabled

`set_nodelay(true)` on all TCP streams for lower latency.

## Usage

```rust
use celeriant_runtimes::{run_executors_and_sidecar, ShardConfig, SidecarConfig, RoutingRule};

let shard_config = ShardConfig {
    node_id: 1,
    num_shards: 16,
    data_root: "/data".into(),
    listen_address: "0.0.0.0:10000".into(),
    routing_rule: RoutingRule::AggregateId,
    slow_client_timeout: Duration::from_secs(30),
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
    1024,  // mesh channel size
    node_id,
    my_store,  // impl SidecarStoreTrait
);
```

## Error Handling

| Error | Code | Cause |
|-------|------|-------|
| `NoRoutingKeyProvided` | 400 | Watch missing required filter for routing rule |
| `MultipleShardRoutes` | 400 | Write/delete spans multiple shards |
| `IncompatibleFilters` | 400 | Filter doesn't match routing rule |
| `AggregateNotExists` | 404 | Aggregate not found |
| `OptimisticConcurrencyViolation` | 409 | OCC check failed |
| `ClientIdempotencyViolation` | 409 | Duplicate client_event_index |
| `IoError` | 500 | Disk/network failure |

## Dependencies

| Crate | Purpose |
|-------|---------|
| `celeriant_shard` | ShardWal, error types |
| `celeriant_sidecar` | Store trait, requests |
| `celeriant_msg` | Request/response types |
| `celeriant_wire` | Wire errors |
| `celeriant_watch` | Watch session types |
| `celeriant_wal` | AggregateKey, compression |
| `glommio` | io_uring async runtime, channel mesh |
| `tokio` | Sidecar async runtime |
| `flume` | Cross-runtime bounded channels |
| `signal-hook` | SIGINT/SIGTERM via atomic flags |