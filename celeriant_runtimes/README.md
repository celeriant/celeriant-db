# celeriant_runtimes

Runtime orchestration for sharded executors and sidecar I/O. Manages thread-per-core glommio executors, inter-shard routing, and bridges to tokio for external operations (S3, object storage).

**README WAS LLM GENERATED AND HUMAN REVIEWED [2025-12-30]**

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Glommio Executor Pool                             │
│  ┌───────────────┐  ┌───────────────┐       ┌───────────────┐               │
│  │   Shard 0     │  │   Shard 1     │  ...  │   Shard N     │               │
│  │  TcpListener  │  │  TcpListener  │       │  TcpListener  │               │
│  │  ShardWal     │  │  ShardWal     │       │  ShardWal     │               │
│  │  SignalHandler│  │               │       │               │               │
│  └───────┬───────┘  └───────┬───────┘       └───────┬───────┘               │
│          │                  │                       │                       │
│          └──────────────────┼───────────────────────┘                       │
│                             │ Channel Mesh (Full)                           │
│                    IntrashardMessages                                       │
└─────────────────────────────┼───────────────────────────────────────────────┘
                              │
                    flume channels (bounded)
                              │
┌─────────────────────────────┼───────────────────────────────────────────────┐
│                    Tokio Sidecar Runtime                                    │
│  ┌─────────────────────────┴─────────────────────────┐                      │
│  │              SidecarRuntime                       │                      │
│  │  ┌─────────────────┐  ┌─────────────────┐         │                      │
│  │  │  Control Lane   │  │   Data Lane     │         │                      │
│  │  │ (leases, member)│  │ (batch uploads) │         │                      │
│  │  └────────┬────────┘  └────────┬────────┘         │                      │
│  │           └────────────────────┘                  │                      │
│  │                    │                              │                      │
│  │           SidecarStoreTrait                       │                      │
│  │           (S3, local, etc.)                       │                      │
│  └───────────────────────────────────────────────────┘                      │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Key Types

| Type | Purpose |
|------|---------|
| `Shard` | Per-core executor: TCP accept, routing, request processing |
| `ShardConfig` | Node-level configuration (paths, timeouts, routing) |
| `RoutingRule` | Aggregate-to-shard mapping strategy |
| `IntrashardMessages` | Inter-shard communication (shutdown, redirect) |
| `SignalHandler` | SIGINT/SIGTERM handling for graceful shutdown |
| `SidecarRuntime` | Tokio runtime for external I/O operations |
| `SidecarSenders` | glommio → tokio channel handles |
| `SidecarConfig` | Sidecar thread pool and queue configuration |

## Shard Routing

Routing determines which shard owns an aggregate based on `RoutingRule`:

| Rule | Routes By | Use Case |
|------|-----------|----------|
| `OrgId` | `aggregate_key.org_id % num_shards` | Multi-tenant isolation |
| `AggregateTypeId` | `aggregate_key.aggregate_type_id % num_shards` | Type locality |
| `AggregateId` | `aggregate_key.aggregate_id % num_shards` | Even distribution (default) |

### Routing Flow

```
Client connects to any shard
         │
         ▼
┌─────────────────────────┐
│  Read first request     │
│  Extract routing key    │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐     Yes    ┌─────────────────────────┐
│ routing_id % num_shards │───────────►│   Process locally       │
│ == current_shard_id?    │            └─────────────────────────┘
└───────────┬─────────────┘
            │ No
            ▼
┌─────────────────────────┐
│  Send ConnectionRedirect│
│  via channel mesh       │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│  Target shard receives  │
│  AcceptedTcpStream +    │
│  first request          │
└─────────────────────────┘
```

### Multi-Aggregate Write Routing

To perform multi-aggregate atomic writes with OCC, aggregates that are part of a write must all be on the same shard:

```rust
// All aggregates must hash to same shard
for aggregate_key in write_request.writes.keys() {
    let routing_id = config.routing_rule.routing_id_for_rule(aggregate_key);
    let shard_id = (routing_id % num_shards) as usize;
    shard_ids.insert(shard_id);
}

if shard_ids.len() > 1 {
    return Err(ShardRoutingError::MultipleShardRoutes);
}
```

### Watch Request Routing

Watch requests must specify filters compatible with the routing rule:

| Routing Rule | Required Watch Filter |
|--------------|----------------------|
| `OrgId` | `orgs` must be specified |
| `AggregateTypeId` | `aggregate_types` must be specified |
| `AggregateId` | `aggregates` must be specified |

## Connection Lifecycle

```
TcpListener::shared_accept()
         │
         ▼
┌─────────────────────────┐
│  set_nodelay(true)      │  ← Disable Nagle for latency
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│  read_client_request()  │  ← With slow_client_timeout
└───────────┬─────────────┘
            │
            ▼
┌───────────────────────────┐
│  check_for_shard_redirect │
└───────────┬───────────────┘
            │
            ▼
┌─────────────────────────────────────────┐
│  handle_request_and_further_pipelining  │
│  ┌─────────────────────────────────────┐│
│  │  Watch? → handle_watch_request      ││
│  │  Other? → process_client_request    ││
│  │  Read next request (pipelining)     ││
│  │  Check redirect again               ││
│  │  Loop until disconnect/shutdown     ││
│  └─────────────────────────────────────┘│
└─────────────────────────────────────────┘
```

## Inter-Shard Communication

Full mesh topology via `glommio::channels::channel_mesh`:

```rust
pub enum IntrashardMessages {
    /// Broadcast from shard 0 on SIGINT/SIGTERM
    Shutdown,
    
    /// Redirect client connection to correct shard
    ConnectionRedirect {
        accepted_tcp_stream: AcceptedTcpStream,
        request: Request,
        message_version: u32,
    }
}
```

## Graceful Shutdown

```
SIGINT/SIGTERM received
         │
         ▼ (shard 0 only)
┌─────────────────────────┐
│  SignalHandler detects  │
│  signal via atomic flag │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│  Set shutdown_requested │
│  Broadcast to all shards│
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│  Each shard:            │
│  • Stop accepting new   │
│  • Finish active reqs   │
│  • Close ShardWal       │
└─────────────────────────┘
```

## Sidecar Architecture

Bridges glommio (io_uring) to tokio for external I/O that can't use direct I/O:

### QoS Lanes

| Lane | Operations | Purpose |
|------|------------|---------|
| Control | Leases, membership | Low-latency, high-priority |
| Data | Batch uploads/downloads | Higher throughput, can queue |

### Channel Bridge

```rust
// From glommio shard
let response = sidecar_senders.send_async(
    SidecarTarget::ControlPlaneLease,
    store_request,
).await?;

// Inside sidecar (tokio)
while let Ok(request) = rx.recv_async().await {
    tokio::spawn(async move {
        let response = sidecar_store.process_request(request.store_request).await;
        let _ = request.response_tx.send(response);
    });
}
```

## Configuration

### ShardConfig

| Field | Purpose |
|-------|---------|
| `node_id` | Unique node identifier |
| `num_shards` | Number of shards (typically = CPU cores) |
| `data_root` | Base directory for shard data |
| `listen_address` | TCP bind address (shared across shards) |
| `routing_rule` | Aggregate-to-shard mapping |
| `slow_client_timeout` | Max time for client read/write ops |
| `max_requested_latency` | Max watch latency clients can request |
| `fsync_delay` | Amortisation window for durability |
| `non_durable_writes` | Ack before fsync (higher throughput) |

### SidecarConfig

| Field | Purpose |
|-------|---------|
| `worker_threads` | Tokio thread pool size |
| `control_lane_capacity` | Bounded queue for control ops |
| `data_lane_capacity` | Bounded queue for data ops |

## Error Handling

### ShardRoutingError

| Error | Cause |
|-------|-------|
| `NoRoutingKeyProvided` | Watch request missing required filter |
| `MultipleShardRoutes` | Write spans multiple shards |
| `IncompatibleFilters` | Filter doesn't match routing rule |

### Error Response Mapping

| ShardError | HTTP-like Code | Meaning |
|------------|----------------|---------|
| `AggregateNotExists` | 404 | Aggregate not found |
| `OptimisticConcurrencyViolation` | 409 | OCC conflict |
| `ClientIdempotencyViolation` | 409 | Duplicate client_event_index |
| `EmptyEventsList` | 400 | Write with no events |
| `IoError` | 500 | Disk/network failure |

## Entry Point

```rust
pub fn run_executors_and_sidecar<S: SidecarStoreTrait>(
    shard_config: ShardConfig,
    sidecar_config: SidecarConfig,
    mesh_channel_size: usize,
    node_id: u128,
    sidecar_store: S,
) {
    // 1. Create full mesh for inter-shard messages
    let mesh = MeshBuilder::<IntrashardMessages, Full>::full(num_shards, mesh_channel_size);
    
    // 2. Start sidecar on tokio runtime
    let (sidecar_senders, _sidecar_runtime) = new_sidecar(sidecar_config, sidecar_store)?;
    
    // 3. Spawn glommio executors, one per shard
    LocalExecutorPoolBuilder::new(PoolPlacement::MaxSpread(num_shards, CpuSet::online()))
        .on_all_shards(|| async {
            // Join mesh, bind TCP, open ShardWal, run shard loop
        })
        .join_all();
}
```

## Thread Model

| Component | Runtime | Threading |
|-----------|---------|-----------|
| Shards | glommio | One executor per CPU core |
| TCP accept | glommio | `shared_accept()` across all shards |
| ShardWal | glommio | Single-threaded per shard |
| Sidecar | tokio | Multi-threaded pool |
| Channels | flume | MPMC bounded queues |

## Dependencies

- `celeriant_shard` - ShardWal and error types
- `celeriant_memcache` - InternalShardConfig
- `celeriant_sidecar` - Store trait and requests
- `celeriant_msg` - Request/response types
- `celeriant_wire` - Wire errors
- `celeriant_watch` - Watch session types
- `glommio` - io_uring async runtime
- `tokio` - Sidecar async runtime
- `flume` - Cross-runtime channels
- `signal-hook` - SIGINT/SIGTERM handling