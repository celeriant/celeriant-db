# celeriant_client_tokio

Async TCP/TLS client for Celeriant using the Tokio runtime. Provides a single-connection client, a topology-aware connection pool with leader routing and failover, streaming iterators, and real-time watch connections.

## Architecture

```
                       ┌──────────────────────────────────────────────┐
                       │              CeleriantPool                   │
                       │  topology-aware, leader routing, failover    │
                       │  per-node connection pooling & idle eviction │
                       └───────────┬──────────────────────────────────┘
                                   │ get_connection() / get_leader_connection()
                                   ▼
                       ┌──────────────────────────────────────────────┐
Caller ──connect()──>  │           CeleriantClient                    │
                       │  single TCP/TLS connection (RAII)            │
                       │  auto-compression, request/response timeout  │
                       └───────────┬──────────────────────────────────┘
                                   │ send_request(ClientRequest, CompressionType)
                                   ▼
                       ┌──────────────────────┐    ┌──────────────────────────┐
                       │  celeriant_wire      │    │  celeriant_msg           │
                       │  (framing/codec)     │───>│  (Request / Response)    │
                       └──────────────────────┘    └──────────────────────────┘

Iterators (borrow &mut CeleriantClient or hold PooledConnection):
  ReadAllIterator              - all event batches for one aggregate (paginated)
  ListOrgsIterator             - orgs across shards
  ListAggregateTypesIterator   - aggregate types across shards
  ListAggregatesIterator       - aggregates with merged stats

Watch:
  WatchConnection              - single or multi-shard real-time watch stream
```

## Invariants

- TCP_NODELAY is always set. Nagle buffering is never permitted.
- Protocol version is set on the first message (Identify or first ClientRequest). No renegotiation.
- Pool idle timeout (25s) must be shorter than the server's `slow_client_timeout` (30s) to prevent server-side disconnects.
- Broken connections are discarded, never returned to the pool.
- `identify()` handshake runs after connection establishment and before data operations.
- Write operations route to the leader. On `NotLeader` redirect, the leader cache is updated and the request retried once at the new address.

## Key Types

| Type | Purpose |
|------|---------|
| `CeleriantClient` | Single TCP/TLS connection; RAII-managed |
| `CeleriantPool` | Topology-aware pool with leader routing, failover, and idle eviction |
| `PoolOptions` | Pool configuration (timeouts, TLS, identity, compression, routing) |
| `PooledConnection` | Borrowed connection; returns on drop, discards if broken |
| `CeleriantPoolApi` | Trait abstracting pool operations for testing/mocking |
| `ClientTlsConfig` | TLS connector + server name |
| `ClientIdentityConfig` | Public/private key pair or API key for identity verification |
| `ClientError` | Client-level error enum; promotes `NotLeader`, `IdentityRequired`, `ServerBusy` for routing |
| `ServerError` | Strongly-typed server error with operation-specific variants |
| `WriteEventsOptions` | Idempotency, optimistic concurrency, and client_id for writes |
| `WatchConnection` | Real-time watch stream (single-shard or multi-shard) |
| `WatchOptions` | Watch configuration (compression, timeout, shard hints, TLS, identity) |
| `ListOptions` | Shared config for list iterators |
| `ReadAllIterator<'a>` | Streaming paginated reads for one aggregate |
| `ListOrgsIterator<'a>` | Streaming orgs across all shards |
| `ListAggregateTypesIterator<'a>` | Streaming aggregate types across all shards |
| `ListAggregatesIterator<'a>` | Streaming aggregates with merged stats |

## Design Decisions

### Two-tier client: raw connection + topology-aware pool

```rust
// Direct single-connection usage
let mut client = CeleriantClient::connect("127.0.0.1:10000").await?;
client.write(request).await?;

// Pool with leader routing, failover, and connection management
let pool = CeleriantPool::new(PoolOptions::new("127.0.0.1:10000")
    .with_seed_addresses(vec!["127.0.0.1:10001".into()])
    .with_identity(identity_config));
pool.write(request).await?;  // automatically routes to leader
```

`CeleriantClient` is a minimal single-connection primitive. `CeleriantPool` adds leader discovery, read distribution, failover, connection lifecycle, and identity management on top.

### Connection pool: per-node pooling with semaphore-based concurrency control

```rust
pub struct PoolOptions {
    pub max_connections_per_node: usize,  // hard cap via semaphore (default: 10)
    pub idle_timeout: Duration,           // evict stale connections (default: 25s)
    pub route_reads_to_followers: bool,   // distribute reads away from leader
    pub max_leader_retries: usize,        // seed nodes to try on leader miss (default: 3)
    ...
}
```

Each node gets its own pool bounded by a `Semaphore`. Idle connections are evicted on checkout (oldest-first). Broken connections are discarded rather than returned. The idle timeout (25s) is intentionally shorter than the server's `slow_client_timeout` (30s) to prevent server-side disconnects.

### Leader routing and failover

Write operations use a `leader_route!` macro that:
1. Tries the cached leader (or primary address)
2. On `NotLeader { Some(addr) }` — updates the leader cache and retries once at the new address
3. On `NotLeader { None }` / `ConnectionFailed` / `ServerBusy` — iterates seed addresses up to `max_leader_retries`

Read operations use a `read_route!` macro with round-robin across read-eligible nodes. When `route_reads_to_followers` is enabled, the leader is excluded from read candidates.

### TLS support

```rust
pub(crate) enum ClientStream {
    Plain(Compat<TcpStream>),
    Tls(Compat<tokio_rustls::client::TlsStream<TcpStream>>),
}
```

Connections are either plaintext or TLS. The `ClientStream` enum dispatches `AsyncRead`/`AsyncWrite` at the type level. TLS uses `tokio-rustls` with a shared `ClientConfig` via `Arc`.

### Client identity verification

Two authentication modes: public/private key pair (nonce-based challenge) or API key. The `identify()` handshake runs after connection establishment and before data operations. The pool handles this automatically for new connections.

### TCP_NODELAY

Set on connect to disable Nagle's algorithm. Celeriant is a request/response protocol where latency matters more than packet coalescing.

### Auto-compression

Write and schema registration requests automatically compress payloads that exceed `auto_compression_threshold` (default: 1024 bytes) using the configured compression algorithm (default: Zstd level 3). Payloads below the threshold are sent uncompressed to avoid overhead on small messages.

### Watch connections: single-shard and multi-shard

`WatchConnection::connect` probes the server with a shard-unscoped watch request. If the server responds with a shard routing error indicating multiple shards, the connection automatically fans out to one connection per shard. All shard streams are multiplexed into a single `mpsc` channel via per-shard Tokio tasks.

If `max_shard_hint` is provided, the multi-shard path is taken directly without probing.

### ReadAllIterator: paginated streaming reads

Follows `next_aggregate_version` cursors automatically, buffering event batches and yielding them one at a time. Exhaustion is detected when the server returns no next cursor.

### Shard discovery via probing

List iterators don't require callers to know the shard count upfront. They probe shards incrementally and stop when a `ShardRoutingError` is received on a fresh shard (cursor = None). Callers who know the shard range can pass `max_shard_hint` to skip discovery overhead.

### Round-robin pagination across shards

```
active_shards: VecDeque<u64>
shard_cursors: HashMap<u64, Option<u64>>
```

Each `fetch_next_page` call pops the front shard, fetches one page, then pushes it back if more pages remain. A new shard is added on each fetch to keep the pipeline moving. This interleaves data from all shards evenly.

### Deduplication across pages and shards

Orgs and aggregate types use `HashSet` keyed on their ID to drop duplicates that appear on multiple shards. `ListAggregatesIterator` goes further: it accumulates stats from all occurrences of an aggregate into a single `AggregateStats` via `merge()`.

### Stats merging in ListAggregatesIterator

An aggregate can appear on multiple shards (replicated or migrated). `AggregateStats::merge` combines entries:

| Field | Strategy |
|-------|----------|
| `event_batch_count` | Sum |
| `compressed_size`, `uncompressed_size` | Sum |
| `min_*` | Min, treating 0 as "no data" |
| `max_*` | Max |
| `is_deleted` | OR (deleted on any shard = deleted) |

### Strongly-typed server errors

Server error responses are parsed into operation-specific enums (`ReadError`, `WriteError`, `SchemaError`, `DeleteError`, `TrimError`, `WatchError`, `DetailsError`, `AuthError`) rather than exposing raw error codes. Each variant carries structured context (e.g., `OptimisticConcurrencyViolation` includes both expected and current batch indices). Three `ClientError` variants are promoted for routing: `NotLeader`, `IdentityRequired`, and `ServerBusy`.
