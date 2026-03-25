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

## Key Types

| Type | Purpose |
|------|---------|
| `CeleriantClient` | Single TCP/TLS connection; RAII-managed |
| `CeleriantPool` | Topology-aware connection pool with leader routing and failover |
| `PoolOptions` | Pool configuration (timeouts, TLS, identity, compression, routing) |
| `PooledConnection` | Borrowed connection from pool; returns on drop, discards if broken |
| `CeleriantPoolApi` | Trait abstracting pool operations for testing/mocking |
| `ClientTlsConfig` | TLS connector + server name for encrypted connections |
| `ClientIdentityConfig` | Public/private key pair or API key for client identity verification |
| `ClientError` | Error enum for client-level failures |
| `ServerError` | Strongly-typed server error with operation-specific variants |
| `WriteEventsOptions` | Options for write_events: idempotency, optimistic concurrency, client_id |
| `WatchConnection` | Real-time watch stream (single-shard or multi-shard) |
| `WatchOptions` | Watch configuration (compression, timeout, shard hints, TLS, identity) |
| `ListOptions` | Shared config for list iterators (compression, deleted, shard hints) |
| `ReadAllIterator<'a>` | Streaming iterator over all event batches for an aggregate |
| `ListOrgsIterator<'a>` | Streaming iterator over orgs across all shards |
| `ListAggregateTypesIterator<'a>` | Streaming iterator over aggregate types across all shards |
| `ListAggregatesIterator<'a>` | Streaming iterator over aggregates with merged stats |
| `AggregateStats` | Merged stats for one aggregate (event counts, timestamps, sizes) |
| `Pooled{ReadAll,ListOrgs,...}Iterator` | Pool-aware iterator variants that own the connection |

## Key Functions

| Function | Purpose |
|----------|---------|
| `CeleriantClient::connect` | Connect without timeout (plaintext) |
| `CeleriantClient::connect_tls` | Connect with TLS |
| `CeleriantClient::connect_with_timeout` | Connect with optional timeout and optional TLS |
| `CeleriantClient::send_request` | Send `ClientRequest`, receive `ClientResponse`; applies timeout if set |
| `CeleriantClient::identify` | Perform client identity verification handshake |
| `CeleriantClient::read` | Read event batches for an aggregate |
| `CeleriantClient::write` | Write events (auto-compresses above threshold) |
| `CeleriantClient::write_events` | Convenience: write events to a single aggregate |
| `CeleriantClient::write_events_with` | Write events with idempotency/OCC options |
| `CeleriantClient::delete` | Delete aggregates |
| `CeleriantClient::trim_start` | Trim event batches from start of an aggregate |
| `CeleriantClient::aggregate_details` | Get aggregate metadata |
| `CeleriantClient::register_schema` | Register a schema (auto-compresses above threshold) |
| `CeleriantPool::new` | Create a pool from `PoolOptions` |
| `CeleriantPool::read` | Read with round-robin across read-eligible nodes |
| `CeleriantPool::write` | Write with leader routing and failover |
| `CeleriantPool::watch` | Create a dedicated watch connection |
| `CeleriantPool::read_all` | Create a pooled streaming read-all iterator |
| `CeleriantPool::list_orgs` | Create a pooled streaming list-orgs iterator |
| `CeleriantPool::list_aggregates` | Create a pooled streaming list-aggregates iterator |
| `WatchConnection::connect` | Connect and establish watch (auto-discovers multi-shard) |
| `WatchConnection::next` | Read next watch response |
| `WatchConnection::next_timeout` | Read next watch response with timeout |
| `json_event` | Create a `DatablockAggregateEvent` by JSON-serializing a value |
| `from_json` | Deserialize event value bytes into a typed struct |

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

Each node gets its own pool of connections bounded by a `Semaphore`. Idle connections are evicted on checkout (oldest-first). Broken connections are discarded rather than returned. The idle timeout (25s) is intentionally shorter than the server's `slow_client_timeout` (30s) to prevent server-side disconnects.

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

Follows `next_event_batch_index` cursors automatically, buffering event batches and yielding them one at a time. Exhaustion is detected when the server returns no next cursor.

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

### ClientError variants

| Variant | Cause |
|---------|-------|
| `ConnectionFailed(io::Error)` | TCP/TLS connect failed |
| `ConnectionTimeout` | connect_with_timeout deadline exceeded |
| `RequestTimeout` | send_request timeout exceeded |
| `WireError(WireError)` | Framing/encoding failure |
| `ReadError(ReadWireDataError)` | Response decode failure |
| `ProtocolError` | Unexpected response variant |
| `NotLeader { leader_address, error_message }` | Node is not leader; includes redirect address if known |
| `Server(ServerError)` | Strongly-typed server error |
| `IdentityRequired` | Server requires `identify()` before data operations |
| `ServerBusy` | Server overloaded; retry after backoff |
| `IdentityError(CryptoError)` | Nonce generation, signing, or verification failure |

## Dependencies

- `celeriant_msg` - Request/Response message types, wire protocol
- `celeriant_wire` - Wire framing, protocol version constants, WireError
- `celeriant_wal` - AggregateKey, AggregateTypeKey, CompressionType
- `celeriant_crypto` - Key generation, nonce signing, identity verification
- `tokio` - Async runtime, TcpStream, timeout, semaphore
- `tokio-util` - Compat layer bridging Tokio AsyncRead/AsyncWrite to futures traits
- `tokio-rustls`, `rustls`, `rustls-pki-types` - TLS support
- `futures-util`, `futures-lite` - Async utility traits
- `serde`, `serde_json` - Event serialization helpers
