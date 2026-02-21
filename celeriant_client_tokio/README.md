# celeriant_client_tokio

Async TCP client for Celeriant using the Tokio runtime. Provides a minimal single-connection client and streaming iterators for listing data across shards.

## Architecture

```
Caller
  │
  │ connect() / connect_with_timeout()
  ▼
┌─────────────────────┐
│  CeleriantClient    │  single TCP connection (RAII)
│  - max_request_size │
│  - timeout          │
└──────────┬──────────┘
           │ send_request(Request, CompressionType)
           │
           ▼
┌──────────────────────┐    ┌──────────────────────────┐
│  celeriant_wire      │    │  celeriant_msg           │
│  (framing/codec)     │───>│  (Request / Response)    │
└──────────────────────┘    └──────────────────────────┘

List iterators (borrow &mut CeleriantClient):
  ListOrgsIterator            - orgs across shards
  ListAggregateTypesIterator  - aggregate types across shards
  ListAggregatesIterator      - aggregates with merged stats
```

**Request flow**: `send_request` writes the framed request via `celeriant_wire`, reads the response, and surfaces `GenericError` responses as `ClientError::CeleriantError`.

**Shard discovery**: List iterators start at `start_shard` and probe incrementally. A `ShardRoutingError` response signals the shard boundary; the iterator records `max_shard` and stops probing.

## Key Types

| Type | Purpose |
|------|---------|
| `CeleriantClient` | Single TCP connection; RAII-managed |
| `ClientError` | Error enum for all client-level failures |
| `ListOptions` | Shared config for list iterators (compression, deleted, shard hints) |
| `ListOrgsIterator<'a>` | Async iterator over orgs across all shards |
| `ListAggregateTypesIterator<'a>` | Async iterator over aggregate types across all shards |
| `ListAggregatesIterator<'a>` | Async iterator over aggregates with accumulated per-aggregate stats |
| `AggregateStats` | Merged stats for one aggregate (event counts, timestamps, sizes) |

## Key Functions

| Function | Purpose |
|----------|---------|
| `CeleriantClient::connect` | Connect without timeout |
| `CeleriantClient::connect_with_timeout` | Connect with optional `Duration` |
| `CeleriantClient::send_request` | Send `Request`, receive `Response`; applies timeout if set |
| `CeleriantClient::with_max_request_size` | Builder: override 10MB default |
| `CeleriantClient::with_timeout` | Builder: set per-request timeout |
| `ListOrgsIterator::next` | Yield next `OrgListItem`, deduplicated |
| `ListAggregatesIterator::next` | Yield next `AggregateStats` with stats merged across shards/pages |
| `{Iterator}::collect` | Drain all items into a `Vec` |

## Design Decisions

### Minimal client: no pooling, retries, or heartbeat

```rust
pub struct CeleriantClient {
    stream: Compat<TcpStream>,
    max_request_size: u64,
    timeout: Option<Duration>,
}
```

One connection per instance. Connection lifetime = struct lifetime. Callers are responsible for reconnection logic. TCP connections are a limited resource; hold them only as long as needed.

### TCP_NODELAY

Set on connect to disable Nagle's algorithm. Celeriant is a request/response protocol where latency matters more than packet coalescing.

### Shard discovery via probing

List iterators don't require callers to know the shard count upfront. They probe shards incrementally and stop when a `ShardRoutingError` is received on a fresh shard (cursor = None). Callers who know the shard range can pass `max_shard_hint` to skip discovery overhead.

### Round-robin pagination across shards

```
active_shards: VecDeque<u64>   // shards still being fetched
shard_cursors: HashMap<u64, Option<u64>>  // cursor per shard
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

### ClientError variants

| Variant | Cause |
|---------|-------|
| `ConnectionFailed(io::Error)` | TCP connect failed |
| `ConnectionTimeout` | connect_with_timeout deadline exceeded |
| `RequestTimeout` | send_request timeout exceeded |
| `WireError(WireError)` | Framing/encoding failure |
| `ReadError(ReadWireDataError)` | Response decode failure |
| `ProtocolError` | Unexpected response variant |
| `CeleriantError(ErrorResponse)` | Server returned a named error |

## Dependencies

- `celeriant_msg` - Request/Response message types
- `celeriant_wire` - Wire framing, protocol version constants, WireError
- `celeriant_wal` - AggregateKey, AggregateTypeKey, CompressionType
- `tokio` - Async runtime, TcpStream, timeout
- `tokio-util` - Compat layer bridging Tokio AsyncRead/AsyncWrite to futures traits
- `futures-util`, `futures-lite` - Async utility traits
