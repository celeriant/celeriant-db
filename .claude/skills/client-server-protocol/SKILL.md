---
name: understanding-client-server-protocol
description: Documents Celeriant's client-server protocol including request/response types, connection model, and multi-shard list operations. Use when working with celeriant_msg, celeriant_client_tokio, or implementing new operations.
---

# Client-Server Protocol

## Connection Model

**Stateless with optional pipelining** (unlike RDBMS):
- No session state, prepared statements, or multi-request transactions
- Each request is self-contained with all context needed
- Connections can be reused for multiple sequential requests (pipelining)

### Pipelining

Connections support **request pipelining** - send multiple requests on the same TCP connection:

```
Client                              Server
  |-- Request 1 ----------------------->|
  |<-- Response 1 ----------------------|
  |-- Request 2 (same connection) ----->|
  |<-- Response 2 ----------------------|
  |-- Request 3 ----------------------->|
  |   (redirect to shard 2 internally)  |
  |<-- Response 3 ----------------------|
  ...
```

Key behaviors:
- Server keeps connection open after each response, waiting for next request
- Each request can route to a different shard (server handles redirect internally)
- Connection closes on: client timeout (default 30s), server side shutdown signal, or watch request backpressure
- Watch requests take over the connection for streaming events

Source: [celeriant_runtimes/src/sharded/connection_handler.rs](../../celeriant_runtimes/src/sharded/connection_handler.rs) `handle_pipelining()`

### Internal Connection Redirect

When a pipelined request routes to a different shard than where the connection landed:

1. Accepting shard reads the request
2. Determines target shard via routing rules
3. Hands off TCP stream + parsed request via inter-shard channel (`IntrashardMessages::ConnectionRedirect`)
4. Target shard processes request and continues pipelining

This is invisible to clients - they just see responses.

### Dual Ports

Server listens on two separate ports:

| Port | Purpose | Allowed Requests |
|------|---------|------------------|
| Client | Application traffic | Read, Write, Exists, Delete, TrimStart, Watch, List* |
| Replication | Server-to-server | ReplicationBatch only |

Sending a request to the wrong port returns error 400.

## Wire Format

```
[header: 17 bytes][payload: variable]
```

| Offset | Size | Field |
|--------|------|-------|
| 0 | 4 | Protocol version (V2=bincode, V3=msgpack) |
| 4 | 4 | Message type (request/response enum variant) |
| 8 | 4 | Compressed length |
| 12 | 4 | Uncompressed length |
| 16 | 1 | Compression type |

See [celeriant_wire](../../celeriant_wire/README.md) for details.

## Serialization Formats

Server auto-detects format from protocol version in header and responds with same format.

| Version | Format | Use Case | Traits Required |
|---------|--------|----------|-----------------|
| V2 | Bincode | Rust clients | `Encode` + `Decode` |
| V3 | MessagePack | Non-Rust clients (Python, JS, etc.) | `Serialize` + `Deserialize` |

**For Rust clients**: Use V2 (bincode) - faster, smaller payloads
**For non-Rust clients**: Use V3 (MessagePack) - widely supported, language-agnostic

### Compression Options

Both formats support optional compression:

| Type | Flag | Notes |
|------|------|-------|
| None | 0 | No compression |
| Zstd | 1 | Best ratio, good speed |
| Snappy | 2 | Fastest, moderate ratio |
| Brotli | 3 | Best ratio, slower |
| Gzip | 4 | Wide compatibility |

Source: [celeriant_wire/src/wire_format.rs](../../celeriant_wire/src/wire_format.rs)

## Request Types

| Request | Purpose |
|---------|---------|
| `ReadRequest` | Read events from aggregate |
| `WriteRequest` | Append events (supports batch) |
| `ExistsRequest` | Check aggregate exists |
| `DeleteRequest` | Soft-delete aggregate |
| `TrimStartRequest` | Remove old events |
| `WatchRequest` | Subscribe to changes |
| `ListOrgsRequest` | List organizations |
| `ListAggregateTypesRequest` | List aggregate types |
| `ListAggregatesRequest` | List aggregates |
| `ReplicationBatchRequest` | Replicate WAL entries |

Source: [celeriant_msg/src/request/requests.rs](../../celeriant_msg/src/request/requests.rs)

## Response Types

| Response | For Request | Key Fields |
|----------|-------------|------------|
| `ReadResponse` | Read | `event_batches`, `next_event_batch_index` |
| `SuccessResponse` | Write, Delete, Trim | `correlation_id` |
| `ExistsResponse` | Exists | `min_event_batch_index` |
| `WatchResponse` | Watch | `events` (HashMap by AggregateKey) |
| `ListOrgsResponse` | ListOrgs | `orgs`, `next_cursor` |
| `ListAggregateTypesResponse` | ListAggregateTypes | `aggregate_types`, `next_cursor` |
| `ListAggregatesResponse` | ListAggregates | `aggregates`, `next_cursor` |
| `ErrorResponse` | Any (on failure) | `error_code`, `error_message` |
| `ProtocolErrorResponse` | Malformed request | (no correlation_id) |

Source: [celeriant_msg/src/response/responses.rs](../../celeriant_msg/src/response/responses.rs)

## correlation_id Pattern

Optional `u128` for request/response matching:
- Client sets it on request
- Server echoes it on response
- Useful for concurrent requests from a client that uses a connection pool
- `None` is valid if its not required

## Multi-Shard List Operations

List operations require explicit `shard_id` because data is distributed. The client library provides streaming iterators that handle:

1. **Shard discovery** - Start at shard 0, increment until routing error
2. **Pagination** - Follow `next_cursor` within each shard
3. **Round-robin** - Interleave pages across shards for responsiveness
4. **Deduplication** - Same entity may appear on multiple shards

### Iterator Pattern

```rust
// Don't manually iterate shards - use the iterators
let mut iter = ListOrgsIterator::new(&mut client, ListOptions::default());
while let Some(result) = iter.next().await {
    let org = result?;
    // process org
}

// Or collect all at once
let all_orgs = ListOrgsIterator::new(&mut client, options).collect().await?;
```

### Available Iterators

| Iterator | Yields | Dedup Key |
|----------|--------|-----------|
| `ListOrgsIterator` | `OrgListItem` | `org_id` |
| `ListAggregateTypesIterator` | `AggregateTypeListItem` | `(org_id, aggregate_type_id)` |
| `ListAggregatesIterator` | `AggregateStats` | `AggregateKey` + merges stats |

### ListAggregatesIterator Special Behavior

Unlike other iterators, `ListAggregatesIterator` **merges statistics** across shards:
- `event_batch_count`: summed
- `min_*` fields: minimum across shards
- `max_*` fields: maximum across shards
- `is_deleted`: true if ANY shard reports deleted
- `compressed_size`, `uncompressed_size`: summed

Source: [celeriant_client_tokio/src/list_operations.rs](../../celeriant_client_tokio/src/list_operations.rs)

## ListOptions

```rust
ListOptions {
    compression: CompressionType::None,  // Request compression
    include_deleted: false,              // Include soft-deleted (ListAggregates only)
    start_shard: 0,                      // Skip shards if you know your range
    max_shard_hint: None,                // Avoid discovery if shard count known
}
```

## Shard Routing

**Automatic routing** (by AggregateKey):
- Read, Write, Exists, Delete, TrimStart
- Server redirects if wrong shard (client follows redirect)
- Watch (can only watch per-shard level)

**Explicit shard_id** (client must iterate):
- ListOrgs, ListAggregateTypes, ListAggregates
- Use streaming iterators to handle this

## Error Handling

```rust
match client.send_request(&request, compression).await {
    Ok(Response::Read(r)) => { /* success */ }
    Ok(Response::Error(e)) => {
        // Server-side error: e.error_code, e.error_message
    }
    Ok(_) => { /* unexpected response type */ }
    Err(ClientError::ConnectionError(_)) => { /* network issue */ }
    Err(ClientError::CeleriantError(e)) => { /* includes shard routing errors */ }
    Err(ClientError::ProtocolError) => { /* malformed response */ }
}
```

Shard routing errors contain "shard routing error" in message - iterators use this for discovery.

## Adding a New Request Type

1. Add request struct in `celeriant_msg/src/request/requests.rs`
2. Add response struct in `celeriant_msg/src/response/responses.rs`
3. Add variants to `Request`/`Response` enums in `process_requests.rs`/`process_responses.rs`
4. Implement handler in `celeriant_shard`
5. Add client method in `celeriant_client_tokio`
