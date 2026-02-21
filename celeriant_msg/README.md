# celeriant_msg

Request and response message types for the Celeriant wire protocol. Defines the application-level messages exchanged between clients, servers, and replication peers.

## Architecture

```
Client Port                               Server
  │                                         │
  │  Request (AggregateDetails, Read,       │
  │           Write, TrimStart, Delete,     │
  │           Watch, ListOrgs,              │
  │           ListAggregateTypes,           │
  │           ListAggregates)               │
  ├────────────────────────────────────────►│
  │                                         │
  │  Response (AggregateDetails, Read,      │
  │            SuccessResponse, Error,      │
  │            Watch, List*)               │
  │◄────────────────────────────────────────┤

Replication Port                          Server
  │                                         │
  │  Request (ReplicationBatch, CatchUp,    │
  │           Heartbeat, KickFollower)      │
  ├────────────────────────────────────────►│
  │                                         │
  │  Response (ReplicationBatch, CatchUp,   │
  │            Heartbeat, KickFollower)     │
  │◄────────────────────────────────────────┤

Message Classification:
┌─────────────────────────────────────────────────────────────┐
│ Fixed-size (no compression, stack buffer)                   │
│   Requests:  AggregateDetails, Read, TrimStart, Delete,     │
│              Watch, ListOrgs, ListAggregateTypes,           │
│              ListAggregates, Heartbeat, KickFollower        │
│   Responses: AggregateDetails, Write, TrimStart, Delete,    │
│              ProtocolError, GenericError,                   │
│              ReplicationBatch, Heartbeat, KickFollower      │
├─────────────────────────────────────────────────────────────┤
│ Variable-size (compression, heap allocation)                │
│   Requests:  Write, ReplicationBatch, CatchUp              │
│   Responses: Read, Watch, ListOrgs,                        │
│              ListAggregateTypes, ListAggregates, CatchUp   │
└─────────────────────────────────────────────────────────────┘
```

## Port Classification

Requests are routed to one of two listener ports based on their type:

| Port | Request Types |
|------|---------------|
| Client port | AggregateDetails, Read, Write, TrimStart, Delete, Watch, ListOrgs, ListAggregateTypes, ListAggregates |
| Replication port | ReplicationBatch, CatchUp, Heartbeat, KickFollower |

```rust
request.is_client_port_request()      // true for all non-replication requests
request.is_replication_port_request() // true only for ReplicationBatch, CatchUp, Heartbeat, KickFollower
```

## Key Types

### Request Types

| Type | ID | Size | Purpose |
|------|----|------|---------|
| `AggregateDetailsRequest` | 1 | Fixed | Get aggregate metadata: min/max batch index, deletion status, last op info |
| `ReadRequest` | 2 | Fixed | Read event batches with filters |
| `WriteRequest` | 3 | Variable | Append events to one or more aggregates |
| `TrimStartRequest` | 4 | Fixed | Remove old event batches from start |
| `DeleteRequest` | 5 | Fixed | Soft-delete one or more aggregates |
| `WatchRequest` | 6 | Fixed | Subscribe to aggregate changes |
| `ListOrgsRequest` | 7 | Fixed | List organizations in a shard |
| `ListAggregateTypesRequest` | 8 | Fixed | List aggregate types (optionally by org) |
| `ListAggregatesRequest` | 9 | Fixed | List aggregates (optionally filtered) |
| `ReplicationBatchRequest` | 10 | Variable | Leader pushes WAL entries to follower |
| `CatchUpRequest` | 11 | Variable | Follower-initiated pull of WAL entries during sync |
| `HeartbeatRequest` | 12 | Fixed | Liveness signal: shard_id + leader timestamp |
| `KickFollowerRequest` | 13 | Fixed | Command to evict follower |

### Response Types

| Type | ID | Size | Purpose |
|------|----|------|---------|
| `AggregateDetailsResponse` | 1 | Fixed | Aggregate metadata: min/max batch index, deletion status, recreate/index flags, last op |
| `ReadResponse` | 2 | Variable | Event batches + next cursor |
| `SuccessResponse` | 3,4,5 | Fixed | Write/TrimStart/Delete success |
| `ProtocolErrorResponse` | 6 | Fixed | Unreadable request (no correlation ID) |
| `ErrorResponse` | 7 | Fixed | Error with code, message, correlation ID |
| `WatchResponse` | 8 | Variable | Aggregate change notifications |
| `ListOrgsResponse` | 9 | Variable | Org list + pagination cursor |
| `ListAggregateTypesResponse` | 10 | Variable | Aggregate type list + cursor |
| `ListAggregatesResponse` | 11 | Variable | Aggregate list with metadata + cursor |
| `ReplicationBatchResponse` | 12 | Fixed | Follower response: timestamp + `ReplicationResult` |
| `CatchUpResponse` | 13 | Variable | Leader returns batches + catch-up continuation flag |
| `HeartbeatResponse` | 14 | Fixed | `HeartbeatResult`: Ack or Rejected |
| `KickFollowerResponse` | 15 | Fixed | Acknowledgement flag |

### Supporting Types

| Type | Purpose |
|------|---------|
| `ReadFilters` | Pagination and filtering for read requests |
| `AggregateEventBatch` | Event batch with metadata (from WAL) |
| `WatchEvent` | Change notification for a single aggregate |
| `SingleAggregateWrite` | Events + options for one aggregate in batch write |
| `SingleAggregateDelete` | Delete options for one aggregate in batch delete |
| `ReplicationBatchItem` | Single metablock + optional datablock; `size_bytes()` for memory accounting |
| `OrgListItem` | `org_id: u128` |
| `AggregateTypeListItem` | `org_id`, `aggregate_type_id` |
| `AggregateListItem` | Full aggregate metadata for list responses |

### Replication Enums

| Type | Variants | Purpose |
|------|----------|---------|
| `ReplicationResult` | `Success { last_follower_metablock }`, `Rejected(FollowerRejection)` | Outcome of a replication batch |
| `FollowerRejection` | `NotAFollower`, `TimeDriftTooHigh`, `WalIndexMismatch`, `TipHashMismatch`, `EmptyBatch`, `MissingDatablock`, `StaleLease` | Why follower refused batch |
| `HeartbeatResult` | `Ack { follower_timestamp_ms }`, `Rejected(HeartbeatRejection)` | Outcome of a heartbeat |
| `HeartbeatRejection` | `ClockDriftTooHigh`, `NotAFollower` | Why follower refused heartbeat |

## Key Functions

| Function | Purpose |
|----------|---------|
| `Request::read_request` | Deserialize request from async reader |
| `Request::write_request` | Serialize request to async writer |
| `Response::read_response` | Deserialize response from async reader |
| `Response::write_response` | Serialize response to async writer |
| `Request::correlation_id` | Extract correlation ID for request/response matching |
| `Request::aggregate_id` | Extract aggregate ID for routing (0 for non-aggregate requests) |
| `Request::org_id` | Extract org ID for routing (0 for non-aggregate requests) |
| `Request::aggregate_type_id` | Extract aggregate type ID for routing (0 for non-aggregate requests) |
| `Request::is_client_port_request` | Returns false for replication/heartbeat/kick |
| `Request::is_replication_port_request` | Returns true only for replication-related requests |
| `Response::determine_compression_type` | Get appropriate compression for response type |

## Design Decisions

### Correlation IDs

All requests carry `Option<u128>` correlation IDs, echoed in responses for async request/response matching. `ProtocolErrorResponse` omits this since the request couldn't be parsed.

### Fixed vs Variable Size

Fixed-size messages use stack buffers and skip compression—optimal for small, predictable payloads. Variable-size messages use heap allocation with configurable compression for larger data.

```rust
// Fixed: small metadata requests/responses
Request::AggregateDetails(_) => wire_header_write_fixed_size(...)

// Variable: payload-carrying messages
Request::Write(_) => wire_header_write_variable_size(...)
Request::ReplicationBatch(_) => wire_header_write_variable_size(...)
Request::CatchUp(_) => wire_header_write_variable_size(...)
```

### Batch Operations

Write and Delete support multiple aggregates per request:

```rust
WriteRequest {
    writes: HashMap<AggregateKey, SingleAggregateWrite>,
    // ...
}
```

This enables atomic multi-aggregate operations and reduces round trips.

### Read Filters

`ReadFilters` supports rich querying without server-side indexing:

| Filter | Purpose |
|--------|---------|
| `from/to_event_batch_index` | Batch range pagination |
| `include/exclude_client_id` | Filter by originating client |
| `include/exclude_user_id` | Filter by user |
| `min/max_server_timestamp` | Server time range |
| `min/max_event_timestamp` | Client time range |
| `min/max_event_index` | Event sequence range |
| `min/max_client_event_index` | Client-assigned event index range |
| `include_event_types` | Whitelist specific event types |

### Watch Subscriptions

`WatchRequest` supports filtering watched aggregates:

```rust
WatchRequest {
    orgs: Option<HashSet<u128>>,           // Filter by org
    aggregate_types: Option<HashSet<u128>>, // Filter by type
    aggregates: Option<HashSet<u128>>,      // Filter by ID
    operation_types: Option<HashSet<u8>>,   // Filter by operation
    requested_latency_ms: Option<u64>,      // Batching hint
}
```

### AggregateDetails Response

`AggregateDetailsResponse` returns richer metadata than a simple exists check:

```rust
AggregateDetailsResponse {
    min_event_batch_index: u64,
    max_event_batch_index: u64,
    max_event_index: u64,
    is_deleted: bool,
    allow_recreate: bool,
    allow_index_continuation: bool,
    last_server_timestamp: u64,
    last_client_id: u128,
    last_user_id: Option<u128>,
}
```

### Aggregate Metadata in List Responses

`AggregateListItem` includes comprehensive metadata for discovery:

```rust
AggregateListItem {
    is_deleted: bool,
    event_batch_count: u64,
    min/max_event_batch_index: u64,
    min/max_event_index: u64,
    min/max_event_timestamp: u64,
    min/max_server_timestamp: u64,
    compressed_size: u64,
    uncompressed_size: u64,
}
```

### Replication Protocol

Leader-to-follower replication uses two flows:

**Push flow** (leader-initiated): Leader sends `ReplicationBatchRequest` containing a `Vec<ReplicationBatchItem>` (each item is a `Metablock` + optional `Datablock`). Follower validates WAL position and tip hash before applying, returning `ReplicationBatchResponse` with a `ReplicationResult`.

**Pull flow** (follower-initiated): Follower sends `CatchUpRequest` with its current WAL position and tip hash. Leader responds with `CatchUpResponse` containing paginated batches and a `continue_catching_up` flag indicating whether more pages remain.

**Heartbeat**: `HeartbeatRequest` carries `leader_timestamp_ms` for clock drift detection. `HeartbeatResponse` returns `HeartbeatResult::Ack { follower_timestamp_ms }` or `HeartbeatResult::Rejected(HeartbeatRejection)`.

`FollowerRejection` variants encode the reason for state mismatch precisely so the leader can take corrective action (e.g., `WalIndexMismatch` triggers catch-up, `StaleLease` triggers lease renewal).

### Compression Strategy

Variable-size responses use the server's configured compression algorithm. Fixed-size responses always use `CompressionType::None`. `determine_compression_type` centralizes this decision:

```rust
// Variable-size data-carrying: use server algorithm
Response::Read(_)                 => server_compression_algorithm,
Response::Watch(_)                => server_compression_algorithm,
Response::ListOrgs(_)             => server_compression_algorithm,
Response::ListAggregateTypes(_)   => server_compression_algorithm,
Response::ListAggregates(_)       => server_compression_algorithm,
Response::CatchUp(_)              => server_compression_algorithm,

// Fixed-size (client and replication): always None
Response::AggregateDetails(_)     => CompressionType::None,
Response::Write(_)                => CompressionType::None,
Response::TrimStart(_)            => CompressionType::None,
Response::Delete(_)               => CompressionType::None,
Response::ProtocolError(_)        => CompressionType::None,
Response::GenericError(_)         => CompressionType::None,
Response::ReplicationBatch(_)     => CompressionType::None,
Response::Heartbeat(_)            => CompressionType::None,
Response::KickFollower(_)         => CompressionType::None,
```

## Usage

```rust
// Writing a request
let request = Request::Read(ReadRequest {
    correlation_id: Some(123),
    aggregate_key: AggregateKey::new(org, type_id, agg_id),
    filters: ReadFilters::new(1).to_event_batch_index(100),
});
Request::write_request(&mut writer, &request, CompressionType::None, max_size, version).await?;

// Reading a response
let response = Response::read_response(&mut reader, max_size).await?;
match response {
    Response::Read(r) => println!("Got {} batches", r.event_batches.len()),
    Response::AggregateDetails(d) => println!("max_batch={}, deleted={}", d.max_event_batch_index, d.is_deleted),
    Response::GenericError(e) => println!("Error {}: {}", e.error_code, e.error_message),
    _ => {}
}

// Port classification
if request.is_replication_port_request() {
    // route to replication listener
} else {
    // route to client listener
}
```

## Dependencies

- `celeriant_wal` - Aggregate keys, compression types, event structures, metablocks, datablocks
- `celeriant_wire` - Wire protocol framing and serialization
- `futures-lite` - Async I/O traits
- `bincode`, `serde` - Serialization
- `deepsize` - Memory size accounting for `ReplicationBatchItem::size_bytes()`
