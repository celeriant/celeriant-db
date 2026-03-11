# celeriant_msg

Request and response message types for the Celeriant wire protocol. Defines the application-level messages exchanged between clients, servers, and replication peers.

## Architecture

```
Client Port                               Server
  │                                         │
  │  ClientRequest (AggregateDetails,      │
  │     Read, Write, TrimStart, Delete,    │
  │     Watch, ListOrgs, ListAggregateTypes,│
  │     ListAggregates, RegisterSchema)    │
  ├────────────────────────────────────────►│
  │                                         │
  │  ClientResponse (AggregateDetails,     │
  │     Read, SuccessResponse, Error,      │
  │     Watch, List*, RegisterSchema)      │
  │◄────────────────────────────────────────┤

Replication Port                          Server
  │                                         │
  │  ClusterRequest (ReplicationBatch,     │
  │     Heartbeat, KickFollower)           │
  ├────────────────────────────────────────►│
  │                                         │
  │  ClusterResponse (ReplicationBatch,    │
  │     Heartbeat, KickFollower)           │
  │◄────────────────────────────────────────┤

Identify (pre-auth, client port only)
  │  IdentifyRequest  ──────────────────►  │
  │  IdentifyResponse ◄──────────────────  │

Message Classification:
┌─────────────────────────────────────────────────────────────┐
│ Fixed-size (no compression, stack buffer)                   │
│   Client Req:  AggregateDetails, Read, TrimStart, Delete,  │
│                Watch, ListOrgs, ListAggregateTypes,         │
│                ListAggregates, Identify                     │
│   Client Res:  AggregateDetails, Write, TrimStart, Delete, │
│                ProtocolError, GenericError, RegisterSchema, │
│                Identify                                     │
│   Cluster Req: Heartbeat, KickFollower                      │
│   Cluster Res: All (ReplicationBatch, Heartbeat,            │
│                KickFollower, ProtocolError, GenericError)    │
├─────────────────────────────────────────────────────────────┤
│ Variable-size (compression, heap allocation)                │
│   Client Req:  Write, RegisterSchema                        │
│   Client Res:  Read, Watch, ListOrgs,                       │
│                ListAggregateTypes, ListAggregates            │
│   Cluster Req: ReplicationBatch                              │
└─────────────────────────────────────────────────────────────┘
```

## Module Structure

| Module | Purpose |
|--------|---------|
| `process_client_requests` | Client request wire protocol: `ClientRequest` enum, read/write |
| `process_client_responses` | Client response wire protocol: `ClientResponse` enum, read/write |
| `process_cluster_requests` | Cluster request wire protocol: `ClusterRequest` enum, read/write |
| `process_cluster_responses` | Cluster response wire protocol: `ClusterResponse` enum, read/write |
| `process_identify` | Pre-auth identify request/response, separate from client/cluster enums |
| `read_wire_data_error` | Wire deserialization error type |
| `request` | Request struct definitions, `ReadFilters` builder |
| `response` | Response struct definitions, `AggregateEventBatch`, `WatchResponseEvent` |

## Port Classification

Requests are routed to one of two listener ports based on their enum type:

| Port | Enum | Request Types |
|------|------|---------------|
| Client port | `ClientRequest` | AggregateDetails, Read, Write, TrimStart, Delete, Watch, ListOrgs, ListAggregateTypes, ListAggregates, RegisterSchema |
| Replication port | `ClusterRequest` | ReplicationBatch, Heartbeat, KickFollower |

Identify is handled on the client port before authentication, outside both enums.

## Key Types

### Client Request Types

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
| `RegisterSchemaRequest` | 10 | Variable | Register event schema (Json/Avro/Protobuf) |

### Cluster Request Types

| Type | ID | Size | Purpose |
|------|----|------|---------|
| `ReplicationBatchRequest` | 100 | Variable | Leader pushes WAL entries to follower |
| `HeartbeatRequest` | 101 | Fixed | Liveness signal: shard_id + leader timestamp |
| `KickFollowerRequest` | 102 | Fixed | Command to evict follower |

### Identify Types

| Type | ID | Size | Purpose |
|------|----|------|---------|
| `IdentifyRequest` | 14 | Fixed | RSA signature + API key authentication |
| `IdentifyResponse` | 16 | Fixed | Returns client_id + `AccessLevel` |

### Client Response Types

| Type | ID | Size | Purpose |
|------|----|------|---------|
| `AggregateDetailsResponse` | 1 | Fixed | Aggregate metadata: min/max batch index, deletion status, recreate/index flags, last op |
| `ReadResponse` | 2 | Variable | Event batches + next cursor |
| `SuccessResponse` | 3,4,5,12 | Fixed | Write/TrimStart/Delete/RegisterSchema success |
| `ProtocolErrorResponse` | 6 | Fixed | Unreadable request (no correlation ID) |
| `ErrorResponse` | 7 | Fixed | Error with code, message, correlation ID |
| `WatchResponse` | 8 | Variable | Aggregate change notifications |
| `ListOrgsResponse` | 9 | Variable | Org list + pagination cursor |
| `ListAggregateTypesResponse` | 10 | Variable | Aggregate type list + cursor |
| `ListAggregatesResponse` | 11 | Variable | Aggregate list with metadata + cursor |

### Cluster Response Types

| Type | ID | Size | Purpose |
|------|----|------|---------|
| `ReplicationBatchResponse` | 100 | Fixed | Follower response: timestamp + `ReplicationResult` |
| `HeartbeatResponse` | 101 | Fixed | `HeartbeatResult`: Ack or Rejected |
| `KickFollowerResponse` | 102 | Fixed | Acknowledgement flag |
| `ProtocolErrorResponse` | 106 | Fixed | Unreadable cluster request |
| `ErrorResponse` | 107 | Fixed | Cluster error with code and message |

### Supporting Types

| Type | Purpose |
|------|---------|
| `ReadFilters` | Pagination and filtering for read requests (builder pattern) |
| `AggregateEventBatch` | Event batch with metadata (from WAL); `from_wal(metablock, datablock)` |
| `WatchResponseEvent` | Change notification for a single aggregate |
| `SingleAggregateWrite` | Events + options for one aggregate in batch write |
| `SingleAggregateDelete` | Delete options for one aggregate in batch delete |
| `ReplicationBatchItem` | Single metablock + optional datablock; `size_bytes()` for memory accounting |
| `OrgListItem` | `org_id: u128` |
| `AggregateTypeListItem` | `org_id`, `aggregate_type_id` |
| `AggregateListItem` | Full aggregate metadata for list responses |
| `AccessLevel` | `ReadWrite` (1), `ReadOnly` (2) |

### Replication Enums

| Type | Variants | Purpose |
|------|----------|---------|
| `ReplicationResult` | `Success { last_follower_metablock }`, `Rejected(FollowerRejection)` | Outcome of a replication batch |
| `FollowerRejection` | `NotAFollower`, `TimeDriftTooHigh { leader_ms, follower_ms, max_allowed_ms }`, `WalIndexMismatch { max_follower_wal_index }`, `TipHashMismatch { follower, follower_wal_index, leader, leader_wal_index }`, `EmptyBatch`, `MissingDatablock`, `StaleLease { follower_lease_index, received_lease_index }` | Why follower refused batch |
| `HeartbeatResult` | `Ack { follower_timestamp_ms }`, `Rejected(HeartbeatRejection)` | Outcome of a heartbeat |
| `HeartbeatRejection` | `ClockDriftTooHigh { leader_ms, follower_ms, max_allowed_ms }`, `NotAFollower` | Why follower refused heartbeat |

### Error Codes

`ErrorResponse` defines well-known error codes:

| Constant | Value | Purpose |
|----------|-------|---------|
| `WRITE_NOT_LEADER` | 2011 | Write rejected, node is not leader |
| `TRIM_NOT_LEADER` | 3005 | Trim rejected, node is not leader |
| `DELETE_NOT_LEADER` | 4006 | Delete rejected, node is not leader |
| `IDENTIFY_REQUIRED` | 10004 | Client must identify before requests |
| `AUTH_REQUIRED` | 1001 | Authentication required |
| `AUTH_INVALID_KEY` | 1002 | Invalid API key |
| `AUTH_INSUFFICIENT_PERMISSIONS` | 1003 | Insufficient permissions |

`ErrorResponse` methods: `is_not_leader()`, `is_identity_required()`, `parse_leader_address()`.

### ReadWireDataError

```rust
pub enum ReadWireDataError {
    UnknownMessageType(u32),
    ReadHeaderFailure(WireError),
    ReadBodyFailure(WireError),
}
```

## Key Functions

### Client

| Function | Purpose |
|----------|---------|
| `ClientRequest::read_from_header` | Deserialize client request given a pre-read wire header |
| `ClientRequest::write_request` | Serialize client request to async writer |
| `ClientRequest::correlation_id` | Extract correlation ID for request/response matching |
| `ClientRequest::aggregate_id` | Extract aggregate ID for routing (0 for non-aggregate requests) |
| `ClientRequest::org_id` | Extract org ID for routing (0 for non-aggregate requests) |
| `ClientRequest::aggregate_type_id` | Extract aggregate type ID for routing (0 for non-aggregate requests) |
| `ClientResponse::read_response` | Deserialize client response from async reader |
| `ClientResponse::read_from_header` | Deserialize client response given a pre-read wire header |
| `ClientResponse::write_response` | Serialize client response to async writer |
| `ClientResponse::determine_compression_type` | Get appropriate compression for response type |

### Cluster

| Function | Purpose |
|----------|---------|
| `ClusterRequest::read_from_header` | Deserialize cluster request given a pre-read wire header |
| `ClusterRequest::write_request` | Serialize cluster request to async writer |
| `ClusterRequest::correlation_id` | Extract correlation ID |
| `ClusterResponse::read_response` | Deserialize cluster response from async reader |
| `ClusterResponse::read_from_header` | Deserialize cluster response given a pre-read wire header |
| `ClusterResponse::write_response` | Serialize cluster response to async writer |
| `ClusterResponse::determine_compression_type` | Always returns `CompressionType::None` |

### Identify

| Function | Purpose |
|----------|---------|
| `read_identify_request` | Deserialize identify request from wire header + reader |
| `write_identify_request` | Serialize identify request to async writer |
| `read_identify_response` | Deserialize identify response from wire header + reader |
| `write_identify_response` | Serialize identify response to async writer |

## Design Decisions

### Client/Cluster Separation

Client and cluster messages use separate enums (`ClientRequest`/`ClientResponse` vs `ClusterRequest`/`ClusterResponse`) with distinct wire ID ranges. Client IDs are 1-12, cluster IDs are 100-107. This makes port routing type-safe and eliminates runtime classification.

### Correlation IDs

All requests carry `Option<u128>` correlation IDs, echoed in responses for async request/response matching. `ProtocolErrorResponse` omits this since the request couldn't be parsed.

### Fixed vs Variable Size

Fixed-size messages use stack buffers and skip compression—optimal for small, predictable payloads. Variable-size messages use heap allocation with configurable compression for larger data.

```rust
// Fixed: small metadata requests/responses
ClientRequest::AggregateDetails(_) => wire_header_write_fixed_size(...)

// Variable: payload-carrying messages
ClientRequest::Write(_) => wire_header_write_variable_size(...)
ClientRequest::RegisterSchema(_) => wire_header_write_variable_size(...)
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
    shard_id: Option<u64>,                 // Filter by shard
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
    org_id: u128,
    aggregate_type_id: u128,
    aggregate_id: u128,
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

Leader-to-follower replication uses a push flow:

**Push flow** (leader-initiated): Leader sends `ReplicationBatchRequest` containing a `Vec<ReplicationBatchItem>` (each item is a `Metablock` + optional `Datablock`). Follower validates WAL position and tip hash before applying, returning `ReplicationBatchResponse` with a `ReplicationResult`.

**Heartbeat**: `HeartbeatRequest` carries `leader_timestamp_ms` for clock drift detection. `HeartbeatResponse` returns `HeartbeatResult::Ack { follower_timestamp_ms }` or `HeartbeatResult::Rejected(HeartbeatRejection)`.

`FollowerRejection` variants carry contextual data (timestamps, WAL indexes, hashes) so the leader can take corrective action (e.g., `WalIndexMismatch` triggers catch-up, `StaleLease` triggers lease renewal).

### Identify Flow

`IdentifyRequest` handles authentication before any client requests are processed. Supports two mechanisms:

- **RSA signature**: `public_key` (Base64 DER SubjectPublicKeyInfo) + `nonce` (epoch ms) + `signature` (Base64 RSASSA-PKCS1-v1_5-SHA256)
- **API key**: `api_key` (Base64 32-byte key)

`IdentifyResponse` returns `client_id` and `AccessLevel` (ReadWrite or ReadOnly).

### Compression Strategy

Variable-size client responses use the server's configured compression algorithm. All fixed-size messages and all cluster responses use `CompressionType::None`.

```rust
// Client variable-size: use server algorithm
ClientResponse::Read(_)               => server_compression_algorithm,
ClientResponse::Watch(_)              => server_compression_algorithm,
ClientResponse::ListOrgs(_)           => server_compression_algorithm,
ClientResponse::ListAggregateTypes(_) => server_compression_algorithm,
ClientResponse::ListAggregates(_)     => server_compression_algorithm,

// Client fixed-size: always None
ClientResponse::AggregateDetails(_)   => CompressionType::None,
ClientResponse::Write(_)              => CompressionType::None,
// ... etc

// All cluster responses: always None
ClusterResponse::determine_compression_type(..) => CompressionType::None,
```

## Usage

```rust
// Writing a client request
let request = ClientRequest::Read(ReadRequest {
    correlation_id: Some(123),
    aggregate_key: AggregateKey::new(org, type_id, agg_id),
    filters: ReadFilters::new(1).to_event_batch_index(100),
});
ClientRequest::write_request(&mut writer, &request, CompressionType::None, max_size, version).await?;

// Reading a client response
let response = ClientResponse::read_response(&mut reader, max_size).await?;
match response {
    ClientResponse::Read(r) => println!("Got {} batches", r.event_batches.len()),
    ClientResponse::AggregateDetails(d) => println!("max_batch={}, deleted={}", d.max_event_batch_index, d.is_deleted),
    ClientResponse::GenericError(e) => {
        if e.is_not_leader() {
            let leader = e.parse_leader_address();
        }
    }
    _ => {}
}
```

## Feature Flags

| Feature | Default | Purpose |
|---------|---------|---------|
| `small-metablock` | off | Propagates to `celeriant_wal` and `celeriant_wire` for testing with smaller block sizes |

## Dependencies

- `celeriant_wal` - Aggregate keys, compression types, event structures, metablocks, datablocks
- `celeriant_wire` - Wire protocol framing and serialization
- `futures-lite` - Async I/O traits
- `bincode`, `serde` - Serialization
- `deepsize` - Memory size accounting for `ReplicationBatchItem::size_bytes()`
