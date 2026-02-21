# celeriant_msg

Request and response message types for the Celeriant wire protocol. Defines the application-level messages exchanged between clients and servers.

## Architecture

```
Client                                    Server
  │                                         │
  │  Request (ExistsRequest, ReadRequest,   │
  │           WriteRequest, WatchRequest,   │
  │           DeleteRequest, TrimStart...)  │
  ├────────────────────────────────────────►│
  │                                         │
  │  Response (ExistsResponse, ReadResponse,│
  │            SuccessResponse, ErrorResponse,│
  │            WatchResponse...)            │
  │◄────────────────────────────────────────┤
  │                                         │

Message Classification:
┌─────────────────────────────────────────────────┐
│ Fixed-size (no compression, stack buffer)       │
│   Requests:  Exists, Read, TrimStart, Delete,   │
│              Watch, ListOrgs, ListAggregateTypes│
│              ListAggregates                     │
│   Responses: Exists, Write, TrimStart, Delete,  │
│              ProtocolError, GenericError        │
├─────────────────────────────────────────────────┤
│ Variable-size (compression, heap allocation)    │
│   Requests:  Write                              │
│   Responses: Read, Watch, ListOrgs,             │
│              ListAggregateTypes, ListAggregates │
└─────────────────────────────────────────────────┘
```

## Key Types

### Request Types

| Type | ID | Size | Purpose |
|------|----|------|---------|
| `ExistsRequest` | 1 | Fixed | Check if aggregate exists, get min batch index |
| `ReadRequest` | 2 | Fixed | Read event batches with filters |
| `WriteRequest` | 3 | Variable | Append events to one or more aggregates |
| `TrimStartRequest` | 4 | Fixed | Remove old event batches from start |
| `DeleteRequest` | 5 | Fixed | Soft-delete one or more aggregates |
| `WatchRequest` | 6 | Fixed | Subscribe to aggregate changes |
| `ListOrgsRequest` | 7 | Fixed | List organizations in a shard |
| `ListAggregateTypesRequest` | 8 | Fixed | List aggregate types (optionally by org) |
| `ListAggregatesRequest` | 9 | Fixed | List aggregates (optionally filtered) |

### Response Types

| Type | ID | Size | Purpose |
|------|----|------|---------|
| `ExistsResponse` | 1 | Fixed | Aggregate existence + min batch index |
| `ReadResponse` | 2 | Variable | Event batches + next cursor |
| `SuccessResponse` | 3,4,5 | Fixed | Write/TrimStart/Delete success |
| `ProtocolErrorResponse` | 6 | Fixed | Unreadable request (no correlation ID) |
| `ErrorResponse` | 7 | Fixed | Error with code, message, correlation ID |
| `WatchResponse` | 8 | Variable | Aggregate change notifications |
| `ListOrgsResponse` | 9 | Variable | Org list + pagination cursor |
| `ListAggregateTypesResponse` | 10 | Variable | Aggregate type list + cursor |
| `ListAggregatesResponse` | 11 | Variable | Aggregate list with metadata + cursor |

### Supporting Types

| Type | Purpose |
|------|---------|
| `ReadFilters` | Pagination and filtering for read requests |
| `AggregateEventBatch` | Event batch with metadata (from WAL) |
| `WatchEvent` | Change notification for a single aggregate |
| `SingleAggregateWrite` | Events + options for one aggregate in batch write |
| `SingleAggregateDelete` | Delete options for one aggregate in batch delete |

## Key Functions

| Function | Purpose |
|----------|---------|
| `Request::read_request` | Deserialize request from async reader |
| `Request::write_request` | Serialize request to async writer |
| `Response::read_response` | Deserialize response from async reader |
| `Response::write_response` | Serialize response to async writer |
| `Request::correlation_id` | Extract correlation ID for request/response matching |
| `Request::aggregate_id` | Extract aggregate ID for routing |
| `Response::determine_compression_type` | Get appropriate compression for response type |

## Design Decisions

### Correlation IDs

All requests carry `Option<u128>` correlation IDs, echoed in responses for async request/response matching. `ProtocolErrorResponse` omits this since the request couldn't be parsed.

### Fixed vs Variable Size

Fixed-size messages use stack buffers and skip compression—optimal for small, predictable payloads. Variable-size messages use heap allocation and Snappy compression for larger data.

```rust
// Fixed: small metadata requests/responses
RequestType::Read.is_fixed_size()  // true - just aggregate key + filters

// Variable: payload-carrying messages  
RequestType::Write.is_fixed_size() // false - contains event data
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

### Compression Strategy

Variable-size responses use Snappy by default (fast decompression). `determine_compression_type` centralizes this decision:

```rust
Response::Read(_) => CompressionType::Snappy,
Response::Watch(_) => CompressionType::Snappy,
Response::AggregateDetails(_) => CompressionType::None,  // Fixed-size
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
let response = Response::read_response(&mut reader).await?;
match response {
    Response::Read(r) => println!("Got {} batches", r.event_batches.len()),
    Response::GenericError(e) => println!("Error {}: {}", e.error_code, e.error_message),
    _ => {}
}
```

## Dependencies

- `celeriant_wal` - Aggregate keys, compression types, event structures
- `celeriant_wire` - Wire protocol framing and serialization
- `futures-lite` - Async I/O traits
- `bincode`, `serde` - Serialization