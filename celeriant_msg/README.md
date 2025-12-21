# celeriant_msg

Request and response message definitions for the Celeriant wire protocol. Defines all client-server message types with serialization support.

**README WAS LLM GENERATED AND HUMAN REVIEWED [2025-12-20]**

## Architecture

```
Client                                    Server
   │                                         │
   │  Request (ExistsRequest, ReadRequest,   │
   │           WriteRequest, etc.)           │
   │────────────────────────────────────────>│
   │                                         │
   │  Response (ExistsResponse, ReadResponse,│
   │            WriteResponse, ErrorResponse)│
   │<────────────────────────────────────────│
   │                                         │

Message Categories:
┌─────────────────────────────────────────────────────┐
│ Fixed-Size (stack buffer, no compression)           │
│   Exists, Read, TrimStart, Delete, Watch requests   │
│   Exists, Write, TrimStart, Delete, Error responses │
├─────────────────────────────────────────────────────┤
│ Variable-Size (heap allocation, compressed)         │
│   Write requests (contain event payloads)           │
│   Read, Watch responses (contain event batches)     │
└─────────────────────────────────────────────────────┘
```

## Key Types

### Requests

| Type | Fixed | Purpose |
|------|-------|---------|
| `ExistsRequest` | ✓ | Check if aggregate exists, get min batch index |
| `ReadRequest` | ✓ | Read event batches with filters/pagination |
| `WriteRequest` | ✗ | Append events to aggregate |
| `TrimStartRequest` | ✓ | Remove old event batches from start |
| `DeleteRequest` | ✓ | Delete entire aggregate |
| `WatchRequest` | ✓ | Subscribe to aggregate changes |

### Responses

| Type | Fixed | Purpose |
|------|-------|---------|
| `ExistsResponse` | ✓ | Returns min_event_batch_index |
| `ReadResponse` | ✗ | Returns event batches + next index |
| `WriteResponse` | ✓ | Returns assigned indexes, timestamp, CRC |
| `SuccessResponse` | ✓ | Generic success (TrimStart, Delete) |
| `WatchResponse` | ✗ | Streaming aggregate change notifications |
| `ProtocolErrorResponse` | ✓ | Unreadable request (no correlation_id) |
| `ErrorResponse` | ✓ | Application error with code and message |

### Supporting Types

| Type | Purpose |
|------|---------|
| `ReadFilters` | Pagination and filtering options for reads |
| `WatchEvent` | Single aggregate change notification |
| `Request` | Enum wrapper for all request types |
| `Response` | Enum wrapper for all response types |

## Key Functions

| Function | Purpose |
|----------|---------|
| `Request::read_request` | Deserialize request from async reader |
| `Request::write_request` | Serialize request to async writer |
| `Response::read_response` | Deserialize response from async reader |
| `Response::write_response` | Serialize response to async writer |
| `ReadFilters::new(from)` | Create filters starting at batch index |

## Design Decisions

### Fixed vs variable size messages

Fixed-size messages use stack-allocated buffers and skip compression — optimal for small metadata operations. Variable-size messages (Write requests, Read/Watch responses) contain event payloads that benefit from compression.

### Correlation IDs

All requests have `correlation_id: Option<u128>`. Clients assign these to match responses to requests. `ProtocolErrorResponse` has no correlation_id since the request couldn't be parsed.

### Type IDs are contiguous from 1

Request types: 1-6, Response types: 1-8. Zero is invalid. This enables fast validation and compact representation.

### ReadFilters builder pattern

```rust
let filters = ReadFilters::new(1)
    .to_event_batch_index(100)
    .include_event_types(vec![1, 2, 3])
    .exclude_client_id(my_client_id)
    .time_range(start_ts, end_ts);
```

Provides ergonomic filter construction without constructor explosion.

### Error response types

| Type | When Used |
|------|-----------|
| `ProtocolErrorResponse` | Wire-level failure, request unreadable |
| `ErrorResponse` | Application error (validation, not found, etc.) |

### Watch subscription model

`WatchRequest` specifies filters (orgs, aggregate types, specific aggregates, operation types). Server pushes `WatchResponse` with batched changes. `WatchEvent` contains batch index ranges and trim notifications.

### WriteRequest fields

| Field | Purpose |
|-------|---------|
| `allow_create` | Create aggregate if doesn't exist |
| `expected_event_batch_index` | Optimistic concurrency check |
| `enforce_client_idempotency` | Reject duplicate client_event_index |
| `compression_type` | How events are compressed in storage |

### WriteResponse completeness

Returns everything needed for client-side verification and caching:
- `event_batch_index`, `start_event_index`: Assigned positions
- `server_timestamp`: Authoritative time
- `compressed_size`: Storage cost
- `node_id`, `lease_index`: Cluster routing info
- `events_crc`: Integrity verification

## Dependencies

- `celeriant_wal` - Aggregate keys, event batch types, compression
- `celeriant_wire` - Wire framing, serialization, protocol versions
- `bincode` - Binary serialization
- `serde` - Serialization framework
- `futures-lite` - Async I/O traits