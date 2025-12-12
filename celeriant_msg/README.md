# celeriant_msg

Request and response message types for the Celeriant wire protocol. This crate defines the message structures that clients and servers exchange over TCP.

## Overview

```
┌─────────────────────────────────────────────────────────────┐
│                      Wire Frame                             │
├─────────────────┬───────────────────────────────────────────┤
│  Header (17B)   │  Request or Response payload              │
│  version, type  │  (serialized via celeriant_wire)          │
└─────────────────┴───────────────────────────────────────────┘
```

This crate provides the message definitions. Serialization and framing are handled by `celeriant_wire`.

## Request Types

| Type | Description | Fixed Size |
|------|-------------|------------|
| `Write` | Append events to an aggregate | No |
| `Read` | Read events with filters | Yes |
| `Exists` | Check aggregate existence and get bounds | Yes |
| `ListOrganisations` | List organisations with filters | Yes |
| `ListAggregates` | List aggregates within an org | Yes |
| `TrimStart` | Remove events before a batch index | Yes |
| `Delete` | Remove an entire aggregate | Yes |

Fixed-size requests use stack-allocated buffers and skip compression. Variable-size requests support compression.

## Response Types

| Type | Description | Fixed Size |
|------|-------------|------------|
| `Write` | Batch index, timestamp, CRC | Yes |
| `Read` | Event batches, next batch index | No |
| `Exists` | Min/max batch index, size | Yes |
| `ListOrganisations` | Organisation info list | No |
| `ListAggregates` | Aggregate info list | No |
| `TrimStart` | Success acknowledgement | Yes |
| `Delete` | Success acknowledgement | Yes |
| `ProtocolError` | Malformed request (no correlation ID) | Yes |

## Error Handling

Errors are indicated by the response type, not by a status field within responses.

**Application errors** return an `ErrorResponse`:

```rust
pub struct ErrorResponse {
    pub correlation_id: Option<u128>,
    pub error_code: u32,
    pub error_message: String,
}
```

The `correlation_id` matches the request that caused the error, allowing clients to associate errors with pending requests in async pipelines.

**Protocol errors** return a `ProtocolErrorResponse`:

```rust
pub struct ProtocolErrorResponse {
    // No correlation_id - request was unparseable
}
```

This is returned when the server cannot parse the request at all. No correlation ID is available because the request body was malformed. V3 of the wire format is used (messagepack).

**Wire-level errors** occur before any response is returned:

| Error | Cause |
|-------|-------|
| `WireError::UnknownRequestType` | Unrecognised request type ID |
| `WireError::UnknownResponseType` | Unrecognised response type ID |
| `WireError::MessageTooLarge` | Payload exceeds size limit |
| `WireError::UnsupportedProtocol` | Unknown protocol version |
| `WireError::NetworkError` | TCP connection failure |

Handle wire errors by closing and reconnecting. Handle `ErrorResponse` based on `error_code`. Handle `ProtocolErrorResponse` by logging and investigating the client's serialisation logic.

## Correlation IDs

All requests include an optional `correlation_id: Option<u128>`. Responses echo this value back, enabling clients to match responses to requests in pipelined or multiplexed connections.

```
Client                          Server
  │                               │
  ├─ Request (corr_id: 42) ──────►│
  ├─ Request (corr_id: 43) ──────►│
  │                               │
  │◄──── Response (corr_id: 43) ──┤
  │◄──── Response (corr_id: 42) ──┤
```

Responses may arrive out of order. Use correlation IDs to dispatch correctly.

## Filters

### ReadFilters

Controls which event batches are returned from a `Read` request:

```rust
ReadFilters::new(1)                          // Start from batch 1
    .to_event_batch_index(100)               // Stop at batch 100
    .include_event_types(vec![1, 2, 3])      // Only these event types
    .exclude_client_id(client_id)            // Skip own writes
    .time_range(start_ms, end_ms)            // Server timestamp bounds
    .event_time_range(min_ts, max_ts)        // Client event timestamp bounds
```

Filters are evaluated server-side using batch metadata before decompression. Bloom filters accelerate event type filtering.

### DirectoryFilters

Controls which organisations or aggregates are returned from list operations:

```rust
pub struct DirectoryFilters {
    pub created_after_or_on: Option<u64>,
    pub created_before_or_on: Option<u64>,
    pub modified_after_or_on: Option<u64>,
    pub modified_before_or_on: Option<u64>,
    pub disk_usage_less_than_or_equal: Option<u64>,
    pub disk_usage_greater_than_or_equal: Option<u64>,
}
```

## Protocol Versions

Two serialisation formats are supported:

| Version | Format | Use Case |
|---------|--------|----------|
| V2 | bincode | Rust clients |
| V3 | msgpack | Non-Rust clients |

Both produce the same logical messages. Choose based on client language:

- **Rust**: Use V2 (bincode) for best performance
- **Other languages**: Use V3 (msgpack) for standard tooling

The protocol version is set per-message.

## Message Size

Fixed-size messages fit in a 1024-byte buffer. Variable-size messages have no inherent limit but servers may enforce `max_request_size`.

When writing a `Write` request with many events, monitor compressed size. Large batches may be rejected with `WireError::MessageTooLarge`.

## Key Structures

### WriteRequest

```rust
pub struct WriteRequest {
    pub correlation_id: Option<u128>,
    pub aggregate_key: AggregateKey,          // org/type/id
    pub client_id: u128,                      // Machine identity
    pub user_id: Option<u128>,                // Human identity
    pub events: Vec<EventItem>,               // Events to append
    pub allow_create: bool,                   // Create aggregate if missing
    pub expected_event_batch_index: Option<u64>, // OCC check
    pub enforce_client_idempotency: bool,     // Dedupe by client_event_index
    pub durable_write_with_delay_us: Option<u64>, // Fsync delay
    pub compression_type: CompressionType,    // For storage
}
```

### WriteResponse

```rust
pub struct WriteResponse {
    pub event_batch_index: u64,    // Assigned batch number
    pub start_event_index: u64,    // First event's server index
    pub server_timestamp: u64,     // When persisted (Unix ms)
    pub compressed_size: u64,      // Bytes on disk
    pub node_id: u128,             // Which node accepted
    pub lease_index: u64,          // Leadership term
    pub events_crc: u32,           // Integrity check
}
```

### ReadResponse

```rust
pub struct ReadResponse {
    pub correlation_id: Option<u128>,
    pub event_batches: Vec<EventBatchItem>,   // Matching batches
    pub next_event_batch_index: Option<u64>,  // Continue from here
}
```

`next_event_batch_index` is `Some(n)` if more batches exist beyond those returned. Use it to paginate through large aggregates.

## Dependencies

- `celeriant_wal` - Event and batch structures
- `celeriant_wire` - Serialisation and framing
- `bincode`, `serde` - Encoding
- `futures-lite` - Async I/O traits