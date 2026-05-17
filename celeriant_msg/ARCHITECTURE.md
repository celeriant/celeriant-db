# celeriant_msg

Request and response message types for the Celeriant wire protocol. Defines the application-level messages exchanged between clients, servers, and replication peers.

## Architecture

```
Client Port                               Server
  │                                         │
  │  ClientRequest (AggregateDetails,       │
  │     Read, Write, TrimStart, Delete,     │
  │     Watch, ListOrgs, ListAggregateTypes,│
  │     ListAggregates, RegisterSchema)     │
  ├────────────────────────────────────────►│
  │                                         │
  │  ClientResponse (AggregateDetails,      │
  │     Read, SuccessResponse, Error,       │
  │     Watch, List*, RegisterSchema)       │
  │◄────────────────────────────────────────┤

Replication Port                          Server
  │                                         │
  │  ClusterRequest (ReplicationBatch,      │
  │     Heartbeat, KickFollower)            │
  ├────────────────────────────────────────►│
  │                                         │
  │  ClusterResponse (ReplicationBatch,     │
  │     Heartbeat, KickFollower)            │
  │◄────────────────────────────────────────┤

Identify (pre-auth, client port only)
  │  IdentifyRequest  ──────────────────►   │
  │  IdentifyResponse ◄──────────────────   │

Message Classification:
┌─────────────────────────────────────────────────────────────┐
│ Fixed-size (no compression, stack buffer)                   │
│   Client Req:  AggregateDetails, Read, TrimStart, Delete,   │
│                Watch, ListOrgs, ListAggregateTypes,         │
│                ListAggregates, Identify                     │
│   Client Res:  AggregateDetails, Write, TrimStart, Delete,  │
│                ProtocolError, GenericError, RegisterSchema, │
│                Identify                                     │
│   Cluster Req: Heartbeat, KickFollower                      │
│   Cluster Res: All (ReplicationBatch, Heartbeat,            │
│                KickFollower, ProtocolError, GenericError)   │
├─────────────────────────────────────────────────────────────┤
│ Variable-size (compression, heap allocation)                │
│   Client Req:  Write, RegisterSchema                        │
│   Client Res:  Read, Watch, ListOrgs,                       │
│                ListAggregateTypes, ListAggregates           │
│   Cluster Req: ReplicationBatch                             │
└─────────────────────────────────────────────────────────────┘
```

## Invariants

- Client and cluster messages use separate enums with distinct wire ID ranges (1-12 client, 100-107 cluster). Port routing is type-safe; there is no runtime classification step.
- All requests carry `Option<u128>` correlation IDs, echoed in responses. `ProtocolErrorResponse` omits the correlation ID since the request could not be parsed.
- Fixed-size messages use stack buffers and skip compression. Variable-size messages use heap allocation with optional compression.
- Error codes are defined exclusively in `error_codes.rs`. No other file may define `u32` error codes.
- Multi-aggregate writes that span multiple shards are rejected with `IncompatibleFilters`.

## Port Classification

| Port | Enum | Request Types |
|------|------|---------------|
| Client port | `ClientRequest` | AggregateDetails, Read, Write, TrimStart, Delete, Watch, ListOrgs, ListAggregateTypes, ListAggregates, RegisterSchema |
| Replication port | `ClusterRequest` | ReplicationBatch, Heartbeat, KickFollower |

Identify is handled on the client port before authentication, outside both enums.

## Key Types

### Client Request Types

| Type | ID | Size | Purpose |
|------|----|------|---------|
| `AggregateDetailsRequest` | 1 | Fixed | Get aggregate metadata: aggregate version, deletion status, last op info |
| `ReadRequest` | 2 | Fixed | Read event batches with filters |
| `WriteRequest` | 3 | Variable | Append events to one or more aggregates |
| `TrimStartRequest` | 4 | Fixed | Remove old event batches from start |
| `DeleteRequest` | 5 | Fixed | Soft-delete one or more aggregates |
| `WatchRequest` | 6 | Fixed | Subscribe to aggregate changes |
| `ListOrgsRequest` | 7 | Fixed | List organizations in a shard |
| `ListAggregateTypesRequest` | 8 | Fixed | List aggregate types (optionally by org) |
| `ListAggregatesRequest` | 9 | Fixed | List aggregates (optionally filtered) |
| `RegisterSchemaRequest` | 10 | Variable | Register event schema (Json/Avro/Protobuf) |

### Client Response Types

| Type | ID | Size | Purpose |
|------|----|------|---------|
| `AggregateDetailsResponse` | 1 | Fixed | Aggregate metadata: aggregate version, deletion status, recreate/index flags, last op |
| `ReadResponse` | 2 | Variable | Event batches + next cursor |
| `SuccessResponse` | 3,4,5,12 | Fixed | Write/TrimStart/Delete/RegisterSchema success |
| `ProtocolErrorResponse` | 6 | Fixed | Unreadable request (no correlation ID) |
| `ErrorResponse` | 7 | Fixed | Error with code, message, correlation ID |
| `WatchResponse` | 8 | Variable | Aggregate change notifications |
| `ListOrgsResponse` | 9 | Variable | Org list + pagination cursor |
| `ListAggregateTypesResponse` | 10 | Variable | Aggregate type list + cursor |
| `ListAggregatesResponse` | 11 | Variable | Aggregate list with metadata + cursor |

### Cluster Request Types

| Type | ID | Size | Purpose |
|------|----|------|---------|
| `ReplicationBatchRequest` | 100 | Variable | Leader pushes WAL entries to follower |
| `HeartbeatRequest` | 101 | Fixed | Liveness signal: shard_id + leader timestamp |
| `KickFollowerRequest` | 102 | Fixed | Command to evict follower |

### Cluster Response Types

| Type | ID | Size | Purpose |
|------|----|------|---------|
| `ReplicationBatchResponse` | 100 | Fixed | Follower response: timestamp + `ReplicationResult` |
| `HeartbeatResponse` | 101 | Fixed | `HeartbeatResult`: Ack or Rejected |
| `KickFollowerResponse` | 102 | Fixed | Acknowledgement flag |
| `ProtocolErrorResponse` | 106 | Fixed | Unreadable cluster request |
| `ErrorResponse` | 107 | Fixed | Cluster error with code and message |

### Identify Types

| Type | ID | Size | Purpose |
|------|----|------|---------|
| `IdentifyRequest` | 14 | Fixed | RSA signature + API key authentication |
| `IdentifyResponse` | 16 | Fixed | Returns client_id + `AccessLevel` |

### Replication Enums

| Type | Variants | Purpose |
|------|----------|---------|
| `ReplicationResult` | `Success { last_follower_metablock }`, `Rejected(FollowerRejection)` | Outcome of a replication batch |
| `FollowerRejection` | `NotAFollower`, `TimeDriftTooHigh { leader_ms, follower_ms, max_allowed_ms }`, `WalSeqMismatch { max_follower_wal_seq }`, `TipHashMismatch { follower, follower_wal_seq, leader, leader_wal_seq }`, `EmptyBatch`, `MissingDatablock`, `StaleLease { follower_lease_epoch, received_lease_epoch }` | Why follower refused batch |
| `HeartbeatResult` | `Ack { follower_timestamp_ms }`, `Rejected(HeartbeatRejection)` | Outcome of a heartbeat |
| `HeartbeatRejection` | `ClockDriftTooHigh { leader_ms, follower_ms, max_allowed_ms }`, `NotAFollower` | Why follower refused heartbeat |

### Error Codes

All error codes are defined in `error_codes.rs`. No other file should define `u32` error codes.

| Range | Category | Examples |
|-------|----------|----------|
| 1xxx | Read errors | `READ_AGGREGATE_NOT_EXISTS` (1001), `READ_FETCH_DATABLOCKS` (1004) |
| 2xxx | Write errors | `WRITE_NOT_LEADER` (2011), `WRITE_OPTIMISTIC_CONCURRENCY_VIOLATION` (2003) |
| 2020–2029 | Schema registration | `REGISTER_SCHEMA_ALREADY_EXISTS` (2020), `REGISTER_SCHEMA_INVALID` (2021) |
| 3xxx | Trim errors | `TRIM_NOT_LEADER` (3005), `TRIM_INDEX_OUT_OF_RANGE` (3004) |
| 4xxx | Delete errors | `DELETE_NOT_LEADER` (4006), `DELETE_OPTIMISTIC_CONCURRENCY_VIOLATION` (4002) |
| 5xxx | Listing errors | `LIST_ORGS_DISK_READ` (5000) |
| 6xxx | Replication batch | `REPLICATION_BATCH_FSYNC` (6000), `REPLICATION_BATCH_wal_seq_GAP` (6002) |
| 7xxx | Exists/aggregate-details | `EXISTS_AGGREGATE_NOT_EXISTS` (7001) |
| 8xxx | Watch errors | `WATCH_REQUEST_INVALID` (8000), `WATCH_LATENCY_TOO_HIGH` (8001) |
| 9xxx | Shard routing | `SHARD_ROUTING_NO_KEY` (9000), `SHARD_ROUTING_MULTIPLE_SHARDS` (9001) |
| 10xxx | Identity & auth | `IDENTIFY_REQUIRED` (10004), `AUTH_REQUIRED` (10005), `AUTH_INVALID_KEY` (10006) |
| 11xxx | Server health | `SERVER_BUSY` (11000) |

`ErrorResponse` methods: `is_not_leader()`, `is_identity_required()`, `is_server_busy()`, `parse_leader_address()`.

## Design Decisions

### Client/Cluster Separation

Client and cluster messages use separate enums with distinct wire ID ranges. This makes port routing type-safe and eliminates runtime classification.

### Correlation IDs

All requests carry `Option<u128>` correlation IDs, echoed in responses for async request/response matching. `ProtocolErrorResponse` omits this since the request couldn't be parsed.

### Fixed vs Variable Size

Fixed-size messages use stack buffers and skip compression - optimal for small, predictable payloads. Variable-size messages use heap allocation with configurable compression for larger data.

### Batch Operations

Write and Delete support multiple aggregates per request. This enables atomic multi-aggregate operations within a shard and reduces round trips. Multi-aggregate writes spanning multiple shards are rejected.

### Read Filters and Watch Subscriptions

`ReadFilters` supports rich querying (aggregate version range, client/user ID filters, server and client timestamp ranges, event type whitelist) without server-side indexing. `WatchRequest` supports filtering by shard, org, aggregate type, aggregate ID, and operation type, with an optional batching latency hint.

### Replication Protocol

Leader-to-follower replication uses a push flow. Leader sends `ReplicationBatchRequest` containing a `Vec<ReplicationBatchItem>` (each item is a `Metablock` + optional `Datablock`). Follower validates WAL position and tip hash before applying, returning `ReplicationBatchResponse` with a `ReplicationResult`. `FollowerRejection` variants carry contextual data (timestamps, WAL sequencees, hashes) so the leader can take corrective action.

### Identify Flow

`IdentifyRequest` handles authentication before any client requests are processed. Supports RSA signature (public key + nonce + signature) and API key. `IdentifyResponse` returns `client_id` and `AccessLevel` (ReadWrite or ReadOnly).

### Compression

Variable-size client responses use the server's configured compression algorithm. All fixed-size messages and all cluster responses use `CompressionType::None`.
