# celeriant_wal

Core data structures for the Celeriant write-ahead log. This crate defines the event, batch, and metadata types used throughout the server and client libraries.

## Overview

This crate provides the serializable data structures that form the foundation of Celeriant's storage and wire protocol. It contains no I/O logic—just types and their serialization implementations.

```
EventBatchItem
├── batch-level metadata (indexes, timestamps, identity)
└── Vec<EventItem>
    ├── event payload (Arc<Vec<u8>>)
    └── event metadata (type, timestamps, indexes)

EventBatchMetadata (separate file)
├── min/max ranges for filtering
├── event type bloom filter or direct array
└── compression/integrity info
```

## Key Types

### EventItem

An individual event within a batch.

```rust
pub struct EventItem {
    pub client_event_index: u64,    // Client-assigned, for idempotency
    pub event_index: u64,           // Server-assigned, globally unique within aggregate
    pub event_id: Option<u128>,     // Optional client-assigned unique ID
    pub event_timestamp: u64,       // Client-side Unix ms timestamp
    pub event_type_major: u64,      // Event schema identifier
    pub event_type_minor: u64,      // Forward-compatible schema version
    pub event_value: Arc<Vec<u8>>,  // Opaque payload
    pub iv: Option<[u8; 12]>,       // AES-GCM initialization vector
}
```

### EventBatchItem

A batch of events from a single client, persisted atomically.

```rust
pub struct EventBatchItem {
    pub event_batch_index: u64,     // Server-assigned batch sequence number
    pub server_timestamp: u64,      // Server-side Unix ms timestamp
    pub client_id: u128,            // Machine identity
    pub user_id: Option<u128>,      // Human identity (optional)
    pub node_id: u128,              // Which cluster node wrote this batch
    pub lease_index: u64,           // Leadership lease at write time
    pub events: Vec<EventItem>,
}
```

### EventBatchMetadata

Metadata written alongside each batch for efficient filtering.

```rust
pub struct EventBatchMetadata {
    pub event_types_data: EventTypesData,  // Bloom or direct array
    pub min_event_index: u64,
    pub max_event_index: u64,
    pub min_event_timestamp: u64,
    pub max_event_timestamp: u64,
    // ... compression, CRC, sizing fields
}
```

### Shard Log Structures

The shard log structures define how WAL data is stored on disk. Each shard has its own WAL, split into multiple ~1GB files that rotate when full.

#### File Layout

```
┌─────────────────────────────────────────────────────────────┐
│ Header (fixed size)                                         │
├─────────────────────────────────────────────────────────────┤
│ Metadata entries (256 bytes each, growing downward →)       │
│ ...                                                         │
├─────────────────────────────────────────────────────────────┤
│                      Free space                             │
├─────────────────────────────────────────────────────────────┤
│                                    ... Event batches        │
│              (compressed, variable size, growing ← upward)  │
├─────────────────────────────────────────────────────────────┤
│ Checkpoint (at end, pre-allocated space, can extend file)   │
└─────────────────────────────────────────────────────────────┘
```

Files are preallocated to 1GB. Metadata grows from the top, event batches grow from the bottom. When they meet, the file rotates. The checkpoint is stored at the file's end with reserved space, but can extend beyond 1GB if tracking many aggregates or clients.

#### ShardLogHeader

File header with version and checkpoint location.

```rust
pub struct ShardLogHeader {
    pub shard_log_version: u32,              // File format version
    pub shard_log_checkpoint_version: u64,   // Checkpoint format version
    pub shard_log_checkpoint_start_pos: u64, // Byte offset to checkpoint
}
```

#### ShardLogCheckpoint

Authoritative source of truth for file state. Tracks write positions and aggregate metadata.

```rust
pub struct ShardLogCheckpoint {
    pub file_size: u64,                                  // Logical file size (not OS-reported)
    pub aggregates: HashMap<AggregateKey, ShardLogAggregate>, // Per-aggregate metadata
    pub metadata_pos: u64,                               // End of metadata region
    pub event_batches_pos: u64,                          // Start of event batches region
}
```

Key behaviors:
- `file_size` is tracked explicitly because direct I/O may pad bytes
- `metadata_pos` advances forward as metadata entries are written
- `event_batches_pos` decreases as batches are written from the end
- `available_space()` returns bytes between the two regions

#### ShardLogAggregate

Per-aggregate state within a shard log file.

```rust
pub struct ShardLogAggregate {
    pub client_event_indexes: Option<HashMap<u128, u64>>, // Client idempotency tracking
    pub last_event_index: u64,
    pub last_event_batch_index: u64,
    pub minimum_available_event_batch_index: u64,         // After trimming
    pub compressed_size_bytes: u64,
    pub uncompressed_size_bytes: u64,
    pub created_at: u64,
    pub updated_at: u64,
    pub read_at: Option<u64>,                             // LRU eviction hint
}
```

- `client_event_indexes` may be `None` if evicted from memory under pressure—requires scanning metadata to reconstruct
- `minimum_available_event_batch_index` reflects trimmed batches
- `read_at` helps identify cold aggregates whose metadata can be evicted

## Design Decisions

### Arc-Wrapped Event Payloads

`event_value` is `Arc<Vec<u8>>` rather than `Vec<u8>`:

```rust
pub event_value: Arc<Vec<u8>>,
```

Arc enables zero-copy transfer across thread boundaries. Celeriant uses a thread-per-core architecture so it doesn't technically need Arcs. We still wrap event_value in arc however to make it easier to move data around in clients on the tokio side, and when Celeriant is embedded inside another tokio based server as a library. Our testing indicates that performance cost is minimal.

### Client Identity vs User Identity

Two identity fields serve different purposes:

| Field | Source | Purpose |
|-------|--------|---------|
| `client_id` | Truncated SHA-256 of client's public key or a UUID | Machine identity, connection authentication |
| `user_id` | Truncated SHA-256 of user's `sub` claim | Human identity, multi-tenant authorization |

A single user may connect from multiple clients (devices). A single client may handle requests for multiple users. Both are truncated to 128 bits for storage efficiency while maintaining collision resistance.

`user_id` is optional—service-to-service writes may have no associated user.

### Client Event Index for Idempotency

```rust
pub client_event_index: u64,
```

Each client maintains a monotonically increasing counter. When `enforce_client_idempotency` is enabled on writes, the server rejects events with a `client_event_index` it has already seen from that `client_id`.

This provides exactly-once semantics for clients that retry on network failures. The server tracks the highest seen index per client and rejects duplicates.

It's designed to be used by clients that have a transactional database of their own, and follow the inbox pattern.

### Dual Timestamps

| Field | Set By | Purpose |
|-------|--------|---------|
| `event_timestamp` | Client | When the event occurred in the real world |
| `server_timestamp` | Server | When the batch was persisted |

Both are Unix milliseconds. They allow for queries against the aggregate that filter by time. Don't use these for ordering though as multiple clients may write batches at the same time.

### Optional Event ID

```rust
pub event_id: Option<u128>,
```

Clients can assign their own unique identifiers to events. Useful for:

- Correlation across systems
- External references to specific events

Most use cases don't need this—`client_event_index` handles idempotency, and `event_index` provides a server-assigned identifier. Clients use the `event_batch_index` on reads get events from the right position in the aggregate.

### Per-Event Encryption Support

```rust
pub iv: Option<[u8; 12]>,
```

When present, indicates `event_value` is encrypted with AES-256-GCM. The 12-byte IV (initialization vector) is the standard size for AES-GCM and must be unique per encryption operation.

The server stores encrypted payloads opaquely. Encryption/decryption happens client-side. This enables end-to-end encryption where the server never sees plaintext event data.

Don't use compression if storing encrypted events.

### Distributed Fencing Fields

```rust
pub node_id: u128,
pub lease_index: u64,
```

These fields prevent split-brain writes in replicated deployments.

Celeriant uses lease-based leadership for writes using a shared control plane. `node_id` identifies which cluster node wrote a batch. `lease_index` is the leadership lease term at write time.

Leaders must write to a quorum of followers to accept writes from clients. Followers don't allow replication from a leader if the leader's `lease_index` is less than the follower's current `lease_index`.

### Metadata Separation

`EventBatchMetadata` exists as a separate structure from the event data itself. `EventBatchMetadata` is always stored as 256 bytes on disk, allowing quick calculation of offsets on disk using the `event_batch_index` (There are never any gaps in the batch index sequence)

**Filtering without decompression**: Event data is compressed. Metadata is not. When filtering by event type or time range, the server reads metadata first to skip irrelevant batches entirely.

**Index acceleration**: Min/max fields enable binary search over batch ranges:

```rust
pub min_event_index: u64,
pub max_event_index: u64,
pub min_event_timestamp: u64,
pub max_event_timestamp: u64,
pub min_client_event_index: u64,
pub max_client_event_index: u64,
```

**Event type filtering**: The `EventTypesData` enum supports two strategies:

```rust
pub enum EventTypesData {
    Bloom([u64; 4]),   // 256-bit bloom filter for many event types
    Direct([u64; 4]),  // Up to 4 event type IDs stored directly
}
```

When a batch contains ≤4 unique event types, they're stored directly for exact matching. With more types, a bloom filter provides probabilistic filtering (false positives possible, false negatives impossible) and further filtering happens at the event level in memory later.

### Serialization Choices

**Short field names**: All serde renames use 2-3 character keys:

```rust
#[serde(rename = "bx")]
pub event_batch_index: u64,
```

This reduces JSON payload size by ~40% compared to full field names. The wire protocol is binary (bincode), but JSON is used for debugging, logging, and the HTTP API.

**Base64-encoded u128**: JSON has no native 128-bit integer type. JavaScript's `Number` loses precision beyond 53 bits. We encode u128 fields as base64 strings:

```rust
#[serde(with = "serde_u128_base64", rename = "ci")]
pub client_id: u128,
```

This produces 22-character strings (128 bits → 22 base64 chars) that round-trip correctly through any JSON parser.

## Compression

The `CompressionType` enum supports multiple algorithms:

```rust
pub enum CompressionType {
    None,
    Zstd { level: i32 },   // Default, best ratio/speed
    Snappy,                 // Fastest decompression
    Brotli { level: i32 }, // Best ratio
    Gzip { level: i32 },   // Compatibility
}
```

Compression is per-batch. The server compresses event data before writing; metadata remains uncompressed.

It's up to you to decide which batches to compress, what algorithm and what compression level. A trade off between speed and size. Consider that large payloads benefit more from compression.

## Dependencies

Minimal by design:

- `serde` - Serialization framework for attributes on structs
- `bincode` - Required due to bincode v2 not using standard serde attributes
- `base64` - u128 JSON encoding support

No async runtime, no I/O, no platform-specific code. See `celeriant_wire` for ser/deser routines.
