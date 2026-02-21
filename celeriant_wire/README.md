# celeriant_wire

Serialization, compression, and wire protocol framing for Celeriant. Handles encoding/decoding for both network messages and WAL/S3 persistence.

## Architecture

```
Wire Frame (network messages)
┌──────────────────┬──────────────────────────────┐
│   Header (17B)   │         Payload              │
│ ver|type|lengths │  serialized + compressed     │
│     |comp_type   │  message body                │
└──────────────────┴──────────────────────────────┘

Versioned Block (WAL/S3 persistence)
┌──────────┬─────────┬───────────────────────────┐
│ CRC (4B) │ Ver(4B) │  bincode payload + zeros  │
└──────────┴─────────┴───────────────────────────┘
```

**Wire frames**: Network protocol with async read/write, version negotiation, compression.
**Versioned blocks**: CRC-protected fixed-size blocks for WAL headers, metablocks, and S3 storage objects.

## Module Structure

| Module | Purpose |
|--------|---------|
| `network::wire_header` | Wire frame header, async read/write, protocol negotiation |
| `network::wire_error` | Network-level error type |
| `disk::versioned_block` | CRC+version framing for WAL and S3 blocks |
| `disk::serialised_datablock` | Datablock serialization with inline/block storage selection |
| `disk::metablock_bytes` | Zero-copy byte-level field access for metablock scanning |
| `disk::disk_format_error` | Disk format error type |
| `codec::bincode` | Fixed-int bincode serialization helpers |
| `codec::msgpack` | MessagePack serialization helpers |
| `codec::compression` | Multi-algorithm compress/decompress |
| `codec::codec_error` | Unified codec error type |

## Key Types

| Type | Purpose |
|------|---------|
| `WireHeader` | Network frame header: version, message type, lengths, compression |
| `SerialisedDatablock` | Result of datablock serialization (inline or external block) |
| `WireError` | Network-level errors |
| `DiskFormatError` | Disk format errors (CRC, version, codec) |
| `CodecError` | Unified serialization/compression error |

## Key Functions

| Function | Purpose |
|----------|---------|
| `wire_header_write_fixed_size` | Write small fixed-size framed message to async stream |
| `wire_header_write_variable_size` | Write variable-length framed message with optional compression |
| `WireHeader::from_reader` | Parse wire header from async stream, validate sizes |
| `WireHeader::read_variable_size` | Read and deserialize variable-size payload |
| `WireHeader::read_fixed_size` | Read and deserialize fixed-size payload into stack buffer |
| `serialize_versioned_message` | Serialize into fixed buffer with CRC+version header |
| `serialize_versioned_message_heap` | Serialize to heap-allocated Vec with CRC+version header |
| `deserialise_metablock` | Deserialize a WAL metablock from a fixed-size buffer |
| `deserialise_shard_log_header` | Deserialize a shard log header |
| `deserialise_fallback_batch` | Deserialize an S3 fallback batch |
| `deserialise_lease` | Deserialize an S3 lease |
| `deserialise_membership` | Deserialize an S3 membership record |
| `SerialisedDatablock::new` | Auto-choose inline (<=512B) or block storage for a datablock |
| `deserialise_datablock` | Deserialize a datablock from inline or external block storage |

## Constants

| Constant | Value | Description |
|----------|-------|-------------|
| `WIRE_HEADER_SIZE` | 17 bytes | Network wire frame header size |
| `WIRE_FIXED_BODY_SIZE` | 1007 bytes | Stack buffer size for small network messages (1024 - 17) |
| `PROTOCOL_VERSION_V2` | 2 | bincode protocol version |
| `PROTOCOL_VERSION_V3` | 3 | msgpack protocol version |
| `HEADER_SIZE` (disk) | 8 bytes | CRC (4B) + version (4B) prefix on versioned blocks |
| `CRC_SIZE` | 4 bytes | CRC32C field size in versioned block header |
| `MINIBATCH_SIZE_BYTES` | 512 bytes | Inline datablock threshold (from `celeriant_wal`) |

## Error Types

### `CodecError` (codec/codec_error.rs)

Unified error for all serialization and compression operations.

```rust
pub enum CodecError {
    Serialization(String),
    Deserialization(String),
    Compression(String),
}
```

Implements `From` for: `bincode::error::DecodeError`, `bincode::error::EncodeError`, `rmp_serde::encode::Error`, `rmp_serde::decode::Error`, `snap::Error`, `std::io::Error`, `CompressionError`.

### `DiskFormatError` (disk/disk_format_error.rs)

Errors from reading versioned blocks off disk.

```rust
pub enum DiskFormatError {
    DatablockExpected,
    ExternalDataMissing,
    ChecksumMismatch { expected: u32, actual: u32 },
    UnsupportedVersion(u32),
    HeaderSizeMismatch { expected: usize, actual: usize },
    Codec(CodecError),
}
```

### `WireError` (network/wire_error.rs)

Errors from network framing.

```rust
pub enum WireError {
    NetworkError(std::io::Error),
    MessageTooLarge { message_length: u64, max_size_bytes: u64 },
    UnsupportedProtocol(u32),
    Codec(CodecError),
}
```

## Design Decisions

### Protocol versions

| Version | Constant | Format | Use Case |
|---------|----------|--------|----------|
| V2 | `PROTOCOL_VERSION_V2` | bincode | Rust clients (~7x faster) |
| V3 | `PROTOCOL_VERSION_V3` | msgpack | Non-Rust clients (standard format) |

Both use identical wire framing and compression; only payload encoding differs.

### Compression types

```rust
CompressionType::None           // No compression
CompressionType::Zstd { level } // Best balance (recommended)
CompressionType::Snappy         // Fastest decompression
CompressionType::Brotli { level } // Best ratio
CompressionType::Gzip { level } // Compatibility
```

Compression is per-message. Type stored as a single byte in the wire header for self-describing frames.

### Fixed vs variable serialization

- **Fixed** (`bincode_fixed_*`): Fixed-width integers, stack buffer, no compression. For small known-size messages.
- **Variable** (`wire_header_write_variable_size`): Optional compression, heap allocation for large payloads. For larger payloads.

**Stack optimization**: When `compressed_length <= WIRE_FIXED_BODY_SIZE` (1007 bytes) and compression is `None`, variable-size writes use a stack buffer instead of heap allocation. This avoids a heap allocation for the common case of small uncompressed messages.

### Inline datablock optimization

`SerialisedDatablock::new` checks serialized size:
- <=512 bytes (`MINIBATCH_SIZE_BYTES`) → stored inline in metablock's `DatablockInlineData` (fixed 512-byte array in the metablock itself)
- >512 bytes → compressed, stored as `DatablockBlockRef` with CRC32C, written to external datablock region

Inline storage avoids an extra disk seek for small event batches. Compression still applies to inline data when a compression type is specified.

### CRC placement for versioned blocks

```
[CRC32C (4B)][Version (4B)][Payload...][zero padding]
```

CRC covers version + payload (everything after the CRC field itself). Validated before deserialization to detect corruption early. Version is checked only after CRC passes, providing meaningful errors for version mismatches vs. corruption.

The buffer is always zero-padded to fill the fixed-size block; this ensures deterministic CRC values across partial writes.

### Decompression bomb prevention

`WireHeader::from_reader` validates **both** `compressed_length` and `uncompressed_length` against `max_size_bytes`. This prevents:
1. Memory exhaustion from large compressed payloads
2. Decompression bombs (tiny compressed input, gigantic uncompressed output)

### Why return uncompressed_size?

Variable serialization returns both sizes. The uncompressed size is required for decompression (zstd/brotli need an allocation hint) and stored in wire headers for validation.

### S3 storage types

The versioned block format is used for S3 coordination objects in addition to WAL blocks:

| Type | Version Constant | Description |
|------|-----------------|-------------|
| `FallbackBatch` | `WIRE_VERSION_S3_FALLBACK_BATCH` | Batched writes for S3 fallback path |
| `Lease` | `WIRE_VERSION_S3_LEASE` | Distributed lease for leader election |
| `Membership` | `WIRE_VERSION_S3_MEMBERSHIP` | Cluster node membership |

All use the same `[CRC32C][Version][Payload]` framing with type-specific version constants.

## Zero-Copy Metablock Scanning (disk/metablock_bytes.rs)

The `metablock_bytes` module provides `#[inline]` functions for reading fields directly from serialized metablock byte slices without full deserialization. Used during WAL scanning to avoid heap allocations in the hot path.

**Offset layout**: All functions account for the versioned block header (`HEADER_SIZE = 8`) plus fixed field offsets defined on `Metablock` and its payload types.

### Common field readers (all metablock types)

| Function | Return | Description |
|----------|--------|-------------|
| `read_metablock_kind_discriminant` | `u8` | Raw enum discriminant for type identification |
| `read_wal_index` | `u64` | WAL sequence number |
| `read_server_timestamp` | `u64` | Server-assigned timestamp |
| `read_compressed_size` | `u64` | Compressed datablock size |
| `read_uncompressed_size` | `u64` | Uncompressed datablock size |

### Kind predicates

| Function | Description |
|----------|-------------|
| `is_metablock_kind_event_batch_metadata` | True for discriminant 0 |
| `is_metablock_kind_soft_delete` | True for discriminant 4 |
| `is_metablock_kind_soft_trim` | True for discriminant 5 |

### Aggregate key matching

| Function | Description |
|----------|-------------|
| `is_matches_aggregate_key` | True if EventBatchMetadata and key matches |
| `is_soft_delete_for_aggregate` | True if SoftDelete and key matches |
| `is_soft_trim_for_aggregate` | True if SoftTrim and key matches |

### EventBatch field readers

| Function | Return |
|----------|--------|
| `read_event_batch_org_id` | `u128` |
| `read_event_batch_aggregate_type_id` | `u128` |
| `read_event_batch_aggregate_id` | `u128` |
| `read_event_batch_aggregate_key` | `AggregateKey` |
| `read_event_batch_event_batch_index` | `u64` |
| `read_event_batch_min_event_batch_index` | `u64` |
| `read_event_batch_min_event_index` | `u64` |
| `read_event_batch_max_event_index` | `u64` |
| `read_event_batch_min_event_timestamp` | `u64` |
| `read_event_batch_max_event_timestamp` | `u64` |
| `read_event_batch_client_id` | `u128` |
| `read_event_batch_max_client_event_index` | `u64` |

### SoftDelete / SoftTrim field readers

| Function | Return |
|----------|--------|
| `read_soft_delete_org_id` | `u128` |
| `read_soft_delete_aggregate_type_id` | `u128` |
| `read_soft_delete_aggregate_id` | `u128` |
| `read_soft_delete_aggregate_key` | `AggregateKey` |
| `read_soft_trim_org_id` | `u128` |
| `read_soft_trim_aggregate_type_id` | `u128` |
| `read_soft_trim_aggregate_id` | `u128` |
| `read_soft_trim_keep_from_event_batch_index` | `u64` |

**MetablockKind discriminants** (from `celeriant_wal`):

| Discriminant | Variant |
|-------------|---------|
| 0 | `EventBatchMetadata` |
| 1 | `SnapshotOrg` |
| 3 | `SnapshotAggregate` |
| 4 | `SoftDelete` |
| 5 | `SoftTrim` |

## Dependencies

- `celeriant_wal` - WAL types (Metablock, Datablock, CompressionType, constants)
- `bincode` - Rust-native binary serialization (fixed-int encoding)
- `rmp-serde` - MessagePack for cross-language clients
- `serde` - Serialization framework
- `zstd`, `snap`, `brotli`, `flate2` - Compression algorithms
- `crc32c` - Hardware-accelerated checksums
- `futures-lite` - Async I/O traits
