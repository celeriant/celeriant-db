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

## Invariants

- Every on-disk block is prefixed with 8 bytes: `[CRC32C (4B LE)] [Version (4B LE)]`. CRC covers everything after the CRC field (version + payload).
- Version is checked after CRC validation. This distinguishes corruption from format incompatibility. Unknown versions are rejected with `UnsupportedVersion`.
- Both `compressed_length` and `uncompressed_length` are validated against `max_size_bytes` before any allocation or decompression. Prevents decompression bombs.
- Messages that fit in a fixed-size buffer (header + body <= 1024 bytes, no compression) must be stack-allocated. Heap allocation for small messages is prohibited.
- Two protocol versions: V2 (bincode, fixed-int, little-endian) and V3 (MessagePack). V0, V1, V4+ are rejected with `UnsupportedProtocol`.
- Protocol version is set on the first message. No renegotiation.
- All on-disk structures use bincode with fixed-width integer encoding and little-endian byte order. No varints.

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
| `WireError` | Network-level errors (size limits, unsupported protocol) |
| `DiskFormatError` | Disk format errors (CRC, version, codec) |
| `CodecError` | Unified serialization/compression error |

## Design Decisions

### Protocol versions

| Version | Format | Use case |
|---------|--------|----------|
| V2 | bincode | Rust clients (~7x faster) |
| V3 | msgpack | Non-Rust clients (standard format) |

Both use identical wire framing and compression; only payload encoding differs.

### Compression

```rust
CompressionType::None
CompressionType::Zstd { level }   // best balance (recommended)
CompressionType::Snappy           // fastest decompression
CompressionType::Brotli { level } // best ratio
CompressionType::Gzip { level }   // compatibility
```

Compression is per-message. Type stored as a single byte in the wire header for self-describing frames.

### Stack allocation for small messages

When `header + body <= 1024 bytes` and compression is `None`, both fixed-size and variable-size writes use a stack buffer. The `WIRE_FIXED_BODY_SIZE` (1007 bytes) constant is `1024 - WIRE_HEADER_SIZE`. Heap allocation for messages below this threshold is a bug.

### Inline datablock optimization

`SerialisedDatablock::new` checks serialized size:
- <=512 bytes (`MINIBATCH_SIZE_BYTES`) → stored inline in `DatablockInlineData` (fixed 512-byte array inside the metablock)
- >512 bytes → compressed, stored as `DatablockBlockRef` with CRC32C, written to the external datablock region

Inline storage avoids an extra disk seek for small event batches.

### CRC placement for versioned blocks

```
[CRC32C (4B)][Version (4B)][Payload...][zero padding]
```

CRC covers version + payload. Validated before deserialization to catch corruption early. Version is checked only after CRC passes, so `ChecksumMismatch` and `UnsupportedVersion` are unambiguous.

The buffer is always zero-padded to the fixed block size to ensure deterministic CRC values across partial writes.

### S3 storage types

The versioned block format is used for S3 coordination objects in addition to WAL blocks. `Lease` and `Membership` are serialized as pretty-printed JSON (no CRC/version envelope). `FallbackBatch` uses the standard versioned block framing.

### Zero-copy metablock scanning

`disk::metablock_bytes` provides `#[inline]` functions that read fields directly from serialized byte slices without full deserialization. Used during WAL scanning to avoid heap allocations in the hot path. All offsets account for the 8-byte versioned block header plus the fixed field layout of `Metablock` and its payload types.
