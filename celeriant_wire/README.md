# celeriant_wire

Internal crate. Wire protocol framing, serialization, and compression for [Celeriant](https://celeriant.io).

Handles encoding/decoding for network messages and WAL/S3 persistence. Supports bincode and MessagePack serialization with Zstd, Snappy, Brotli, and Gzip compression.

If you're building a client application, use [`celeriant_client_tokio`](https://crates.io/crates/celeriant_client_tokio) instead.

## License

Apache 2.0
