# EventPlaneDB Rust Client

High-performance Rust client for EventPlaneDB with connection pooling, automatic retries, and FFI support for other languages.

## Features

- **Connection Pooling**: Efficient connection reuse with configurable pool sizes
- **Pipelining**: Multiple concurrent requests on the same connection
- **Automatic Retries**: Exponential backoff for transient failures
- **Timeout Handling**: Configurable timeouts for all operations
- **Health Checking**: Automatic connection health monitoring
- **FFI Support**: C-compatible API for bindings in C#, Java, Go, C++, etc.
- **Compression**: Support for multiple compression algorithms (Zstd, Snappy, Brotli, Gzip)

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
eventplanedb_client = "0.1.0"
tokio = { version = "1.40", features = ["full"] }