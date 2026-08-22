# celeriant_wal

Internal crate. Write-ahead log data structures and serialization types for [Celeriant](https://celeriant.io). No I/O logic, just types and serialization.

Defines the WAL storage format (metablocks, datablocks, headers, segment summaries), aggregate keys, bloom filters, compression types, and S3 coordination types.

If you're building a client application, use [`celeriant_client_tokio`](https://crates.io/crates/celeriant_client_tokio) instead.

## License

Apache 2.0
