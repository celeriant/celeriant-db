# celeriant_msg

Internal crate. Request and response message types for the [Celeriant](https://celeriant.io) wire protocol.

Client operations (read, write, delete, watch, list) and cluster operations (replication, heartbeat). You may need this crate directly for types like `ReadRequest`, `ReadFilters`, and `WriteRequest`.

If you're building a client application, start with [`celeriant_client_tokio`](https://crates.io/crates/celeriant_client_tokio).

## License

Apache 2.0
