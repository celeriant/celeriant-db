# celeriant_client_wire

Send-safe client frame I/O over the Celeriant wire protocol: frame building,
compression/decompression with shared zstd dictionaries, and async read/write
helpers usable from multi-threaded (Tokio) executors.

You normally don't depend on this crate directly — `celeriant_client_tokio` uses it
and exposes the client API. It exists as a separate crate so client-side frame
handling stays free of the server's single-threaded executor assumptions.

- [Celeriant](https://celeriant.io)
- [GitHub](https://github.com/celeriant/celeriant-db)

## License

Apache 2.0
