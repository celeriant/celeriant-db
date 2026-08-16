# Changelog

All notable changes to Celeriant are documented in this file. Client crate versions
track the workspace version; this log covers the server and the published client
crates together.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-08-16

Initial public release.

- Celeriant server: sharded event store with WAL durability, S3-backed leader
  election, replication, schema validation, watch streams, and chaos-tested
  data integrity.
- Published client crates: `celeriant_client_tokio` (async Tokio client with
  connection pooling, leader routing, TLS/mTLS, client identity) and its
  dependency closure (`celeriant_wal`, `celeriant_crypto`, `celeriant_wire`,
  `celeriant_msg`, `celeriant_client_wire`).
- Pool routing: reads and watch subscriptions go to the leader by default
  (read-your-writes); `route_reads_to_followers` opts into follower routing
  with the leader as last resort when every follower is unreachable.
- Docker image: `ghcr.io/celeriant/celeriant` (amd64).
