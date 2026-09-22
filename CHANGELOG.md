# Changelog

All notable changes to Celeriant are documented in this file. Client crate versions
track the workspace version; this log covers the server and the published client
crates together.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-09-22

Client API has three breaking changes.

### Breaking

- `ClientError` is `#[non_exhaustive]` and gains four variants:
  `ConnectionLostAfterSend`, `PoolTimeout`, `InvalidShardRange`, and
  `DictUnavailable`. The first two variants exist so a
  caller can tell a request that was never sent from one whose outcome is
  unknown.
- `celeriant_client_wire::build_frame` takes a `max_size_bytes` cap. An
  oversized body is now refused while it is still uncompressed.
- `ListOperations::new` returns `Result<Self, ClientError>`. Shard range checks.

### Added

- `ClientTlsConfig::from_paths` builds a client config from PEM paths directly.
  Pass an identity pair for mTLS, `None` for a client that verifies the server
  and presents nothing. `ClientTlsConfig::new` is unchanged for callers that
  already hold a `rustls::ClientConfig`.
- `sni_host` takes the host out of a `host:port` address for use as the TLS
  SNI, including bracketed IPv6 literals.
- `CeleriantPool::stats()` snapshots pool counters: per-node connection stats,
  leader redirects followed and skipped, walks exhausted, pool timeouts, and
  post-send losses. Each counter costs one relaxed increment on the hot path.

### Changed

- The node ID derives from the private key at every start. The public key file
  is no longer read or written. Derivation itself is unchanged,
  `SHA-256(DER public key)[..16]`, so an existing node keeps its identity
  across the upgrade as long as its `private_key` is intact. There is no longer
  a second key file to corrupt, swap, or leave half written.
- Key PEMs are created with mode 0600 instead of being chmod'ed after the
  write. A key file that already existed is tightened on rewrite.
- The server TLS accept path interleaves handshakes, and a slot semaphore
  bounds how many run at once. Handshakes are CPU bound and still block, but a
  burst of cold connections no longer swamps the accept loop.
- Metric descriptions and the Grafana dashboard corrected.

### Fixed

- Waiting for a local pool slot reported `no leader found`. It is now
  `PoolTimeout`, which says no node was contacted and the request is safe to
  retry.
- A timeout that reached the NIC surfaces to the caller instead of being
  retried. The request may have been applied, so re-sending it anywhere is
  wrong. Handshake steps still retry.
- Pooled connections are polled before reuse, so a socket the server has
  already closed is not handed to a caller.
- The tokio client splits the header stream read from deserialisation. A
  connection stays reusable when a body fails to deserialise.
- Watch streams apply the configured max response size and timeout, neither of
  which was wired up. The trim floor no longer moves backwards when events are
  merged.
- A shard cannot start a second S3 catchup while one is in flight, tracked per
  shard by `s3_catchup_in_flight` rather than by node status.
- The protocol version cannot change on an open connection.

## [0.1.2] - 2026-08-24

### Fixed

- Client requests split across two TCP segments broke request deserialisation
  under ktls: userspace read the first segment, the kernel took the second.
  The connection now stays in userspace until handover is safe, with a bound on
  the buffered bytes so a malformed peer cannot exhaust server memory.
- `celeriant_client_tokio` and the glommio client are cancellation safe. A
  dropped future no longer leaves a connection with a half-read frame.
- Watch streams return an error when a notification could have been missed, for
  example across a node leadership transition. Silence used to be
  indistinguishable from no events.
- Follower takeover did not update `client_seq` from S3 catchup, so a client
  writing through the new leader appended duplicate events to the WAL.
- Shard boot no longer panics on transient failures. It backs off and retries,
  holding its current lease state while it waits.
- Shard 0 no longer queues elections requested by other shards. Requests in
  flight are tracked and collapsed.
- DMA correctness on the disk path: dirty buffers, short reads, reads past end
  of file, and fd headroom accounting that could panic under pressure.
- Mesh channel size back to 8192 with the heartbeat connection held open. Fixes
  server-busy errors and dropped heartbeats under load.

### Changed

- Glommio preempt timer set to 250us, down from the 100ms default. Latency
  aware queues alone did not deliver the intended behaviour. Read p99 moves
  from 20ms to 0.8ms.
- Metablock and datablock writes are submitted to io_uring together. They are
  safe to parallelise and EBS latency improves by 25%.
- Catchup drain settle shortened for follower takeover.

## [0.1.1] - 2026-08-16

- Docker image builds for linux/amd64 and linux/arm64, cross-compiled natively
  in the builder stage rather than emulated under QEMU.
- Crate publish is idempotent. Already-published versions are skipped, so a
  re-dispatch after a mid-chain failure resumes where the previous run stopped.

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
