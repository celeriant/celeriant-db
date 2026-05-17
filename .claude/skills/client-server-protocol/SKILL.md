---
name: client-server-protocol
description: Celeriant's client-server protocol invariants, network behavior, failure modes, and shard routing. Use when working with celeriant_msg, celeriant_wire, celeriant_client_tokio, or connection handling.
---

# Client-Server Protocol

## Connection Model

Stateless. No sessions, no prepared statements. Each request carries all context. Connections are reused for sequential pipelining - send request, get response, send next. Not multiplexed. One in-flight request per connection.

Two ports: client port for application traffic, replication port for cluster traffic. Wrong port = error 400. TCP_NODELAY on everything.

## TLS

TLS 1.3 only. No fallback. kTLS offloads encryption to the kernel after the handshake, so io_uring reads/writes pass through without touching ciphertext in userspace. Session tickets are disabled (they desync kTLS sequence counters).

Two separate CAs: client CA on the client port, intracluster CA on the replication port. A client cert can't authenticate to the replication port. Replication always requires mTLS. Client port mTLS is configurable (default: require).

Hot-reload of certs is supported via configurable interval.

## Identity & Auth

Optional. When `require_client_identity` is set, the first message on a client connection must be Identify. Two modes: RSA signature (public_key + nonce + signature) or API key. Once identified, access level is stored on the connection for its lifetime.

API keys are stored as SHA-256 hashes only, never raw. Four slots: two ReadWrite, two ReadOnly. 

API keys and client identity require TLS unless `--insecure-allow-plaintext-auth` is explicitly set (server exits at startup otherwise).

Access levels: ReadWrite (all operations) or ReadOnly (reads, details, lists only). Write/Delete/TrimStart/RegisterSchema on a ReadOnly connection = error 10007.

## Pipelining

Sequential request-response loop. Server reads a request, processes it, sends response, reads next. Connection stays open until client timeout, server shutdown, or a Watch request.

Watch is connection-terminal. Once a Watch starts, the connection streams events until the client disconnects or filters empty out. No further regular requests.

## Shard Routing

Deterministic. Aggregate key hashed, mod by num_shards. Same key always hits the same shard.

If a request lands on the wrong shard, the server redirects internally via glommio mesh channel. Invisible to the client. If the mesh channel is full - no queue, no retry - immediate SERVER_BUSY (11000).

Multi-aggregate writes: each aggregate hashed independently. If they map to different shards, the write is rejected (9001). You can't do cross-shard atomic writes.

RegisterSchema always routes to shard 0.

List operations (ListOrgs, ListAggregateTypes, ListAggregates) require explicit `shard_id` from the client. The client library iterators handle shard discovery, pagination, and deduplication automatically.

## Failure Modes

**Not leader:** Write/Delete/TrimStart to a non-leader returns an error with the leader address embedded as JSON. Client can parse and redirect.

**Server busy:** Mesh channel full on internal redirect. Client should back off.

**Decompression bomb:** Both compressed and uncompressed sizes validated against max_size_bytes before decompression. Prevents amplification attacks.

**Oversized message:** Exceeds max_size_bytes -> connection closed.

**Incomplete message:** Connection closed.

**Cleartext to TLS port:** Server expects a TLS handshake. A cleartext client sends bytes that aren't a ClientHello, the TLS layer rejects it, connection drops. Client sees a network EOF with no error response.

## Replication Invariants

Writes are replicated synchronously to the follower before the client gets an ACK. Both leader and follower fsync to disk. Acknowledged writes are durable on two nodes.

Follower validates each batch: WAL sequence continuity, tip hash match, clock drift threshold, lease fencing. Any mismatch -> explicit rejection with reason. Not a generic error - the FollowerRejection enum tells you exactly what went wrong.

Heartbeats are pure liveness signals. No WAL data. Separate validation path.

## Wire Format

17-byte fixed header, then payload. Protocol version V2 (bincode, Rust clients) or V3 (msgpack, non-Rust). Server auto-detects from the header and responds in kind. Compression optional (none, zstd, snappy, brotli, gzip).

See `celeriant_wire/src/network/wire_header.rs` for the header layout. See `celeriant_msg/src/error_codes.rs` for all error codes.
