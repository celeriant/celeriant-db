# celeriant_client_glommio

Async TCP client for Celeriant using the Glommio runtime. Provides a minimal single-connection client optimised for microsecond-latency workloads. Used internally for replication and integration tests.

Supports both plaintext and kTLS connections. kTLS offloads symmetric encryption to the kernel after the TLS handshake, avoiding extra copies in userspace.

**Must be used within a Glommio `LocalExecutor` context.**

## Architecture

```
Caller
  │
  │ connect_with_timeout() / connect_with_timeout_tls()
  ▼
┌─────────────────────────┐
│  CeleriantClient        │  single TCP connection (RAII)
│  - max_request_size     │
│  - max_response_size    │
│  - timeout_duration     │
└──────────┬──────────────┘
           │ send_request(ClientRequest)
           │ send_cluster_request(ClusterRequest)
           │
           ▼
┌──────────────────────┐    ┌──────────────────────────┐
│  celeriant_wire      │    │  celeriant_msg           │
│  (framing/codec)     │───>│  ClientRequest/Response  │
└──────────────────────┘    │  ClusterRequest/Response │
                            └──────────────────────────┘
```

**Two request paths**: `send_request` handles client-facing operations (`ClientRequest` / `ClientResponse`). `send_cluster_request` handles cluster-internal operations (`ClusterRequest` / `ClusterResponse`). Both share the same timeout, framing, and error-handling logic.

**Timeout**: When `timeout_duration` is set, both send methods race the request future against a `glommio::timer::Timer` using the `futures_lite::future::or` combinator. `None` from `or` means the timer won.

## Key Types

| Type | Purpose |
|------|---------|
| `CeleriantClient` | Single TCP connection; RAII-managed |
| `GlommioTlsConfig` | Holds `rustls::ClientConfig` + `ServerName` for kTLS connections |
| `ClientError` | Error enum for all client-level failures |

## Key Functions

| Function | Purpose |
|----------|---------||
| `CeleriantClient::connect_with_timeout` | Plaintext TCP connect with optional `Duration` |
| `CeleriantClient::connect_with_timeout_tls` | Connect with optional TLS (kTLS); superset of plaintext connect |
| `CeleriantClient::send_request` | Send `ClientRequest`, receive `ClientResponse`; applies timeout if set |
| `CeleriantClient::send_cluster_request` | Send `ClusterRequest`, receive `ClusterResponse`; same timeout logic |
| `CeleriantClient::with_max_request_size` | Builder: override 10MB default |
| `CeleriantClient::with_timeout` | Builder: set per-request timeout |
| `CeleriantClient::set_nodelay` | Toggle TCP_NODELAY after connect |
| `CeleriantClient::local_addr` / `peer_addr` | Connection introspection |
| `CeleriantClient::close` | Explicit async shutdown; consumes self |
| `GlommioTlsConfig::new` | Construct from pre-built `ClientConfig` and `ServerName` |
| `GlommioTlsConfig::from_address` | Parse host from `"host:port"` string into `ServerName`; used for replication connections |

## Design Decisions

### Glommio-native, no Compat layer

The tokio client wraps `TcpStream` in a `tokio-util` `Compat` adapter to bridge Tokio's `AsyncRead`/`AsyncWrite` to `futures` traits. This client uses Glommio's `TcpStream` directly -- no adapter needed, no extra allocation.

### kTLS instead of userspace TLS

After the rustls handshake completes, `celeriant_ktls` installs the negotiated keys into the kernel's TLS offload path. The returned `TcpStream` is the same Glommio `TcpStream` -- reads and writes go through the normal syscall path with the kernel handling encryption. This avoids the extra buffer copies that a userspace TLS wrapper would introduce.

### Timeout via `or` combinator

```rust
or(
    async { Some(request_future.await) },
    async { Timer::new(duration).await; None }
).await
```

Two futures race. `None` from `or` means the `Timer` fired first; the request is abandoned and `ClientError::RequestTimeout` is returned.

### TCP keepalive on connect

Keepalive is enabled immediately after connection with aggressive timers (idle: 10s, interval: 3s). This detects dead peers quickly, which matters for replication connections that may sit idle between write bursts.

### TCP_NODELAY set on connect

Nagle's algorithm is disabled immediately after the connection is established. Celeriant is a request/response protocol where latency matters more than packet coalescing.

### NotLeader error with redirect

`from_error_response` inspects `GenericError` responses and promotes not-leader errors into a dedicated `NotLeader` variant that carries the leader's address. Callers can reconnect to the correct node without parsing error strings.

### Explicit close consumes self

`close()` takes ownership, preventing any use of the client after shutdown. The connection is also closed on drop.

### ClientError variants

| Variant | Cause |
|---------|-------|
| `NoAddress` | No address provided |
| `ConnectionTimeout` | Connect deadline exceeded |
| `ConnectionFailed(GlommioError)` | TCP connect or socket operation failed |
| `SetNoDelayError(GlommioError)` | Failed to set TCP_NODELAY |
| `KtlsError(KtlsError)` | kTLS handshake or key installation failed |
| `RequestTimeout` | `send_request` / `send_cluster_request` timeout exceeded |
| `RequestProtocolError` | Server returned a `ProtocolError` response |
| `NotLeader { leader_address, error }` | Server is not the shard leader; `leader_address` contains redirect target |
| `CeleriantError(ErrorResponse)` | Server returned a named error |
| `WriteRequestError(WireError)` | Framing/encoding failure |
| `ReadResponseError(ReadWireDataError)` | Response decode failure |

## Dependencies

- `celeriant_msg` - ClientRequest/ClientResponse and ClusterRequest/ClusterResponse message types
- `celeriant_wire` - Wire framing, protocol version constants, WireError
- `celeriant_wal` - CompressionType
- `celeriant_ktls` - kTLS handshake and kernel TLS offload
- `glommio` - Async runtime, TcpStream, Timer
- `futures-lite` - `or` combinator
- `rustls` / `rustls-pki-types` - TLS configuration types
- `libc` - TCP keepalive socket options
