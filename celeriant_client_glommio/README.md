# celeriant_client_glommio

Async TCP client for Celeriant using the Glommio runtime. Provides a minimal single-connection client optimised for microsecond-latency workloads. Used internally for replication and integration tests.

**Must be used within a Glommio `LocalExecutor` context.**

## Architecture

```
Caller
  │
  │ connect_with_timeout()
  ▼
┌─────────────────────┐
│  CeleriantClient    │  single TCP connection (RAII)
│  - max_request_size │
│  - max_response_size│
│  - timeout_duration │
└──────────┬──────────┘
           │ send_request(Request, CompressionType)
           │
           ▼
┌──────────────────────┐    ┌──────────────────────────┐
│  celeriant_wire      │    │  celeriant_msg           │
│  (framing/codec)     │───>│  (Request / Response)    │
└──────────────────────┘    └──────────────────────────┘
```

**Request flow**: `send_request` writes the framed request via `celeriant_wire`, reads the response, and surfaces `GenericError` responses as `ClientError::CeleriantError`.

**Timeout**: When `timeout_duration` is set, `send_request` races the request future against a `glommio::timer::Timer` using the `futures_lite::future::or` combinator. `None` from `or` means the timer won.

## Key Types

| Type | Purpose |
|------|---------|
| `CeleriantClient` | Single TCP connection; RAII-managed |
| `ClientError` | Error enum for all client-level failures |

## Key Functions

| Function | Purpose |
|----------|---------|
| `CeleriantClient::connect_with_timeout` | Connect with optional `Duration`; only entry point |
| `CeleriantClient::send_request` | Send `Request`, receive `Response`; applies timeout if set |
| `CeleriantClient::with_max_request_size` | Builder: override 10MB default |
| `CeleriantClient::with_timeout` | Builder: set per-request timeout |
| `CeleriantClient::set_nodelay` | Toggle TCP_NODELAY after connect |
| `CeleriantClient::local_addr` / `peer_addr` | Connection introspection |
| `CeleriantClient::close` | Explicit async shutdown; consumes self |

## Design Decisions

### Glommio-native, no Compat layer

The tokio client wraps `TcpStream` in a `tokio-util` `Compat` adapter to bridge Tokio's `AsyncRead`/`AsyncWrite` to `futures` traits. This client uses Glommio's `TcpStream` directly—no adapter needed, no extra allocation.

### Timeout via `or` combinator

```rust
or(
    async { Some(request_future.await) },
    async { Timer::new(duration).await; None }
).await
```

Two futures race. `None` from `or` means the `Timer` fired first; the request is abandoned and `ClientError::RequestTimeout` is returned.

### Larger default size limits

| Limit | Tokio client | Glommio client |
|-------|-------------|----------------|
| Max request | 10 MB | 10 MB |
| Max response | — (single limit) | 64 MB |

The glommio client tracks request and response sizes separately. The larger response limit accommodates replication payloads.

### Explicit close consumes self

`close()` takes ownership, preventing any use of the client after shutdown. The connection is also closed on drop.

### TCP_NODELAY set on connect

Nagle's algorithm is disabled immediately after the connection is established. Celeriant is a request/response protocol where latency matters more than packet coalescing.

### ClientError variants

| Variant | Cause |
|---------|-------|
| `NoAddress` | No address provided |
| `ConnectionTimeout` | connect deadline exceeded |
| `ConnectionFailed(GlommioError)` | TCP connect or socket operation failed |
| `SetNoDelayError(GlommioError)` | Failed to set TCP_NODELAY |
| `RequestTimeout` | send_request timeout exceeded |
| `RequestProtocolError` | Server returned a `ProtocolError` response |
| `CeleriantError(ErrorResponse)` | Server returned a named error |
| `WriteRequestError(WireError)` | Framing/encoding failure |
| `ReadResponseError(ReadWireDataError)` | Response decode failure |

## Dependencies

- `celeriant_msg` - Request/Response message types
- `celeriant_wire` - Wire framing, protocol version constants, WireError
- `celeriant_wal` - CompressionType
- `glommio` - Async runtime, TcpStream, Timer
- `futures-lite` - `or` combinator
