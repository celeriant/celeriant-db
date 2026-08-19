# celeriant_ktls

Linux kernel TLS (kTLS) integration for Celeriant. Performs TLS 1.3 handshakes in userspace via rustls, then offloads encryption/decryption to the kernel for zero-overhead data transfer.

## Architecture

```
Handshake Phase (userspace, rustls unbuffered API):
┌──────────────┐     ┌──────────────────────┐     ┌──────────────┐
│  TcpStream   │────>│ drive_handshake_     │────>│  rustls      │
│  (plaintext) │<────│ server / client      │<────│  unbuffered  │
└──────────────┘     └──────────┬───────────┘     └──────────────┘
                                │ handshake complete
                                ▼
                     ┌──────────────────────┐
                     │ dangerous_into_      │──── ExtractedSecrets (keys, IVs, seq numbers)
                     │ kernel_connection()  │──── trailing bytes (app data already in buffer)
                     └──────────┬───────────┘
                                │
Kernel Configuration Phase:     │
                                ▼
                     ┌──────────────────────┐
                     │ enable_tls_ulp()     │──── setsockopt(SOL_TCP, TCP_ULP, "tls")
                     └──────────┬───────────┘
                                │
                     ┌──────────────────────┐
                     │ set_tls_crypto()     │──── setsockopt(SOL_TLS, TLS_TX/TLS_RX, crypto_info)
                     │ (TX + RX)           │
                     └──────────┬───────────┘
                                │
Post-Handshake:                 ▼
                     ┌──────────────────────┐
                     │  TcpStream           │──── Kernel encrypts/decrypts transparently
                     │  (kernel TLS active) │──── No userspace crypto overhead
                     └──────────────────────┘
```

## Invariants

- TLS 1.3 is the only permitted version. No fallback.
- Session tickets are prohibited. They desync kernel TLS sequence counters.
- Trailing bytes from the handshake must be processed before reading from the kernel-encrypted stream. Dropping them silently loses application data.
- `ktls_accept` and `ktls_connect` return in bounded time for any byte sequence a peer sends. A handshake that never yields starves every task on that executor, the caller's own timeout included.
- The post-handshake drain reads no further than the end of the one partially received record. Trailing bytes are bounded by the resident buffer plus one record. Everything past that stays in the kernel where kTLS decrypts it.
- kTLS support is verified at startup via `setsockopt` probe. Fatal if missing.
- Handshake buffer is capped at 128KB. Prevents unbounded allocation from oversized handshake messages.
- All `unsafe` blocks are limited to Linux system calls. No pointer arithmetic, no aliasing.

## Key Types

| Type | Purpose |
|------|---------|
| `KtlsError` | Error enum covering TLS, I/O, kernel support, cipher, setsockopt, and post-handshake drain failures |

## Key Functions

| Function | Purpose |
|----------|---------|
| `verify_ktls_support` | Synchronous startup check - creates dummy socket to probe kTLS kernel module |
| `ktls_accept` | Async server-side TLS 1.3 handshake → kernel TLS. Returns `(TcpStream, trailing_bytes)` |
| `ktls_connect` | Async client-side TLS 1.3 handshake → kernel TLS. Returns `(TcpStream, trailing_bytes)` |

## Usage

```rust
// Startup: fail fast if kernel doesn't support kTLS
verify_ktls_support()?;

// Server config must enable secret extraction and disable session tickets
let mut server_config = rustls::ServerConfig::builder()
    .with_no_client_auth()
    .with_single_cert(certs, key)?;
server_config.enable_secret_extraction = true;
server_config.send_tls13_tickets = 0;

// Accept a connection with kernel TLS
let (stream, trailing) = ktls_accept(tcp_stream, Arc::new(server_config)).await?;
// Handle trailing bytes (app data already decrypted during handshake)
// Then use stream as normal - kernel handles encryption transparently

// Client config must enable secret extraction
let mut client_config = rustls::ClientConfig::builder()
    .with_root_certificates(roots)
    .with_no_client_auth();
client_config.enable_secret_extraction = true;

// Connect with kernel TLS
let (stream, trailing) = ktls_connect(
    tcp_stream,
    Arc::new(client_config),
    ServerName::try_from("hostname")?,
).await?;
```

## Design Decisions

### Unbuffered rustls API for secret extraction

The standard rustls API hides the handshake completion boundary. The unbuffered API gives precise control over when the handshake finishes, allowing extraction of session secrets at exactly the right moment via `dangerous_into_kernel_connection()`. This is the only way to obtain the key material needed for `setsockopt`.

### Trailing bytes returned to caller

```
TCP buffer: [handshake records | app data records]
                                  ^^^^^^^^^^^^^^^^^
                                  decrypted by rustls during handshake,
                                  but kernel TLS not yet installed
```

During the handshake, `TcpStream::read` may pull application data records into the userspace buffer alongside the final handshake flight. Rustls decrypts these records, but kernel TLS hasn't been installed yet so the kernel doesn't know about them. These bytes are collected and returned as `trailing_bytes`. The caller must process them before reading from the now-kernel-encrypted stream.

That last record is often torn. TCP splits the peer's first application-data record across segments, so the handshake finishes holding a record header that promises more bytes than have arrived. Leaving them to the kernel does not work: userspace already consumed the record's head, and kTLS picks up at the next record boundary it sees. The drain reads the remainder itself and stops at that record's last byte.

A peer that never sends the remainder gets `KtlsError::TrailingRecordTimeout` after `DRAIN_DEADLINE` (5s). The deadline guards against a slow dribble, nothing more. The size bound comes from the sized read, not the clock.

### Session tickets disabled for internode connections

```rust
server_config.send_tls13_tickets = 0;
```

Session tickets cause the server to send post-handshake NewSessionTicket messages. With kTLS, these would be encrypted by the kernel but rustls wouldn't know about them, causing sequence number divergence between kernel and peer. Disabling tickets avoids this entirely.

### Dynamic handshake buffer with cap

```rust
const MAX_HANDSHAKE_BUF: usize = 131_072; // 128KB
```

The incoming buffer starts at 16KB and doubles when full. The 128KB cap prevents a malicious peer from forcing unbounded allocation with oversized handshake messages. Normal TLS 1.3 handshakes fit well within this limit.

### Supported cipher suites

| Cipher | Key | IV | Salt | Kernel Constant |
|--------|-----|-----|------|-----------------|
| AES-128-GCM | 16B | 8B | 4B (from IV prefix) | 51 |
| AES-256-GCM | 32B | 8B | 4B (from IV prefix) | 52 |
| ChaCha20-Poly1305 | 32B | 12B | none | 54 |

For GCM ciphers, the 12-byte IV from rustls is split: first 4 bytes become `salt`, remaining 8 bytes become `iv`. ChaCha20 uses the full 12 bytes as `iv` with no salt field.

### Startup verification avoids runtime surprises

```rust
verify_ktls_support()?; // called once at boot
```

Creates a dummy TCP socket and attempts `setsockopt(SOL_TCP, TCP_ULP, "tls")`. If the `tls` ULP module isn't loaded, returns `KtlsError::KernelNotSupported` immediately rather than failing on the first real connection. The socket is closed after the probe.

### Unsafe code is limited to syscalls

All `unsafe` blocks wrap Linux system calls (`socket`, `setsockopt`, `close`, `__errno_location`). No pointer arithmetic, no aliasing, no lifetime tricks. The `#[repr(C)]` structs ensure correct ABI layout for the kernel interface.

