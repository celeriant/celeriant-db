# celeriant_crypto

Cryptographic key generation, management, authentication, and PKI for Celeriant. Covers three concerns: distributed node identity, nonce-based client authentication, and X.509 certificate management for TLS/mTLS.

## Architecture

```
Client Authentication Flow
┌─────────────────────────────────────────────────────────┐
│ Client                                                  │
│   generate_nonce()  →  UTC epoch ms string              │
│   sign_nonce(private_key, nonce)  →  base64 signature   │
│   send: (public_key, nonce, signature)                  │
├─────────────────────────────────────────────────────────┤
│ Server                                                  │
│   validate_with_public_key(public_key, nonce, sig)      │
│     validate_nonce(nonce)        ← time window check    │
│     validate_signature(...)      ← RSASSA-PKCS1-v1_5    │
│     decode base64 → DER bytes                           │
│     generate_short_client_identity(DER bytes)           │
│   returns u128 client identity                          │
└─────────────────────────────────────────────────────────┘

Node ID Derivation
┌──────────────────────────────────────────────┐
│ load_or_generate_node_id(data_root)          │
│   if keys on disk → read public_key file     │
│   else → generate_keypair(), fsync both      │
│   SHA-256(DER public key bytes)[..16] → u128 │
└──────────────────────────────────────────────┘

API Key Authentication
┌──────────────────────────────────────────────┐
│ generate_api_key()  →  [u8; 32] random      │
│ hash_api_key(key)   →  SHA-256 digest        │
│ constant_time_compare(a, b)  →  bool         │
│   Server stores hash, compares in const time │
└──────────────────────────────────────────────┘

PKI / TLS Certificate Management
┌──────────────────────────────────────────────────────┐
│ PkiManager::create_ca(ca_dir, validity_days)         │
│   → ca.crt + ca.key (ECDSA P-256, self-signed)      │
│                                                      │
│ PkiManager::create_node_cert(ca_dir, cert_dir, ...)  │
│   → node.crt + node.key (EKU: server+client auth)   │
│                                                      │
│ PkiManager::create_client_cert(ca_dir, cert_dir, ..) │
│   → client-{name}.crt + client-{name}.key            │
│     (EKU: client auth only)                          │
│                                                      │
│ build_server_config / build_client_config             │
│   → Arc<rustls::ServerConfig/ClientConfig> (TLS 1.3) │
└──────────────────────────────────────────────────────┘
```

## Invariants

- TLS 1.3 is the only permitted version. No fallback.
- All certs use ECDSA P-256. CA certs have `pathLen:0`. Node certs carry both `serverAuth` and `clientAuth` EKU. Client certs carry only `clientAuth`.
- Two separate CAs: client CA and intracluster CA. A client cert cannot authenticate to the replication port and vice versa.
- API keys are stored as SHA-256 hashes only; raw keys are never stored server-side. Comparison is constant-time.
- Client identity is `SHA-256(DER public key bytes)[0..16]` as little-endian u128. Deterministic and reproducible — same keypair always produces the same u128.
- Nonces expire after 2 minutes with 60-second forward clock-skew tolerance.
- Key files are written with mode `0600` (owner read/write only).
- `load_or_generate_node_id` calls `sync_all()` on both key files before returning. Partial writes are never observable.

## Key Types

| Type | Purpose |
|------|---------|
| `Crypto` | Stateless cipher operations — all methods are static |
| `KeyPair` | Container for base64-encoded private and public RSA keys |
| `PkiManager` | X.509 certificate generation, loading, and rustls config building |
| `ClientAuthMode` | Controls mTLS client verification: `Require`, `Optional`, `None` |
| `CryptoError` | Error enum covering authentication failure modes |
| `PkiError` | Error enum for certificate and TLS operations |

## Design Decisions

### Nonce as epoch milliseconds

The nonce is the current UTC epoch time in milliseconds serialized as a plain decimal string. This encodes the timestamp directly in the nonce, allowing the server to enforce expiration without any server-side state. Replay attacks are bounded to the 2-minute window.

### Identity derived from public key, not from a random UUID

`generate_short_client_identity` hashes the raw public key bytes with SHA-256 and takes the first 16 bytes as a little-endian u128. This makes identity deterministic and reproducible: the same keypair always produces the same u128 regardless of which server processes the auth. No central identity registry required.

Both `validate_with_public_key` and `load_or_generate_node_id` decode the base64 to DER bytes before hashing, so the same key produces the same identity through either path.

### DER + base64 for key storage

Keys are encoded as DER (standard binary format) then base64-encoded for storage and wire transport. DER is the canonical format for PKCS8 private keys and SubjectPublicKeyInfo public keys, ensuring interoperability with standard tooling.

### fsync on key write

`load_or_generate_node_id` calls `sync_all()` on both key files before returning. A node restart after a partial write would find inconsistent key files and fail to load, rather than silently deriving a different identity.

### All Crypto methods are static

`Crypto` has no fields. All operations are stateless functions. No allocator pressure from long-lived instances, no locking, no initialization ordering concerns.

### URL-safe base64 for path-embedded client IDs

`decode_base64_u128_from_path` accepts URL-safe base64 (`-` and `_` instead of `+` and `/`) and handles missing padding. This supports client IDs embedded in URL paths without percent-encoding.

### API key hashing with constant-time comparison

API keys are stored as SHA-256 hashes. Comparison uses `subtle::ConstantTimeEq` to prevent timing side-channels against stored hashes.

### ECDSA P-256 for certificates, RSA for client auth

PKI certificates use ECDSA P-256 (smaller keys, faster handshakes) while client nonce authentication retains RSA 2048 (existing protocol). The two systems serve different purposes and don't share keys.

### TLS 1.3 only

`build_server_config` and `build_client_config` pin to TLS 1.3 exclusively. No fallback to older protocol versions.

### Key file permissions

PKI private key files are written with mode `0600` (owner read/write only) to prevent accidental exposure.

### Certificate validity window

Certificates start 60 seconds in the past to tolerate clock skew between nodes during initial deployment.
