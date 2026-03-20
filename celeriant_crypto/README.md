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

## Key Types

### Public Types

| Type | Purpose |
|------|---------|
| `CryptoError` | Error enum covering authentication failure modes |
| `KeyPair` | Container for base64-encoded private and public RSA keys |
| `Crypto` | Stateless cipher operations — all methods are static |
| `PkiError` | Error enum for certificate and TLS operations |
| `ClientAuthMode` | Controls mTLS client verification: `Require`, `Optional`, `None` |
| `PkiManager` | X.509 certificate generation, loading, and rustls config building |

### CryptoError Variants

| Variant | Cause |
|---------|-------|
| `InvalidSignature` | Signature verification failed or malformed |
| `InvalidNonce` | Nonce expired, too far in future, or not a valid u64 |
| `KeyGenerationFailed(String)` | RSA key generation error |
| `KeyEncodingFailed(String)` | DER/PKCS8 encoding failure |
| `KeyDecodingFailed(String)` | Base64 or DER decode failure |
| `SigningFailed(String)` | Signing operation failure |
| `TimeError(String)` | `SystemTime` before UNIX_EPOCH |

### PkiError Variants

| Variant | Cause |
|---------|-------|
| `Io(io::Error)` | File I/O failure |
| `CertGen(rcgen::Error)` | Certificate generation error |
| `Tls(rustls::Error)` | TLS configuration error |
| `Verifier(VerifierBuilderError)` | Client verifier construction error |
| `NoPrivateKey(PathBuf)` | No private key found in PEM file |
| `NoCertificates(PathBuf)` | No certificates found in PEM file |
| `InvalidDnsName(String)` | Host string not a valid DNS name |
| `InvalidValidity(String)` | Validity duration overflows datetime range |

### KeyPair Fields

| Field | Type | Content |
|-------|------|---------|
| `private_key_base64` | `String` | PKCS8 DER bytes, standard base64-encoded |
| `public_key_base64` | `String` | SubjectPublicKeyInfo DER bytes, standard base64-encoded |

## Crypto Methods

| Method | Signature | Purpose |
|--------|-----------|---------|
| `load_or_generate_node_id` | `(data_root: &Path) -> Result<u128, String>` | Load or create keypair on disk, return stable node ID |
| `generate_keypair` | `(key_size: Option<usize>) -> Result<KeyPair, CryptoError>` | Generate new RSA keypair, defaults to 2048 bits |
| `generate_nonce` | `() -> Result<String, CryptoError>` | UTC epoch time in milliseconds as a decimal string |
| `sign_nonce` | `(private_key_base64: &str, nonce: &str) -> Result<String, CryptoError>` | RSASSA-PKCS1-v1_5 + SHA-256 signature, base64-encoded output |
| `validate_signature` | `(public_key: &str, nonce: &str, signature: &str) -> Result<(), CryptoError>` | Verify signature against nonce using DER public key |
| `validate_nonce` | `(nonce: &str) -> Result<(), CryptoError>` | Enforce 2-minute expiration window with 60s clock skew tolerance |
| `validate_with_public_key` | `(public_key: &str, nonce: &str, signature: &str) -> Result<u128, CryptoError>` | Combined validation returning client identity |
| `generate_short_client_identity` | `(public_key: &[u8]) -> u128` | SHA-256 hash of raw bytes, first 16 bytes as little-endian u128 |
| `decode_base64_u128_from_path` | `(client_id_b64: &str) -> Result<u128, CryptoError>` | Decode URL-safe base64 client ID back to u128 |

## API Key Functions

| Function | Signature | Purpose |
|----------|-----------|---------|
| `generate_api_key` | `() -> [u8; 32]` | Generate 32 random bytes via `rand::thread_rng` |
| `hash_api_key` | `(key: &[u8; 32]) -> [u8; 32]` | SHA-256 digest of the raw key |
| `constant_time_compare` | `(a: &[u8; 32], b: &[u8; 32]) -> bool` | Constant-time equality via `subtle::ConstantTimeEq` |

## PkiManager Methods

| Method | Signature | Purpose |
|--------|-----------|---------|
| `create_ca` | `(ca_dir: &Path, validity_days: u32) -> Result<(), PkiError>` | Generate self-signed CA. ECDSA P-256, `CA:true pathLen:0`. Writes `ca.crt` + `ca.key` |
| `create_node_cert` | `(ca_dir, cert_dir, hosts, validity_days) -> Result<(), PkiError>` | CA-signed node cert with IP/DNS SANs. EKU: serverAuth + clientAuth. Writes `node.crt` + `node.key` |
| `create_client_cert` | `(ca_dir, cert_dir, client_name, validity_days) -> Result<(), PkiError>` | CA-signed client cert. EKU: clientAuth. CN: `celeriant-client-{name}`. Writes `client-{name}.crt` + `client-{name}.key` |
| `load_ca_bundle` | `(ca_cert_path: &Path) -> Result<Vec<CertificateDer>, PkiError>` | Parse PEM file into DER certificates. Supports concatenated bundles |
| `load_identity` | `(cert_path, key_path) -> Result<(Vec<CertificateDer>, PrivateKeyDer), PkiError>` | Load certificate chain + private key from PEM files |
| `build_server_config` | `(ca_bundle, cert_chain, key, client_auth) -> Result<Arc<ServerConfig>, PkiError>` | TLS 1.3 server config with configurable mTLS |
| `build_client_config` | `(ca_bundle, cert_chain, key) -> Result<Arc<ClientConfig>, PkiError>` | TLS 1.3 client config with mutual TLS |
| `build_client_config_no_auth` | `(ca_bundle) -> Result<Arc<ClientConfig>, PkiError>` | TLS 1.3 client config without client certificate |

## Constants

| Constant | Value | Purpose |
|----------|-------|---------|
| `MAX_NONCE_TIME_MINUTES` | `2.0` | Nonce expiry window in minutes |
| `DEFAULT_KEY_SIZE` | `2048` | RSA key size in bits |
| `CLOCK_SKEW_TOLERANCE_MS` | `60_000` | Future nonce tolerance (60 seconds) |

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

## Dependencies

| Crate | Purpose |
|-------|---------|
| `rsa` | RSASSA-PKCS1-v1_5 sign/verify, RSA key generation |
| `sha2` | SHA-256 for signing digest and identity derivation |
| `base64` | Standard and URL-safe base64 encoding/decoding |
| `rand` | Entropy source for RSA key generation and API keys |
| `thiserror` | Error type derivation for `CryptoError` |
| `tracing` | Structured logging on key load/generate |
| `rcgen` | X.509 certificate generation (CA, node, client) |
| `time` | Certificate validity window calculation |
| `rustls` | TLS 1.3 server and client configuration |
| `rustls-pki-types` | Certificate and key type definitions |
| `rustls-pemfile` | PEM file parsing for certs and keys |
| `x509-parser` | Certificate inspection (dev/test only) |
| `subtle` | Constant-time comparison for API key hashes |
