# celeriant_crypto

Cryptographic key generation, management, and authentication for distributed node identity and nonce-based client signing. Used to establish stable node IDs and authenticate clients connecting to the cluster.

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
│     generate_short_client_identity(public_key.as_bytes) │
│   returns u128 client identity                          │
└─────────────────────────────────────────────────────────┘

Node ID Derivation
┌──────────────────────────────────────────────┐
│ load_or_generate_node_id(data_root)          │
│   if keys on disk → read public_key file     │
│   else → generate_keypair(), fsync both      │
│   SHA-256(DER public key bytes)[..16] → u128 │
└──────────────────────────────────────────────┘
```

## Key Types

### Public Types

| Type | Purpose |
|------|---------|
| `CryptoError` | Error enum covering all failure modes |
| `KeyPair` | Container for base64-encoded private and public keys |
| `Crypto` | Stateless cipher operations — all methods are static |

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
| `generate_short_client_identity` | `(public_key: &[u8]) -> u128` | SHA-256 hash of raw bytes → first 16 bytes as little-endian u128 |
| `decode_base64_u128_from_path` | `(client_id_b64: &str) -> Result<u128, CryptoError>` | Decode URL-safe base64 client ID back to u128 |

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

Note: `validate_with_public_key` hashes `public_key.as_bytes()` (the base64 string bytes), while `load_or_generate_node_id` hashes the decoded DER bytes. These two paths produce different u128 values for the same key. Node IDs and client IDs are intentionally separate identities.

### DER + base64 for key storage

Keys are encoded as DER (standard binary format) then base64-encoded for storage and wire transport. DER is the canonical format for PKCS8 private keys and SubjectPublicKeyInfo public keys, ensuring interoperability with standard tooling.

### fsync on key write

`load_or_generate_node_id` calls `sync_all()` on both key files before returning. A node restart after a partial write would find inconsistent key files and fail to load, rather than silently deriving a different identity.

### All methods are static

`Crypto` has no fields. All operations are stateless functions. No allocator pressure from long-lived instances, no locking, no initialization ordering concerns.

### URL-safe base64 for path-embedded client IDs

`decode_base64_u128_from_path` accepts URL-safe base64 (`-` and `_` instead of `+` and `/`) and handles missing padding. This supports client IDs embedded in URL paths without percent-encoding.

## Dependencies

| Crate | Purpose |
|-------|---------|
| `rsa` | RSASSA-PKCS1-v1_5 sign/verify, key generation |
| `sha2` | SHA-256 for signing digest and identity derivation |
| `base64` | Standard and URL-safe base64 encoding/decoding |
| `rand` | Entropy source for RSA key generation |
| `thiserror` | Error type derivation |
| `tracing` | Structured logging on key load/generate |
