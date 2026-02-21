# celeriant_sidecar

Object store abstraction for S3 operations. Provides a trait-based interface for conditional puts, batch deletes, and listing. Designed to run in a tokio sidecar runtime bridged from glommio shards.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Glommio Shards                              │
│         (send Request via flume channels)                       │
└──────────────────────────┬──────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────┐
│                   SidecarStoreTrait                             │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │                    SidecarStore                            │ │
│  │  ┌──────────────────────────────────────────────────────┐  │ │
│  │  │              S3Client (private)                      │  │ │
│  │  │  • Subfolder prefix resolution                       │  │ │
│  │  │  • Conditional puts (create, etag match)             │  │ │
│  │  │  • Batch delete streaming                            │  │ │
│  │  └──────────────────────────────────────────────────────┘  │ │
│  └────────────────────────────────────────────────────────────┘ │
└──────────────────────────┬──────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────┐
│                  object_store (AmazonS3)                        │
└─────────────────────────────────────────────────────────────────┘
```

`S3Client` is a private struct. All S3 interaction is encapsulated within `SidecarStore`; callers only see `SidecarStoreTrait`.

## Key Types

| Type | Purpose |
|------|---------|
| `SidecarStoreTrait` | Async trait for dependency injection and testing |
| `SidecarStore` | Production implementation backed by S3 |
| `StoreConfig` | Top-level configuration (optional S3) |
| `S3Config` | Bucket, region, credentials, endpoint, subfolder |
| `Request` | Operation enum: Put, Get, Head, Delete, DeleteBatch, List |
| `Response` | Result enum with data, etags, metadata |
| `ObjectMetadata` | Path, size, etag, last_modified |
| `PutCondition` | None, CreateOnly, IfMatchETag |
| `StoreError` | Error types with kind classification |
| `ErrorKind` | NotFound, AlreadyExists, PreconditionFailed, S3, etc. |

## Key Functions

| Function | Purpose |
|----------|---------|
| `SidecarStore::new` | Build store from config, initialize S3 client |
| `process_request` | Route request to appropriate operation |
| `put_object` | Conditional PUT with overwrite/create/update modes |
| `get_object` | GET with data, etag, size |
| `head_object` | HEAD for metadata only |
| `delete_object` | Single object DELETE |
| `delete_objects` | Batch DELETE via streaming, returns failed paths |
| `list_objects` | List objects under prefix |

## Request/Response Types

```rust
enum Request {
    ObjectPut { path, data: Bytes, condition: PutCondition },
    ObjectGet { path },
    ObjectHead { path },
    ObjectDelete { path },
    ObjectDeleteBatch { paths: Vec<String> },
    ObjectList { prefix },
}

enum Response {
    ObjectPut { e_tag: Option<String> },
    ObjectGet { data: Bytes, e_tag, size },
    ObjectHead(ObjectMetadata),
    ObjectDelete,
    ObjectDeleteBatch { failed_paths: Vec<String> },
    ObjectList { objects: Vec<ObjectMetadata> },
}
```

## Design Decisions

### Trait-based interface

`SidecarStoreTrait` enables dependency injection for testing. Mock implementations can be swapped in without touching production code paths.

### Conditional puts

`PutCondition` maps directly to `object_store::PutMode`:

| Condition | object_store PutMode | Use Case |
|-----------|---------------------|----------|
| `None` | `PutMode::Overwrite` | Unconditional write |
| `CreateOnly` | `PutMode::Create` | Fail if exists (leases) |
| `IfMatchETag(etag)` | `PutMode::Update(UpdateVersion { e_tag })` | Optimistic concurrency |

### Subfolder prefix

All paths resolved through `S3Client::resolve_path`. Configured subfolder prepended automatically:

```rust
// config.subfolder = Some("cluster-1")
// path = "leases/node-1"
// resolved = "cluster-1/leases/node-1"
```

### Batch delete streaming

Uses `object_store::delete_stream` for efficient bulk deletes. Non-fatal failures (NotFound) collected and returned rather than failing entire batch.

### Error mapping

`StoreError` implements `From<object_store::Error>` and `From<object_store::path::Error>` for ergonomic `?` propagation throughout store operations:

| object_store | StoreError |
|--------------|------------|
| `NotFound` | `NotFound { path }` |
| `AlreadyExists` | `AlreadyExists { path }` |
| `Precondition` | `PreconditionFailed { path }` |
| `InvalidPath` | `InvalidPath { path }` |
| `path::Error` | `InvalidPath { path }` |
| Other | `S3Error { message }` |

### Debug impls

Both `Request` and `SidecarStore` have custom `Debug` implementations:

- `Request::ObjectPut` redacts the `data` field and emits `data_len` (byte count) instead, preventing large payloads from flooding logs.
- `SidecarStore` emits `s3_configured: bool` instead of the full internal struct, hiding credentials from debug output.

### Optional S3

Store can be created without S3 config. Operations return `S3NotConfigured` error. Useful for local-only deployments or gradual feature enablement.

## Usage

```rust
use celeriant_sidecar::{
    store::{SidecarStore, SidecarStoreTrait},
    store_config::StoreConfig,
    s3_config::S3Config,
    request::{Request, PutCondition},
};
use bytes::Bytes;

// Configure S3
let config = StoreConfig {
    s3: Some(S3Config {
        region: "us-east-1".to_string(),
        bucket: "my-bucket".to_string(),
        access_key_id: Some("AKIA...".to_string()),
        secret_access_key: Some("secret".to_string()),
        endpoint: None,  // Use default AWS endpoint
        subfolder: Some("cluster-1".to_string()),
        skip_signature: false,
        allow_http: false,
    }),
};

let store = SidecarStore::new(config)?;

// Conditional create (fails if exists)
let response = store.process_request(Request::ObjectPut {
    path: "leases/node-1".to_string(),
    data: Bytes::from("node-1-data"),
    condition: PutCondition::CreateOnly,
}).await?;

// Update with etag check
let response = store.process_request(Request::ObjectPut {
    path: "membership/cluster.json".to_string(),
    data: Bytes::from(r#"{"nodes": [1, 2, 3]}"#),
    condition: PutCondition::IfMatchETag("abc123".to_string()),
}).await?;

// List objects
let Response::ObjectList { objects } = store.process_request(
    Request::ObjectList { prefix: "leases/".to_string() }
).await? else { panic!() };
```

## Error Handling

| Error | Cause |
|-------|-------|
| `S3NotConfigured` | S3 config not provided |
| `NotFound` | Object does not exist |
| `AlreadyExists` | CreateOnly put, object exists |
| `PreconditionFailed` | ETag mismatch on conditional put |
| `InvalidPath` | Malformed object path |
| `S3Error` | Network, auth, or other S3 failure |

```rust
match store.process_request(request).await {
    Ok(response) => { /* handle response */ }
    Err(e) => match e.kind() {
        ErrorKind::NotFound => { /* 404 */ }
        ErrorKind::PreconditionFailed => { /* retry with fresh etag */ }
        ErrorKind::S3 => { /* log, retry with backoff */ }
        _ => { /* propagate */ }
    }
}
```

## Dependencies

| Crate | Purpose |
|-------|---------|
| `object_store` | S3 client (AmazonS3Builder) |
| `async-trait` | Async trait support |
| `bytes` | Zero-copy byte buffers |
| `futures` | Stream combinators for batch ops |
| `thiserror` | Error derive macros |
| `tracing` | Structured logging |
