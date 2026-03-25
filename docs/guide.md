# Celeriant Rust Client Guide

This guide covers the concepts and patterns you need to build event-sourced systems with Celeriant. For installation and a minimal example, see the [client README](../celeriant_client_tokio/README.md).

## Aggregates and keys

Every event in Celeriant lives inside an aggregate, addressed by three IDs:

    org_id / aggregate_type_id / aggregate_id

Think of it as a hierarchy. Organisations at the top, aggregate types within orgs, and individual aggregates at the leaves.

```rust
let key = AggregateKey::new(org_id, aggregate_type_id, aggregate_id);
```

All three are `u128`. You define them. Celeriant doesn't care what they mean, it just guarantees ordering and isolation within each aggregate.

A few modelling examples:

- `AcmeCorp / Orders / order-123` - classic DDD aggregate
- `AcmeCorp / UserProfiles / user-456` - one stream per user
- `AcmeCorp / Devices / device-789` - one stream per IoT device

There's no cardinality limit. Millions of aggregates, billions of events. Celeriant's storage engine uses bloom filters and bounded memory to stay efficient regardless of scale.

If you're working with UUIDs, convert with `Uuid::as_u128()` and `Uuid::from_u128()`. The [reference example](../celeriant_reference/src/constants.rs) uses `Uuid::new_v5` to generate deterministic IDs from names.

## Connections and the pool

### Connections are cheap

Celeriant connections are plain TCP sockets with no session state or per-connection server overhead. Connect, send requests, drop.

```rust
let mut client = CeleriantClient::connect("localhost:10000").await?;
```

A single connection is fine for scripts, admin tools, or simple use cases. The client reuses the TCP connection across multiple requests.

### The pool

For production workloads, `CeleriantPool` is what you want. It manages connections and routes operations to the right node:

- **Writes** always go to the leader. If the leader moves (failover), the pool detects this via `NotLeader` errors and reroutes automatically.
- **Reads** are distributed across all nodes via round-robin. Set `route_reads_to_followers` if you want to keep the leader free for writes.

```rust
let pool = CeleriantPool::new(
    PoolOptions::new("localhost:10000")
        .with_seed_addresses(vec!["localhost:10002".into()])
        .with_max_connections(20)
        .with_route_reads_to_followers(true)
);
```

The pool is safe to share across tasks. Wrap it in `Arc` and clone. It handles connection lifecycle, failover, and node discovery.

Key pool options:

| Option | Default | Description |
|--------|---------|-------------|
| `max_connections` | 10 | Connection pool ceiling per node |
| `connection_timeout` | 5s | TCP connect timeout |
| `request_timeout` | 30s | Per-request timeout |
| `idle_timeout` | 25s | Evict idle connections (must be below server's `slow_client_timeout`) |
| `route_reads_to_followers` | false | Keep leader free for writes |
| `compression` | Zstd level 3 | Auto-compress large payloads |
| `auto_compression_threshold` | 1024 bytes | Minimum payload size before compression kicks in |
| `max_leader_retries` | 3 | Seed nodes to try on failover |

## TLS and mTLS

```rust
use celeriant_client_tokio::ClientTlsConfig;
use celeriant_crypto::pki::PkiManager;

let ca = PkiManager::load_ca_bundle(Path::new("ca.crt"))?;
let (certs, key) = PkiManager::load_identity(Path::new("client.crt"), Path::new("client.key"))?;
let tls_config = PkiManager::build_client_config(&ca, certs, key)?;
let tls = ClientTlsConfig::new(tls_config, "localhost".try_into()?);

// Direct connection
let mut client = CeleriantClient::connect_tls("localhost:10000", tls.clone()).await?;

// Or via pool
let pool = CeleriantPool::new(
    PoolOptions::new("localhost:10000")
        .with_tls(tls)
);
```

For server-only TLS (no client cert), build the `rustls::ClientConfig` without a client identity.

## Client identity

When the server has `require_client_identity` enabled, the first message on a connection must be an Identify request. Two authentication modes, both sent via the same Identify message:

### API key

Server stores four key slots (two ReadWrite, two ReadOnly) as SHA-256 hashes. The client sends the raw key in the Identify message; the server hashes it and checks against its stored hashes.

```rust
use celeriant_client_tokio::ClientIdentityConfig;

let identity = ClientIdentityConfig::from_api_key("base64-encoded-32-byte-key");
```

### RSA key pair

Generate a keypair with `Crypto::generate_keypair`. The client ID is derived deterministically from the public key: `SHA-256(DER public key bytes)[0..16]` as little-endian u128. Same keypair, same identity, on any server. No central identity registry needed.

```rust
use celeriant_crypto::Crypto;
use base64::Engine;

// Generate once, persist the keypair
let keypair = Crypto::generate_keypair(None)?;
// keypair.public_key_base64, keypair.private_key_base64

let identity = ClientIdentityConfig::from_key_pair(
    keypair.public_key_base64.clone(),
    keypair.private_key_base64,
);

// Derive the client_id (u128) from the public key if you need it for WriteRequest
let public_key_bytes = base64::engine::general_purpose::STANDARD
    .decode(&keypair.public_key_base64)?;
let my_client_id = Crypto::generate_short_client_identity(&public_key_bytes);
```

When `identify()` is called, the client library generates a nonce (current epoch timestamp), signs it with the private key (RSA-2048 PKCS1v15-SHA256), and sends the public key, nonce, and signature in the Identify message. The server validates the signature and checks the nonce hasn't expired (2-minute window, 60-second clock skew tolerance). All automatic, no manual nonce handling needed.

### Using identity

```rust
// Direct connection
let mut client = CeleriantClient::connect("localhost:10000").await?;
client.identify(&identity).await?;

// Or via pool (identifies automatically on each new connection)
let pool = CeleriantPool::new(
    PoolOptions::new("localhost:10000")
        .with_identity(identity)
);
```

Access levels are connection-scoped. ReadOnly blocks write/delete/trim/schema operations. The pool handles identity automatically on every new connection it creates.

## Serialization

Events are domain structs. The client provides `json_event` and `from_json` helpers for JSON serialization via serde:

```rust
use celeriant_client_tokio::{json_event, from_json};

#[derive(Serialize, Deserialize)]
struct OrderPlaced { order_id: Uuid, amount: f64 }

// Create an event
let evt = json_event(1, &OrderPlaced { order_id, amount: 99.95 })?;

// Read it back
let order: OrderPlaced = from_json(&response.event_batches[0].events[0])?;
```

`json_event` takes an `event_type_major` and a serializable value. It returns a `DatablockAggregateEvent` with `event_type_minor` defaulted to 0 and other fields zeroed. Set `client_event_index`, `event_timestamp`, etc. on the returned event if you need them.

For raw `Vec<u8>` payloads (protobuf wire format, pre-serialized data), set `event_value` directly on a `DatablockAggregateEvent` instead of using the JSON helpers.

## Client ID

Every write carries a `client_id: u128` that identifies the writing service. Celeriant uses it for exactly-once tracking: the highest `client_event_index` is tracked per `(aggregate_key, client_id)`.

When client identity is enabled on the server, the write `client_id` must match the `client_id` derived from your Identify handshake. The server enforces this on every write, delete, trim, and schema request. A mismatch is rejected with `IDENTIFY_MISMATCH`. So your RSA-derived identity or API key identity IS your write client_id. See the [RSA key pair](#rsa-key-pair) section for how to derive it with `Crypto::generate_short_client_identity`.

When identity is not enabled, `client_id` is self-declared. Generate a deterministic one from your service name so it's stable across restarts:

```rust
const NAMESPACE: Uuid = uuid::uuid!("a1b2c3d4-e5f6-7890-abcd-ef1234567890");
let my_client_id: u128 = Uuid::new_v5(&NAMESPACE, b"OrderService").as_u128();
```

All instances of the same service should share a `client_id`. Different services writing to the same aggregates should use different IDs. See the [reference example](../celeriant_reference/src/constants.rs) for this pattern.

## Writing events

The simplest write pushes events into a single aggregate:

```rust
let events = vec![json_event(1, &OrderPlaced { order_id, amount: 99.95 })?];
pool.write_events(key.clone(), events).await?;
```

`event_type_major` and `event_type_minor` identify the event's schema version. Use major for breaking changes, minor for backwards-compatible additions. These tie into the schema registry.

### Optimistic concurrency control

Pass `expected_event_batch_index` to guard a write. If another writer has appended to the aggregate since you last read it, the write is rejected. This is how you enforce business invariants at write time. No distributed locks needed.

```rust
pool.write_events_with(
    key,
    events,
    WriteEventsOptions {
        client_id: my_client_id,
        expected_event_batch_index: Some(current_batch_index),
        ..Default::default()
    },
).await?;
```

When a concurrency conflict happens, the error tells you exactly what went wrong:

```rust
match result {
    Err(ClientError::Server(ServerError::Write {
        kind: WriteError::OptimisticConcurrencyViolation {
            expected_event_batch_index,
            current_event_batch_index,
        }, ..
    })) => {
        // Re-read, re-validate, retry
    }
    // ...
}
```

There is no automatic retry on OCC failures. That's by design. Only your domain logic knows whether a retry is safe. Catch up to the tip of the aggregate event stream, re-validate your business rules, and try again.

### Exactly-once writes

Set `enforce_client_idempotency: true` and provide a `client_event_index` on each event. Celeriant tracks the highest `client_event_index` per `(aggregate_key, client_id)`. If a write is retried due to a timeout and the original already landed, the server rejects the duplicate with `ClientIdempotencyViolation` instead of writing it twice.

The retry behaviour depends on why the write failed:

- **OCC failure**: re-derive `client_event_index` from fresh state (the aggregate moved, your index assumption was wrong)
- **Timeout**: hold `client_event_index` constant (the write may have already landed, changing the index would bypass the dedup check)
- **Idempotency violation**: the prior attempt already landed. Treat as success.

See the [reference example](../celeriant_reference/src/account_service.rs) for the full retry loop with all three cases handled.

### Dynamic consistency boundaries

In traditional event sourcing you pick a single aggregate as your consistency boundary. Business rules that span multiple aggregates? You're stuck with sagas, process managers, eventual consistency.

Celeriant lets you atomically write events across multiple aggregates in a single request, each with its own OCC guard. The server rejects the entire batch if any concurrency check fails. No partial writes, no distributed transactions.

```rust
let writes = HashMap::from([
    (from_key, SingleAggregateWrite {
        events: vec![transfer_out_event],
        allow_create: true,
        expected_event_batch_index: Some(from_batch_index),
        enforce_client_idempotency: true,
        compression_type_id: 0,
        compression_level: None,
    }),
    (to_key, SingleAggregateWrite {
        events: vec![transfer_in_event],
        allow_create: true,
        expected_event_batch_index: Some(to_batch_index),
        enforce_client_idempotency: true,
        compression_type_id: 0,
        compression_level: None,
    }),
]);

pool.write(WriteRequest {
    correlation_id: None,
    client_id: my_client_id,
    user_id: None,
    writes,
}).await?;
```

`expected_event_batch_index: Some(0)` means "this aggregate must not have any writes yet". It's how you guard creates. For existing aggregates, use the batch index from your last read. If anything has moved, the entire request is rejected atomically.

This eliminates a whole class of problems that normally require sagas. Transfer between two accounts? Atomic. Reserve inventory while placing an order? Atomic. Any business rule that spans aggregates within the same shard can be enforced in a single request.

The constraint: all aggregates in a single write must belong to the same shard. Shard assignment is deterministic (by aggregate ID, type, or org, configured server-side), so you know at design time which aggregates can participate in the same atomic write.

## Reading events

Read events from an aggregate starting at a batch index:

```rust
let response = pool.read(ReadRequest {
    correlation_id: None,
    aggregate_key: key,
    filters: ReadFilters::new(1),
}).await?;
```

`ReadFilters` supports a range of filtering options. You don't have to pull everything and filter client-side:

```rust
let filters = ReadFilters::new(1)
    .to_event_batch_index(100)
    .include_event_types(vec![1, 2, 3])
    .min_event_timestamp(start_ts)
    .max_event_timestamp(end_ts)
    .include_client_id(my_client_id);
```

Available filters: event type, client ID, user ID, server timestamp range, client event timestamp range, event index range, client event index range.

### Streaming reads

For aggregates with a lot of history, `read_all` handles pagination automatically:

```rust
let mut iter = pool.read_all(key.clone(), Some(ReadFilters::new(1))).await?;
while let Some(result) = iter.next().await {
    let batch = result?;
    for evt in &batch.events {
        let order: OrderPlaced = from_json(evt)?;
    }
}

// Or collect all at once
let all_batches = pool.read_all(key.clone(), Some(ReadFilters::new(1))).await?.collect().await?;
```

### Aggregate details

To check the state of an aggregate without pulling events:

```rust
let details = pool.aggregate_details(AggregateDetailsRequest {
    correlation_id: None,
    aggregate_key: key,
}).await?;

// details.min_event_batch_index, details.max_event_batch_index
// details.is_deleted, details.last_server_timestamp, etc.
```

## Schemas

Celeriant validates events against registered schemas at write time. Server-side enforcement. Malformed events are rejected before they hit the log.

```rust
pool.register_schema(RegisterSchemaRequest {
    correlation_id: None,
    client_id: my_client_id,
    user_id: None,
    schema_key: SchemaKey::new(org_id, aggregate_type_id, 1, 0),
    schema_type: SchemaType::Json.into(),
    schema: json_schema_string,
}).await?;
```

Supported schema types: JSON Schema, Apache Avro, and Protocol Buffers (compiled `FileDescriptorSet`).

Register schemas as part of your deployment pipeline. When you introduce a breaking change, bump `event_type_major` and register a new schema. Backwards-compatible changes bump `event_type_minor`. Old events remain valid. The schema only applies to new writes.

## Watching for changes

The watch API gives you a live stream of changes across the cluster. Build reactive read models, trigger side effects, or feed downstream systems without polling.

```rust
let request = WatchRequest {
    correlation_id: None,
    requested_latency_ms: None,
    shard_id: None,
    orgs: Some(HashSet::from([org_id])),
    aggregate_types: Some(HashSet::from([order_type_id])),
    aggregates: None,
    operation_types: Some(HashSet::from([1])), // Write
};

let mut watch = pool.watch(request, WatchOptions::default()).await?;

loop {
    let response = watch.next().await?;
    for evt in &response.events {
        // evt.org_id, evt.aggregate_type_id, evt.aggregate_id
        // evt.operation - Write, Create, Delete, TrimStart
        // evt.from_event_batch_index, evt.to_event_batch_index
    }
}
```

Watch events tell you *what changed*, not *what the events contain*. You then read the aggregate to get the actual data. This keeps the watch stream lightweight and lets you decide what to fetch.

You can filter by org, aggregate type, specific aggregates, and operation types. Only subscribe to what you need.

The pool handles multi-shard routing internally. If the watch request spans multiple shards, the pool spawns per-shard connections and multiplexes the results.

Use `watch.next_timeout(duration)` if you need a non-blocking check.

## Trimming and deleting

### Trimming old events

Over time you might want to discard old events to free up disk space. `trim_start` removes all event batches before a given index:

```rust
pool.trim_start(TrimStartRequest {
    correlation_id: None,
    aggregate_key: key,
    keep_from_event_batch_index: 100, // batches 1-99 are gone
    client_id: my_client_id,
    user_id: None,
}).await?;
```

Useful for aggregates with high event volume where you've already built snapshots or projections from the older events.

### Deleting aggregates

```rust
pool.delete(DeleteRequest {
    correlation_id: None,
    client_id: my_client_id,
    user_id: None,
    deletes: HashMap::from([(key, SingleAggregateDelete {
        allow_recreate: true,
        allow_index_continuation: false,
        expected_event_batch_index: None,
    })]),
}).await?;
```

Two flags control what happens after deletion:

- `allow_recreate` - can this aggregate be written to again? Set `false` for a permanent, irreversible delete.
- `allow_index_continuation` - if recreated, do event indices continue from where they left off, or restart from 1?

You can also pass `expected_event_batch_index` for optimistic concurrency on deletes.

## Listing and discovery

The pool provides streaming iterators to discover what's in the store. These handle shard discovery, pagination, and deduplication automatically:

```rust
// List all orgs
let mut iter = pool.list_orgs(ListOptions::default()).await?;
while let Some(result) = iter.next().await {
    let org = result?;
    println!("{}", org.org_id);
}

// List aggregate types in an org
let mut iter = pool.list_aggregate_types(Some(org_id), ListOptions::default()).await?;

// List aggregates (stats merged across shards)
let mut iter = pool.list_aggregates(Some(org_id), Some(type_id), ListOptions::default()).await?;
while let Some(result) = iter.next().await {
    let agg = result?;
    // agg.aggregate_id, agg.event_batch_count
    // agg.min_event_timestamp, agg.max_event_timestamp
    // agg.compressed_size, agg.uncompressed_size
    // agg.is_deleted
}

// Or collect
let all_orgs = pool.list_orgs(ListOptions::default()).await?.collect().await?;
```

`ListOptions` lets you include deleted aggregates and hint the shard count:

```rust
let options = ListOptions {
    include_deleted: true,
    max_shard_hint: Some(4), // skip shard discovery if you know the count
    ..Default::default()
};
```

## Compression

Celeriant compresses request payloads automatically when they exceed a threshold. The default is Zstd with a 1024-byte threshold. Payloads under 1KB are sent uncompressed.

Supported algorithms: Zstd, Snappy, Brotli, Gzip, or None.

Configure via pool options:

```rust
PoolOptions::new("localhost:10000")
    .with_compression(CompressionType::Zstd { level: 3 })
    .with_auto_compression_threshold(2048)
```

## Error handling

Errors are strongly typed. Match on specific variants instead of parsing strings:

```rust
use celeriant_client_tokio::{ClientError, server_error::*};

match pool.write(request).await {
    Ok(response) => { /* success */ }
    Err(ClientError::Server(ServerError::Write {
        kind: WriteError::OptimisticConcurrencyViolation { expected_event_batch_index, current_event_batch_index }, ..
    })) => { /* OCC conflict - retry with fresh state */ }
    Err(ClientError::Server(ServerError::Write {
        kind: WriteError::ClientIdempotencyViolation { .. }, ..
    })) => { /* prior attempt already landed - treat as success */ }
    Err(ClientError::NotLeader { leader_address, .. }) => { /* pool handles this automatically */ }
    Err(ClientError::ServerBusy) => { /* back off and retry */ }
    Err(ClientError::RequestTimeout) => { /* ambiguous - hold idempotency key constant on retry */ }
    Err(e) => { /* unexpected error */ }
}
```

## Examples

- [celeriant_demo](../celeriant_demo) - browser-based banking demo. Basic read/write patterns, OCC conflicts, watch API with server-sent events.
- [celeriant_reference](../celeriant_reference) - production-grade reference API. Postgres read projections, exactly-once writes, OCC retry loops with exponential backoff, multi-aggregate atomic transfers, idempotency caching.
