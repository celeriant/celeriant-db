# Celeriant Rust Client

Async Tokio client for [Celeriant](https://celeriant.io), the distributed event store for event sourcing.

Celeriant lets you enforce business invariants at write time across multiple streams, without distributed transactions. Optimistic concurrency control, strict ordering, exactly-once writes, schema validation, cluster wide durability. All built in.

- [Website](https://celeriant.io)
- [Documentation](https://docs.celeriant.io)
- [GitHub](https://github.com/celeriant/celeriant-db)

Single connection client, connection pool with leader routing and failover, streaming iterators, and real-time watch connections.

## Install

```bash
cargo add celeriant_client_tokio
```

## Quick start

### 1. Start the server

Celeriant uses io_uring, so the container needs `seccomp=unconfined`.

```bash
docker run -d --name celeriant \
  --security-opt seccomp=unconfined \
  -p 10000:10000 \
  ghcr.io/celeriant/celeriant \
  --standalone --data-root /var/lib/celeriant --num-shards 1
```

### 2. Connect and write an event

```rust
use celeriant_client_tokio::{CeleriantClient, json_event};
use celeriant_wal::AggregateKey;

let mut client = CeleriantClient::connect("localhost:10000").await?;

let key = AggregateKey::new(org_id, aggregate_type_id, aggregate_id);
let events = vec![json_event(1, 0, &OrderPlaced { order_id, amount: 99.95 })?];

client.write_events(&key, &events).await?;
```

### 3. Read it back

```rust
use celeriant_client_tokio::from_json;
use celeriant_msg::request::read_filters::ReadFilters;
use celeriant_msg::request::requests::ReadRequest;

let response = client.read(&ReadRequest {
    correlation_id: None,
    aggregate_key: key,
    filters: ReadFilters::new(1),
}).await?;

let order: OrderPlaced = from_json(&response.event_batches[0].events[0])?;
```

## Connection pool

`CeleriantPool` manages connections, routes writes to the leader, distributes reads across nodes. Implements `CeleriantPoolApi` for testing and DI.

```rust
use celeriant_client_tokio::{CeleriantPool, PoolOptions};

let pool = CeleriantPool::new(PoolOptions::new("localhost:10000")
    .with_seed_addresses(vec!["localhost:10001".into()]));

pool.write(request).await?;
let response = pool.read(read_request).await?;
```

The pool also supports `delete`, `trim_start`, `aggregate_details`, `register_schema`, `watch`, `read_all` (streaming), and listing operations (`list_orgs`, `list_aggregate_types`, `list_aggregates`).

## TLS / mTLS

```rust
use celeriant_client_tokio::ClientTlsConfig;
use celeriant_crypto::PkiManager;

let ca = PkiManager::load_ca_bundle(Path::new("ca.crt"))?;
let (certs, key) = PkiManager::load_identity(Path::new("client.crt"), Path::new("client.key"))?;
let tls_config = PkiManager::build_client_config(&ca, &certs, &key)?;
let tls = ClientTlsConfig::new(tls_config, "localhost");

let mut client = CeleriantClient::connect_tls("localhost:10010", &tls).await?;
```

## Examples

- [celeriant_demo](https://github.com/celeriant/celeriant-db/tree/main/celeriant_demo) - browser-based banking demo. Basic read/write patterns, OCC conflicts, watch API with SSE.
- [celeriant_reference](https://github.com/celeriant/celeriant-db/tree/main/celeriant_reference) - production-grade reference API. Postgres read projections, exactly-once writes, OCC retry loops, multi-aggregate transfers.

## Running tests

```bash
# unit tests
cargo test -p celeriant_client_tokio

# integration tests (requires docker compose up -d)
cargo test -p celeriant_integration_tests
```

## License

Apache 2.0
