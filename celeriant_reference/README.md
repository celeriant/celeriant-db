# Celeriant Reference

A production-grade reference API showing how to build an event-sourced system with [Celeriant](https://celeriant.io) and a Postgres read projection, using the Rust tokio client.

Banking domain: deposits, withdrawals, and atomic multi-aggregate transfers with server-side balance validation.

## Prerequisites

- **Linux / macOS**: Docker
- **Windows**: WSL2 + Docker Desktop

## Run

The reference uses atomic multi-aggregate writes (transfers), which require all aggregates to route to the same shard. Use `aggregate_type_id` routing so all accounts share a shard:

```bash
cd deploy/local-cluster
CELERIANT_ROUTING_RULE=aggregate_type_id docker compose up -d
docker run -d --name celeriant-reference-pg \
  -e POSTGRES_DB=celeriant_reference \
  -e POSTGRES_USER=demo \
  -e POSTGRES_PASSWORD=demo \
  -p 5432:5432 \
  postgres:16
```

Then run the reference API (from root dir):

```bash
cargo run -p celeriant_reference
```

Open http://localhost:5001.

## What it demonstrates

- **Lazy catch-up projection** Postgres read model rebuilt on-demand from Celeriant, no background projection service
- **Exactly-once writes** `client_seq` derived from catch-up + `enforce_client_idempotency` on the server
- **OCC retry loops** re-derive state on conflict, retry with fresh `expected_version`
- **Atomic multi-aggregate transfers** debit and credit written in a single `WriteRequest` with OCC on both
- **HTTP idempotency cache** duplicate POST protection via `Idempotency-Key` header
- **Self-healing Postgres projection** stale values auto-corrected by catch-up replay

## Running with a standalone server (Linux only)

The Celeriant server uses io_uring and only runs natively on Linux. On macOS and Windows, use the Docker setup above.

On Linux, if you don't need a full cluster, you can run a single server directly:

```bash
cargo run --release -p celeriant -- --standalone --data-root /tmp/celeriant-reference --routing-rule aggregate_type_id
```

Start Postgres:

```bash
docker run -d --name celeriant-reference-pg \
  -e POSTGRES_DB=celeriant_reference \
  -e POSTGRES_USER=demo \
  -e POSTGRES_PASSWORD=demo \
  -p 5432:5432 \
  postgres:16
```

Then in a separate terminal:

```bash
cargo run -p celeriant_reference
```

## Configuration

| Variable | Default | Description |
|---|---|---|
| `CELERIANT_ADDRESS` | `localhost:10000` | Celeriant server address |
| `POSTGRES_URL` | `host=localhost dbname=celeriant_reference user=demo password=demo` | Postgres connection string |
