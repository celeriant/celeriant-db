# Celeriant Reference

A production-grade reference API showing how to build an event-sourced system with [Celeriant](https://celeriant.io) and a Postgres read projection, using the Rust tokio client.

Banking domain: deposits, withdrawals, and atomic multi-aggregate transfers with server-side balance validation.

## Prerequisites

- **Linux / macOS**: Docker
- **Windows**: WSL2 + Docker Desktop

## Run

The reference uses atomic multi-aggregate writes (transfers), which require all aggregates to route to the same shard. Use `aggregate_type_id` routing so all accounts share a shard.

**1. Start the Celeriant cluster** (re-run any time you change Rust source):

```bash
cd deploy/local-cluster
CELERIANT_ROUTING_RULE=aggregate_type_id docker compose up -d --build
```

Always pass `--build` — Compose reuses cached images otherwise and your source changes won't take effect. Volumes survive rebuilds.

**2. Start Postgres** (one-time setup — the `--name` flag means `docker run` won't re-run cleanly):

```bash
docker run -d --name celeriant-reference-pg \
  -e POSTGRES_DB=celeriant_reference \
  -e POSTGRES_USER=demo \
  -e POSTGRES_PASSWORD=demo \
  -p 5432:5432 \
  postgres:16
```

If the container already exists from a previous run, just start it: `docker start celeriant-reference-pg`.

**3. Run the reference API** (from repo root):

```bash
cargo run -p celeriant_reference
```

Open http://localhost:5001.

### Reset

Wipe everything and start over:

```bash
cd deploy/local-cluster && docker compose down -v   # cluster + observability + data
docker rm -f celeriant-reference-pg                 # postgres
```

Then re-run steps 1–3 above.

## What it demonstrates

- **Lazy catch-up projection** Postgres read model rebuilt on-demand from Celeriant, no background projection service
- **Exactly-once writes** `client_seq` derived from catch-up + `enforce_client_idempotency` on the server
- **OCC retry loops** re-derive state on conflict, retry with fresh `expected_version`
- **Atomic multi-aggregate transfers** debit and credit written in a single `WriteRequest` with OCC on both
- **HTTP idempotency via `event_id`** the `Idempotency-Key` header is plumbed into the WriteRequest as `event_id`, then mirrored back into the in-memory cache during catch-up so retries land on cold BFF instances without writing duplicates
- **Self-healing Postgres projection** stale values auto-corrected by catch-up replay

## Idempotency layers

The reference layers four dedup mechanisms, each catching a different failure mode:

| Layer | Where | Keyed by | Catches |
|---|---|---|---|
| Frontend retry-with-same-key | browser | one UUIDv4 per user intent (button click) | network blips during the request |
| In-memory `IdempotencyCache` | BFF (per instance, 90s TTL) | `(event_id, aggregate_id)` | fast in-instance retries; warmed by catch-up for cross-instance K-FAIL recovery |
| `enforce_client_idempotency` (CEI) | Celeriant server | `(client_id, aggregate_key, client_seq)` | retries that hold `client_seq` constant after a timeout |
| OCC via `expected_version` | Celeriant server | aggregate version | concurrent writers racing on the same aggregate |

The cache is an optimisation, not a correctness layer. CEI on the server is the underlying dedup. The frontend reuses the same `Idempotency-Key` on retry; the BFF stamps it as `event_id` on the WriteRequest; `catch_up()` replays events from Celeriant and warms the cache from any `event_id` it sees — so a retry hitting a cold BFF instance after a K-FAIL (BFF crashed between fsync and ack) finds the cached response instead of writing a duplicate.

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
