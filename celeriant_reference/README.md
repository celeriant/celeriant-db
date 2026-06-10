# Celeriant Reference

A production-grade reference API showing how to build an event-sourced system with [Celeriant](https://celeriant.io), using the Rust tokio client.

Banking domain: deposits, withdrawals, and atomic multi-aggregate transfers with server-side balance validation.

Two read-projection backends, selected by `PROJECTION` (both safe to run as a fleet of replicas sharing one client id):

- **`postgres`** (default): projection in Postgres, rebuilt lazily by catch-up (`account_service_pg.rs`)
- **`memory`**: projection in process memory, each replica folds the stream itself; no Postgres at all (`account_service_mem.rs`)

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

Always pass `--build`. Compose reuses cached images otherwise and your source changes won't take effect. Volumes survive rebuilds.

**2. Start Postgres** (skip this step and run with `PROJECTION=memory` for the in-memory backend; one-time setup, since the `--name` flag means `docker run` won't re-run cleanly):

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
cargo run -p celeriant_reference                     # postgres projection (default)
PROJECTION=memory cargo run -p celeriant_reference   # in-memory projection, no Postgres needed
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

- **Lazy catch-up projection** read model rebuilt on-demand from Celeriant, no background projection service
- **Exactly-once writes** `client_seq` derived from catch-up + `enforce_client_idempotency` on the server
- **OCC retry loops** re-derive state on conflict, retry with fresh `expected_version`
- **Atomic multi-aggregate transfers** debit and credit written in a single `WriteRequest` with OCC on both
- **HTTP idempotency via `event_id`** the `Idempotency-Key` header is plumbed into the WriteRequest as `event_id`; a request-dedup index living *with the projection cursor* (in the fold for `memory`, in the `request_responses` table for `postgres`) lets a retried request get its original response back on any replica without writing a duplicate
- **Stream-verified conflict resolution** a `ClientIdempotencyViolation` is never taken at face value: `verify.rs` point-reads the contested `client_seq` from the stream to tell "my prior attempt landed" from "a sibling took my sequence number"
- **Self-healing projection** stale values auto-corrected by catch-up replay

## Idempotency layers

The reference layers four dedup mechanisms, each catching a different failure mode:

| Layer | Where | Keyed by | Catches |
|---|---|---|---|
| Frontend retry-with-same-key | browser | one UUIDv4 per user intent (button click) | network blips during the request |
| Request-dedup index (90s window) | BFF, colocated with the projection cursor | `(event_id, aggregate_id)` | retried requests getting back their original response on any replica, without writing |
| `enforce_client_idempotency` (CEI) | Celeriant server | `(client_id, aggregate_key, client_seq)` | retries that hold `client_seq` constant after a timeout |
| OCC via `expected_version` | Celeriant server | aggregate version | concurrent writers racing on the same aggregate |

The index is not what prevents double-writes; CEI on the server is the underlying dedup. The index restores the *lost response*: the frontend reuses the same `Idempotency-Key` on retry, the BFF stamps it as `event_id` on the WriteRequest, and the index answers "this request already landed, here is what it returned". The rule that makes it fleet-safe is colocation: the index lives wherever the projection cursor lives and moves with it. In the `memory` backend each replica's fold maintains its own index while replaying (so any replica can answer for any other); in the `postgres` backend the index is a table updated atomically with the cursor bump, because once the shared cursor moves past an event no other replica will ever replay it.

When a write comes back `ClientIdempotencyViolation` (2002), the seq was consumed, but with concurrent requests sharing one client id, possibly by a *sibling*, in which case your event never landed. `verify.rs` settles it from the stream itself: a point read of the contested `client_seq` (filtered on batch metadata, so non-matching batches are skipped without reading payloads) and an `event_id` comparison. If the event is yours, done. If a sibling owns it, re-derive and go again.

The fleet behaviour is pinned by the `reference_account_service` integration test, which drives two replicas of each backend through cross-replica retries, concurrent duplicates, and sibling races.

## Running with a standalone server (Linux only)

The Celeriant server uses io_uring and only runs natively on Linux. On macOS and Windows, use the Docker setup above.

On Linux, if you don't need a full cluster, you can run a single server directly:

```bash
cargo run --release -p celeriant -- --standalone --data-root /tmp/celeriant-reference --routing-rule aggregate_type_id
```

Then in a separate terminal, the fastest path is the in-memory backend, which needs nothing else:

```bash
PROJECTION=memory cargo run -p celeriant_reference
```

For the Postgres backend, start Postgres (as in step 2 above) and run without `PROJECTION`:

```bash
cargo run -p celeriant_reference
```

## Tests

The `reference_account_service` integration test starts a fresh standalone server and drives **two replicas of each backend** through the dedup scenarios: same-replica and cross-replica retries, concurrent duplicate requests, sibling races on one aggregate, and retried transfers. It asserts every request lands exactly once and every retry gets its original response back.

```bash
cargo build -p celeriant --release    # the test spawns this server binary
cargo run -p celeriant_integration_tests --release -- --test reference_account_service
```

The in-memory fleet always runs. The Postgres fleet runs when `POSTGRES_URL` is set, and is skipped otherwise:

```bash
docker run -d --rm --name ref-test-pg -e POSTGRES_USER=demo -e POSTGRES_PASSWORD=demo \
  -e POSTGRES_DB=celeriant_reference -p 127.0.0.1:5439:5432 postgres:16-alpine

POSTGRES_URL="host=localhost port=5439 dbname=celeriant_reference user=demo password=demo" \
  cargo run -p celeriant_integration_tests --release -- --test reference_account_service

docker stop ref-test-pg
```

## Configuration

| Variable | Default | Description |
|---|---|---|
| `CELERIANT_ADDRESS` | `localhost:10000` | Celeriant server address |
| `PROJECTION` | `postgres` | Read-projection backend: `postgres` or `memory` |
| `POSTGRES_URL` | `host=localhost dbname=celeriant_reference user=demo password=demo` | Postgres connection string (`postgres` backend only) |
