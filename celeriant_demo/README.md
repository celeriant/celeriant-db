# Celeriant Demo

A simple browser-based banking demo that shows basic [Celeriant](https://celeriant.io) read/write patterns using the Rust tokio client.

Three seeded accounts (Alice, Bob, Charlie) with deposits, withdrawals, and atomic multi-aggregate transfers. The UI lets you pick a client ID and see OCC conflicts in action. A live watch feed shows changes as they happen via SSE.

## Prerequisites

- **Linux / macOS**: Docker
- **Windows**: WSL2 + Docker Desktop

## Run

The demo uses atomic multi-aggregate writes (transfers), which require all aggregates to route to the same shard. Use `aggregate_type_id` routing so all accounts share a shard:

```bash
cd deploy/local-cluster
CELERIANT_ROUTING_RULE=aggregate_type_id docker compose up -d
```

Then run the demo (from root dir):

```bash
cargo run -p celeriant_demo
```

Open http://localhost:5000.

## What it demonstrates

- Writing events with `CeleriantPool` and `json_event`
- Optimistic concurrency control (`expected_event_batch_index`)
- Atomic multi-aggregate writes (transfers across two accounts)
- Watch API with SSE broadcast to the browser

## Running with a standalone server (Linux only)

The Celeriant server uses io_uring and only runs natively on Linux. On macOS and Windows, use the Docker setup above.

On Linux, if you don't need a full cluster, you can run a single server directly:

```bash
cargo run --release -p celeriant -- --standalone --data-root /tmp/celeriant-demo --routing-rule aggregate_type_id
```

Then in a separate terminal:

```bash
cargo run -p celeriant_demo
```

## Configuration

| Variable | Default | Description |
|---|---|---|
| `CELERIANT_ADDRESS` | `localhost:10000` | Celeriant server address |
