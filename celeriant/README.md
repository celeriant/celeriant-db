# celeriant

Server executable crate. Parses CLI configuration, validates the environment, and launches the sharded runtime.

## Startup Sequence

```
startup()
  │
  ├── install_crash_handler()
  │     └── SIGBUS / SIGSEGV / SIGILL → process::abort()
  │         (prevents silent corruption; crash is always visible)
  │
  ├── load_dotenv()
  │     └── load .env file if present (dotenvy); fatal on parse error
  │
  ├── ServerConfig::parse_from(args)
  │     └── clap parser; env vars override flag defaults
  │
  ├── tracing_subscriber init
  │     └── RUST_LOG env var takes precedence over --log-level
  │
  ├── std::panic::set_hook
  │     └── route panics through tracing (captured by test harness)
  │
  ├── server_config.log_non_defaults()
  │     └── trace every setting that differs from its default
  │
  ├── dio_check::verify_direct_io(data_root)
  │     └── unaligned O_DIRECT write; EINVAL = pass, success = fail
  │         fails fast with exit(1) if filesystem ignores O_DIRECT
  │
  ├── Crypto::load_or_generate_node_id(data_root)
  │     └── persistent 128-bit node identity (survives restarts)
  │
  ├── port pre-check: client_port + replication_port
  │     └── TcpStream::connect_timeout(100ms) to 127.0.0.1
  │         Glommio uses SO_REUSEPORT so bind() would not fail
  │
  ├── num_shards = num_shards.unwrap_or(num_cpus::get())
  │
  ├── SidecarStore::new(sidecar_store_config)
  │     └── initialises S3 client if s3_enabled
  │
  └── run_executors_and_sidecar(shard_config, sidecar_config, ...)
        └── one Glommio executor per shard + sidecar thread pool
```

## Modules

| Module | Purpose |
|--------|---------|
| `main.rs` | Entry point; sets MiMalloc as global allocator |
| `lib.rs` | `startup()`: env load, config parse, signal handlers, DIO check, node ID, executor launch |
| `server_config.rs` | `ServerConfig` clap struct; conversion to `ShardConfig` / `SidecarConfig` |
| `dio_check.rs` | O_DIRECT enforcement check via intentional unaligned write |

## Design Notes

**MiMalloc allocator** — replaces the system allocator globally for reduced fragmentation and lower tail latency.

**Crash handler** — SIGBUS, SIGSEGV, and SIGILL call `process::abort()` rather than unwind. An unmapped memory access that reaches a signal handler means the process state cannot be trusted; aborting is always safer than attempting recovery.

**Port pre-check** — Glommio binds with `SO_REUSEPORT`, so calling `bind()` succeeds even when another process already owns the port. A short `connect_timeout` detects this before the executors start.

**Direct I/O check** — writes 41 unaligned bytes at offset 77 via `O_DIRECT`. A real Direct I/O filesystem returns `EINVAL`; a filesystem that silently falls back to buffered I/O returns success. Silent fallback is unsafe for the WAL correctness guarantees, so the server refuses to start.

**Node ID** — a 128-bit UUID persisted to `data_root` by `celeriant_crypto`. Stable across restarts; used to identify this node in replication membership and S3 paths.

**Shard count** — defaults to the physical CPU count (thread-per-core). Override with `--num-shards` when CPU count is not the right bound (e.g. constrained containers).

## Configuration Reference

All flags accept an equivalent environment variable. Environment variables take precedence over defaults; flags take precedence over environment variables.

### Network

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--listen-address` | `CELERIANT_LISTEN_ADDRESS` | `0.0.0.0` | Bind address for all ports |
| `--client-port` | `CELERIANT_CLIENT_PORT` | `10000` | Client TCP connections |
| `--replication-port` | `CELERIANT_REPLICATION_PORT` | `10001` | Leader-to-follower replication |
| `--advertised-replication-address` | `CELERIANT_ADVERTISED_REPLICATION_ADDRESS` | _(listen_address:replication_port)_ | Override address published to S3 membership; useful when routing through a TCP proxy |

### Storage

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--data-root` | `CELERIANT_DATA_ROOT` | `data` | Directory for WAL log files and node ID |
| `--shard-log-preallocate-bytes` | `CELERIANT_SHARD_LOG_PREALLOCATE_BYTES` | `1073741824` (1 GB) | Size of each log segment file |
| `--max-open-files` | `CELERIANT_MAX_OPEN_FILES` | `1000` | LRU cap on open log file handles per shard |
| `--read-max-chunk-size` | `CELERIANT_READ_MAX_CHUNK_SIZE` | `32768` (32 KB) | DMA read chunk size |
| `--write-max-chunk-size` | `CELERIANT_WRITE_MAX_CHUNK_SIZE` | `32768` (32 KB) | DMA write chunk size |

### Sharding and Routing

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--num-shards` | `CELERIANT_NUM_SHARDS` | CPU count | Number of shards (Glommio executors) |
| `--routing-rule` | `CELERIANT_ROUTING_RULE` | `aggregate_id` | Shard key: `org_id`, `aggregate_type_id`, or `aggregate_id` |
| `--mesh-channel-size` | `CELERIANT_MESH_CHANNEL_SIZE` | `1024` | Channel depth for cross-shard message routing |

### Timestamps

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--timestamp-precision` | `CELERIANT_TIMESTAMP_PRECISION` | `milliseconds` | Server timestamp precision: `milliseconds`, `microseconds`, `nanoseconds` |
| `--timestamp-epoch-offset-secs` | `CELERIANT_TIMESTAMP_EPOCH_OFFSET_SECS` | `0` | Seconds offset from Unix epoch for custom epoch (e.g. `1704067200` for 2024-01-01) |

### Memory Bounds

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--recent-write-cache-bytes` | `CELERIANT_RECENT_WRITE_CACHE_BYTES` | `536870912` (512 MB) | Per-shard hot write cache; avoids disk reads for recent data |
| `--aggregate-snapshots-cache-bytes` | `CELERIANT_AGGREGATE_SNAPSHOTS_CACHE_BYTES` | `67108864` (64 MB) | Per-shard LRU cache for aggregate position metadata |
| `--aggregate-client-snapshots-cache-bytes` | `CELERIANT_AGGREGATE_CLIENT_SNAPSHOTS_CACHE_BYTES` | `67108864` (64 MB) | Per-shard LRU cache for client idempotency indices |
| `--list-wal-index-cache-bytes` | `CELERIANT_LIST_WAL_INDEX_CACHE_BYTES` | `12582912` (12 MB) | Per-shard WAL position cache for fast list pagination |

### Request and Response Limits

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--max-request-size` | `CELERIANT_MAX_REQUEST_SIZE` | `16777216` (16 MiB) | Maximum incoming message size |
| `--max-response-size` | `CELERIANT_MAX_RESPONSE_SIZE` | `67108864` (64 MiB) | Maximum outgoing message size |
| `--client-connection-timeout-ms` | `CELERIANT_CLIENT_CONNECTION_TIMEOUT_MS` | `30000` (30 s) | Max time a client has to drain a server response over TCP |
| `--max-requested-latency-ms` | `CELERIANT_MAX_REQUESTED_LATENCY_MS` | `2000` (2 s) | Maximum latency a watch subscriber may request |

### List Operations

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--list-page-size` | `CELERIANT_LIST_PAGE_SIZE` | `20000` | Max entities returned per list page |
| `--list-max-duration-ms` | `CELERIANT_LIST_MAX_DURATION_MS` | `2000` (2 s) | Max wall time spent scanning the WAL for a single list call |

### Compression

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--server-compression-algorithm` | `CELERIANT_SERVER_COMPRESSION_ALGORITHM` | `snappy` | Response compression: `none`, `zstd`, `snappy`, `brotli`, `gzip` |
| `--server-compression-level` | `CELERIANT_SERVER_COMPRESSION_LEVEL` | _(algorithm default)_ | Compression level for `zstd`, `brotli`, `gzip`; ignored for `none` / `snappy` |

### Durability and Batching

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--fsync-delay-us` | `CELERIANT_FSYNC_DELAY_US` | `17000` (17 ms) | Fsync amortisation window; writers arriving within this window share one fdatasync |
| `--replication-delay-us` | `CELERIANT_REPLICATION_DELAY_US` | `17000` (17 ms) | Replication amortisation window |

### Cluster Mode

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--standalone` | `CELERIANT_STANDALONE` | `false` | Disable replication and S3 election; single-node mode |
| `--heartbeat-interval-ms` | `CELERIANT_HEARTBEAT_INTERVAL_MS` | `500` | Leader heartbeat interval to followers |
| `--heartbeat-lease-duration-ms` | `CELERIANT_HEARTBEAT_LEASE_DURATION_MS` | `1500` | Time before a missed heartbeat expires the follower lease |
| `--max-clock-drift-ms` | `CELERIANT_MAX_CLOCK_DRIFT_MS` | `500` | Allowed clock skew added to heartbeat lease calculations |
| `--max-cluster-time-drift-ms` | `CELERIANT_MAX_CLUSTER_TIME_DRIFT_MS` | `5000` (5 s) | Max tolerated wall-clock skew between leader and follower |
| `--internode-connection-timeout-ms` | `CELERIANT_INTERNODE_CONNECTION_TIMEOUT_MS` | _(OS default)_ | TCP connect timeout for inter-node connections |
| `--internode-request-timeout-ms` | `CELERIANT_INTERNODE_REQUEST_TIMEOUT_MS` | `10000` (10 s) | Round-trip timeout for inter-node requests |

### Replication Backpressure

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--pending-replication-high-water-bytes` | `CELERIANT_PENDING_REPLICATION_HIGH_WATER_BYTES` | `67108864` (64 MB) | In-memory replication queue depth that triggers S3 fallback |
| `--max-catchup-gap-bytes` | `CELERIANT_MAX_CATCHUP_GAP_BYTES` | `104857600` (100 MB) | Max workset size before switching from TCP catchup to S3 fallback |
| `--max-s3-fallback-batch-bytes` | `CELERIANT_MAX_S3_FALLBACK_BATCH_BYTES` | `104857600` (100 MB) | Max bytes per S3 fallback upload chunk |

### S3 Integration

`--s3-enabled` requires `--s3-region` and `--s3-bucket`.

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--s3-enabled` | `CELERIANT_S3_ENABLED` | `false` | Enable S3 integration for replication fallback and leader election |
| `--s3-region` | `CELERIANT_S3_REGION` | — | AWS region (e.g. `us-east-1`) |
| `--s3-bucket` | `CELERIANT_S3_BUCKET` | — | S3 bucket name |
| `--s3-access-key-id` | `CELERIANT_S3_ACCESS_KEY_ID` | — | AWS access key ID |
| `--s3-secret-access-key` | `CELERIANT_S3_SECRET_ACCESS_KEY` | — | AWS secret access key |
| `--s3-subfolder` | `CELERIANT_S3_SUBFOLDER` | — | Single-level prefix to isolate cluster data within the bucket |
| `--s3-endpoint-override` | `CELERIANT_S3_ENDPOINT_OVERRIDE` | — | Override S3 endpoint URL (e.g. for MinIO or LocalStack) |
| `--s3-skip-signature` | `CELERIANT_S3_SKIP_SIGNATURE` | `false` | Disable AWS Signature V4 authentication |
| `--s3-allow-http` | `CELERIANT_S3_ALLOW_HTTP` | `false` | Allow plaintext HTTP instead of HTTPS |
| `--s3-catchup-max-rounds` | `CELERIANT_S3_CATCHUP_MAX_ROUNDS` | `3` | Max list-download rounds per follower catchup cycle |

### Logging

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--log-level` | `CELERIANT_LOG_LEVEL` | `info` | Log level: `trace`, `debug`, `info`, `warn`, `error` |

`RUST_LOG` takes precedence over `--log-level` when set.

## Example: Standalone Single Node

```sh
celeriant \
  --standalone \
  --data-root /var/lib/celeriant \
  --client-port 10000 \
  --num-shards 8
```

## Example: Clustered with S3

```sh
celeriant \
  --data-root /var/lib/celeriant \
  --listen-address 10.0.0.1 \
  --client-port 10000 \
  --replication-port 10001 \
  --s3-enabled \
  --s3-region us-east-1 \
  --s3-bucket my-celeriant-cluster \
  --s3-subfolder prod
```

Or equivalently via `.env`:

```
CELERIANT_DATA_ROOT=/var/lib/celeriant
CELERIANT_LISTEN_ADDRESS=10.0.0.1
CELERIANT_S3_ENABLED=true
CELERIANT_S3_REGION=us-east-1
CELERIANT_S3_BUCKET=my-celeriant-cluster
CELERIANT_S3_SUBFOLDER=prod
CELERIANT_S3_ACCESS_KEY_ID=AKIA...
CELERIANT_S3_SECRET_ACCESS_KEY=...
```

## Dependencies

| Crate | Purpose |
|-------|---------|
| `celeriant_runtimes` | Executor launch, `ShardConfig`, `SidecarConfig`, `RoutingRule` |
| `celeriant_shard` | `TimestampConfig`, `TimestampPrecision` |
| `celeriant_sidecar` | `SidecarStore`, `S3Config`, `StoreConfig` |
| `celeriant_distributed` | `ReplicationConfig` |
| `celeriant_crypto` | Persistent node ID load/generate |
| `mimalloc` | Global allocator (lower fragmentation, reduced tail latency) |
| `clap` | CLI argument parsing and env var binding |
| `dotenvy` | `.env` file support |
| `tracing` / `tracing-subscriber` | Structured logging |
| `glommio` | Async runtime (thread-per-core) |
| `num_cpus` | Default shard count |
| `libc` | Signal handler installation, O_DIRECT check |
