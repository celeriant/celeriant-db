# celeriant

Server executable crate. Parses CLI configuration, validates the environment, and launches the sharded runtime. Also provides `cert` and `keys` subcommands for TLS certificate lifecycle and API key management.

## Quick Start

### Standalone (single node, no dependencies)

Requires a filesystem that honours O_DIRECT (XFS, ext4 — not tmpfs, overlayfs, or macOS).

```sh
cargo build --release

# Create data directory on a Direct I/O capable filesystem
mkdir -p /var/lib/celeriant

./target/release/celeriant \
  --standalone \
  --data-root /var/lib/celeriant \
  --num-shards 4
```

The server is now listening on `0.0.0.0:10000` (client) and `0.0.0.0:10001` (replication, unused in standalone). Metrics are available at `http://localhost:9090/metrics`.

### Cluster (2 nodes + MinIO via Docker Compose)

A ready-made compose file lives at `deploy/local-cluster/docker-compose.yml`. It starts two Celeriant nodes, MinIO (S3-compatible storage for leader election and replication fallback), Prometheus, Loki, Promtail, and Grafana.

```sh
cd deploy/local-cluster

# Build and start everything
docker compose up -d --build

# Tail server logs
docker compose logs -f celeriant-node-1 celeriant-node-2

# Stop (data preserved in Docker volumes)
docker compose down

# Stop and wipe all data (fresh cluster)
docker compose down -v
```

Endpoints once running:

| Service | URL |
|---------|-----|
| Node 1 (client) | `localhost:10000` |
| Node 2 (client) | `localhost:10002` |
| Node 1 metrics | `http://localhost:19090/metrics` |
| Node 2 metrics | `http://localhost:29090/metrics` |
| MinIO Console | `http://localhost:9001` (minioadmin / minioadmin) |
| Grafana | `http://localhost:3000` (admin / admin) |
| Prometheus | `http://localhost:9090` |

Leader election happens automatically via S3. Connect a client to either node — writes to a follower return a `NotLeader` error with the leader's address for redirect.

To rebuild just the server nodes (keeping observability stack running):

```sh
docker compose up -d --build celeriant-node-1 celeriant-node-2
```

## Subcommands

The binary dispatches on the first argument before clap parses `ServerConfig`:

| Command | Purpose |
|---------|---------|
| _(none)_ | Normal server startup |
| `cert` | Certificate lifecycle: `create-ca`, `create-node`, `create-client`, `list` |
| `keys` | API key management: `generate`, `regenerate`, `list` |

## Startup Sequence

```
startup()
  │
  ├── install_crash_handler()
  │     └── SIGBUS / SIGSEGV / SIGILL → process::abort()
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
  ├── fs_warmup::warm_fs_metadata(data_root)
  │     └── walk shard_*//*.wal, stat + open each file to warm XFS
  │         inode and extent metadata into page cache (non-fatal)
  │
  ├── ntp_check::check_clock_synchronized()
  │     └── adjtimex() syscall; warns if STA_UNSYNC (no NTP for ~11 min)
  │         works in containers (shared host kernel clock)
  │
  ├── fs_check::verify_same_filesystem(data_root, compaction_temp_dir)
  │     └── stat() both paths, compare st_dev; atomic rename requires same fs
  │         only runs if --compaction-temp-dir is set
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
  ├── server_meta::validate_or_create(data_root, current_meta)
  │     └── persists num_shards, timestamp_precision, epoch_offset,
  │         routing_rule to server_meta.toml on first start;
  │         rejects mismatches on subsequent starts (immutable config)
  │
  ├── memory_budget::detect_available_memory()
  │     └── min(physical RAM from /proc/meminfo, cgroup v2 memory.max)
  │         then apply memory_consumption_percent or explicit budget
  │         splits into per-shard cache allocations by fixed ratios
  │
  ├── build_tls_config()
  │     └── loads CA bundle, node cert/key, optional client cert/key
  │         builds separate rustls configs for client and replication listeners
  │         supports dual CA (client CA vs intracluster CA)
  │
  ├── verify_ktls_support()  [only if TLS enabled]
  │     └── checks kernel CONFIG_TLS / tls module; required for kTLS offload
  │
  ├── api_keys::load_api_keys(data_root)
  │     └── loads api_keys.toml (SHA-256 hashes); enforces TLS when present
  │         unless --insecure-allow-plaintext-auth
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
| `main.rs` | Entry point; sets MiMalloc as global allocator; dispatches `cert` / `keys` subcommands |
| `lib.rs` | `startup()`: env load, config parse, all pre-flight checks, executor launch |
| `server_config.rs` | `ServerConfig` clap struct; conversion to `ShardConfig` / `SidecarConfig`; TLS config builder |
| `dio_check.rs` | O_DIRECT enforcement check via intentional unaligned write |
| `fs_warmup.rs` | Walks shard directories, stats + opens WAL files to warm XFS metadata into page cache |
| `fs_check.rs` | Verifies compaction temp dir is on the same filesystem as data_root (atomic rename requirement) |
| `ntp_check.rs` | Clock synchronization check via `adjtimex()` syscall |
| `server_meta.rs` | Persists and validates immutable config (shard count, timestamp precision, routing rule) across restarts |
| `memory_budget.rs` | Detects system/cgroup memory, computes per-shard cache budgets with fixed ratios |
| `api_keys.rs` | Loads/saves `api_keys.toml` (SHA-256 hashes of API keys); TOML parsing and hex conversion |
| `cert_cmd.rs` | `celeriant cert` subcommand: create-ca, create-node, create-client, list |
| `keys_cmd.rs` | `celeriant keys` subcommand: generate, regenerate, list |

## Design Notes

**MiMalloc allocator** — replaces the system allocator globally for reduced fragmentation and lower tail latency.

**Crash handler** — SIGBUS, SIGSEGV, and SIGILL call `process::abort()` rather than unwind. An unmapped memory access that reaches a signal handler means the process state cannot be trusted; aborting is always safer than attempting recovery.

**Port pre-check** — Glommio binds with `SO_REUSEPORT`, so calling `bind()` succeeds even when another process already owns the port. A short `connect_timeout` detects this before the executors start.

**Direct I/O check** — writes 41 unaligned bytes at offset 77 via `O_DIRECT`. A real Direct I/O filesystem returns `EINVAL`; a filesystem that silently falls back to buffered I/O returns success. Silent fallback is unsafe for the WAL correctness guarantees, so the server refuses to start.

**Filesystem metadata warmup** — with O_DIRECT, data bypasses the page cache but metadata (XFS inodes, extent trees) does not. After a restart, cold metadata causes severe throughput degradation. The warmup step walks all shard directories and stat+opens every `.wal` file, pulling metadata into the VFS cache without touching file data.

**NTP check** — uses the `adjtimex()` syscall to detect whether the kernel clock is NTP-disciplined. Works inside containers because all containers share the host kernel clock. Warns (non-fatal) if `STA_UNSYNC` is set, indicating no NTP daemon has updated the clock for ~11 minutes.

**Immutable config validation** — `server_meta.toml` is written on first startup with the shard count, timestamp precision, epoch offset, and routing rule. On subsequent starts, any mismatch is a fatal error. These settings are baked into the on-disk WAL format and cannot change without data corruption.

**Automatic memory budget** — detects available memory from `/proc/meminfo` and cgroup v2 `memory.max` (takes the minimum). Applies `--memory-consumption-percent` (default 80%) to get the total budget, then divides across shards. Per-shard budget is split into caches by fixed ratios: recent write cache (71.5%), aggregate snapshots (9%), client idempotency snapshots (9%), schema cache (9%), WAL index positions (1.5%). Can be overridden entirely with `--memory-budget-bytes`.

**TLS with kTLS** — TLS is handled via rustls with kernel TLS (kTLS) offload. Supports dual CA isolation: a client CA for client connections and an intracluster CA for replication. Client-facing and replication listeners get separate `ServerConfig` instances. TLS 1.3 session tickets are disabled to prevent kTLS sequence counter desync.

**API key authentication** — keys are stored as SHA-256 hashes in `api_keys.toml`. Four key slots: primary/secondary × read-write/read-only. Dual slots enable zero-downtime key rotation. Requires TLS unless `--insecure-allow-plaintext-auth` is set. The `celeriant keys` subcommand manages generation and rotation.

**Node ID** — a 128-bit UUID persisted to `data_root` by `celeriant_crypto`. Stable across restarts; used to identify this node in replication membership and S3 paths.

**Shard count** — defaults to the physical CPU count (thread-per-core). Override with `--num-shards` when CPU count is not the right bound (e.g. constrained containers).

**Compaction filesystem check** — when `--compaction-temp-dir` is set, verifies it resides on the same filesystem as `data_root` by comparing `st_dev` from `stat()`. Compaction uses `rename(2)` to atomically swap log segments; cross-device rename returns `EXDEV`.

## Configuration Reference

All flags accept an equivalent environment variable. Environment variables take precedence over defaults; flags take precedence over environment variables.

### Network

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--listen-address` | `CELERIANT_LISTEN_ADDRESS` | `0.0.0.0` | Bind address for all ports |
| `--client-port` | `CELERIANT_CLIENT_PORT` | `10000` | Client TCP connections |
| `--replication-port` | `CELERIANT_REPLICATION_PORT` | `10001` | Leader-to-follower replication |
| `--advertised-replication-address` | `CELERIANT_ADVERTISED_REPLICATION_ADDRESS` | _(listen_address:replication_port)_ | Override address published to S3 membership; useful when routing through a TCP proxy |
| `--advertised-client-address` | `CELERIANT_ADVERTISED_CLIENT_ADDRESS` | _(listen_address:client_port)_ | Override address advertised in S3 membership and returned in NotLeader errors; set when clients connect through a load balancer |

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
| `--mesh-channel-size` | `CELERIANT_MESH_CHANNEL_SIZE` | `8192` | Channel depth for cross-shard message routing |

### Timestamps

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--timestamp-precision` | `CELERIANT_TIMESTAMP_PRECISION` | `milliseconds` | Server timestamp precision: `milliseconds`, `microseconds`, `nanoseconds` |
| `--timestamp-epoch-offset-secs` | `CELERIANT_TIMESTAMP_EPOCH_OFFSET_SECS` | `0` | Seconds offset from Unix epoch for custom epoch (e.g. `1704067200` for 2024-01-01) |

### Memory

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--memory-consumption-percent` | `CELERIANT_MEMORY_CONSUMPTION_PERCENT` | `80` | Percentage of detected memory to use for caches (1–95) |
| `--memory-budget-bytes` | `CELERIANT_MEMORY_BUDGET_BYTES` | _(auto-detected)_ | Explicit total memory budget in bytes; overrides detection and percentage |

Memory is auto-detected from `/proc/meminfo` and cgroup v2 `memory.max`. The per-shard budget is split into caches:

| Cache | Ratio | Purpose |
|-------|-------|---------|
| Recent write cache | 71.5% | Avoids disk reads for recent data |
| Aggregate snapshots | 9.0% | LRU cache for aggregate position metadata |
| Client idempotency snapshots | 9.0% | LRU cache for client idempotency indices |
| Schema cache | 9.0% | LRU cache for schema definitions |
| WAL index positions | 1.5% | WAL position cache for fast list pagination |

### Request and Response Limits

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--max-request-size` | `CELERIANT_MAX_REQUEST_SIZE` | `16777216` (16 MiB) | Maximum incoming message size |
| `--max-response-size` | `CELERIANT_MAX_RESPONSE_SIZE` | `67108864` (64 MiB) | Maximum outgoing message size |
| `--client-connection-timeout-ms` | `CELERIANT_CLIENT_CONNECTION_TIMEOUT_MS` | `30000` (30 s) | Max time a client has to drain a server response over TCP |
| `--max-requested-latency-ms` | `CELERIANT_MAX_REQUESTED_LATENCY_MS` | `2000` (2 s) | Maximum latency a watch subscriber may request |
| `--max-schema-size-bytes` | `CELERIANT_MAX_SCHEMA_SIZE_BYTES` | `16384` (16 KB) | Maximum size of a single schema definition |

### Concurrency Limits

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--list-max-concurrent` | `CELERIANT_LIST_MAX_CONCURRENT` | `16` | Maximum concurrent list operations per shard |
| `--read-max-concurrent` | `CELERIANT_READ_MAX_CONCURRENT` | `64` | Maximum concurrent in-flight backwards metablock scans per shard |

### List Operations

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--list-page-size` | `CELERIANT_LIST_PAGE_SIZE` | `2000` | Max entities returned per list page |
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

### Compaction

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--compaction-check-interval-secs` | `CELERIANT_COMPACTION_CHECK_INTERVAL_SECS` | `7200` (2 hrs) | How often to scan for compaction-eligible segments |
| `--compaction-min-reclaimable-ratio` | `CELERIANT_COMPACTION_MIN_RECLAIMABLE_RATIO` | `0.20` (20%) | Minimum fraction of reclaimable bytes to trigger compaction |
| `--compaction-temp-dir` | `CELERIANT_COMPACTION_TEMP_DIR` | _(shard_dir/.compaction_tmp/)_ | Temp directory for in-progress compaction files; must be on same filesystem as data_root |

### Cache Warmup

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--cache-warmup-max-secs` | `CELERIANT_CACHE_WARMUP_MAX_SECS` | _(no limit)_ | Maximum time to spend warming caches on shard open |

### TLS

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--tls-mode` | `CELERIANT_TLS_MODE` | `disabled` | TLS mode: `disabled` (plaintext) or `strict` (TLS only) |
| `--tls-ca-cert` | `CELERIANT_TLS_CA_CERT` | — | Path to client CA certificate (PEM, supports bundles) |
| `--tls-intracluster-ca-cert` | `CELERIANT_TLS_INTRACLUSTER_CA_CERT` | _(same as tls-ca-cert)_ | Separate CA for replication listener; enables dual CA isolation |
| `--tls-node-cert` | `CELERIANT_TLS_NODE_CERT` | — | Path to node certificate (PEM) |
| `--tls-node-key` | `CELERIANT_TLS_NODE_KEY` | — | Path to node private key (PEM) |
| `--tls-client-cert` | `CELERIANT_TLS_CLIENT_CERT` | _(node cert)_ | Client-facing server certificate; enforces CA isolation between client and intracluster trust domains |
| `--tls-client-key` | `CELERIANT_TLS_CLIENT_KEY` | _(node key)_ | Client-facing server private key; required with `--tls-client-cert` |
| `--tls-client-auth` | `CELERIANT_TLS_CLIENT_AUTH` | `require` | Client cert auth: `require` (mTLS), `optional`, `none` |
| `--tls-cert-reload-interval-secs` | `CELERIANT_TLS_CERT_RELOAD_INTERVAL_SECS` | `0` | Hot-reload interval for TLS certs; 0 = disabled |

### Authentication

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--require-client-identity` | `CELERIANT_REQUIRE_CLIENT_IDENTITY` | `false` | Require clients to send IdentifyRequest as first message |
| `--insecure-allow-plaintext-auth` | `CELERIANT_INSECURE_ALLOW_PLAINTEXT_AUTH` | `false` | Allow API key auth without TLS (development only) |

API keys are managed via `celeriant keys` and stored as SHA-256 hashes in `data_root/api_keys.toml`. When the file exists, all client connections must authenticate. TLS is required unless `--insecure-allow-plaintext-auth` is set.

### Cluster Mode

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--standalone` | `CELERIANT_STANDALONE` | `false` | Disable replication and S3 election; single-node mode |
| `--heartbeat-interval-ms` | `CELERIANT_HEARTBEAT_INTERVAL_MS` | `500` | Leader heartbeat interval to followers |
| `--heartbeat-lease-duration-ms` | `CELERIANT_HEARTBEAT_LEASE_DURATION_MS` | `1500` | Time before a missed heartbeat expires the follower lease |
| `--s3-lease-duration-ms` | `CELERIANT_S3_LEASE_DURATION_MS` | `30000` (30 s) | S3 lease TTL for leader election, independent of heartbeat timing |
| `--max-clock-drift-ms` | `CELERIANT_MAX_CLOCK_DRIFT_MS` | `500` | Allowed clock skew added to heartbeat lease calculations |
| `--internode-connection-timeout-ms` | `CELERIANT_INTERNODE_CONNECTION_TIMEOUT_MS` | `5000` (5 s) | TCP connect timeout for inter-node connections |
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
| `--s3-retry-max-duration-secs` | `CELERIANT_S3_RETRY_MAX_DURATION_SECS` | _(indefinite)_ | Max total retry duration for S3 operations with exponential backoff |

### Metrics

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--metrics-enabled` | `CELERIANT_METRICS_ENABLED` | `true` | Enable Prometheus metrics and health HTTP endpoint |
| `--metrics-port` | `CELERIANT_METRICS_PORT` | `9090` | Port for `/metrics` and `/health` HTTP server |

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

## Example: Clustered with S3 and TLS

```sh
celeriant \
  --data-root /var/lib/celeriant \
  --listen-address 10.0.0.1 \
  --client-port 10000 \
  --replication-port 10001 \
  --s3-enabled \
  --s3-region us-east-1 \
  --s3-bucket my-celeriant-cluster \
  --s3-subfolder prod \
  --tls-mode strict \
  --tls-ca-cert /etc/celeriant/ca.crt \
  --tls-node-cert /etc/celeriant/node.crt \
  --tls-node-key /etc/celeriant/node.key
```

## Example: Certificate and Key Setup

```sh
# Generate cluster CA
celeriant cert create-ca --ca-dir /etc/celeriant/ca

# Generate node certificate
celeriant cert create-node 10.0.0.1 node1.example.com \
  --ca-dir /etc/celeriant/ca \
  --cert-dir /etc/celeriant/node

# Generate client certificate
celeriant cert create-client myapp \
  --ca-dir /etc/celeriant/ca \
  --cert-dir /etc/celeriant/clients

# Generate API keys
celeriant keys generate --data-root /var/lib/celeriant

# Rotate a single key
celeriant keys regenerate primary-rw --data-root /var/lib/celeriant
```

## Dependencies

| Crate | Purpose |
|-------|------------|
| `celeriant_runtimes` | Executor launch, `ShardConfig`, `SidecarConfig`, `RoutingRule`, `TlsConfig` |
| `celeriant_shard` | `TimestampConfig`, `TimestampPrecision` |
| `celeriant_sidecar` | `SidecarStore`, `S3Config`, `StoreConfig` |
| `celeriant_distributed` | `S3LeaseConfig` |
| `celeriant_crypto` | Node ID, API key generation/hashing, `PkiManager` for TLS cert operations |
| `celeriant_ktls` | Kernel TLS support verification |
| `mimalloc` | Global allocator (lower fragmentation, reduced tail latency) |
| `clap` | CLI argument parsing and env var binding |
| `dotenvy` | `.env` file support |
| `tracing` / `tracing-subscriber` | Structured logging |
| `glommio` | Async runtime (thread-per-core) |
| `num_cpus` | Default shard count |
| `libc` | Signal handler installation, O_DIRECT check, NTP check (`adjtimex`) |
| `toml` / `serde` | `server_meta.toml` and `api_keys.toml` serialization |
| `x509-parser` | Certificate inspection for `cert list` subcommand |
| `base64` | API key display encoding |
