# celeriant

Server executable crate. Parses CLI configuration, validates the environment, and launches the sharded runtime. Also provides `cert` and `keys` subcommands for TLS certificate lifecycle and API key management.

## Quick Start

### Standalone (single node, no dependencies)

Requires a filesystem that honours O_DIRECT (XFS, ext4 - not tmpfs, overlayfs, or macOS).

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

Leader election happens automatically via S3. Connect a client to either node - writes to a follower return a `NotLeader` error with the leader's address for redirect.

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

## Invariants

These checks run before the server accepts connections. Fatal checks abort the process.

- Direct I/O is verified at startup by attempting an unaligned write with `O_DIRECT`. If the write succeeds, the filesystem is silently falling back to buffered I/O - fatal. `EINVAL` confirms DIO is enforced. Silent fallback is unsafe for WAL correctness guarantees.
- Five immutable config parameters are persisted to `server_meta.toml` on first startup and must never change: `num_shards`, `timestamp_precision`, `timestamp_epoch_offset_secs`, `routing_rule`, `reserve_coordinator_shard`. Mismatch on a subsequent start is fatal. These values are baked into the on-disk WAL format.
- Filesystem metadata must be warm before serving requests. Startup walks all `shard_*` directories, stats and opens every `.wal` file to preload XFS inode and extent metadata into the OS page cache. No data is read - only metadata. Cold metadata causes latency spikes on first access.
- Both client and replication ports must be probed via TCP connect before binding. `bind()` alone cannot detect a running instance because glommio uses `SO_REUSEPORT`. Fatal on conflict.
- If TLS is enabled, kTLS kernel support is verified at startup via `setsockopt(SOL_TCP, TCP_ULP, "tls")`. Fatal if missing.
- If `compaction_temp_dir` is configured, it must be on the same filesystem as `data_root` (validated via `st_dev`). Cross-device `rename(2)` is not atomic - fatal.
- Total memory is bounded by `detected_memory * memory_consumption_percent / 100` (default 80%, range 1–95%). Detection takes the minimum of `/proc/meminfo` physical RAM and cgroup v2 `memory.max`. Per-shard budget categories sum to exactly 100%.

## Design Notes

**MiMalloc allocator** - replaces the system allocator globally for reduced fragmentation and lower tail latency.

**Crash handler** - SIGBUS, SIGSEGV, and SIGILL call `process::abort()` rather than unwind. An unmapped memory access that reaches a signal handler means the process state cannot be trusted; aborting is always safer than attempting recovery.

**Port pre-check** - Glommio binds with `SO_REUSEPORT`, so calling `bind()` succeeds even when another process already owns the port. A short `connect_timeout` detects this before the executors start.

**Direct I/O check** - writes 41 unaligned bytes at offset 77 via `O_DIRECT`. A real Direct I/O filesystem returns `EINVAL`; a filesystem that silently falls back to buffered I/O returns success. Silent fallback is unsafe for the WAL correctness guarantees, so the server refuses to start.

**Filesystem metadata warmup** - with O_DIRECT, data bypasses the page cache but metadata (XFS inodes, extent trees) does not. After a restart, cold metadata causes severe throughput degradation. The warmup step walks all shard directories and stat+opens every `.wal` file, pulling metadata into the VFS cache without touching file data.

**NTP check** - uses the `adjtimex()` syscall to detect whether the kernel clock is NTP-disciplined. Works inside containers because all containers share the host kernel clock. Warns (non-fatal) if `STA_UNSYNC` is set, indicating no NTP daemon has updated the clock for ~11 minutes.

**Immutable config validation** - `server_meta.toml` is written on first startup with the shard count, timestamp precision, epoch offset, and routing rule. On subsequent starts, any mismatch is a fatal error. These settings are baked into the on-disk WAL format and cannot change without data corruption.

**Automatic memory budget** - detects available memory from `/proc/meminfo` and cgroup v2 `memory.max` (takes the minimum). Applies `--memory-consumption-percent` (default 80%) to get the total budget, then divides across shards. Per-shard budget is split into caches by fixed ratios: recent write cache (71.5%), aggregate snapshots (9%), client idempotency snapshots (9%), schema cache (9%), WAL sequence positions (1.5%). Can be overridden entirely with `--memory-budget-bytes`.

**TLS with kTLS** - TLS is handled via rustls with kernel TLS (kTLS) offload. Supports dual CA isolation: a client CA for client connections and an intracluster CA for replication. Client-facing and replication listeners get separate `ServerConfig` instances. TLS 1.3 session tickets are disabled to prevent kTLS sequence counter desync.

**API key authentication** - keys are stored as SHA-256 hashes in `api_keys.toml`. Four key slots: primary/secondary × read-write/read-only. Dual slots enable zero-downtime key rotation. Requires TLS unless `--insecure-allow-plaintext-auth` is set. The `celeriant keys` subcommand manages generation and rotation.

**Node ID** - a 128-bit UUID persisted to `data_root` by `celeriant_crypto`. Stable across restarts; used to identify this node in replication membership and S3 paths.

**Shard count** - defaults to the physical CPU count (thread-per-core). Override with `--num-shards` when CPU count is not the right bound (e.g. constrained containers).

**Compaction filesystem check** - when `--compaction-temp-dir` is set, verifies it resides on the same filesystem as `data_root` by comparing `st_dev` from `stat()`. Compaction uses `rename(2)` to atomically swap log segments; cross-device rename returns `EXDEV`.
