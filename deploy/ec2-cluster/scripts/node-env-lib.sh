#!/bin/bash
# Node env-file generation for the campaign knobs. Sourced by deploy.sh (full deploy) and
# run-cell-sweep.sh (per-cell env push, no binary copy).
#
# Single source of truth on purpose. The prior campaign's harness drifted from the binary it
# was driving — CELERIANT_DIAG_DISABLE_FSYNC and the WAL gates no longer existed, so NO_FSYNC=1
# produced a fsync-ENABLED run labelled "nofsync". Two copies of this generator would
# reintroduce exactly that failure, so there is one.
#
# Contract: callers set REGION and BUCKET before calling generate_env. Knobs are read from the
# environment; init_campaign_knobs validates them and must run first.

# --- Campaign knobs -------------------------------------------------------
#
# Every one of these is a measurement axis in session/goal.md.
#
# An unset knob emits NO line in the node env file, so the server's own default applies. The
# harness deliberately does not restate defaults: a restated default drifts silently when the
# binary's changes.
#
# Server defaults as of this binary, for reference only — do not mirror them here:
#   fsync_delay_us 17000, replication_delay_us 17000, mesh_channel_size 512,
#   num_shards = CPU count, reserve_coordinator_shard false, standalone false.
#
# Changing any of these on an existing data directory is a HARD START FAILURE, because they
# are immutable in server_meta.toml — the sweep driver must wipe the data root between cells
# that vary them:
#   num_shards, reserve_coordinator_shard, routing_rule, timestamp_precision,
#   timestamp_epoch_offset_secs, compression.level
# The delays and mesh_channel_size are NOT immutable; those cells need only a restart.
init_campaign_knobs() {
  NUM_SHARDS="${NUM_SHARDS:-}"
  FSYNC_DELAY_US="${FSYNC_DELAY_US:-}"
  REPLICATION_DELAY_US="${REPLICATION_DELAY_US:-}"
  RESERVE_COORDINATOR_SHARD="${RESERVE_COORDINATOR_SHARD:-}"
  MESH_CHANNEL_SIZE="${MESH_CHANNEL_SIZE:-}"
  STANDALONE="${STANDALONE:-}"
  # Campaign numbers are cleartext; the stack default is strict mTLS. F-43 prices mTLS at
  # -4.6% throughput / +16.7% p99, so the two are not interchangeable — set this per run.
  TLS_MODE="${TLS_MODE:-strict}"

  check_bool RESERVE_COORDINATOR_SHARD "$RESERVE_COORDINATOR_SHARD"
  check_bool STANDALONE "$STANDALONE"

  if [[ "$TLS_MODE" != "strict" && "$TLS_MODE" != "disabled" ]]; then
    echo "ERROR: TLS_MODE must be 'strict' or 'disabled' (got '$TLS_MODE')." >&2
    exit 1
  fi

  # Standalone means no replication and no S3 election. Leaving S3 on would keep the lease
  # manager and offload path in the measurement, which is the confound the standalone rungs
  # exist to remove. Override with S3_ENABLED=true if a run genuinely wants it.
  if [[ "$STANDALONE" == "true" ]]; then
    S3_ENABLED="${S3_ENABLED:-false}"
  else
    S3_ENABLED="${S3_ENABLED:-true}"
  fi
  check_bool S3_ENABLED "$S3_ENABLED"
}

# clap binds these as bool flags and accepts only the literal strings. `0` and `1` are a hard
# startup failure ("invalid value '0' ... [possible values: true, false]"), so catch it here
# rather than after a full deploy.
check_bool() {
  local name=$1 val=${2:-}
  if [[ -n "$val" && "$val" != "true" && "$val" != "false" ]]; then
    echo "ERROR: $name must be 'true' or 'false' (got '$val'). The server rejects 0/1." >&2
    exit 1
  fi
}

# Emit "NAME=value" only when value is non-empty. `return 0` keeps the empty case from
# tripping set -e.
emit() { [[ -n "${2:-}" ]] && echo "$1=$2"; return 0; }

# Render the node env file for the data node at $1 (private IP). Requires REGION and BUCKET.
generate_env() {
  local NODE_IP=$1
  cat <<EOF
CELERIANT_DATA_ROOT=/var/lib/celeriant
CELERIANT_LISTEN_ADDRESS=0.0.0.0
CELERIANT_CLIENT_PORT=10000
CELERIANT_REPLICATION_PORT=10001
CELERIANT_LOG_LEVEL=info
CELERIANT_METRICS_ENABLED=true
CELERIANT_METRICS_PORT=9090
CELERIANT_ADVERTISED_CLIENT_ADDRESS=${NODE_IP}:10000
CELERIANT_ADVERTISED_REPLICATION_ADDRESS=${NODE_IP}:10001
CELERIANT_S3_ENABLED=${S3_ENABLED}
CELERIANT_S3_REGION=${REGION}
CELERIANT_S3_BUCKET=${BUCKET}
CELERIANT_MEMORY_CONSUMPTION_PERCENT=60
CELERIANT_SHARD_LOG_PREALLOCATE_BYTES=134217728
EOF

  if [[ "$TLS_MODE" == "strict" ]]; then
    cat <<EOF
CELERIANT_TLS_MODE=strict
CELERIANT_TLS_CA_CERT=/etc/celeriant/certs/client-ca.crt
CELERIANT_TLS_INTRACLUSTER_CA_CERT=/etc/celeriant/certs/intracluster-ca.crt
CELERIANT_TLS_NODE_CERT=/etc/celeriant/certs/node.crt
CELERIANT_TLS_NODE_KEY=/etc/celeriant/certs/node.key
CELERIANT_TLS_CLIENT_CERT=/etc/celeriant/certs/client-server.crt
CELERIANT_TLS_CLIENT_KEY=/etc/celeriant/certs/client-server.key
CELERIANT_TLS_CLIENT_AUTH=require
EOF
  else
    # build_tls_config() returns early on Disabled, so the cert paths are simply omitted.
    echo "CELERIANT_TLS_MODE=disabled"
  fi

  emit CELERIANT_NUM_SHARDS "$NUM_SHARDS"
  emit CELERIANT_FSYNC_DELAY_US "$FSYNC_DELAY_US"
  emit CELERIANT_REPLICATION_DELAY_US "$REPLICATION_DELAY_US"
  emit CELERIANT_RESERVE_COORDINATOR_SHARD "$RESERVE_COORDINATOR_SHARD"
  emit CELERIANT_MESH_CHANNEL_SIZE "$MESH_CHANNEL_SIZE"
  emit CELERIANT_STANDALONE "$STANDALONE"
}
