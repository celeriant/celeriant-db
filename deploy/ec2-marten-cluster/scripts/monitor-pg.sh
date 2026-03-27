#!/bin/bash
# Monitor PostgreSQL stats during a benchmark run.
#
# Captures every INTERVAL seconds:
#   - Active connections and state breakdown
#   - PostgreSQL RSS memory usage
#   - WAL write rate (bytes/s)
#   - Checkpoint activity
#   - Transaction commit/rollback rate
#
# Run in a separate terminal alongside `make run-benchmark`.
# Output goes to both stdout and a timestamped file in results/.
#
# Usage: INTERVAL=5 bash scripts/monitor-pg.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CDK_DIR="$(dirname "$SCRIPT_DIR")"
CLUSTER_ENV="$CDK_DIR/.cluster-env"

if [[ ! -f "$CLUSTER_ENV" ]]; then
  echo "ERROR: $CLUSTER_ENV not found"
  exit 1
fi

source "$CLUSTER_ENV"

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
if [[ -n "${KEY_FILE:-}" ]]; then
  SSH_OPTS="$SSH_OPTS -i $KEY_FILE"
fi

SSH="ssh $SSH_OPTS ec2-user"
INTERVAL="${INTERVAL:-5}"

TIMESTAMP=$(date +%Y%m%d-%H%M%S)
RESULT_DIR="$CDK_DIR/results"
MONITOR_FILE="$RESULT_DIR/${TIMESTAMP}_pg-monitor.csv"
mkdir -p "$RESULT_DIR"

echo "==> Monitoring PostgreSQL on $PG_PUB (every ${INTERVAL}s)"
echo "==> Output: $MONITOR_FILE"
echo "==> Press Ctrl+C to stop"
echo ""

# CSV header
HEADER="timestamp,connections_active,connections_idle,connections_idle_in_tx,connections_total,pg_rss_mb,xact_commit,xact_rollback,blks_hit,blks_read,tup_inserted,tup_updated,tup_deleted,checkpoints_timed,checkpoints_req,buffers_checkpoint,buffers_backend,wal_bytes"
echo "$HEADER" | tee "$MONITOR_FILE"

# Monitoring query — single-line JSON output for easy parsing
MONITOR_SQL=$(cat <<'SQL'
SELECT json_build_object(
  'active', (SELECT count(*) FROM pg_stat_activity WHERE state = 'active'),
  'idle', (SELECT count(*) FROM pg_stat_activity WHERE state = 'idle'),
  'idle_tx', (SELECT count(*) FROM pg_stat_activity WHERE state = 'idle in transaction'),
  'total', (SELECT count(*) FROM pg_stat_activity),
  'xact_commit', s.xact_commit,
  'xact_rollback', s.xact_rollback,
  'blks_hit', s.blks_hit,
  'blks_read', s.blks_read,
  'tup_inserted', s.tup_inserted,
  'tup_updated', s.tup_updated,
  'tup_deleted', s.tup_deleted,
  'checkpoints_timed', b.checkpoints_timed,
  'checkpoints_req', b.checkpoints_req,
  'buffers_checkpoint', b.buffers_checkpoint,
  'buffers_backend', b.buffers_backend,
  'wal_bytes', w.wal_bytes
)::text
FROM pg_stat_database s
CROSS JOIN pg_stat_bgwriter b
CROSS JOIN pg_stat_wal w
WHERE s.datname = 'marten_bench';
SQL
)

while true; do
  TS=$(date +%Y-%m-%dT%H:%M:%S)

  # Get PostgreSQL stats
  PG_JSON=$($SSH@${PG_PUB} "sudo -u postgres psql -t -A -c \"${MONITOR_SQL}\"" 2>/dev/null || echo "{}")

  # Get PostgreSQL RSS (sum of all postgres processes)
  PG_RSS=$($SSH@${PG_PUB} "ps aux | grep '[p]ostgres' | awk '{sum+=\$6} END {printf \"%.0f\", sum/1024}'" 2>/dev/null || echo "0")

  if [[ -n "$PG_JSON" && "$PG_JSON" != "{}" ]]; then
    # Parse JSON fields (using grep/sed since jq might not be installed locally)
    get_field() { echo "$PG_JSON" | grep -oP "\"$1\"\\s*:\\s*\\K[0-9]+" || echo "0"; }

    LINE="$TS,$(get_field active),$(get_field idle),$(get_field idle_tx),$(get_field total),$PG_RSS,$(get_field xact_commit),$(get_field xact_rollback),$(get_field blks_hit),$(get_field blks_read),$(get_field tup_inserted),$(get_field tup_updated),$(get_field tup_deleted),$(get_field checkpoints_timed),$(get_field checkpoints_req),$(get_field buffers_checkpoint),$(get_field buffers_backend),$(get_field wal_bytes)"
    echo "$LINE" | tee -a "$MONITOR_FILE"
  else
    echo "$TS,,,,,${PG_RSS},,,,,,,,,,," | tee -a "$MONITOR_FILE"
  fi

  sleep "$INTERVAL"
done
