#!/bin/bash
# Deploy PostgreSQL configuration and marten-bench binary to the EC2 cluster.
#
# Reads IPs from CDK stack outputs. Run after `npx cdk deploy`.
#
# Usage:
#   ./deploy.sh [--key-file ~/.ssh/your-key.pem]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CDK_DIR="$(dirname "$SCRIPT_DIR")"

# Parse args
KEY_FILE=""
while [[ $# -gt 0 ]]; do
  case $1 in
    --key-file) KEY_FILE="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
if [[ -n "$KEY_FILE" ]]; then
  SSH_OPTS="$SSH_OPTS -i $KEY_FILE"
fi

# Read CDK outputs
STACK_NAME="MartenBenchmarkStack"
echo "==> Reading stack outputs from $STACK_NAME"

get_output() {
  aws cloudformation describe-stacks --stack-name "$STACK_NAME" \
    --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text
}

PG_PUB=$(get_output PostgreSQLPublicIp)
PG_IP=$(get_output PostgreSQLPrivateIp)
PG_INSTANCE_ID=$(get_output PostgreSQLInstanceId)
REGION=$(get_output Region)
INSTANCE_TYPE=$(get_output InstanceType)
CLIENT_INSTANCE_TYPE=$(get_output ClientInstanceType)
CLIENT_COUNT=$(get_output ClientCount 2>/dev/null || echo "2")
STORAGE_TYPE=$(get_output StorageType)
ARCH=$(get_output Architecture)
PG_VERSION=$(get_output PgVersion)

CLIENT_PUBS=""
for i in $(seq 1 "$CLIENT_COUNT"); do
  PUB=$(get_output "Client${i}PublicIp")
  if [[ -n "$CLIENT_PUBS" ]]; then CLIENT_PUBS="$CLIENT_PUBS,"; fi
  CLIENT_PUBS="$CLIENT_PUBS$PUB"
done

echo "  PostgreSQL: $PG_PUB ($PG_IP)"
echo "  Clients:    $CLIENT_PUBS ($CLIENT_COUNT nodes)"
echo "  Region:     $REGION"
echo "  Instance:   $INSTANCE_TYPE ($STORAGE_TYPE storage)"
echo "  Client:     $CLIENT_INSTANCE_TYPE"
echo "  PostgreSQL: $PG_VERSION"

SSH="ssh $SSH_OPTS ec2-user"
SCP="scp $SSH_OPTS"

# Wait for user-data to finish (PostgreSQL installed + initdb complete)
echo ""
echo "==> Waiting for PostgreSQL installation to complete on $PG_PUB"
for i in $(seq 1 60); do
  if $SSH@${PG_PUB} "sudo test -d /var/lib/pgsql/data/base" 2>/dev/null; then
    echo "  PostgreSQL initialized (attempt $i)"
    break
  fi
  if [[ $i -eq 60 ]]; then
    echo "ERROR: PostgreSQL not initialized after 60 attempts"
    echo "Check user-data logs: ssh ec2-user@$PG_PUB 'sudo cat /var/log/cloud-init-output.log'"
    exit 1
  fi
  sleep 5
done

# Configure PostgreSQL for benchmark workload
echo ""
echo "==> Configuring PostgreSQL for benchmark workload"

# Generate postgresql.conf tuned for 32-core NVMe write-heavy benchmark
cat > /tmp/marten-postgresql.conf <<'EOF'
# --- Connections ---
listen_addresses = '*'
max_connections = 10000
superuser_reserved_connections = 3

# --- Memory (tuned for i4i.8xlarge — 256GB RAM) ---
shared_buffers = 64GB
effective_cache_size = 192GB
work_mem = 64MB
maintenance_work_mem = 4GB
wal_buffers = 256MB
huge_pages = try

# --- WAL ---
wal_level = replica
max_wal_size = 8GB
min_wal_size = 2GB
synchronous_commit = on
fsync = on
full_page_writes = on
wal_compression = lz4
wal_writer_delay = 200ms

# --- Checkpoints ---
checkpoint_timeout = 15min
checkpoint_completion_target = 0.9

# --- I/O (NVMe tuning) ---
effective_io_concurrency = 200
random_page_cost = 1.1

# --- Autovacuum (aggressive for write-heavy) ---
autovacuum_max_workers = 6
autovacuum_naptime = 10s
autovacuum_vacuum_cost_limit = 2000

# --- Logging ---
log_min_duration_statement = 5000
log_checkpoints = on
log_connections = off
log_disconnections = off
log_lock_waits = on

# --- Stats ---
track_activities = on
track_counts = on
track_io_timing = on
track_wal_io_timing = on
EOF

$SCP /tmp/marten-postgresql.conf ec2-user@${PG_PUB}:/tmp/postgresql.conf
$SSH@${PG_PUB} "sudo cp /var/lib/pgsql/data/postgresql.conf /var/lib/pgsql/data/postgresql.conf.bak && \
  sudo mv /tmp/postgresql.conf /var/lib/pgsql/data/postgresql.conf && \
  sudo chown postgres:postgres /var/lib/pgsql/data/postgresql.conf"

# Configure pg_hba.conf — allow connections from VPC without TLS
cat > /tmp/marten-pg_hba.conf <<'EOF'
# TYPE  DATABASE  USER   ADDRESS        METHOD
local   all       all                   trust
host    all       all    0.0.0.0/0      scram-sha-256
host    all       all    ::/0           scram-sha-256
EOF

$SCP /tmp/marten-pg_hba.conf ec2-user@${PG_PUB}:/tmp/pg_hba.conf
$SSH@${PG_PUB} "sudo mv /tmp/pg_hba.conf /var/lib/pgsql/data/pg_hba.conf && \
  sudo chown postgres:postgres /var/lib/pgsql/data/pg_hba.conf"

# Start PostgreSQL
echo ""
echo "==> Starting PostgreSQL"
$SSH@${PG_PUB} "sudo systemctl enable postgresql && sudo systemctl start postgresql"
sleep 2

# Verify it's running
$SSH@${PG_PUB} "sudo systemctl is-active postgresql" || {
  echo "ERROR: PostgreSQL failed to start"
  $SSH@${PG_PUB} "sudo journalctl -u postgresql --no-pager -n 30"
  exit 1
}

# Create benchmark database and user
echo "==> Creating benchmark database and user"
$SSH@${PG_PUB} "sudo -u postgres psql <<'SQL'
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'bench') THEN
    CREATE ROLE bench WITH LOGIN PASSWORD 'bench' CREATEDB;
  END IF;
END
\$\$;
SELECT 'role: bench OK';

SELECT pg_database.datname FROM pg_database WHERE datname = 'marten_bench'
UNION ALL SELECT 'creating...' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'marten_bench');
SQL
"
$SSH@${PG_PUB} "sudo -u postgres createdb -O bench marten_bench 2>/dev/null || echo '  Database marten_bench already exists'"

echo "  PostgreSQL is ready: host=$PG_IP port=5432 db=marten_bench user=bench"

# Deploy marten-bench to clients (full publish directory — Marten needs runtime assemblies for codegen)
BENCH_DIR="$CDK_DIR/bin/publish"
if [[ -d "$BENCH_DIR" && -f "$BENCH_DIR/marten-bench" ]]; then
  echo ""
  echo "==> Deploying marten-bench to clients"
  for HOST in ${CLIENT_PUBS//,/ }; do
    $SSH@${HOST} 'sudo rm -rf /opt/marten-bench && mkdir -p /tmp/marten-bench-publish'
    $SCP -r "$BENCH_DIR/"* ec2-user@${HOST}:/tmp/marten-bench-publish/
    $SSH@${HOST} 'sudo mv /tmp/marten-bench-publish /opt/marten-bench && sudo chmod +x /opt/marten-bench/marten-bench'
    echo "  Deployed to $HOST"
  done
else
  echo ""
  echo "WARNING: marten-bench not found at $BENCH_DIR"
  echo "Run: make build"
  echo "(Skipping benchmark binary deploy)"
fi

# Write .cluster-env for Makefile consumption
ENV_FILE="$CDK_DIR/.cluster-env"
cat > "$ENV_FILE" <<EOF
PG_PUB=$PG_PUB
PG_IP=$PG_IP
PG_INSTANCE_ID=$PG_INSTANCE_ID
CLIENT_PUBS=$CLIENT_PUBS
CLIENT_COUNT=$CLIENT_COUNT
REGION=$REGION
INSTANCE_TYPE=$INSTANCE_TYPE
CLIENT_INSTANCE_TYPE=$CLIENT_INSTANCE_TYPE
STORAGE_TYPE=$STORAGE_TYPE
ARCH=$ARCH
PG_VERSION=$PG_VERSION
KEY_FILE=$KEY_FILE
EOF
echo ""
echo "==> Wrote $ENV_FILE (used by Makefile)"

echo ""
echo "==> Done! Use the Makefile to manage the cluster:"
echo "  make status         # Check PostgreSQL status"
echo "  make run-benchmark  # Run marten-bench"
echo "  make monitor-pg     # Monitor PostgreSQL during benchmark"
echo "  make stop           # Stop PostgreSQL"
