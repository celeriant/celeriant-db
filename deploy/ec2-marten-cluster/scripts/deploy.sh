#!/bin/bash
# Deploy PostgreSQL primary + synchronous standby with mTLS, then push
# marten-bench to client nodes.
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
STANDBY_PUB=$(get_output StandbyPublicIp)
STANDBY_IP=$(get_output StandbyPrivateIp)
STANDBY_INSTANCE_ID=$(get_output StandbyInstanceId)
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

echo "  Primary:  $PG_PUB ($PG_IP)"
echo "  Standby:  $STANDBY_PUB ($STANDBY_IP)"
echo "  Clients:  $CLIENT_PUBS ($CLIENT_COUNT nodes)"
echo "  Region:   $REGION"
echo "  Instance: $INSTANCE_TYPE ($STORAGE_TYPE storage)"
echo "  Client:   $CLIENT_INSTANCE_TYPE"
echo "  PG:       $PG_VERSION"

SSH="ssh $SSH_OPTS ec2-user"
SCP="scp $SSH_OPTS"

# =========================================================================
# Wait for user-data to complete on primary + standby
# =========================================================================

wait_for_pg_install() {
  local host="$1" label="$2"
  echo ""
  echo "==> Waiting for PostgreSQL install on $label ($host)"
  for i in $(seq 1 60); do
    if $SSH@${host} "rpm -q postgresql${PG_VERSION}-server &>/dev/null" 2>/dev/null; then
      echo "  PostgreSQL installed on $label (attempt $i)"
      return 0
    fi
    if [[ $i -eq 60 ]]; then
      echo "ERROR: PostgreSQL not installed on $label after 60 attempts"
      exit 1
    fi
    sleep 5
  done
}

wait_for_pg_install "$PG_PUB" "primary"

# Primary also needs initdb to have completed
echo "==> Waiting for initdb on primary"
for i in $(seq 1 60); do
  if $SSH@${PG_PUB} "sudo test -d /var/lib/pgsql/data/base" 2>/dev/null; then
    echo "  initdb complete (attempt $i)"
    break
  fi
  if [[ $i -eq 60 ]]; then
    echo "ERROR: initdb not complete after 60 attempts"
    exit 1
  fi
  sleep 5
done

wait_for_pg_install "$STANDBY_PUB" "standby"

# =========================================================================
# Generate TLS certificates (self-signed CA + server + client)
# =========================================================================

echo ""
echo "==> Generating TLS certificates"

CERT_DIR="/tmp/pg-bench-certs"
rm -rf "$CERT_DIR"
mkdir -p "$CERT_DIR"

# CA
openssl genrsa -out "$CERT_DIR/ca.key" 4096 2>/dev/null
openssl req -new -x509 -key "$CERT_DIR/ca.key" -out "$CERT_DIR/ca.crt" \
  -days 30 -subj "/CN=pg-bench-ca" 2>/dev/null

# Server cert (SAN covers both primary and standby private IPs)
openssl genrsa -out "$CERT_DIR/server.key" 2048 2>/dev/null
cat > "$CERT_DIR/server-ext.cnf" <<EOF
[req]
distinguished_name = req_dn
req_extensions = v3_req
prompt = no

[req_dn]
CN = pg-server

[v3_req]
subjectAltName = IP:${PG_IP},IP:${STANDBY_IP}
EOF
openssl req -new -key "$CERT_DIR/server.key" -out "$CERT_DIR/server.csr" \
  -config "$CERT_DIR/server-ext.cnf" 2>/dev/null
openssl x509 -req -in "$CERT_DIR/server.csr" -CA "$CERT_DIR/ca.crt" -CAkey "$CERT_DIR/ca.key" \
  -CAcreateserial -out "$CERT_DIR/server.crt" -days 30 \
  -extfile "$CERT_DIR/server-ext.cnf" -extensions v3_req 2>/dev/null

# Client cert for benchmark user (CN must match PG role name)
openssl genrsa -out "$CERT_DIR/client.key" 2048 2>/dev/null
openssl req -new -key "$CERT_DIR/client.key" -out "$CERT_DIR/client.csr" \
  -subj "/CN=bench" 2>/dev/null
openssl x509 -req -in "$CERT_DIR/client.csr" -CA "$CERT_DIR/ca.crt" -CAkey "$CERT_DIR/ca.key" \
  -CAcreateserial -out "$CERT_DIR/client.crt" -days 30 2>/dev/null

echo "  CA:     $CERT_DIR/ca.crt"
echo "  Server: $CERT_DIR/server.crt (SAN: $PG_IP, $STANDBY_IP)"
echo "  Client: $CERT_DIR/client.crt (CN=bench)"

# =========================================================================
# Deploy certs to primary
# =========================================================================

deploy_server_certs() {
  local host="$1" label="$2"
  echo "==> Deploying server certs to $label ($host)"
  $SSH@${host} "sudo mkdir -p /var/lib/pgsql/certs"
  $SCP "$CERT_DIR/ca.crt" "$CERT_DIR/server.crt" "$CERT_DIR/server.key" \
    ec2-user@${host}:/tmp/
  $SSH@${host} "sudo mv /tmp/ca.crt /tmp/server.crt /tmp/server.key /var/lib/pgsql/certs/ && \
    sudo chown -R postgres:postgres /var/lib/pgsql/certs && \
    sudo chmod 600 /var/lib/pgsql/certs/server.key"
}

deploy_server_certs "$PG_PUB" "primary"
deploy_server_certs "$STANDBY_PUB" "standby"

# =========================================================================
# Configure primary
# =========================================================================

echo ""
echo "==> Configuring primary for mTLS + synchronous replication"

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

# --- TLS ---
ssl = on
ssl_cert_file = '/var/lib/pgsql/certs/server.crt'
ssl_key_file = '/var/lib/pgsql/certs/server.key'
ssl_ca_file = '/var/lib/pgsql/certs/ca.crt'

# --- WAL + Replication ---
wal_level = replica
max_wal_size = 8GB
min_wal_size = 2GB
synchronous_commit = on
fsync = on
full_page_writes = on
wal_compression = lz4
wal_writer_delay = 200ms
max_wal_senders = 10
wal_keep_size = 2GB
# synchronous_standby_names is set via ALTER SYSTEM after standby is streaming
# (avoids chicken-and-egg: writes hang if set before standby exists)

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
  sudo chown postgres:postgres /var/lib/pgsql/data/postgresql.conf /var/lib/pgsql/data/postgresql.conf.bak"

# pg_hba.conf — mTLS for clients, password-over-SSL for replication
cat > /tmp/marten-pg_hba.conf <<'EOF'
# TYPE  DATABASE     USER        ADDRESS     METHOD
local   all          all                     trust
hostssl replication  replicator  0.0.0.0/0   scram-sha-256
hostssl all          bench       0.0.0.0/0   cert clientcert=verify-full
EOF

$SCP /tmp/marten-pg_hba.conf ec2-user@${PG_PUB}:/tmp/pg_hba.conf
$SSH@${PG_PUB} "sudo mv /tmp/pg_hba.conf /var/lib/pgsql/data/pg_hba.conf && \
  sudo chown postgres:postgres /var/lib/pgsql/data/pg_hba.conf"

# Start primary
echo ""
echo "==> Starting primary"
$SSH@${PG_PUB} "sudo systemctl enable postgresql && sudo systemctl start postgresql"
sleep 2

$SSH@${PG_PUB} "sudo systemctl is-active postgresql" || {
  echo "ERROR: Primary failed to start"
  $SSH@${PG_PUB} "sudo journalctl -u postgresql --no-pager -n 30"
  exit 1
}

# Create replication role + benchmark user/database
echo "==> Creating roles and benchmark database"
$SSH@${PG_PUB} "sudo -u postgres psql -c \"CREATE ROLE replicator WITH LOGIN REPLICATION PASSWORD 'replicator';\" 2>/dev/null || true"
$SSH@${PG_PUB} "sudo -u postgres psql -c \"CREATE ROLE bench WITH LOGIN PASSWORD 'bench' CREATEDB;\" 2>/dev/null || true"
$SSH@${PG_PUB} "sudo -u postgres createdb -O bench marten_bench 2>/dev/null || echo '  Database marten_bench already exists'"

echo "  Primary ready: host=$PG_IP port=5432 db=marten_bench (mTLS)"

# =========================================================================
# Bootstrap standby with pg_basebackup
# =========================================================================

echo ""
echo "==> Bootstrapping standby via pg_basebackup from primary"

# pg_basebackup uses replicator role over SSL (password auth, not cert)
$SSH@${STANDBY_PUB} "sudo -u postgres PGPASSWORD=replicator pg_basebackup \
  -h ${PG_IP} -p 5432 -U replicator \
  -D /var/lib/pgsql/data -R -Xs -P \
  --checkpoint=fast \
  -d 'sslmode=verify-ca sslrootcert=/var/lib/pgsql/certs/ca.crt'"

# Set application_name so primary recognizes this standby for synchronous_standby_names
$SSH@${STANDBY_PUB} "sudo -u postgres bash -c \"
  # Append application_name to primary_conninfo written by pg_basebackup -R
  sed -i \\\"s|^primary_conninfo = '\\(.*\\)'|primary_conninfo = '\\1 application_name=standby1'|\\\" /var/lib/pgsql/data/postgresql.auto.conf
\""

# Copy the same postgresql.conf to standby (SSL paths are identical)
$SCP /tmp/marten-postgresql.conf ec2-user@${STANDBY_PUB}:/tmp/postgresql.conf
$SSH@${STANDBY_PUB} "sudo mv /tmp/postgresql.conf /var/lib/pgsql/data/postgresql.conf && \
  sudo chown postgres:postgres /var/lib/pgsql/data/postgresql.conf"

# Start standby
echo ""
echo "==> Starting standby"
$SSH@${STANDBY_PUB} "sudo systemctl enable postgresql && sudo systemctl start postgresql"
sleep 2

$SSH@${STANDBY_PUB} "sudo systemctl is-active postgresql" || {
  echo "ERROR: Standby failed to start"
  $SSH@${STANDBY_PUB} "sudo journalctl -u postgresql --no-pager -n 30"
  exit 1
}

# Wait for standby to start streaming before enabling synchronous mode
echo "==> Waiting for standby to start streaming"
for i in $(seq 1 30); do
  STATE=$($SSH@${PG_PUB} "sudo -u postgres psql -t -A -c \
    \"SELECT state FROM pg_stat_replication WHERE application_name = 'standby1';\"" 2>/dev/null || echo "")
  if [[ "$STATE" == "streaming" ]]; then
    echo "  Standby is streaming (attempt $i)"
    break
  fi
  if [[ $i -eq 30 ]]; then
    echo "ERROR: Standby not streaming after 30 attempts (got: '$STATE')"
    echo "  Check: ssh ec2-user@$PG_PUB 'sudo -u postgres psql -c \"SELECT * FROM pg_stat_replication;\"'"
    exit 1
  fi
  echo "  Waiting... (state: '${STATE:-not connected}', attempt $i)"
  sleep 3
done

# Now enable synchronous replication (safe — standby is already streaming)
echo "==> Enabling synchronous replication"
$SSH@${PG_PUB} "sudo -u postgres psql -c \"ALTER SYSTEM SET synchronous_standby_names = 'FIRST 1 (standby1)';\""
$SSH@${PG_PUB} "sudo -u postgres psql -c 'SELECT pg_reload_conf();'"

# Verify sync state
sleep 1
SYNC_STATE=$($SSH@${PG_PUB} "sudo -u postgres psql -t -A -c \
  \"SELECT sync_state FROM pg_stat_replication WHERE application_name = 'standby1';\"" 2>/dev/null || echo "")
if [[ "$SYNC_STATE" != "sync" ]]; then
  echo "ERROR: Expected sync state after enabling synchronous_standby_names (got: '$SYNC_STATE')"
  exit 1
fi
echo "  Replication is synchronous"

# Verify replication is working
echo ""
echo "==> Replication status:"
$SSH@${PG_PUB} "sudo -u postgres psql -c \
  \"SELECT application_name, state, sync_state, sent_lsn, replay_lsn FROM pg_stat_replication;\""

# =========================================================================
# Deploy marten-bench + client certs to client nodes
# =========================================================================

BENCH_DIR="$CDK_DIR/bin/publish"

echo ""
echo "==> Deploying client certs and marten-bench to clients"
for HOST in ${CLIENT_PUBS//,/ }; do
  # Deploy client certs
  $SSH@${HOST} "sudo mkdir -p /opt/pg-certs"
  $SCP "$CERT_DIR/ca.crt" "$CERT_DIR/client.crt" "$CERT_DIR/client.key" \
    ec2-user@${HOST}:/tmp/
  $SSH@${HOST} "sudo mv /tmp/ca.crt /tmp/client.crt /tmp/client.key /opt/pg-certs/ && \
    sudo chmod 644 /opt/pg-certs/ca.crt /opt/pg-certs/client.crt && \
    sudo chmod 600 /opt/pg-certs/client.key"
  echo "  Certs deployed to $HOST"

  # Deploy benchmark binary
  if [[ -d "$BENCH_DIR" && -f "$BENCH_DIR/marten-bench" ]]; then
    $SSH@${HOST} 'sudo rm -rf /opt/marten-bench && mkdir -p /tmp/marten-bench-publish'
    $SCP -r "$BENCH_DIR/"* ec2-user@${HOST}:/tmp/marten-bench-publish/
    $SSH@${HOST} 'sudo mv /tmp/marten-bench-publish /opt/marten-bench && sudo chmod +x /opt/marten-bench/marten-bench'
    echo "  Benchmark deployed to $HOST"
  fi
done

if [[ ! -d "$BENCH_DIR" || ! -f "$BENCH_DIR/marten-bench" ]]; then
  echo ""
  echo "WARNING: marten-bench not found at $BENCH_DIR"
  echo "Run: make build"
fi

# =========================================================================
# Write .cluster-env
# =========================================================================

ENV_FILE="$CDK_DIR/.cluster-env"
cat > "$ENV_FILE" <<EOF
PG_PUB=$PG_PUB
PG_IP=$PG_IP
PG_INSTANCE_ID=$PG_INSTANCE_ID
STANDBY_PUB=$STANDBY_PUB
STANDBY_IP=$STANDBY_IP
STANDBY_INSTANCE_ID=$STANDBY_INSTANCE_ID
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
echo "==> Wrote $ENV_FILE"

echo ""
echo "==> Done! Cluster: primary ($PG_IP) + sync standby ($STANDBY_IP), mTLS enabled"
echo "  make status         # Check replication status"
echo "  make run-benchmark  # Run marten-bench"
echo "  make run-sweep      # Concurrency sweep"
