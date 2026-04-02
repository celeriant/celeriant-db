#!/usr/bin/env bash
# One-time OS prep for a Celeriant data node (RPi 5).
# Usage: setup-nodes.sh <hostname> <infra_hostname> <memory_pct> <log_prealloc> <reserve_shard>
set -euo pipefail

HOST="$1"
INFRA_HOST="$2"
MEMORY_CONSUMPTION_PERCENT="$3"
SHARD_LOG_PREALLOCATE_BYTES="$4"
RESERVE_COORDINATOR_SHARD="$5"

printf "\n=== OS prep on %s ===\n" "$HOST"

ssh "$HOST" bash -s <<'REMOTE_SETUP'
set -euo pipefail

echo ">>> Updating packages..."
sudo apt update -qq && sudo apt upgrade -y -qq

echo ">>> Setting file descriptor limits..."
cat <<'LIMITS' | sudo tee /etc/security/limits.d/celeriant.conf > /dev/null
*  soft  nofile  1048576
*  hard  nofile  1048576
LIMITS

echo ">>> Setting memlock limits for io_uring..."
cat <<'MEMLOCK' | sudo tee /etc/security/limits.d/memlock.conf > /dev/null
*  soft  memlock  unlimited
*  hard  memlock  unlimited
MEMLOCK

echo ">>> Setting sysctl params..."
cat <<'SYSCTL' | sudo tee /etc/sysctl.d/99-celeriant.conf > /dev/null
fs.file-max = 1048576
SYSCTL
sudo sysctl -p /etc/sysctl.d/99-celeriant.conf

echo ">>> Installing xfsprogs..."
sudo apt install -y -qq xfsprogs

echo ">>> Creating data directory..."
sudo mkdir -p /var/lib/celeriant

echo ">>> OS prep complete."
REMOTE_SETUP

# --- Systemd service ---
printf "\n=== Deploying systemd service to %s ===\n" "$HOST"

ADVERTISED_HOST="$HOST"

# Generate service file with host-specific advertised addresses
cat > /tmp/celeriant-"$HOST".service <<EOF
[Unit]
Description=Celeriant Database
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/local/bin/celeriant
Restart=on-failure
RestartSec=3
LimitNOFILE=1048576
LimitMEMLOCK=infinity

Environment=CELERIANT_DATA_ROOT=/var/lib/celeriant
Environment=CELERIANT_LISTEN_ADDRESS=0.0.0.0
Environment=CELERIANT_CLIENT_PORT=10000
Environment=CELERIANT_REPLICATION_PORT=10001
Environment=CELERIANT_LOG_LEVEL=info
Environment=CELERIANT_METRICS_ENABLED=true
Environment=CELERIANT_METRICS_PORT=9090

Environment=CELERIANT_ADVERTISED_CLIENT_ADDRESS=${ADVERTISED_HOST}:10000
Environment=CELERIANT_ADVERTISED_REPLICATION_ADDRESS=${ADVERTISED_HOST}:10001

Environment=CELERIANT_S3_ENABLED=true
Environment=CELERIANT_S3_REGION=us-east-1
Environment=CELERIANT_S3_BUCKET=celeriant-cluster
Environment=CELERIANT_S3_ACCESS_KEY_ID=minioadmin
Environment=CELERIANT_S3_SECRET_ACCESS_KEY=minioadmin
Environment=CELERIANT_S3_ENDPOINT_OVERRIDE=http://${INFRA_HOST}:9000
Environment=CELERIANT_S3_ALLOW_HTTP=true

Environment=CELERIANT_TLS_MODE=strict
Environment=CELERIANT_TLS_CA_CERT=/etc/celeriant/certs/client-ca.crt
Environment=CELERIANT_TLS_INTRACLUSTER_CA_CERT=/etc/celeriant/certs/intracluster-ca.crt
Environment=CELERIANT_TLS_NODE_CERT=/etc/celeriant/certs/node.crt
Environment=CELERIANT_TLS_NODE_KEY=/etc/celeriant/certs/node.key
Environment=CELERIANT_TLS_CLIENT_CERT=/etc/celeriant/certs/client-server.crt
Environment=CELERIANT_TLS_CLIENT_KEY=/etc/celeriant/certs/client-server.key
Environment=CELERIANT_TLS_CLIENT_AUTH=require

Environment=CELERIANT_MEMORY_CONSUMPTION_PERCENT=${MEMORY_CONSUMPTION_PERCENT}
Environment=CELERIANT_SHARD_LOG_PREALLOCATE_BYTES=${SHARD_LOG_PREALLOCATE_BYTES}
Environment=CELERIANT_RESERVE_COORDINATOR_SHARD=${RESERVE_COORDINATOR_SHARD}

[Install]
WantedBy=multi-user.target
EOF

scp /tmp/celeriant-"$HOST".service "$HOST":/tmp/celeriant.service
ssh "$HOST" 'sudo mv /tmp/celeriant.service /etc/systemd/system/ && sudo systemctl daemon-reload && sudo systemctl enable celeriant'
rm -f /tmp/celeriant-"$HOST".service

# --- Promtail ---
printf "\n=== Deploying promtail to %s ===\n" "$HOST"

cat > /tmp/promtail-"$HOST".yml <<EOF
server:
  http_listen_port: 9080
  grpc_listen_port: 0

positions:
  filename: /tmp/positions.yaml

clients:
  - url: http://${INFRA_HOST}:3100/loki/api/v1/push

scrape_configs:
  - job_name: journal
    journal:
      json: false
      max_age: 12h
      labels:
        job: celeriant
        node: ${HOST}
    relabel_configs:
      - source_labels: ['__journal__systemd_unit']
        target_label: unit
      - source_labels: ['__journal__systemd_unit']
        regex: celeriant.service
        action: keep
EOF

cat > /tmp/promtail.service <<'EOF'
[Unit]
Description=Promtail
After=network-online.target

[Service]
Type=simple
ExecStart=/usr/local/bin/promtail -config.file=/etc/promtail/config.yml
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF

ssh "$HOST" bash -s <<'PROMTAIL_INSTALL'
set -euo pipefail
if ! command -v /usr/local/bin/promtail &>/dev/null; then
    echo ">>> Installing promtail..."
    cd /tmp
    curl -sLO https://github.com/grafana/loki/releases/latest/download/promtail-linux-arm64.zip
    unzip -o promtail-linux-arm64.zip
    sudo mv promtail-linux-arm64 /usr/local/bin/promtail
    sudo chmod +x /usr/local/bin/promtail
    rm -f promtail-linux-arm64.zip
else
    echo ">>> Promtail already installed, skipping download."
fi
sudo mkdir -p /etc/promtail
PROMTAIL_INSTALL

scp /tmp/promtail-"$HOST".yml "$HOST":/tmp/promtail.yml
scp /tmp/promtail.service "$HOST":/tmp/promtail.service
ssh "$HOST" 'sudo mv /tmp/promtail.yml /etc/promtail/config.yml && sudo mv /tmp/promtail.service /etc/systemd/system/ && sudo systemctl daemon-reload && sudo systemctl enable --now promtail'
rm -f /tmp/promtail-"$HOST".yml /tmp/promtail.service

printf "\n=== %s setup complete ===\n" "$HOST"
