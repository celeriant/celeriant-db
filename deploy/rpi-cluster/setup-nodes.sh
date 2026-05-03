#!/usr/bin/env bash
# One-time OS prep for a Celeriant data node (RPi 5). The systemd unit and
# promtail config refresh live in update-service.sh — re-runnable on every
# `make deploy` so a config.env change takes effect without redoing this prep.
# Usage: setup-nodes.sh <hostname> <infra_hostname> <memory_pct> <log_prealloc> <reserve_shard>
set -euo pipefail

HOST="$1"
INFRA_HOST="$2"
MEMORY_CONSUMPTION_PERCENT="$3"
SHARD_LOG_PREALLOCATE_BYTES="$4"
RESERVE_COORDINATOR_SHARD="$5"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

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

# --- Promtail binary install + systemd unit (one-time; config gets refreshed by update-service.sh) ---
printf "\n=== Installing promtail binary on %s ===\n" "$HOST"

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

scp /tmp/promtail.service "$HOST":/tmp/promtail.service
ssh "$HOST" 'sudo mv /tmp/promtail.service /etc/systemd/system/ && sudo systemctl daemon-reload && sudo systemctl enable promtail'
rm -f /tmp/promtail.service

# --- Systemd service + promtail config (re-runnable; called again on every `make deploy`) ---
bash "$SCRIPT_DIR/update-service.sh" "$HOST" "$INFRA_HOST" "$MEMORY_CONSUMPTION_PERCENT" "$SHARD_LOG_PREALLOCATE_BYTES" "$RESERVE_COORDINATOR_SHARD"

printf "\n=== %s setup complete ===\n" "$HOST"
