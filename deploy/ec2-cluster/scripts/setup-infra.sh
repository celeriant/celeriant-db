#!/bin/bash
# Stand up the self-hosted observability stack (Prometheus + Loki + Grafana) on
# client #1, and point the data nodes' log shipping at it. Mirrors the rpi
# cluster's infra (deploy/rpi-cluster) — minus MinIO, since EC2 uses real S3.
#
# Prometheus scrapes the data nodes' :9090; Promtail on each data node ships
# journald to Loki on client #1. Reach Grafana on :3000 from the IP that
# `make infra` opened (auto-detected, or HOME_IP=... override).
#
# Reads .cluster-env (written by deploy.sh). Run after `make deploy`.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CDK_DIR="$(dirname "$SCRIPT_DIR")"
REPO_ROOT="$(cd "$CDK_DIR/../.." && pwd)"
CLUSTER_ENV="$CDK_DIR/.cluster-env"
INFRA_SRC="$CDK_DIR/infra"

if [[ ! -f "$CLUSTER_ENV" ]]; then
  echo "ERROR: $CLUSTER_ENV not found — run 'make deploy' or 'make sync-env' first"
  exit 1
fi
source "$CLUSTER_ENV"

if [[ -z "${CLIENT_PRIVS:-}" ]]; then
  echo "ERROR: CLIENT_PRIVS missing from $CLUSTER_ENV — re-run 'make deploy' or 'make sync-env'"
  exit 1
fi

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
if [[ -n "${KEY_FILE:-}" ]]; then
  SSH_OPTS="$SSH_OPTS -i $KEY_FILE"
fi
SSH="ssh $SSH_OPTS ec2-user"
SCP="scp $SSH_OPTS"

# Client #1 hosts the stack: public IP for our SSH/deploy + your browser,
# private IP for the data nodes to push logs to over the VPC.
INFRA_PUB="${CLIENT_PUBS%%,*}"
INFRA_PRIV="${CLIENT_PRIVS%%,*}"

# promtail release asset name from the cluster architecture.
# Pinned: Loki dropped the promtail binary after v3.6.0 (deprecated for Alloy),
# so `latest` no longer carries promtail-linux-*.zip.
PROMTAIL_ARCH="amd64"
[[ "${ARCH:-x86_64}" == "arm64" ]] && PROMTAIL_ARCH="arm64"
PROMTAIL_VERSION="v3.6.0"

echo "==> Observability stack on client #1 ($INFRA_PUB / $INFRA_PRIV)"
echo "  Scrape targets: $LEADER_IP:9090, $FOLLOWER_IP:9090"
echo "  Loki ingest:    $INFRA_PRIV:3100"

# --- Stack on client #1 ---
$SSH@"$INFRA_PUB" bash -s <<'INSTALL'
set -euo pipefail
if ! command -v docker >/dev/null 2>&1; then
  echo ">>> Installing Docker"
  sudo dnf install -y docker >/dev/null
  sudo systemctl enable --now docker
fi
if ! sudo docker compose version >/dev/null 2>&1; then
  echo ">>> Installing Docker Compose plugin"
  sudo mkdir -p /usr/local/lib/docker/cli-plugins
  sudo curl -fsSL "https://github.com/docker/compose/releases/latest/download/docker-compose-linux-$(uname -m)" \
    -o /usr/local/lib/docker/cli-plugins/docker-compose
  sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
fi
mkdir -p ~/celeriant-infra/grafana-provisioning/datasources \
         ~/celeriant-infra/grafana-provisioning/dashboards \
         ~/celeriant-infra/dashboards
INSTALL

echo "==> Shipping compose, prometheus config, and provisioning to client #1"
$SCP "$INFRA_SRC/docker-compose.yml" ec2-user@"$INFRA_PUB":~/celeriant-infra/
$SCP "$INFRA_SRC/grafana-provisioning/datasources/datasources.yml" \
     ec2-user@"$INFRA_PUB":~/celeriant-infra/grafana-provisioning/datasources/
$SCP "$INFRA_SRC/grafana-provisioning/dashboards/dashboards.yml" \
     ec2-user@"$INFRA_PUB":~/celeriant-infra/grafana-provisioning/dashboards/

sed -e "s|LEADER_HOST_PLACEHOLDER|$LEADER_IP|g" \
    -e "s|FOLLOWER_HOST_PLACEHOLDER|$FOLLOWER_IP|g" \
    -e "s|METRICS_PORT_PLACEHOLDER|9090|g" \
    "$INFRA_SRC/prometheus.yml" | $SSH@"$INFRA_PUB" 'cat > ~/celeriant-infra/prometheus.yml'

# Reuse the same dashboard JSON as the rpi/local clusters.
DASH_SRC="$REPO_ROOT/deploy/local-cluster/grafana/dashboards"
if compgen -G "$DASH_SRC/*.json" >/dev/null; then
  $SCP "$DASH_SRC"/*.json ec2-user@"$INFRA_PUB":~/celeriant-infra/dashboards/
else
  echo "  WARNING: no dashboards found at $DASH_SRC"
fi

echo "==> Starting stack"
$SSH@"$INFRA_PUB" 'cd ~/celeriant-infra && sudo docker compose up -d'

# --- Promtail on data nodes (journald -> Loki) ---
for HOST in $LEADER_PUB $FOLLOWER_PUB; do
  echo "==> Installing promtail on $HOST"
  $SSH@"$HOST" "PROMTAIL_ARCH=$PROMTAIL_ARCH PROMTAIL_VERSION=$PROMTAIL_VERSION bash -s" <<'PROMTAIL'
set -euo pipefail
command -v unzip >/dev/null 2>&1 || sudo dnf install -y unzip >/dev/null
if [[ ! -x /usr/local/bin/promtail ]]; then
  cd /tmp
  curl -fsSLO "https://github.com/grafana/loki/releases/download/${PROMTAIL_VERSION}/promtail-linux-${PROMTAIL_ARCH}.zip"
  unzip -o "promtail-linux-${PROMTAIL_ARCH}.zip" >/dev/null
  sudo mv "promtail-linux-${PROMTAIL_ARCH}" /usr/local/bin/promtail
  sudo chmod +x /usr/local/bin/promtail
  rm -f "promtail-linux-${PROMTAIL_ARCH}.zip"
fi
sudo mkdir -p /etc/promtail
sudo tee /etc/systemd/system/promtail.service >/dev/null <<'UNIT'
[Unit]
Description=Promtail
After=network-online.target

[Service]
Type=simple
ExecStart=/usr/local/bin/promtail -config.file=/etc/promtail/config.yml
Restart=on-failure

[Install]
WantedBy=multi-user.target
UNIT
PROMTAIL

  $SSH@"$HOST" "sudo tee /etc/promtail/config.yml >/dev/null" <<EOF
server:
  http_listen_port: 9080
  grpc_listen_port: 0

positions:
  filename: /tmp/positions.yaml

clients:
  - url: http://${INFRA_PRIV}:3100/loki/api/v1/push

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
  $SSH@"$HOST" 'sudo systemctl daemon-reload && sudo systemctl enable --now promtail && sudo systemctl restart promtail'
done

echo ""
echo "==> Observability stack up. Reachable from the IP 'make infra' opened:"
echo "  Grafana:    http://$INFRA_PUB:3000 (admin/admin)"
echo "  Prometheus: http://$INFRA_PUB:9090"
echo "  Loki:       http://$INFRA_PUB:3100"
echo ""
echo "  Logs:    {job=\"celeriant\"} in Loki"
echo "  Metrics: {cluster=\"ec2-ktls-test\"} in Prometheus"
