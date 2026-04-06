#!/usr/bin/env bash
# Deploy Docker and the infra stack (MinIO, Prometheus, Loki, Grafana) to the infra node.
set -euo pipefail

source config.env

PROJECT_ROOT="$(cd ../.. && pwd)"

GREEN='\033[0;32m'
BOLD='\033[1m'
RESET='\033[0m'

printf "${BOLD}>>> Setting up infra node: %s${RESET}\n" "$INFRA_HOST"

# Install Docker if not present
ssh "$INFRA_HOST" bash -s <<'DOCKER_INSTALL'
if ! command -v docker &>/dev/null; then
    echo ">>> Installing Docker..."
    curl -fsSL https://get.docker.com | sh
    sudo usermod -aG docker $USER
    echo "Docker installed. You may need to log out and back in for group changes."
else
    echo ">>> Docker already installed."
fi
DOCKER_INSTALL

# Create directory structure on infra node
ssh "$INFRA_HOST" 'mkdir -p ~/celeriant-infra/grafana-provisioning/datasources ~/celeriant-infra/grafana-provisioning/dashboards ~/celeriant-infra/dashboards'

# Deploy compose file and prometheus config (templated from config.env)
scp docker-compose.yml "$INFRA_HOST":~/celeriant-infra/
sed -e "s/LEADER_HOST_PLACEHOLDER/$LEADER_HOST/g" \
    -e "s/FOLLOWER_HOST_PLACEHOLDER/$FOLLOWER_HOST/g" \
    -e "s/METRICS_PORT_PLACEHOLDER/$METRICS_PORT/g" \
    prometheus.yml | ssh "$INFRA_HOST" 'cat > ~/celeriant-infra/prometheus.yml'

# Deploy Grafana provisioning from local-cluster (reuse existing configs)
GRAFANA_SRC="${PROJECT_ROOT}/deploy/local-cluster/grafana"
if [ -d "$GRAFANA_SRC" ]; then
    printf ">>> Deploying Grafana provisioning from local-cluster...\n"

    # Datasources — rewrite URLs from Docker-internal DNS to infra node hostname
    ssh "$INFRA_HOST" bash -s <<EOF
cat > ~/celeriant-infra/grafana-provisioning/datasources/datasources.yml <<'DSEOF'
apiVersion: 1
datasources:
  - name: Prometheus
    uid: celeriant-prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
  - name: Loki
    uid: celeriant-loki
    type: loki
    access: proxy
    url: http://loki:3100
DSEOF
EOF

    # Dashboard provisioning
    scp "${GRAFANA_SRC}/provisioning/dashboards/dashboards.yml" "$INFRA_HOST":~/celeriant-infra/grafana-provisioning/dashboards/

    # Dashboards
    if [ -d "${GRAFANA_SRC}/dashboards" ]; then
        scp "${GRAFANA_SRC}"/dashboards/*.json "$INFRA_HOST":~/celeriant-infra/dashboards/ 2>/dev/null || true
    fi
else
    printf ">>> No local-cluster grafana config found, skipping dashboard provisioning.\n"
fi

# Start the stack
printf ">>> Starting infra stack...\n"
ssh "$INFRA_HOST" 'cd ~/celeriant-infra && docker compose up -d'

printf "\n${GREEN}Infra stack deployed on %s.${RESET}\n" "$INFRA_HOST"
printf "  Grafana:        http://%s:3000 (admin/admin)\n" "$INFRA_HOST"
printf "  Prometheus:     http://%s:9090\n" "$INFRA_HOST"
printf "  Loki:           http://%s:3100\n" "$INFRA_HOST"
printf "  MinIO Console:  http://%s:9001 (minioadmin/minioadmin)\n" "$INFRA_HOST"
printf "  MinIO API:      http://%s:9000\n" "$INFRA_HOST"
