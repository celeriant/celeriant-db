#!/usr/bin/env bash
# Deploy Docker and the infra stack (MinIO, Prometheus, Loki, Grafana) locally on the Ubuntu PC.
set -euo pipefail

source config.env

PROJECT_ROOT="$(cd ../.. && pwd)"
INFRA_DIR="$HOME/celeriant-infra"

GREEN='\033[0;32m'
BOLD='\033[1m'
RESET='\033[0m'

printf "${BOLD}>>> Setting up infra locally${RESET}\n"

# Install Docker if not present
if ! command -v docker &>/dev/null; then
    echo ">>> Installing Docker..."
    curl -fsSL https://get.docker.com | sh
    sudo usermod -aG docker "$USER"
    echo "Docker installed. You may need to log out and back in for group changes."
else
    echo ">>> Docker already installed."
fi

# Create directory structure
mkdir -p "$INFRA_DIR/grafana-provisioning/datasources" \
         "$INFRA_DIR/grafana-provisioning/dashboards" \
         "$INFRA_DIR/dashboards"

# Deploy compose file and prometheus config (templated from config.env)
cp docker-compose.yml "$INFRA_DIR/"
sed -e "s/LEADER_HOST_PLACEHOLDER/$LEADER_HOST/g" \
    -e "s/FOLLOWER_HOST_PLACEHOLDER/$FOLLOWER_HOST/g" \
    -e "s/METRICS_PORT_PLACEHOLDER/$METRICS_PORT/g" \
    prometheus.yml > "$INFRA_DIR/prometheus.yml"

# Deploy Grafana provisioning from local-cluster (reuse existing configs)
GRAFANA_SRC="${PROJECT_ROOT}/deploy/local-cluster/grafana"
if [ -d "$GRAFANA_SRC" ]; then
    printf ">>> Deploying Grafana provisioning from local-cluster...\n"

    # Datasources
    cat > "$INFRA_DIR/grafana-provisioning/datasources/datasources.yml" <<'DSEOF'
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
  - name: Loki
    type: loki
    access: proxy
    url: http://loki:3100
DSEOF

    # Dashboard provisioning
    cp "${GRAFANA_SRC}/provisioning/dashboards/dashboards.yml" \
       "$INFRA_DIR/grafana-provisioning/dashboards/"

    # Dashboards
    if [ -d "${GRAFANA_SRC}/dashboards" ]; then
        cp "${GRAFANA_SRC}"/dashboards/*.json "$INFRA_DIR/dashboards/" 2>/dev/null || true
    fi
else
    printf ">>> No local-cluster grafana config found, skipping dashboard provisioning.\n"
fi

# Start the stack
printf ">>> Starting infra stack...\n"
cd "$INFRA_DIR" && docker compose up -d

printf "\n${GREEN}Infra stack deployed locally.${RESET}\n"
printf "  Grafana:        http://localhost:3000 (admin/admin)\n"
printf "  Prometheus:     http://localhost:9090\n"
printf "  Loki:           http://localhost:3100\n"
printf "  MinIO Console:  http://localhost:9001 (minioadmin/minioadmin)\n"
printf "  MinIO API:      http://localhost:9000\n"
printf "\n  RPis reach these services via %s\n" "$INFRA_HOST"
