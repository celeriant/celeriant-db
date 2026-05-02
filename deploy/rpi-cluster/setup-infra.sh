#!/usr/bin/env bash
# Deploy the infra stack (MinIO, Prometheus, Loki, Grafana).
# INFRA_MODE=remote  — SSH into INFRA_HOST (rpi4), run docker there.
# INFRA_MODE=local   — run docker compose on this machine.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/config.env"

PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

GREEN='\033[0;32m'
BOLD='\033[1m'
RESET='\033[0m'

case "${INFRA_MODE:-}" in
    remote|local) ;;
    *) printf "ERROR: INFRA_MODE must be 'remote' or 'local' (got '%s')\n" "${INFRA_MODE:-}" >&2; exit 1 ;;
esac

print_remote_urls() {
    printf "\n${GREEN}Infra stack deployed on %s.${RESET}\n" "$INFRA_HOST"
    printf "  Grafana:        http://%s:3000 (admin/admin)\n" "$INFRA_HOST"
    printf "  Prometheus:     http://%s:9090\n" "$INFRA_HOST"
    printf "  Loki:           http://%s:3100\n" "$INFRA_HOST"
    printf "  MinIO Console:  http://%s:9001 (minioadmin/minioadmin)\n" "$INFRA_HOST"
    printf "  MinIO API:      http://%s:9000\n" "$INFRA_HOST"
}

print_local_urls() {
    printf "\n${GREEN}Infra stack running locally.${RESET}\n"
    printf "  Grafana:        http://localhost:3000 (admin/admin)\n"
    printf "  Prometheus:     http://localhost:9090\n"
    printf "  Loki:           http://localhost:3100\n"
    printf "  MinIO Console:  http://localhost:9001 (minioadmin/minioadmin)\n"
    printf "  MinIO API:      http://localhost:9000\n"
    printf "  Data nodes reach MinIO at: http://%s:9000\n" "$INFRA_HOST"
}

if [ "$INFRA_MODE" = "remote" ]; then

    printf "${BOLD}>>> Setting up infra node: %s${RESET}\n" "$INFRA_HOST"

    # Best-effort: wipe any leftover local minio-data volume from a prior local run.
    printf ">>> Wiping any local minio-data volume (best-effort)...\n"
    (cd "$SCRIPT_DIR" && docker compose down -v 2>/dev/null) \
        && printf "    (local stack torn down)\n" \
        || printf "    (skipped — no local docker stack)\n"

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
    sed -e "s|LEADER_HOST_PLACEHOLDER|$LEADER_HOST|g" \
        -e "s|FOLLOWER_HOST_PLACEHOLDER|$FOLLOWER_HOST|g" \
        -e "s|METRICS_PORT_PLACEHOLDER|$METRICS_PORT|g" \
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

    print_remote_urls

else  # local

    printf "${BOLD}>>> Setting up infra stack locally (INFRA_HOST=%s — must be this machine's LAN IP)${RESET}\n" "$INFRA_HOST"

    if ! command -v docker &>/dev/null; then
        printf "ERROR: docker not found on PATH.\n" >&2
        printf "  On WSL2: install Docker Desktop for Windows with WSL2 backend enabled.\n" >&2
        printf "  On Linux/macOS: install Docker Engine or Docker Desktop.\n" >&2
        exit 1
    fi

    # If switching from remote mode, the rpi4's MinIO state cannot be wiped from here
    # because INFRA_HOST now points at this machine, not the rpi4.
    printf "WARNING: If switching from remote mode, run 'make teardown-data' against the rpi4\n"
    printf "         first to wipe its MinIO state. This script cannot reach the rpi4 because\n"
    printf "         INFRA_HOST is now the build machine.\n"

    # Wipe local minio-data volume for a clean slate.
    printf ">>> Wiping local minio-data volume...\n"
    (cd "$SCRIPT_DIR" && docker compose down -v 2>/dev/null) || true

    # Template prometheus config. The compose file references ./prometheus.yml (a
    # placeholder template); we generate the real config to prometheus.generated.yml
    # and use docker-compose.local-override.yml to remap the bind mount.
    printf ">>> Generating prometheus.generated.yml...\n"
    sed -e "s|LEADER_HOST_PLACEHOLDER|$LEADER_HOST|g" \
        -e "s|FOLLOWER_HOST_PLACEHOLDER|$FOLLOWER_HOST|g" \
        -e "s|METRICS_PORT_PLACEHOLDER|$METRICS_PORT|g" \
        "$SCRIPT_DIR/prometheus.yml" > "$SCRIPT_DIR/prometheus.generated.yml"

    # Stage Grafana provisioning locally. The compose file's relative paths
    # (./grafana-provisioning/, ./dashboards/) resolve to this directory.
    GRAFANA_SRC="${PROJECT_ROOT}/deploy/local-cluster/grafana"
    if [ -d "$GRAFANA_SRC" ]; then
        printf ">>> Staging Grafana provisioning locally...\n"

        mkdir -p "$SCRIPT_DIR/grafana-provisioning/datasources" \
                 "$SCRIPT_DIR/grafana-provisioning/dashboards" \
                 "$SCRIPT_DIR/dashboards"

        cat > "$SCRIPT_DIR/grafana-provisioning/datasources/datasources.yml" <<'DSEOF'
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

        cp "${GRAFANA_SRC}/provisioning/dashboards/dashboards.yml" \
           "$SCRIPT_DIR/grafana-provisioning/dashboards/"

        if [ -d "${GRAFANA_SRC}/dashboards" ]; then
            cp "${GRAFANA_SRC}"/dashboards/*.json "$SCRIPT_DIR/dashboards/" 2>/dev/null || true
        fi
    else
        printf ">>> No local-cluster grafana config found, skipping dashboard provisioning.\n"
    fi

    # Start the stack. The override file remaps the prometheus bind mount to the
    # generated config so we don't overwrite the placeholder prometheus.yml.
    printf ">>> Starting infra stack...\n"
    (cd "$SCRIPT_DIR" && docker compose -f docker-compose.yml -f docker-compose.local-override.yml up -d)

    print_local_urls

fi
