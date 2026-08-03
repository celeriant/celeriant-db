#!/usr/bin/env bash
# Stage the bind-mount sources the local compose override needs:
# prometheus.generated.yml, grafana-provisioning/, dashboards/.
# Idempotent. No-op unless INFRA_MODE=local. Safe to run before every `up`.
#
# These paths are gitignored. If docker starts the stack before they exist it
# creates root-owned directories in their place and prometheus fails to mount.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/config.env"

PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

[ "${INFRA_MODE:-}" = "local" ] || exit 0

# Clear a stub directory left by a previous failed `up`.
if [ -d "$SCRIPT_DIR/prometheus.generated.yml" ]; then
    printf ">>> Removing stub directory at prometheus.generated.yml\n"
    rmdir "$SCRIPT_DIR/prometheus.generated.yml" 2>/dev/null \
        || sudo rm -rf "$SCRIPT_DIR/prometheus.generated.yml"
fi

sed -e "s|LEADER_HOST_PLACEHOLDER|$LEADER_HOST|g" \
    -e "s|FOLLOWER_HOST_PLACEHOLDER|$FOLLOWER_HOST|g" \
    -e "s|METRICS_PORT_PLACEHOLDER|$METRICS_PORT|g" \
    "$SCRIPT_DIR/prometheus.yml" > "$SCRIPT_DIR/prometheus.generated.yml"

# Compose resolves ./grafana-provisioning/ and ./dashboards/ relative to this dir.
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

GRAFANA_SRC="${PROJECT_ROOT}/deploy/local-cluster/grafana"
if [ -d "$GRAFANA_SRC" ]; then
    cp "${GRAFANA_SRC}/provisioning/dashboards/dashboards.yml" \
       "$SCRIPT_DIR/grafana-provisioning/dashboards/"
    if [ -d "${GRAFANA_SRC}/dashboards" ]; then
        cp "${GRAFANA_SRC}"/dashboards/*.json "$SCRIPT_DIR/dashboards/" 2>/dev/null || true
    fi
else
    printf ">>> No local-cluster grafana config found, skipping dashboard provisioning.\n"
fi
