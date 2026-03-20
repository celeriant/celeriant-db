#!/bin/bash
# Import the Celeriant cluster dashboard into Grafana Cloud.
#
# Uses the same dashboard JSON as the RPi and local clusters
# (deploy/local-cluster/grafana/dashboards/celeriant-cluster.json).
#
# Usage:
#   ./deploy-dashboard.sh --grafana-url https://your-stack.grafana.net --grafana-token glsa_...
#
# The token needs Editor or Admin role (the MetricsPublisher key used by Alloy
# is not sufficient — create a Service Account token in Grafana Cloud).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
DASHBOARD_JSON="$REPO_ROOT/deploy/local-cluster/grafana/dashboards/celeriant-cluster.json"

GRAFANA_URL=""
GRAFANA_TOKEN=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --grafana-url) GRAFANA_URL="$2"; shift 2 ;;
    --grafana-token) GRAFANA_TOKEN="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

if [[ -z "$GRAFANA_URL" || -z "$GRAFANA_TOKEN" ]]; then
  echo "Usage: $0 --grafana-url https://your-stack.grafana.net --grafana-token glsa_..."
  exit 1
fi

if [[ ! -f "$DASHBOARD_JSON" ]]; then
  echo "ERROR: Dashboard not found at $DASHBOARD_JSON"
  exit 1
fi

# Wrap the dashboard model in the Grafana import API payload
PAYLOAD=$(jq -n --argjson dashboard "$(cat "$DASHBOARD_JSON")" '{
  dashboard: $dashboard,
  overwrite: true,
  message: "Imported by ec2-cluster deploy"
}')

echo "==> Importing dashboard to ${GRAFANA_URL}"

HTTP_CODE=$(curl -s -o /tmp/grafana-import-response.json -w "%{http_code}" \
  -X POST "${GRAFANA_URL}/api/dashboards/db" \
  -H "Authorization: Bearer ${GRAFANA_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "$PAYLOAD")

if [[ "$HTTP_CODE" == "200" ]]; then
  DASHBOARD_URL=$(jq -r '.url // empty' /tmp/grafana-import-response.json)
  echo "==> Dashboard imported successfully"
  if [[ -n "$DASHBOARD_URL" ]]; then
    echo "  ${GRAFANA_URL}${DASHBOARD_URL}"
  fi
else
  echo "ERROR: Import failed (HTTP $HTTP_CODE)"
  cat /tmp/grafana-import-response.json
  exit 1
fi
