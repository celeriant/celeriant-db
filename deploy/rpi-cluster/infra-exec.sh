#!/usr/bin/env bash
# Dispatch a `docker compose <args>` invocation to the right place based on INFRA_MODE.
# Usage: infra-exec.sh <docker compose args...>
#   INFRA_MODE=remote — ssh into INFRA_HOST and run docker compose in ~/celeriant-infra/
#   INFRA_MODE=local  — run docker compose with the local-override file from this directory
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/config.env"

case "${INFRA_MODE:-}" in
    remote|local) ;;
    *) printf "ERROR: INFRA_MODE must be 'remote' or 'local' (got '%s')\n" "${INFRA_MODE:-}" >&2; exit 1 ;;
esac

if [ "$INFRA_MODE" = "remote" ]; then
    ssh "$INFRA_HOST" "cd ~/celeriant-infra && docker compose $*"
else
    cd "$SCRIPT_DIR" && docker compose -f docker-compose.yml -f docker-compose.local-override.yml "$@"
fi
