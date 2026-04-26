#!/usr/bin/env bash
# Run the integration test suite inside a Linux container.
#
# Required on macOS / Windows (celeriant uses glommio/io_uring — Linux only).
# Also works on Linux for hermetic runs.
#
# Usage: ./celeriant_integration_tests/run-mac.sh [test runner args...]
#
# Examples:
#   ./celeriant_integration_tests/run-mac.sh --list
#   ./celeriant_integration_tests/run-mac.sh --test single
#   ./celeriant_integration_tests/run-mac.sh --include-or correctness --standalone
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
IMAGE_TAG="celeriant-tests:local"

if ! command -v docker >/dev/null 2>&1; then
    echo "error: docker CLI not found on PATH" >&2
    exit 1
fi

if ! docker info >/dev/null 2>&1; then
    echo "error: docker daemon is not reachable. Is Docker Desktop running?" >&2
    exit 1
fi

# On Docker Desktop (macOS / Windows) --network host requires an explicit
# toggle (Settings → Resources → Network → Enable host networking). If off,
# MinIO sibling containers bound to 127.0.0.1:PORT won't be reachable from
# inside the test container, and every S3 test will hang waiting for MinIO.
# Probe by running a throwaway container and checking whether the Docker
# VM's loopback is the same as the test container's loopback.
if [[ "$(uname -s)" == "Darwin" ]]; then
    if ! docker run --rm --network host alpine:latest \
            sh -c 'ip addr show 2>/dev/null | grep -q "inet 127.0.0.1"' \
            >/dev/null 2>&1; then
        cat >&2 <<'EOF'
error: --network host is not available or not returning a host loopback.

On Docker Desktop for Mac this must be explicitly enabled:
  Settings → Resources → Network → Enable host networking

Without host networking, tests cannot reach the MinIO containers they spawn.
EOF
        exit 1
    fi
fi

# Docker Desktop's LinuxKit kernel is built without CONFIG_TLS, so the
# strict-mTLS tests that depend on kernel TLS cannot run on Mac. Auto-exclude
# them via the `requires_ktls` category unless the caller already supplied
# their own --exclude-or filter or pinned a specific test.
EXTRA_TEST_ARGS=()
if [[ "$(uname -s)" == "Darwin" ]]; then
    has_override=0
    for arg in "$@"; do
        case "$arg" in
            --exclude-or|--exclude-or=*|--test|--test=*) has_override=1 ;;
        esac
    done
    if [[ $has_override -eq 0 ]]; then
        EXTRA_TEST_ARGS+=(--exclude-or requires_ktls)
        echo "==> Mac detected: auto-excluding requires_ktls tests (LinuxKit lacks CONFIG_TLS)"
    fi
fi

echo "==> Building $IMAGE_TAG (cached after first run)"
DOCKER_BUILDKIT=1 docker build \
    -f "$REPO_ROOT/Dockerfile.tests" \
    -t "$IMAGE_TAG" \
    "$REPO_ROOT"

echo "==> Running tests: $*"
TTY_ARGS=("-i")
if [ -t 0 ] && [ -t 1 ]; then
    TTY_ARGS+=("-t")
fi
exec docker run --rm "${TTY_ARGS[@]}" \
    --network host \
    --security-opt seccomp=unconfined \
    --ulimit memlock=-1:-1 \
    -v /var/run/docker.sock:/var/run/docker.sock \
    "$IMAGE_TAG" "${EXTRA_TEST_ARGS[@]}" "$@"
