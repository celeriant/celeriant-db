#!/bin/bash
set -e

IMAGE_NAME="claude-code-sandbox"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HOST_UID=$(id -u)

# Build the image if it doesn't exist or if --build flag is passed
if [[ "$1" == "--build" ]] || ! docker image inspect "$IMAGE_NAME" &> /dev/null; then
    echo "Building Docker image..."
    docker build --build-arg HOST_UID="$HOST_UID" -t "$IMAGE_NAME" "$SCRIPT_DIR"
fi

# Create claude config directory if it doesn't exist
mkdir -p "$HOME/.claude"

# Run Claude Code in the container
# Mounts:
#   - Current repo as workspace
#   - ~/.claude for OAuth credentials (persists login between runs)
echo "Starting Claude Code in Docker container..."
docker run -it --rm \
    --security-opt seccomp=unconfined \
    -v "$SCRIPT_DIR":/home/developer/workspace \
    -v "$HOME/.claude":/home/developer/.claude \
    "$IMAGE_NAME"
