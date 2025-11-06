#!/bin/bash

set -e

IMAGE_NAME="eventplanedb-server"
IMAGE_TAG="latest"
CONTAINER_NAME="eventplanedb-server"
TCP_PORT="10000"

# Detect number of CPUs
NUM_CPUS=$(nproc)
echo "🔍 Detected $NUM_CPUS CPU cores"

echo "🔨 Building Docker image..."
docker build -t ${IMAGE_NAME}:${IMAGE_TAG} .

echo "🧹 Stopping and removing existing container (if any)..."
docker stop ${CONTAINER_NAME} 2>/dev/null || true
docker rm ${CONTAINER_NAME} 2>/dev/null || true

echo "🚀 Starting new container with performance optimizations..."
docker run -d \
  --name ${CONTAINER_NAME} \
  --restart unless-stopped \
  --privileged \
  --network host \
  --cpus="${NUM_CPUS}" \
  --cpuset-cpus="0-$((NUM_CPUS-1))" \
  --ulimit memlock=-1:-1 \
  --ulimit nofile=1048576:1048576 \
  -e RUST_LOG=info \
  -e RUST_BACKTRACE=1 \
  -v eventplanedb-data:/app/data:rw,Z \
  --mount type=tmpfs,destination=/tmp,tmpfs-size=1G \
  ${IMAGE_NAME}:${IMAGE_TAG}

echo "⏳ Waiting for server to start..."
sleep 2

if docker ps --filter name=${CONTAINER_NAME} --filter status=running | grep -q ${CONTAINER_NAME}; then
    echo "✅ Container started successfully!"
    echo ""
    echo "📊 Performance optimizations applied:"
    echo "   ✓ Host networking (--network host)"
    echo "   ✓ All CPUs available (--cpus=${NUM_CPUS})"
    echo "   ✓ CPU affinity enabled (--cpuset-cpus=0-$((NUM_CPUS-1)))"
    echo "   ✓ Direct data volume (no overlay)"
    echo ""
    echo "📝 View logs: docker logs -f ${CONTAINER_NAME}"
    echo "🔌 Connect: nc localhost ${TCP_PORT}"
else
    echo "❌ Container failed to start. Showing logs:"
    docker logs ${CONTAINER_NAME}
    exit 1
fi