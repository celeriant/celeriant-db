#!/bin/bash

set -e

IMAGE_NAME="eventplanedb-tcp-server"
IMAGE_TAG="latest"
CONTAINER_NAME="eventplanedb-server"
TCP_PORT="10000"
NUM_CPUS=$(nproc)

echo "🔨 Building Docker image..."
docker build -t ${IMAGE_NAME}:${IMAGE_TAG} .

echo "🧹 Cleaning up..."
docker stop ${CONTAINER_NAME} 2>/dev/null || true
docker rm ${CONTAINER_NAME} 2>/dev/null || true

echo "🚀 Starting container (cloud-compatible configuration)..."
docker run -d \
  --name ${CONTAINER_NAME} \
  --restart unless-stopped \
  --privileged \
  -p ${TCP_PORT}:10000 \
  --cpus="${NUM_CPUS}" \
  --cpuset-cpus="0-$((NUM_CPUS-1))" \
  --ulimit memlock=-1:-1 \
  --ulimit nofile=1048576:1048576 \
  -e RUST_LOG=info \
  -v eventplanedb-data:/app/data:rw \
  --mount type=tmpfs,destination=/tmp \
  ${IMAGE_NAME}:${IMAGE_TAG}

echo "✅ Container started successfully!"
echo ""
echo "📊 Container status:"
docker ps --filter name=${CONTAINER_NAME}
echo ""
echo "📝 View logs: docker logs -f ${CONTAINER_NAME}"
echo "🔌 Connect: nc localhost ${TCP_PORT}"