# Multi-stage build for eventplanedb_tcp_server
FROM rust:1.91-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy workspace files
COPY Cargo.toml Cargo.lock ./
COPY eventplanedb_structures ./eventplanedb_structures
COPY eventplanedb_core ./eventplanedb_core
COPY eventplanedb_tcp_server ./eventplanedb_tcp_server
COPY eventplanedb_tcp_client ./eventplanedb_tcp_client
COPY eventstoredb_client ./eventstoredb_client
COPY redpanda_client ./redpanda_client

# Build the TCP server in release mode
RUN cargo build --release -p eventplanedb_tcp_server

# Runtime stage - minimal image with newer kernel support
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/target/release/eventplanedb_tcp_server /app/

# Create data directory
RUN mkdir -p /app/data

# Expose the TCP port
EXPOSE 10000

# Set environment variables
ENV RUST_LOG=info
ENV RUST_BACKTRACE=1

# Run as root to allow io_uring operations (needed for Glommio)
CMD ["./eventplanedb_tcp_server"]