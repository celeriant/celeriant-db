# Celeriant uses io_uring via Glommio, which requires seccomp=unconfined:
#   docker run --security-opt seccomp=unconfined ...
#
# Multi-arch without QEMU: the builder always runs on the build host's native
# platform and cross-compiles for TARGETARCH (same recipe as
# deploy/rpi-cluster's `make build`).
FROM --platform=$BUILDPLATFORM rust:latest AS builder
ARG TARGETARCH
WORKDIR /build
COPY . .
RUN case "$TARGETARCH" in \
      arm64) \
        rustup target add aarch64-unknown-linux-gnu && \
        apt-get update && apt-get install -y gcc-aarch64-linux-gnu && \
        rm -rf /var/lib/apt/lists/* && \
        CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc \
          cargo build --release --target aarch64-unknown-linux-gnu -p celeriant && \
        cp target/aarch64-unknown-linux-gnu/release/celeriant /celeriant ;; \
      amd64) \
        cargo build --release -p celeriant && \
        cp target/release/celeriant /celeriant ;; \
      *) echo "unsupported TARGETARCH: $TARGETARCH" && exit 1 ;; \
    esac

FROM ubuntu:24.04
LABEL org.opencontainers.image.title="Celeriant" \
      org.opencontainers.image.description="Distributed event store for event sourcing" \
      org.opencontainers.image.url="https://celeriant.io" \
      org.opencontainers.image.source="https://github.com/celeriant/celeriant-db" \
      org.opencontainers.image.licenses="Apache-2.0"
# No RUN steps in this stage: the binary is statically linked against rustls
# (no system OpenSSL), and a RUN-free target stage cross-builds without QEMU.
COPY --from=builder /celeriant /usr/local/bin/celeriant
EXPOSE 10000
ENTRYPOINT ["celeriant"]
