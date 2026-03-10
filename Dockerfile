# Celeriant uses io_uring via Glommio, which requires seccomp=unconfined:
#   docker run --security-opt seccomp=unconfined ...
FROM rust:latest AS builder
WORKDIR /build
COPY . .
RUN cargo build --release -p celeriant

FROM ubuntu:24.04
RUN apt-get update && apt-get install -y libssl3t64 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/celeriant /usr/local/bin/celeriant
EXPOSE 10000
ENTRYPOINT ["celeriant"]
