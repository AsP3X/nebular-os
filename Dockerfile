FROM rust:1.88-slim-bookworm AS builder
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        pkg-config \
        libssl-dev \
        libsqlite3-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY Cargo.toml .
COPY Cargo.lock .
RUN mkdir src && echo 'fn main() {}' > src/main.rs && cargo build --release && rm -rf src
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
# Human: curl is required for Docker / Compose health probes (see ownly and sugarai compose).
# Agent: INSTALL ca-certificates + curl; HEALTHCHECK hits GET /health on NOS_BIND_ADDR.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/nebular-os /usr/local/bin/nebular-os
RUN printf '#!/bin/sh\nmkdir -p /data/blobs /data/meta\nexec nebular-os "$@"\n' > /entrypoint.sh && chmod +x /entrypoint.sh
EXPOSE 9000

HEALTHCHECK --interval=10s --timeout=5s --start-period=15s --retries=5 \
    CMD curl -fsS "http://127.0.0.1:9000/health" || exit 1

ENTRYPOINT ["/entrypoint.sh"]
