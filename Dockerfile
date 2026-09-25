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
COPY migrations ./migrations
RUN cargo build --release

FROM debian:bookworm-slim
# Human: curl is required for Docker / Compose health probes (see ownly and sugarai compose).
# Agent: INSTALL ca-certificates + curl; HEALTHCHECK hits GET /health on NOS_BIND_ADDR.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
# Human: The server runs as `nos` (uid/gid 10001); the entrypoint starts as root only to prepare the data
# directories (see docker/entrypoint.sh). Data lives under the /data volume unless NOS_DATA_DIR/NOS_META_PATH say
# otherwise — the binary's own defaults are relative to the working directory.
RUN groupadd --system --gid 10001 nos \
    && useradd --system --uid 10001 --gid nos --home-dir /data --no-create-home --shell /usr/sbin/nologin nos \
    && mkdir -p /app /data \
    && chown nos:nos /app /data
ENV NOS_DATA_DIR=/data/blobs \
    NOS_META_PATH=/data/meta/metadata.db
WORKDIR /app
COPY --from=builder /app/target/release/nebular-os /usr/local/bin/nebular-os
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod 0755 /entrypoint.sh
EXPOSE 9000

HEALTHCHECK --interval=10s --timeout=5s --start-period=15s --retries=5 \
    CMD curl -fsS "http://127.0.0.1:9000/health/ready" || exit 1

ENTRYPOINT ["/entrypoint.sh"]
