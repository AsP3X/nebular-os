FROM rust:1.88-slim-bookworm AS builder
WORKDIR /app
COPY Cargo.toml .
COPY Cargo.lock .
RUN mkdir src && echo 'fn main() {}' > src/main.rs && cargo build --release && rm -rf src
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/nebular-os /usr/local/bin/nebular-os
RUN printf '#!/bin/sh\nmkdir -p /data/blobs /data/meta\nexec nebular-os "$@"\n' > /entrypoint.sh && chmod +x /entrypoint.sh
EXPOSE 9000
ENTRYPOINT ["/entrypoint.sh"]
