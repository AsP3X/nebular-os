# Nebular OS

Standalone, self-hosted object storage with an S3-like HTTP API. Blobs live on disk; metadata is tracked in SQLite. JWT auth uses the same `Claims` shape as typical Aurora-style backends (`sub`, `email`, `role`, `exp`, `iat`).

Originally developed as `nebula-os` inside the [Aurora](https://github.com/) monorepo; this repository is the extracted, independently versioned crate.

## Quick start

```bash
cp .env.example .env
# Edit .env: set NOS_JWT_SECRET and NOS_SIGNING_SECRET (each >= 32 chars, not placeholders)

cargo run
# or
docker compose up --build
```

Server listens on `NOS_BIND_ADDR` (default `0.0.0.0:9000`).

## Configuration

| Variable | Description |
|----------|-------------|
| `NOS_JWT_SECRET` | HS256 JWT secret (required, min 32 chars) |
| `NOS_SIGNING_SECRET` | HMAC secret for presigned URLs (required, min 32 chars) |
| `NOS_BIND_ADDR` | Listen address (default `0.0.0.0:9000`) |
| `NOS_DATA_DIR` | Blob directory (default `./data/blobs`) |
| `NOS_META_PATH` | SQLite metadata path (default `./data/meta/metadata.db`) |
| `NOS_MAX_BODY_SIZE` | Max upload bytes (default `104857600`) |
| `NOS_ALLOW_PUBLIC_READ` | Allow unauthenticated GET when `true` (default `false`) |
| `RUST_LOG` | Tracing filter (default `info`) |

## HTTP API

| Method | Route | Auth | Description |
|--------|-------|------|-------------|
| `PUT` | `/:bucket/*key` | Bearer JWT | Stream upload |
| `GET` | `/:bucket/*key` | Bearer JWT or presigned query | Download (supports `Range`) |
| `HEAD` | `/:bucket/*key` | Bearer JWT | Object metadata |
| `DELETE` | `/:bucket/*key` | Bearer JWT | Hard delete |
| `GET` | `/:bucket` | Bearer JWT | List objects (`prefix`, `delimiter`, `limit`, `start_after`) |
| `GET` | `/health` | None | Health check |
| `GET` | `/metrics` | None | Storage stats |

## Use as a Rust dependency

**Git:**

```toml
[dependencies]
nebular-os = { git = "https://github.com/YOUR_ORG/nebular-os.git", tag = "v0.1.0" }
```

**Path (local development):**

```toml
[dependencies]
nebular-os = { path = "../nebular-os" }
```

The library crate name is `nebular_os`:

```rust
use nebular_os::server::create_app;
use nebular_os::storage::engine::StorageEngine;
```

## Development

```bash
cargo test
cargo build --release
```

## Docker

```bash
docker build -t nebular-os .
docker run --rm -p 9000:9000 \
  -e NOS_JWT_SECRET='your-32-char-or-longer-secret-here!!' \
  -e NOS_SIGNING_SECRET='another-32-char-or-longer-secret!!' \
  -v nebular_data:/data \
  nebular-os
```

## License

See repository license file when added.
