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
| `NOS_UPLOAD_BUFFER_SIZE` | Read buffer for streaming uploads (default `262144`) |
| `NOS_ALLOW_PUBLIC_READ` | Allow unauthenticated GET/HEAD on `/{bucket}/{key}` when `true` (default `false`) |
| `NOS_RECONCILE_ON_STARTUP` | Run metadata/blob reconciliation at boot (default `false`) |
| `NOS_RECONCILE_INTERVAL_SECS` | Periodic reconciliation interval; `0` disables (default `0`) |
| `NOS_SOFT_DELETE_TTL_SECS` | Seconds before purging soft-deleted objects (default `86400`) |
| `NOS_METRICS_TOKEN` | Bearer token required for `/metrics` when set |
| `NOS_RATE_LIMIT_RPS` | Per-IP request limit; `0` disables (default `0`) |
| `NOS_RATE_LIMIT_BURST` | Burst size for rate limiting (default `50`) |
| `NOS_LIST_SCAN_CAP` | Max keys scanned per delimiter listing page (default `4096`) |
| `NOS_MULTIPART_PART_SIZE` | Max bytes per multipart part (default `8388608`) |
| `NOS_READ_POOL_SIZE` | SQLite read pool connections (default `4`) |
| `NOS_CORS_ORIGINS` | Comma-separated allowed origins; empty = permissive |
| `RUST_LOG` | Tracing filter (default `info`) |

## HTTP API

See [docs/openapi.yaml](docs/openapi.yaml) for the full contract.

| Method | Route | Auth | Description |
|--------|-------|------|-------------|
| `PUT` | `/:bucket/*key` | Bearer JWT | Stream upload; `x-nd-copy-source` for server-side copy |
| `GET` | `/:bucket/*key` | Bearer JWT or presigned query | Download (`Range`, `If-None-Match`, suffix ranges) |
| `HEAD` | `/:bucket/*key` | Bearer JWT | Metadata + `x-nd-custom-meta-*` headers |
| `DELETE` | `/:bucket/*key` | Bearer JWT | Soft delete (purged after TTL) |
| `GET` | `/:bucket` | Bearer JWT | List objects with pagination + delimiter prefixes |
| `POST` | `/:bucket/_multipart?key=...` | Bearer JWT | Init multipart upload |
| `PUT` | `/:bucket/_multipart/{upload_id}/parts/{n}` | Bearer JWT | Upload part |
| `POST` | `/:bucket/_multipart/{upload_id}/complete` | Bearer JWT | Complete multipart upload |
| `DELETE` | `/:bucket/_multipart/{upload_id}` | Bearer JWT | Abort multipart upload |
| `GET` | `/health` | None | Health check |
| `GET` | `/metrics` | Optional Bearer (`NOS_METRICS_TOKEN`) | JSON or Prometheus (`Accept: text/plain`) |

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
use nebular_os::config::NosConfig;
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

## Scale notes

- SQLite uses separate read/write connection pools (`NOS_READ_POOL_SIZE`) for concurrent listing and downloads.
- Postgres migration is not included in this release; plan a metadata store migration when single-node SQLite becomes a bottleneck.

## License

See repository license file when added.
