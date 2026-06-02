# Changelog

All notable changes to Nebular OS are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- **Postgres metadata backend** (`NOS_METADATA_BACKEND=postgres`): object index in Postgres (`nos_objects`, `nos_multipart_uploads`, `nos_multipart_parts`) while blobs stay on disk under `NOS_DATA_DIR`. Migrations ship in `migrations/001_nos_object_index.sql`.
- **SQLite metadata backend** remains the default (`NOS_METADATA_BACKEND=sqlite` or unset); behavior for existing deployments is unchanged when env vars are not set.
- **Per-node logical byte cap** via `NOS_MAX_LOGICAL_BYTES` (0 = unlimited). PUT and multipart complete reject when active `logical_bytes` plus incoming size would exceed the cap, returning **HTTP 507** with `{"error":"insufficient storage"}`.
- **`GET /metrics`** JSON fields: `max_logical_bytes`, `metadata_backend`, and existing `logical_bytes` (sum of non-deleted object sizes in the active metadata backend).
- **`GET /health`** field: `metadata_backend`.
- **`GET /health/ready`** extended checks: `metadata_backend`, `metadata_write`, `metadata_read`, `postgres_ok` (postgres mode), plus system SQLite and data-directory probes.
- **`ObjectMetaStore`** abstraction (`src/storage/object_meta.rs`) to share PUT/GET/LIST/DELETE/multipart logic across SQLite and Postgres.
- Integration tests: `test_max_logical_bytes_rejects_second_put` (SQLite), `tests/postgres_metadata.rs` (testcontainers Postgres; skips when Docker is unavailable).

### Changed

- Cluster runtime config persistence uses the active metadata backend (SQLite `cluster_runtime_config` or Postgres `nos_cluster_runtime_config`).
- In postgres mode, a sidecar SQLite file at `NOS_META_PATH` still hosts `replication_log` for cluster replication; **postgres + non-standalone cluster modes are rejected at startup**.

### Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `NOS_METADATA_BACKEND` | `sqlite` | `sqlite` or `postgres` |
| `NOS_METADATA_DATABASE_URL` | — | Required when `NOS_METADATA_BACKEND=postgres` |
| `NOS_MAX_LOGICAL_BYTES` | `0` | Hard cap on logical bytes stored on this node |

See `.env.example` and `README.md` for full operator notes, including alignment with Ownly `storage_nodes.target_capacity_bytes` and `storage_metadata_mode=ownly`.

### Notes for Ownly operators

- Ownly continues to own user/file placement (`files`, `file_storage_parts`); Nebular Postgres tables are the **object-store index** (bucket/key → blob), not a duplicate of Ownly’s catalog.
- Set `NOS_MAX_LOGICAL_BYTES` on each Nebular instance to match the admin target capacity for that node so direct uploads cannot exceed the planner’s assumptions.
- Postgres metadata mode is intended for **standalone** Nebular nodes first; use env-only cluster config or bootstrap API as today.

---

## [0.1.0] — prior releases

### Added

- Standalone object storage with JWT auth, streaming PUT/GET, zstd compression, soft delete, multipart uploads, presigned URLs, and optional S3-compatible list/copy headers.
- SQLite metadata with separate read/write pools (`NOS_READ_POOL_SIZE`).
- Cluster modes: standalone, replicated, assigned, replicated+assigned; runtime config via `PUT /_cluster/config` and bootstrap token.
- Write preconditions (`If-Match`, `If-None-Match`), readiness probe (`/health/ready`), and metrics (`/metrics` JSON or Prometheus text).

[Unreleased]: https://github.com/AsP3X/nebular-os/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/AsP3X/nebular-os/releases/tag/v0.1.0
