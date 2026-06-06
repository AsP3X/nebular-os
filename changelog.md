# Changelog

All notable changes to Nebular OS are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [0.1.3] — 2026-06-07

### Changed

- GitHub release assets are published as bare platform binaries (`nebular-os-<version>-<platform>`, `.exe` on Windows) instead of tar.gz/zip archives.

## [0.1.2] — 2026-06-06

### Added

- **GitHub Releases** with standalone binaries for Linux (x86_64, aarch64, i686, armv7, riscv64, ppc64le, s390x), Windows (x86_64, aarch64, i686), and macOS (x86_64, aarch64), plus `SHA256SUMS.txt`.
- **Postgres metadata backend** (`NOS_METADATA_BACKEND=postgres`): object index in Postgres (`nos_objects`, `nos_multipart_uploads`, `nos_multipart_parts`) while blobs stay on disk under `NOS_DATA_DIR`. Migrations ship in `migrations/001_nos_object_index.sql`.
- **SQLite metadata backend** remains the default (`NOS_METADATA_BACKEND=sqlite` or unset); behavior for existing deployments is unchanged when env vars are not set.
- **Per-node logical byte cap** via `NOS_MAX_LOGICAL_BYTES` (0 = unlimited). PUT and multipart complete reject when active `logical_bytes` plus incoming size would exceed the cap, returning **HTTP 507** with `{"error":"insufficient storage"}`.
- **`GET /metrics`** JSON fields: `max_logical_bytes`, `metadata_backend`, and existing `logical_bytes` (sum of non-deleted object sizes in the active metadata backend).
- **`GET /health`** field: `metadata_backend`.
- **`GET /health/ready`** extended checks: `metadata_backend`, `metadata_write`, `metadata_read`, `postgres_ok` (postgres mode), plus system SQLite and data-directory probes.
- **`ObjectMetaStore`** abstraction (`src/storage/object_meta.rs`) to share PUT/GET/LIST/DELETE/multipart logic across SQLite and Postgres.
- Integration tests: `test_max_logical_bytes_rejects_second_put` (SQLite), `tests/postgres_metadata.rs` (testcontainers Postgres; skips when Docker is unavailable).
- **Tiered zstd compression**: fast uploads via `NOS_ZSTD_LEVEL_UPLOAD` (default `3`) and stronger background encoding via `NOS_ZSTD_LEVEL` (default `22`). New compressed blobs use the **NOS2** on-disk header (magic + logical size + dict id + stored level); legacy **NOSZ** blobs remain readable.
- **Blob re-recompression** (`recompress_blobs`): maintenance now upgrades legacy raw blobs, **NOSZ** v1, and low-level **NOS2** objects when a pass at `NOS_ZSTD_LEVEL` would shrink or level-stamp the payload. `recompress_legacy_blobs` remains as an alias.
- **Global zstd dictionary** (`NOS_ZSTD_DICT_ENABLED`): periodic training from recent object samples; dictionaries stored under `{NOS_DATA_DIR}/.dict/`. Training runs with startup/interval recompression when enabled.
- **Block-level deduplication** (`NOS_DEDUP_ENABLED`): objects at or above `NOS_DEDUP_MIN_SIZE` are chunked into content-addressed blocks under `.blocks/` with **NOSD** manifest blobs; SQLite `dedup_blocks` tracks refcounts for copy/delete/overwrite.

### Changed

- HTTP client uses **rustls** instead of OpenSSL-native TLS for easier cross-platform release builds.
- Cluster runtime config persistence uses the active metadata backend (SQLite `cluster_runtime_config` or Postgres `nos_cluster_runtime_config`).
- In postgres mode, a sidecar SQLite file at `NOS_META_PATH` still hosts `replication_log` for cluster replication; **postgres + non-standalone cluster modes are rejected at startup**.
- PUT and multipart complete now finalize through `blob_finalize` (compress with upload level, or dedup when enabled). GET transparently reads **NOSZ**, **NOS2**, **NOSD**, and legacy raw blobs.
- Copy and delete release dedup block refcounts when manifest blobs are unlinked.

### Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `NOS_METADATA_BACKEND` | `sqlite` | `sqlite` or `postgres` |
| `NOS_METADATA_DATABASE_URL` | — | Required when `NOS_METADATA_BACKEND=postgres` |
| `NOS_MAX_LOGICAL_BYTES` | `0` | Hard cap on logical bytes stored on this node |
| `NOS_ZSTD_LEVEL` | `22` | Background / maintenance zstd level (1–22) |
| `NOS_ZSTD_LEVEL_UPLOAD` | `3` | Fast upload zstd level (1–22) |
| `NOS_ZSTD_DICT_ENABLED` | `false` | Train and use a global zstd dictionary |
| `NOS_ZSTD_DICT_MAX_BYTES` | `112640` | Max trained dictionary size |
| `NOS_ZSTD_DICT_TRAIN_BATCH` | `32` | Sample count per dictionary training pass |
| `NOS_DEDUP_ENABLED` | `false` | Block-level deduplication for large objects |
| `NOS_DEDUP_BLOCK_SIZE` | `262144` | Dedup chunk size in bytes |
| `NOS_DEDUP_MIN_SIZE` | `1048576` | Minimum logical size to store via dedup manifest |

See `.env.example` and `README.md` for full operator notes, including alignment with Ownly `storage_nodes.target_capacity_bytes` and `storage_metadata_mode=ownly`.

### Notes for Ownly operators

- Ownly continues to own user/file placement (`files`, `file_storage_parts`); Nebular Postgres tables are the **object-store index** (bucket/key → blob), not a duplicate of Ownly’s catalog.
- Set `NOS_MAX_LOGICAL_BYTES` on each Nebular instance to match the admin target capacity for that node so direct uploads cannot exceed the planner’s assumptions.
- Postgres metadata mode is intended for **standalone** Nebular nodes first; use env-only cluster config or bootstrap API as today.
- For minimum disk use after a period of fast uploads: set `NOS_RECOMPRESS_ON_STARTUP=true` (and/or `NOS_RECOMPRESS_INTERVAL_SECS`), keep `NOS_ZSTD_LEVEL=22`, and optionally enable `NOS_ZSTD_DICT_ENABLED` plus `NOS_DEDUP_ENABLED` for repetitive or large payloads. Ownly configures these via Compose/env only; compression logic lives in Nebular.

---

## [0.1.0] — prior releases

### Added

- Standalone object storage with JWT auth, streaming PUT/GET, zstd compression, soft delete, multipart uploads, presigned URLs, and optional S3-compatible list/copy headers.
- SQLite metadata with separate read/write pools (`NOS_READ_POOL_SIZE`).
- Cluster modes: standalone, replicated, assigned, replicated+assigned; runtime config via `PUT /_cluster/config` and bootstrap token.
- Write preconditions (`If-Match`, `If-None-Match`), readiness probe (`/health/ready`), and metrics (`/metrics` JSON or Prometheus text).

[Unreleased]: https://github.com/AsP3X/nebular-os/compare/v0.1.3...HEAD
[0.1.3]: https://github.com/AsP3X/nebular-os/releases/tag/v0.1.3
[0.1.2]: https://github.com/AsP3X/nebular-os/releases/tag/v0.1.2
[0.1.0]: https://github.com/AsP3X/nebular-os/releases/tag/v0.1.0
