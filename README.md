# Nebular OS

> **License:** Source-available under [NOCL-1.0](LICENSE) — free only for
> **private, non-profit** use. Commercial and for-profit use requires a
> [commercial license](COMMERCIAL-LICENSE.md).

Standalone, self-hosted object storage with an S3-like HTTP API. Blobs live on disk; object metadata is stored in **SQLite** (default) or **Postgres** (blob-only nodes for [Ownly](https://github.com/AsP3X/nebular-os) when `storage_metadata_mode=ownly`). JWT auth uses the same `Claims` shape as typical Aurora-style backends (`sub`, `email`, `role`, `exp`, `iat`).

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
| `NOS_JWT_ISSUER` / `NOS_JWT_AUDIENCE` | When set, JWTs must carry this `iss` / `aud` claim |
| `NOS_SIGNING_SECRET` | HMAC secret for presigned URLs (min 32 chars); without it presigned URLs are rejected |
| `NOS_PRESIGN_MAX_TTL_SECS` | Reject presigned URLs expiring further out than this (default `604800` = 7 days; `0` = no cap). Presigned URLs only work on `/{bucket}/{key}` |
| `NOS_BIND_ADDR` | Listen address (default `0.0.0.0:9000`) |
| `NOS_DATA_DIR` | Blob directory (default `./data/blobs`) |
| `NOS_META_PATH` | SQLite path for object index (default) and system tables (replication log in cluster modes) |
| `NOS_METADATA_BACKEND` | `sqlite` (default) or `postgres` |
| `NOS_METADATA_DATABASE_URL` | Postgres URL when `NOS_METADATA_BACKEND=postgres` |
| `NOS_METADATA_MODE` | `full` (default) or `blob_only` (bytes only; an external index such as Ownly owns object metadata) |
| `NOS_MAX_LOGICAL_BYTES` | Optional per-node logical byte cap (`0` = unlimited); returns HTTP 507 when exceeded |
| `NOS_MAX_BODY_SIZE` | Max upload bytes (default `104857600`) |
| `NOS_UPLOAD_BUFFER_SIZE` | Read buffer for streaming uploads (default `262144`) |
| `NOS_UPLOAD_MAX_IN_FLIGHT_BYTES` | Budget for upload bodies in flight across all requests; uploads over budget get `503` + `Retry-After: 1`. One upload larger than the whole budget is admitted when nothing else is uploading. `0` disables (default `33554432`) |
| `NOS_UPLOAD_IDLE_TIMEOUT_SECS` | Abort an upload body (PUT or multipart part) that sends no bytes for this long with `408`; `0` disables (default `60`) |
| `NOS_HEADER_READ_TIMEOUT_SECS` | Close a connection that takes longer than this to send a request's headers, including keep-alive connections idle that long; keep it above the idle timeout of proxies and client connection pools in front of Nebular (AWS ALB: 60 s); `0` disables (default `75`) |
| `NOS_SEND_STALL_TIMEOUT_SECS` | Close a connection whose client stops reading a response for this long (a stalled download holds a file handle and buffers); `0` disables (default `300`) |
| `NOS_MAX_CONNECTIONS` | Maximum simultaneous connections; further clients wait in the listen backlog; `0` = unlimited (default `0`) |
| `NOS_SHUTDOWN_GRACE_SECS` | On SIGTERM (`docker stop`) or Ctrl-C, stop accepting connections and give requests in progress this long to finish before exiting (default `8`, under Docker's 10 s stop timeout) |
| `NOS_FSYNC_WRITES` | fsync blob data and its directory before a write becomes visible (default `true`) |
| `NOS_UPLOAD_PERMIT_UNIT` | Granularity of budget charges; uploads without `Content-Length` are charged `NOS_MAX_BODY_SIZE` (default `5242880`) |
| `NOS_ALLOW_PUBLIC_READ` | Allow unauthenticated GET/HEAD on `/{bucket}/{key}` when `true` (default `false`) |
| `NOS_RECONCILE_ON_STARTUP` | Run metadata/blob reconciliation at boot (default `false`) |
| `NOS_RECONCILE_INTERVAL_SECS` | Periodic reconciliation interval; `0` disables (default `0`) |
| `NOS_SOFT_DELETE_TTL_SECS` | Seconds before purging soft-deleted metadata; `0` hard-deletes immediately (default `86400`) |
| `NOS_SOFT_DELETE_DROP_BLOB` | Remove blob file on soft-delete while keeping tombstone until TTL (default `false`) |
| `NOS_MULTIPART_UPLOAD_TTL_SECS` | Purge abandoned multipart sessions after this many seconds (default `86400`; `0` disables) |
| `NOS_RECOMPRESS_ON_STARTUP` | Re-compress raw blobs, migrate legacy formats, and upgrade NOSI at boot (default `false`) |
| `NOS_RECOMPRESS_INTERVAL_SECS` | Periodic blob recompression / migration interval; each pass continues after the previous one and starts over after the last key; `0` disables (default `0`) |
| `NOS_RECOMPRESS_BATCH_SIZE` | Max objects scanned per recompression pass (default `100`) |
| `NOS_VERIFY_INTERVAL_SECS` | Periodic integrity scrub interval; each pass continues after the previous one and starts over after the last key; `0` disables (default `0`) |
| `NOS_ORPHAN_GC_INTERVAL_SECS` | Periodic removal of blob files that have no metadata row (up to 500 per run); `0` disables (default `0`) |
| `NOS_VERIFY_BATCH_SIZE` | Max objects verified per integrity pass (default `100`) |
| `NOS_SCRUB_SAMPLE_DENOM` | Periodic scrub verifies 1/N of the objects in each full pass, a different slice every pass, so each object is verified once every N passes; `1` verifies everything on every pass (default `1024`) |
| `NOS_SCRUB_MODE` | `light` (headers/sizes) or `deep` (checksums/decode; default `deep`) |
| `NOS_VERIFY_ON_READ` | When `true`, full raw-object GET verifies on-disk xxh3 against metadata etag |
| `NOS_READ_BUFFER_SIZE` | Pooled read buffer for raw GET streaming (default `262144`) |
| `NOS_WEBHOOKS_JSON` | Per-bucket webhook URLs, e.g. `{"music":["https://app/hooks/storage"]}` |
| `NOS_METRICS_TOKEN` | Bearer token required for `/metrics` when set; unset, `/metrics` is open — it reports only aggregate counters (object and byte totals, replication counts), no bucket or key names |
| `NOS_RATE_LIMIT_RPS` | Per-client-IP request limit, also applied to failed authentication; `0` disables (default `0`). Behind a reverse proxy every client shares the proxy's IP, so limit at the proxy instead |
| `NOS_RATE_LIMIT_BURST` | Burst size for rate limiting (default `50`) |
| `NOS_RATE_LIMIT_BYPASS_ROLES` | Comma-separated JWT roles exempt from rate limiting (default `admin`) |
| `NOS_BULK_DELETE_CONCURRENCY` | Blob files removed in parallel by prefix and batch deletes (default `32`) |
| `NOS_BULK_DELETE_BATCH_LIMIT` | Most objects one prefix delete removes; larger prefixes continue with `start_after` (default `1000`) |
| `NOS_LIST_SCAN_CAP` | Max keys scanned per delimiter listing page (default `4096`) |
| `NOS_MULTIPART_PART_SIZE` | Max bytes per multipart part (default `8388608`) |
| `NOS_READ_POOL_SIZE` | SQLite read pool connections (default `4`) |
| `NOS_ZSTD_LEVEL` | Background / maintenance zstd level 1–22 (default `22`; lower = faster recompression passes) |
| `NOS_ZSTD_LEVEL_UPLOAD` | Fast upload zstd level 1–22 for NOSI block writes (default `3`) |
| `NOS_ZSTD_DICT_ENABLED` | Train a zstd dictionary from stored objects (on the recompression schedule, once enough exist) and compress new blobs with it (default `false`). `POST /_nos/maintenance/train_dictionary` trains a newer one when the data has changed; every blob keeps decoding with the dictionary it was written with, and recompression moves blobs to the newest |
| `NOS_ZSTD_DICT_MAX_BYTES` | Max trained dictionary size in bytes (default `112640`) |
| `NOS_ZSTD_DICT_TRAIN_BATCH` | Sample count for dictionary training (default `32`) |
| `NOS_DEDUP_ENABLED` | Block-level deduplication for large objects (default `false`) |
| `NOS_BLOCK_SIZE` | Unified logical block size for compression and dedup (default `1048576`; min `4096`) |
| `NOS_DEDUP_BLOCK_SIZE` | Dedup chunk size override (defaults to `NOS_BLOCK_SIZE`) |
| `NOS_DEDUP_MIN_SIZE` | Minimum logical object size to use dedup (default `1048576`) |
| `NOS_COMPRESS_MIN_SIZE` | Minimum object size in bytes before attempting NOSI compression (default `4096`) |
| `NOS_COMPRESS_BLOCK_SIZE` | Compression block size override (defaults to `NOS_BLOCK_SIZE`) |
| `NOS_BLOCK_CACHE_ENTRIES` | LRU decoded-block cache for range GET on NOSI; `0` disables (default `256`) |
| `NOS_BLOCK_CACHE_MAX_BYTES` | Byte budget for that cache (default `67108864` = 64 MiB); full-object reads use but don't fill it |
| `NOS_COMPRESS_EXCLUDE_EXTENSIONS` | Comma-separated extra file extensions to skip compression (e.g. `sqlite,bak`) |
| `NOS_S3_COMPAT` | Enable S3-style XML list/errors and `x-amz-copy-source` (default `false`) |
| `NOS_BUCKET_POLICY` | JSON map of `sub` → allowed bucket names; empty = no extra restriction |
| `NOS_S3_ACCESS_KEY` / `NOS_S3_SECRET_KEY` | Optional access key for AWS Signature V4 (header-signed requests on every route, presigned URLs on object routes; e.g. from AWS SDKs or `curl --aws-sigv4`); the payload SHA-256 and `Content-MD5` are verified |
| `NOS_S3_ACCESS_KEY_ROLE` | Role granted to access-key requests: `admin` (default), `editor`, `uploader`, `listener` or `readonly` |
| `NOS_LEGACY_ACCESS_KEY_AUTH` | Also accept the old `Authorization: NOS <key>:<sig>` scheme (default `false`). It signs only method and bucket and never expires — move clients to SigV4 |
| `NOS_CORS_ORIGINS` | Comma-separated origins browsers may call from; empty = any origin. Credentials travel only in the `Authorization` header or a presigned URL (never cookies), so an open policy exposes nothing a caller doesn't already hold; set it to keep other sites' scripts off the API |
| `RUST_LOG` | Tracing filter (default `info,tower_http=debug`; request spans log the path, never the query string) |

### Cluster modes (optional, experimental)

> **Experimental.** Replication is asynchronous and nodes that accept writes for the same key converge on the last write by wall-clock time — keep node clocks synchronized (NTP). Standalone mode is the tested path for data you can't afford to lose.

Unset `NOS_CLUSTER_MODE` for standalone (default). See [docs/plans/cluster-modes.md](docs/plans/cluster-modes.md).

**Ownly / admin-driven clusters:** Start each Nebular node with only JWT secrets plus `NOS_CLUSTER_BOOTSTRAP_TOKEN` (≥ 32 chars). Leave `NOS_CLUSTER_MODE` unset. After the node is reachable, Ownly (setup wizard or Storage Nodes admin) calls `PUT /_cluster/config` with the bootstrap token to set mode, `cluster_token`, peers, and assignment rules. Config is stored in SQLite and survives restarts; no container env churn when adding nodes.

| Variable | Description |
|----------|-------------|
| `NOS_CLUSTER_MODE` | `standalone` (default), `replicated`, `assigned`, or `replicated+assigned` |
| `NOS_NODE_ID` | Stable node identity (also the tie-break between changes made at the same microsecond) |
| `NOS_INSTANCE_ID` / `NOS_REGION_LABEL` | Optional labels reported by `/_cluster/health` and capabilities |
| `NOS_CLUSTER_TOKEN` | Bearer token for `/_cluster/*` routes |
| `NOS_CLUSTER_PEERS` | `node-b=http://host:9000;class-a,class-b` (optional `;classes` per peer) |
| `NOS_STORAGE_CLASSES` | Comma-separated classes this node accepts |
| `NOS_REPLICATION_FACTOR` | Target copies including self; default `2` in replicating modes (`1` keeps no peer copies and logs a warning) |
| `NOS_REPLICATION_GROUP` | Replication group of this node's writes; peers with `;group=` receive only their group (default `default`) |
| `NOS_REPLICATION_ROLE` | `member` (default) or `readonly` (rejects writes with `503`, still receives replication) |
| `NOS_ASSIGNMENT_RULES` | JSON rules file path or inline JSON |
| `NOS_DEFAULT_STORAGE_CLASS` | Default class when no rule matches |
| `NOS_ASSIGNMENT_FORWARD` | When `true`, proxy PUT/copy/multipart writes to the assigned peer instead of `409`, and send single-object deletes to every node (placement rules may depend on `Content-Type` or size, which a DELETE lacks). Only requests with a bearer token can be forwarded; presigned and SigV4 requests get `409` |
| `NOS_REPLICATION_ASYNC` | Must be `true` (default); `false` is rejected at startup (quorum deferred) |
| `NOS_REPLICATION_READ_REPAIR` | When `true`, fetch missing blobs from peers on GET |
| `NOS_REPLICATION_HEAL_ON_READ` | When `true` with read repair, persist peer bytes locally on GET miss (heal-on-read) |
| `NOS_REPLICATION_PREFIXES` | Comma-separated key prefixes to replicate (empty = all keys) |
| `NOS_REPLICATION_EXCLUDE_PREFIXES` | Comma-separated prefixes to skip (wins over include) |
| `NOS_REPLICATION_MAX_ATTEMPTS` | Push attempts before dead-letter (`20` default) |
| `NOS_REPLICATION_PEER_CONCURRENCY` | Events delivered in parallel by the replication worker (`4` default) |
| `NOS_CLUSTER_BOOTSTRAP_TOKEN` | Operator token for `GET`/`PUT /_cluster/config` before cluster is configured (optional) |
| `x-nd-storage-class` | Optional client header (ignored in standalone) |

Multi-node local dev: `docker compose --profile cluster up --build` (hot on port 9001, cold on 9002; default single-node service unchanged on 9000).

Peer list format: `node-b=http://host:9000;class-a,class-b;group=replication-group` (classes and group are optional).

### Replication lag and consistency

Replication is **asynchronous** (`NOS_REPLICATION_ASYNC=true` by default). A successful PUT on the origin node does not guarantee immediate visibility on peers; monitor `replication_lag_events` on `GET /health` or `replication_pending_events` on `GET /metrics`. Replication events carry `content_type` and `custom_meta` so peers apply the same metadata as the origin.

- **Versions:** every change to a replicated key is versioned when it commits (the origin's clock in microseconds, then the node id). A node applies a replicated change only when it is newer than what it has, so a late retry can't overwrite newer data and an older put can't undo a delete; deletes leave a tombstone for that. Only the newest queued change per key is sent; older ones are marked `superseded`.
- **Delivery:** an event goes to the peers still missing it until `NOS_REPLICATION_FACTOR - 1` have it. One that falls short retries with exponential backoff (up to an hour) and moves to dead-letter after `NOS_REPLICATION_MAX_ATTEMPTS`; replay it with `POST /_nos/maintenance/replication_replay?event_id=`. A peer that can't be reached is skipped for the rest of a delivery batch, and every peer request is bounded by timeouts.
- **History:** sent, superseded and applied events, and tombstones, are pruned after 7 days; pending, failed and dead-letter events are kept.
- `GET /_nos/maintenance/replication_status` (admin JWT) reports pending, failed, dead-letter, sent, applied and superseded counts. `POST /_cluster/replication/backfill?limit=&start_after=` (cluster token) enqueues one batch of existing objects that match prefix rules; repeat with `start_after` set to the response's `next_start_after` while `is_truncated` is `true`.

**Read repair and heal:** With `NOS_REPLICATION_READ_REPAIR=true`, a GET on a peer that lacks the object streams from another peer without persisting. With `NOS_REPLICATION_HEAL_ON_READ=true`, the peer writes the fetched bytes locally first (heal-on-read). Periodic `POST /_nos/maintenance/verify_blobs` can attempt peer recovery when checksum verification fails and `NOS_REPLICATION_FACTOR` > 1. A peer's copy is used only when its bytes match its ETag (and, when restoring damaged bytes, the version this node's metadata names), and a key this node deleted is never served or restored from a peer that hasn't applied the delete yet.

**Runtime configuration:** a configuration saved through `PUT /_cluster/config` replaces the `NOS_CLUSTER_*` environment settings at every start (the server logs a warning when both exist); reloading it stops the old configuration's replication worker once the new one runs.

### Split-brain and dual writes

There is no distributed lock. Two clients writing the same `bucket/key` on different nodes converge on the change with the later version (wall-clock time, then node id) once replication catches up; until then each node serves its own. Use **assigned** mode plus client routing (e.g. Ownly) to steer writes, or **readonly** replicas for read scaling. Symmetric `NOS_CLUSTER_PEERS` lists are recommended; the server logs a warning if `NOS_NODE_ID` is missing from the local peer list. Requests one node forwards to another carry `x-nd-forwarded` and are never forwarded or fanned out again.

### Blob storage format (NOSI)

New writes use the **indexed block** layout (`NOSI` magic):

- Fixed header: logical size, block size, block count, optional dict id, stored zstd level
- Per-block index for seek/range without decoding the whole object
- Per-block xxh3 checksums; optional dedup refs into content-addressed `.blocks/` (NOSK-compressed when smaller)
- Each inline block is independently zstd-compressed or stored raw, whichever is smaller

Incompressible types (e.g. `video/*`, `.mp3`, `.zip`) and objects below `NOS_COMPRESS_MIN_SIZE` stay as raw bytes with no header. If block compression does not shrink the payload, the raw file is kept.

**Legacy formats** (`NOSB`, `NOSZ`, `NOS2`, `NOSD`) remain readable. Background recompression (`NOS_RECOMPRESS_*`) migrates them to `NOSI` and upgrades upload-level blobs to `NOS_ZSTD_LEVEL` / the newest trained dictionary when enabled. Each zstd frame names the dictionary it was compressed with, so blobs written with an older dictionary stay readable after retraining. The byte-level layouts are described in [docs/storage-format.md](docs/storage-format.md).

**Integrity:** `POST /_nos/maintenance/verify_blobs` (admin JWT) supports `?mode=light|deep&sample_denom=&start_after=` and returns `next_start_after` / `is_truncated` for the next batch; periodic scrub uses `NOS_SCRUB_*` with a rotating key cursor. `NOS_VERIFY_ON_READ` checks raw blob etags on full-object GET.

## HTTP API

See [docs/openapi.yaml](docs/openapi.yaml) for the full contract.

ETags are the xxh3-64 of the object's content (16 hex digits), not an MD5. S3 tools treat them as opaque; one that insists on MD5 ETags needs that check turned off. For end-to-end upload checks send `Content-MD5` (or sign the payload with SigV4): the server verifies it before committing the object and answers `400` on a mismatch.

| Method | Route | Auth | Description |
|--------|-------|------|-------------|
| `PUT` | `/:bucket/*key` | Bearer JWT | Stream upload; `If-Match` / `If-None-Match`; `x-nd-copy-source` for server-side copy (caller needs read access to the source bucket; not available to presigned URLs) |
| `GET` | `/:bucket/*key` | Bearer JWT or presigned query | Download (single `Range` incl. suffix ranges → `206`, unsatisfiable → `416`; `If-None-Match`) |
| `HEAD` | `/:bucket/*key` | Bearer JWT | Metadata + `x-nd-custom-meta-*` headers |
| `DELETE` | `/:bucket/*key` | Bearer JWT | Soft delete (or hard delete when TTL is `0`); optional `If-Match` |
| `GET` | `/:bucket` | Bearer JWT | List objects with pagination + delimiter prefixes |
| `POST` | `/:bucket/_multipart?key=...` | Bearer JWT | Init multipart upload |
| `PUT` | `/:bucket/_multipart/{upload_id}/parts/{n}` | Bearer JWT | Upload part |
| `POST` | `/:bucket/_multipart/{upload_id}/complete` | Bearer JWT | Complete multipart upload; optional body `{"parts":[{"part_number":1,"etag":"..."}]}`, otherwise parts must be contiguous from 1 |
| `DELETE` | `/:bucket/_multipart/{upload_id}` | Bearer JWT | Abort multipart upload |
| `GET` | `/health` | None | Liveness check (process up) |
| `GET` | `/health/ready` | None | Readiness check (metadata backend + `NOS_DATA_DIR` writable; `postgres_ok` when using Postgres) |
| `GET` | `/_nos/capabilities` | Bearer JWT | Node limits and cluster mode (when enabled) |
| `GET` | `/metrics` | Optional Bearer (`NOS_METRICS_TOKEN`) | JSON or Prometheus (`Accept: text/plain`) |
| `GET` | `/_nos/maintenance/orphans` | Admin JWT | Blob files without a metadata row (`bucket`, `prefix`, `limit`) |
| `POST` | `/_nos/maintenance/gc_orphans` | Admin JWT | Remove orphan blob files |
| `POST` | `/_nos/maintenance/verify_blobs` | Admin JWT | Integrity scrub batch (`mode`, `sample_denom`, `start_after`, `limit`) |
| `POST` | `/_nos/maintenance/migrate_blobs` | Admin JWT | Move legacy layouts/formats to the current ones (`limit`, `start_after`) |
| `POST` | `/_nos/maintenance/train_dictionary` | Admin JWT | Train a new zstd dictionary for new writes (`400` unless `NOS_ZSTD_DICT_ENABLED`) |
| `GET` | `/_nos/maintenance/replication_status` | Admin JWT | Replication queue counts (zero in standalone mode) |
| `POST` | `/_nos/maintenance/replication_replay` | Admin JWT | Re-queue a dead-letter event (`event_id`) |

Cluster routes (Bearer `NOS_CLUSTER_TOKEN`, or `NOS_CLUSTER_BOOTSTRAP_TOKEN` until configured): `GET`/`PUT /_cluster/config`, `GET /_cluster/health`, `POST /_cluster/replicate`, `POST /_cluster/assignment/resolve`, etc.

## Use as a Rust dependency

**Git:**

```toml
[dependencies]
nebular-os = { git = "https://github.com/AsP3X/nebular-os.git", tag = "v0.1.3" }
```

**Path (local development):**

```toml
[dependencies]
nebular-os = { path = "../nebular-os" }
```

The library crate name is `nebular_os`:

```rust
use std::sync::Arc;

use nebular_os::config::NosConfig;
use nebular_os::cluster::build_backend;
use nebular_os::observability::NosMetrics;
use nebular_os::server::create_app;
use nebular_os::storage::engine::StorageEngine;

// Embedders must share one NosMetrics instance between build_backend and create_app:
let cfg = Arc::new(NosConfig::from_env()?);
let storage = StorageEngine::new(&cfg.meta_path, &cfg.data_dir).await?;
let metrics = NosMetrics::new();
let backend = build_backend(storage.clone(), &cfg.cluster, metrics.clone())?;
let app = create_app(backend, storage, cfg, metrics).await?;
```

**Breaking change (cluster branch):** `build_backend(engine, &cluster, metrics)` and `create_app(backend, engine, cfg, metrics)` require a shared `Arc<NosMetrics>`.

Object **list** JSON and **GET** response headers may include `storage_class` and `origin_node` when set in metadata (`x-nd-storage-class`, `x-nd-origin-node` on GET).

## Development

```bash
cargo test
cargo build --release
```

## Docker

Published images (linux/amd64 and linux/arm64) are on GitHub Container Registry:

```bash
docker pull ghcr.io/asp3x/nebular-os:latest
```

Tagged releases publish standalone binaries on
[GitHub Releases](https://github.com/AsP3X/nebular-os/releases). Push a `v*` tag
to run the release workflow. Assets are named `nebular-os-<version>-<platform>`
(`.exe` on Windows) with a `SHA256SUMS.txt` checksum file.

| Platform | Architectures |
|----------|---------------|
| Linux | x86_64, aarch64, i686, armv7, riscv64, ppc64le, s390x |
| Windows | x86_64, aarch64, i686 |
| macOS | x86_64 (Intel), aarch64 (Apple Silicon) |

Example (Linux x86_64):

```bash
VERSION=0.1.3
curl -LO "https://github.com/AsP3X/nebular-os/releases/download/v${VERSION}/nebular-os-${VERSION}-linux-x86_64"
chmod +x "nebular-os-${VERSION}-linux-x86_64"
./nebular-os-${VERSION}-linux-x86_64
```

```bash
docker build -t nebular-os .
docker run --rm -p 9000:9000 \
  -e NOS_JWT_SECRET='your-32-char-or-longer-secret-here!!' \
  -e NOS_SIGNING_SECRET='another-32-char-or-longer-secret!!' \
  -v nebular_data:/data \
  nebular-os
```

The server runs as the unprivileged user `nos` (uid/gid `10001`), with data under the `/data` volume (`NOS_DATA_DIR=/data/blobs`, `NOS_META_PATH=/data/meta/metadata.db`). The entrypoint starts as root only to create the data directories and hand them to `nos` — including volumes written by earlier images, which ran as root, on their first start. That full pass is recorded by a `.nos-owned` marker in each directory, and later starts only check the top level; delete the marker to run the pass again. Started with `--user <uid>`, the image changes no ownership and runs as that user.

## Scale notes

- SQLite uses separate read/write connection pools (`NOS_READ_POOL_SIZE`) for concurrent listing and downloads.
- Set `NOS_METADATA_BACKEND=postgres` and `NOS_METADATA_DATABASE_URL` for blob-only nodes (object index in `nos_*` tables; replication log stays in sidecar SQLite at `NOS_META_PATH`).
- `GET /metrics` exposes `logical_bytes`, `max_logical_bytes`, and `metadata_backend` for Ownly placement and admin UIs.

## License

**Nebular OS is source-available, not open source.** It is licensed under the
[Nebular OS Private Non-Commercial License (NOCL-1.0)](LICENSE).

### Free use (no fee)

You may use Nebular OS **without a commercial license** only when **all** of the
following apply:

- **Private** — not distributed, not published as open source, not offered as a
  hosted service to third parties, and not embedded in a product offered to others
- **Non-profit** — you are an individual in a personal, non-commercial capacity,
  or a registered non-profit organization not controlled by a for-profit entity
- **Non-commercial** — not primarily for monetary or commercial advantage

This covers self-hosted deployment and use as a Rust library dependency (`nebular_os`)
in qualifying private projects.

### Commercial license required

Any other use requires a **written Commercial License Agreement** and **fee**,
including but not limited to:

- Use by or for a **for-profit company** (even internal/private repositories)
- **Distribution** (public forks, packages, binaries, open source release)
- **Hosted services** for customers, tenants, or users
- Products or services that incorporate or expose Nebular OS to third parties

See [COMMERCIAL-LICENSE.md](COMMERCIAL-LICENSE.md) and open a
["Commercial License Request" issue](https://github.com/AsP3X/nebular-os/issues/new)
(contact channel only; not a license tracker).

### Bug and security reports

Report bugs, defects, and security vulnerabilities via the
[Project Issue Tracker](https://github.com/AsP3X/nebular-os/issues/new).
Exploiting vulnerabilities or using them for illegal activity is prohibited.
See [LICENSE](LICENSE) Sections 6.4–6.5.

### Liability and prohibited use

Licensor is **not liable** for misuse, damages, data loss, or security incidents
arising from your use of Nebular OS. Use by **terrorist organizations** and for
**illegal activity** is prohibited.

### Licensor

Niklas Vorberg retains unrestricted rights to the Software. Copyright notice:
[NOTICE](NOTICE).
