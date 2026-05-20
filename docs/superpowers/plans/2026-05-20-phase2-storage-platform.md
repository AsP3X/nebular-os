# Phase 2 Storage Platform Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend Nebular OS with multipart uploads, conditional GET, metadata round-trip, reconciliation, S3-like API gaps, security/ops hardening, scale-oriented storage paths, and developer observability—without breaking existing JSON error contracts.

**Architecture:** Add focused modules under `src/storage/` (multipart, reconcile, copy) and `src/middleware/` (rate limit, metrics auth). Extend SQLite schema via engine init migrations. Wire new routes in `server.rs`; expose Prometheus at `GET /metrics` (JSON legacy + `Accept: text/plain` Prometheus). Use `governor` for per-IP limits and a second SQLite pool for read-heavy queries.

**Tech Stack:** Rust / Axum 0.8, SQLx SQLite, `governor` + `tower_governor`, `metrics` + `metrics-exporter-prometheus`, GitHub Actions.

---

## File map

| Area | Create | Modify |
|------|--------|--------|
| Multipart | `src/storage/multipart.rs`, `src/routes/multipart.rs` | `src/storage/mod.rs`, `src/server.rs` |
| Reconcile | `src/storage/reconcile.rs` | `src/main.rs`, `src/config.rs` |
| Copy / soft delete | — | `src/storage/engine.rs`, `src/storage/types.rs` |
| Conditional GET / meta / range | — | `src/routes/object.rs` |
| Metrics | `src/metrics.rs` | `src/routes/metrics.rs`, `src/server.rs` |
| Rate limit | `src/middleware/rate_limit.rs` | `src/server.rs`, `src/lib.rs` |
| Config | — | `src/config.rs`, `.env.example`, `README.md` |
| OpenAPI | `docs/openapi.yaml` | `README.md` |
| CI | `.github/workflows/ci.yml` | — |
| Tests | — | `tests/integration.rs` |

---

## Task 1: Config and schema foundation

**Files:** `src/config.rs`, `src/storage/engine.rs`, `.env.example`

- [ ] Add env: `NOS_RECONCILE_ON_STARTUP`, `NOS_RECONCILE_INTERVAL_SECS`, `NOS_SOFT_DELETE_TTL_SECS`, `NOS_METRICS_TOKEN`, `NOS_RATE_LIMIT_RPS`, `NOS_RATE_LIMIT_BURST`, `NOS_LIST_SCAN_CAP`, `NOS_MULTIPART_PART_SIZE`, `NOS_READ_POOL_SIZE`, `NOS_CORS_ORIGINS`
- [ ] Schema: `objects.deleted_at`, tables `multipart_uploads`, `multipart_parts`
- [ ] Second pool `read_pool` for SELECT paths

---

## Task 2: Multipart uploads

**API:**
- `POST /{bucket}/_multipart/{*key}` → `{ "upload_id", "part_size" }`
- `PUT /{bucket}/_multipart/{*key}/{upload_id}/parts/{part_number}` → `{ "etag" }`
- `POST /{bucket}/_multipart/{*key}/{upload_id}/complete` → `{ "etag" }`
- `DELETE /{bucket}/_multipart/{*key}/{upload_id}` → abort

**Tests:** init → 2 parts → complete → GET object; abort cleans temp

---

## Task 3: Conditional GET + custom metadata + suffix ranges

- [ ] `If-None-Match` / `If-Modified-Since` → 304 empty body on GET/HEAD
- [ ] Emit `x-nd-custom-meta-*` from stored JSON on GET/HEAD
- [ ] `parse_range` supports `bytes=-N` suffix form

**Tests:** 304 with matching etag; metadata header round-trip; suffix range body

---

## Task 4: Copy, soft delete, reconciliation

- [ ] `x-nd-copy-source: {bucket}/{key}` on PUT copies blob+metadata server-side (`copy_file_range` / async copy)
- [ ] DELETE sets `deleted_at`; GET/HEAD/LIST exclude soft-deleted; purge past TTL on interval
- [ ] `reconcile()` on startup + optional interval

**Tests:** copy object; soft-deleted not visible; purge removes; reconcile removes orphan blob

---

## Task 5: Security and ops

- [ ] `/metrics` requires `Authorization: Bearer <NOS_METRICS_TOKEN>` when token set
- [ ] CORS from `NOS_CORS_ORIGINS` (comma-separated) instead of permissive when set
- [ ] `GovernorLayer` per peer IP on protected routes
- [ ] `.github/workflows/ci.yml`: test + clippy -D warnings

---

## Task 6: Scale and observability

- [ ] Configurable `NOS_LIST_SCAN_CAP`; list uses read pool
- [ ] `GET /metrics` Prometheus text via `metrics` crate; JSON when `Accept: application/json`
- [ ] `docs/openapi.yaml` documents all routes and `{ "error" }` shape

**Tests:** metrics 401 without token; prometheus body contains `nos_` prefix counters

---

## Verification (every task)

```bash
cargo clippy --all-targets -- -D warnings
cargo test
```

---

## Deferred (document only in README)

- **Postgres migration:** out of scope; SQLite dual-pool is the v0.2 read-scale step.
