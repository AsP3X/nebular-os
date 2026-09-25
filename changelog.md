# Changelog

All notable changes to Nebular OS are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Upgrading from 0.1.4

- Existing data is read as it is: NOS2 compressed blobs (with or without the trained dictionary), NOSD dedup manifests and raw blobs. New writes, recompression and `POST /_nos/maintenance/migrate_blobs` store the block-indexed NOSI format. `tests/fixtures/v0.1.4` holds storage written by the released 0.1.4 binary, and a test checks on every run that all of it reads back, scrubs clean and survives recompression and migration.
- **Don't roll back to 0.1.4** once this version has written or rewritten objects. 0.1.4 doesn't recognise NOSI and answers `200` with the stored container bytes instead of the object. Back up the data directory and metadata database before upgrading.
- The first start builds a metadata index (see "Maintenance walks use an index" below).
- Blobs written by unreleased `master` builds are read too: NOSB (block-compressed, before NOSI) is covered by `tests/fixtures/nosb`, written by the last build that produced it.
- **Access keys:** the `Authorization: NOS <key>:<sig>` scheme is now off by default. It signs only method and bucket and never expires. AWS Signature V4 replaces it (see Security). Set `NOS_LEGACY_ACCESS_KEY_AUTH=true` to keep the old scheme while clients move over. Access-key requests still act as `admin` unless `NOS_S3_ACCESS_KEY_ROLE` says otherwise.
- **Secrets** with fewer than 8 distinct characters (e.g. 32 × `a`) are now refused at startup, like the known placeholder values.
- **Docker:** the image runs the server as the unprivileged user `nos` (uid/gid `10001`). On its first start, the entrypoint gives the `/data` volume to that user; earlier images wrote it as root. Started with `--user`, the image changes no ownership. `NOS_DATA_DIR` and `NOS_META_PATH` now default to `/data/blobs` and `/data/meta/metadata.db` in the image; the binary's defaults are relative paths the unprivileged user can't write.
- **Postgres:** the first start records the schema in a new `nos_schema_migrations` table. It then builds the new key index in the background with `CREATE INDEX CONCURRENTLY`, so writes aren't blocked.
- **Cluster modes:** upgrade every node. Replication is now versioned per key (see "Fixed — cluster replication"); nodes on 0.1.4 apply events unconditionally and ignore versions. `NOS_REPLICATION_FACTOR` now defaults to `2` in replicating modes. A node in `replicated` mode used to copy nothing to peers unless it was set.
- **Multipart uploads** follow S3's limits: completing without a part list requires parts numbered 1..N without gaps (earlier releases assembled whatever parts existed, so a part that failed to upload silently went missing), and part numbers above 10,000 are refused. Send the part list to complete an upload with gaps.
- The minimum Rust version is 1.88 (`rust-version` in `Cargo.toml`).

### Security

- **Server-side copy checks the source.** `x-nd-copy-source` / `x-amz-copy-source` now require read access to the source bucket (role and `NOS_BUCKET_POLICY`). Previously any caller that could write the destination could copy any object from any bucket, including through a presigned PUT URL. Presigned URLs can no longer copy at all, and a copy-source header that is not `bucket/key` returns `400` instead of silently falling through to a plain upload.
- **No more `sub: "presigned"` bypass.** Any JWT whose subject was `presigned` skipped role and bucket-policy checks, so a read-only token could PUT and DELETE anywhere. Presigned requests are now identified by how they authenticated, and JWTs carrying that reserved subject are rejected with `401`.
- **Presigned URLs address exactly one object.** A post-routing path normalizer showed auth a different key than the handler used, so a URL signed for `a` could write `a/`; it is removed (it never affected routing). Presigned auth is now accepted only on the object route `/{bucket}/{key}` — list, prefix/batch delete and multipart are steered by query strings or bodies the signature doesn't cover, so they require a JWT. Lifetimes are capped by `NOS_PRESIGN_MAX_TTL_SECS` (default 7 days plus 5 minutes of clock skew, `0` = unlimited).
- **Bucket names can't reach system directories.** Names starting with `.` or containing `/` (e.g. `%2F`), `\` or control characters are rejected. They could land on `.tmp` (swept hourly), `.blocks` or `.multipart`, nest inside another bucket's shards, or — as `.nos-ready-probe` — break readiness.
- **Rate limiting counts per client IP**, not per connection (the key included the source port, so every connection got a fresh budget), and evicts idle clients. Failed authentication now spends a per-IP budget too (it was never throttled): once it is empty, further failures get `429`, while valid credentials from the same IP always pass, so clients behind a shared proxy can't lock each other out. Still off unless `NOS_RATE_LIMIT_RPS` > 0.
- **Objects are served with `X-Content-Type-Options: nosniff`**, and everything except plainly passive types (non-SVG images, audio/video, PDF, `text/plain`) additionally with `Content-Security-Policy: sandbox`, so user uploads opened through presigned links can't run script in Nebular's origin — including HTML hidden behind type lists like `text/plain, text/html` or `+xml` types.
- **Connections that never finish a request are closed.** axum 0.8's `serve` never gives hyper a timer, so hyper's header-read timeout never applied. A client could hold a connection open indefinitely by sending headers slowly or not at all, or by leaving a keep-alive connection idle, until the process ran out of file descriptors. Nebular now runs its own HTTP/1 accept loop with hyper's timer: request headers must arrive within `NOS_HEADER_READ_TIMEOUT_SECS` (default `75`, `0` = off), counted from when the connection opens. Keep-alive connections idle for that long are closed. Keep the value above the idle timeout of any proxy or client pool in front of Nebular (AWS ALB defaults to 60 s).
- **Presigned signatures stay out of logs.** The request span logged the full URI, including `signature=…`, on every line of a request under the default filter. Spans now record the path only.
- **AWS Signature V4 for access keys.** Header-signed and presigned SigV4 requests are verified against `NOS_S3_ACCESS_KEY` / `NOS_S3_SECRET_KEY`, so AWS SDKs, `curl --aws-sigv4` and other S3 tools can authenticate.
  - Every `x-amz-*` and `x-nd-*` header sent must be signed.
  - Requests must be within 15 minutes of their `X-Amz-Date`, and presigned SigV4 URLs are capped at 7 days.
  - A signed payload hash binds the body. Uploads are checked while they stream, other bodies up to 1 MiB before the request runs; a mismatch fails with `400` before anything is stored.
  - Presigned SigV4 URLs work only on object routes (`/{bucket}/{key}`), like Nebular's own presigned URLs. Their payload is unsigned, so a URL presigned for a batch delete would have let whoever held it delete any key in the bucket.
  - The old `NOS` scheme is off by default (see Upgrading).
- **JWT issuer and audience checks.** With `NOS_JWT_ISSUER` / `NOS_JWT_AUDIENCE` set, tokens must carry that `iss` / `aud`. Without them, any service holding the shared HS256 secret could mint tokens this node accepts.
- **Objects system routes would shadow are refused.** Requests for them reach a system endpoint instead, so they could never be read back: anything in the buckets `_nos` and `_cluster`, `health/ready`, and the keys `_batch_delete` and `_multipart…` paths within any bucket. Other objects in buckets named `health` or `metrics` are unaffected; only listing those buckets is shadowed.
- **Connection limits.** A connection whose client stops reading a response is closed after `NOS_SEND_STALL_TIMEOUT_SECS` (default `300`). Each stalled download of a compressed object held a few MB of buffers and a file handle indefinitely. `NOS_MAX_CONNECTIONS` caps simultaneous connections.
- **Graceful shutdown.** The server ignored SIGTERM: as PID 1 in a container nothing handled it, so `docker stop` and rolling updates waited out the kill timeout and cut requests off. On SIGTERM or Ctrl-C it now stops accepting connections, lets requests in progress finish for up to `NOS_SHUTDOWN_GRACE_SECS` (default `8`), and exits.
- **`/metrics` can't be used to load the database.** Every scrape scanned the metadata tables, and the endpoint is open unless `NOS_METRICS_TOKEN` is set. Totals are now computed at most once every 10 seconds, and concurrent scrapes share one computation.
- **Multipart part numbers are limited to 1–10000**, like S3.
- **The Docker image no longer runs as root** (see Upgrading). The ownership pass over `/data` runs until one has completed, which a `.nos-owned` marker records; later starts check only the directory's top level, so a large volume doesn't delay startup.
- **Dependencies:** `lru` (two RustSec "unsound" advisories) is replaced by `hashlink`. A non-blocking `cargo audit` job runs in CI.

### Fixed — data integrity

- **Objects written by 0.1.4 are readable again.** Every compressed object that 0.1.2–0.1.4 wrote is a NOS2 blob. The unreleased block-format work read 12 header bytes where NOS2 needs 16, so it could no longer stream those objects: GETs broke off mid-response, and scrub and blob migration, which now decode through the same path, failed on them. Reads also loaded the zstd dictionary only while `NOS_ZSTD_DICT_ENABLED` was on, so turning the setting off made dictionary-compressed objects unreadable ("Dictionary mismatch"). Reads now always load the dictionary.
- **Overwrites are atomic and durable.** A new version is encoded into `.tmp/`, fsynced, and renamed over the blob path only after preconditions and the storage cap pass; metadata is committed next, and the previous version's dedup refs are released only after that. Before, the old blob was deleted first and the new one written in place (raw uploads via a full `fs::copy`), so a crash lost the object and concurrent GETs saw `404` or torn bodies. If the metadata write fails, the previous blob is restored (from a hard link, or a copy where links aren't available). `NOS_FSYNC_WRITES=false` skips the fsyncs. Blob and metadata are still two writes, so each overwrite is journaled in `.tmp/`. If the process dies between the rename and the metadata commit, the next start puts the previous blob back — but only while the metadata still names that previous version, so a later write is never undone. A client that disconnects can no longer interrupt a write halfway: requests now run to completion (hyper used to drop a handler as soon as its connection closed). GET and HEAD read the metadata and open the blob under the object's lock, so they never pair one version's bytes with another's metadata.
- **Per-object write locking.** PUT, copy, delete, bulk/prefix delete, multipart complete, soft-delete purge, reconcile and maintenance rewrites of the same object are serialized in-process. `If-Match` / `If-None-Match` are now checked under that lock (eight concurrent `If-None-Match: *` creators all succeeded before; now exactly one does). Bulk deletes lock 64 keys at a time, so a large delete doesn't stall reads and writes of unrelated objects.
- **A PUT refused by the storage cap no longer destroys the existing object**, and concurrent writes can't overshoot `NOS_MAX_LOGICAL_BYTES` together: each write reserves its room under a short lock and holds the reservation until it commits.
- **Uploads that start with a Nebular format magic** (`NOSI`, `NOSB`, `NOSZ`, `NOS2`, `NOSD`) are stored wrapped in NOSI instead of raw, so they read back intact; before, GET failed or returned `500` while HEAD said `200`. Objects already stored raw with such a prefix are readable again: every container records the logical size at bytes 4–12, and a file whose header disagrees with the object's size, but whose length matches it, is read as raw.
- **Multipart completion is verified.** Parts are received into scratch files and renamed only when complete, so a dropped retry can't truncate a good part. Complete accepts an optional `{"parts":[{"part_number":N,"etag":"..."}]}` body (S3 semantics); any other body is ignored as before. Without a part list, stored parts must be contiguous from 1 (clients that upload non-contiguous part numbers must now send the list). Every part is checked against its recorded size and ETag while assembling; mismatches return `400` naming the part.
- **Background jobs no longer delete or roll back live data.** Soft-delete purge removes a row only if it is still soft-deleted past the TTL (a re-created key used to be purged along with its new blob). Reconcile re-checks metadata under the key lock before removing a "stale" row or an "orphan" blob, so uploads that commit during a scan survive, and it now releases dedup refs for removed orphans. The `.tmp` janitor also counts a file's inode change time, so staging/backup hard links (which inherit an old blob's mtime) are no longer deleted while in use. Recompression and blob migration only swap in their output if the object is unchanged since it was read.
- **Dedup ref accounting in blob migration**: relocating a legacy blob released refs the object still held, and re-encoding never counted the new blob's refs, which could delete blocks still in use.
- **Prefix matching is case-sensitive on SQLite.** `LIKE` ignored ASCII case, so deleting prefix `users/` also deleted `Users/…`, and delimiter listings over such keys could panic.
- **Keys that differ only in case no longer overwrite each other on macOS and Windows.** On case-insensitive filesystems, `Photo.jpg` and `photo.jpg` mapped to the same blob file: writing one replaced the other's bytes while both metadata rows remained. The engine detects such filesystems at startup and then encodes uppercase and other ambiguous characters in filenames. Existing blobs are still found under their old names (only when the name on disk matches exactly, never a case-alike neighbour's file), and rewrites move them. Blobs are found under either naming scheme on any filesystem, so a data directory moved from macOS or Windows to Linux, or back, stays readable. Linux filesystems are otherwise unaffected.
- **End-to-end upload checks.** `Content-MD5` on PUT and multipart parts was ignored. It is now verified while the body streams, and a mismatch fails with `400` before anything is committed. The same applies to a SigV4-signed payload hash.
- **Deep scrub checks the object, not just its blocks.** A blob from the wrong version of an object passed deep scrub, because every block matched its own checksum. Deep scrub now also hashes the decoded content against the object's ETag.
- **Deletes remove metadata before bytes.** A crash in between leaves an unreferenced file for orphan GC, never a listed object without bytes.
- **Dedup (`NOS_DEDUP_ENABLED`) can't mix up content.**
  - Blocks are addressed by a 64-bit non-cryptographic hash in one store shared by all buckets, and an existing block was reused without comparing bytes. A collision, accidental or crafted, could therefore serve another object's content. A block is now reused only when its bytes are identical; otherwise the new block stays inside its own blob.
  - Block reads verify the bytes against their address.
  - Reference counts are decremented atomically. An unreferenced block is deleted only after an hour, and only if nothing used it again meanwhile. Before, a delete racing an upload that was sharing the block could remove it.

### Fixed — cluster replication (experimental)

- **Replicas received Nebular's on-disk container instead of the object.** The worker streamed the blob file, so every compressible object was stored on peers as NOSI bytes and served back that way. It now streams the object's logical bytes (and a JSON put without a body reads its payload through the same decode path); the wire checksum is the object ETag, which is the xxh3 of exactly those bytes — also for events queued before the upgrade, which hashed the on-disk container.
- **Replicated objects of any size.** The receiver spools payloads to disk while hashing instead of buffering them; axum's 2 MiB multipart default refused anything larger, and buffered payloads were capped at `NOS_MAX_BODY_SIZE`.
- **HEAD uses read repair like GET** in replicated mode, instead of answering `404` for objects GET serves from a peer.
- **Changes are versioned per key.** A late retry could overwrite newer data, and an older put arriving after a delete brought the object back.
  - Every change to a replicated key now gets a version when it commits: the origin's clock in microseconds, then its node id. Versions are recorded in the same write lock as the change, and deletes leave a tombstone.
  - A node applies a replicated change only when it is newer than its own version of the key. An older one is acknowledged and dropped.
  - Nodes that accept writes for the same key converge on the later write (keep clocks synchronized).
  - Only the newest queued change per key is sent; older ones are marked `superseded`.
- **Read repair and heal honour deletes.** A node that deleted a key served it from, and heal-on-read restored it from, a peer that hadn't applied the delete yet. Keys with a tombstone are now answered with `404`.
- **Delivery no longer stalls or loops.**
  - An event that reached some peers but not enough stayed pending: it was resent to every peer each second, never dead-lettered, and 32 such events blocked the queue for good. The worker now records which peers have each event and retries only the others, with exponential backoff up to an hour. Such events dead-letter after `NOS_REPLICATION_MAX_ATTEMPTS`.
  - A peer that can't be reached is skipped for the rest of a batch.
  - Batches drain until nothing is due; the worker used to send at most 32 events a second.
  - Delivery runs `NOS_REPLICATION_PEER_CONCURRENCY` events in parallel; the setting was ignored before.
- **Peer requests have timeouts.** Replication, read repair, heal, forwarding and health checks used HTTP clients without any, so one peer that accepted connections but never answered stalled replication for good. Connecting is now bounded to 10 s, and every wait for a response to 60 s of silence. Pushes and forwarded uploads instead get a total budget of a minute plus a second per MiB, since an upload can take longer than any silence limit. Shared clients reuse connections.
- **A config reload no longer stops replication.** `PUT /_cluster/config` bumped a process-wide worker generation after the new backend's worker had started. That could stop the new worker too, leaving the node replicating nothing. Each backend now stops only its own worker and health checks when it is replaced, or when it is dropped. A configuration is validated (peers, assignment rules) and its backend built before it is saved, so a rejected one isn't persisted and can't stop the next start.
- **Heal spools peer copies to disk** instead of holding whole objects in memory. A copy's version is recorded only when the peer is still serving that version, so a write racing the transfer can't label older bytes with its newer version.
- **Keys with `#`, `?`, `%` or spaces reach peers.** Peer URLs were built from raw keys, so such keys were cut short or misrouted in read repair, heal and forwarding.
- **Assigned mode:**
  - Symmetric peer lists sent prefix and batch deletes back and forth between nodes until a hop failed. Forwarded requests now carry `x-nd-forwarded` and are never forwarded or fanned out again.
  - A single-object DELETE on a node the object isn't assigned to reported success while the object stayed on its node. With forwarding on, it now goes to every node, because placement rules can depend on `Content-Type` or size, which a DELETE lacks. If-Match is honoured where the object is.
  - Conditional writes that are forwarded have their preconditions evaluated by the owning node, not by the forwarding node's (absent) copy.
  - Only requests with a bearer token are forwarded. A presigned URL or a SigV4 signature covers the original request, not the forwarded one, so such requests failed with `500`; they now get `409` (not assigned), and prefix and batch deletes list the other nodes as not asked.
  - The owning node's answer reaches the client: a forwarded `If-None-Match: *` on an existing key now returns `412` (it was a `500`). A read-only replica is skipped by delete fan-outs, since replication brings it the delete.
  - Keys with `.` or `..` segments are refused (`400`) before anything is deleted. URL parsing resolves such segments, so `users/./b` reached `users/b` on the other nodes.
  - If recording one key of a bulk delete for replication fails, the other keys are still recorded (their rows are already gone, so a retry wouldn't reach them).
- **Replication history is pruned.** Sent, superseded and applied events and delete tombstones are removed after 7 days. `replication_log` grew by one row per write forever.
- **Settings:**
  - `NOS_REPLICATION_FACTOR` defaults to `2` in replicating modes, and a factor of 1 logs a warning.
  - The server warns when a configuration saved through `PUT /_cluster/config` replaces `NOS_CLUSTER_*` environment settings.
- **Heal can't replace an object with a different version.** Scrub recovery and heal-on-read took whatever the first peer returned, so a lagging peer's older copy, or a truncated transfer, could replace the object. A peer copy is now used only if its ETag is the version this node's metadata names (or, with no local object, if nothing was written here meanwhile) and its bytes hash to that ETag. The write is conditional, so a concurrent client write wins.
- **Backfilled objects in the legacy nested layout replicate.** Events recorded the flat blob path, so the worker couldn't find those blobs and they dead-lettered. The worker now locates the blob when it sends.
- The bootstrap token only works until a cluster token is configured, and only for `/_cluster/config`, `/_cluster/health` and `/_cluster/capabilities` (it used to grant every cluster route, including raw object reads, forever). Cluster tokens are compared in constant time.

### Changed — memory

- **Compressed objects are encoded and served block by block.** Uploads used to collect every compressed block in RAM before writing, and every GET or Range GET of a block-compressed object read the whole blob into memory. Blocks are now streamed to disk behind a reserved header, and reads load only the header, index and the blocks they return. On a 200 MB compressible object peak server RSS went from 299 MB (PUT), 306 MB (Range GET) and 474 MB (full GET) to 18, 18 and 20 MB. The on-disk format is byte-for-byte unchanged.
- **Maintenance streams objects too.** Recompression, blob migration (`/_nos/maintenance/migrate_blobs`) and scrub read each whole blob into memory and re-encoded in memory; they now read headers only to decide, decode legacy containers to a scratch file, encode from files, and move raw blobs to the flat layout by hard link. Migrating a 200 MB legacy-layout video peaks at 12 MB RSS.
- **The decoded-block cache has a byte budget** (`NOS_BLOCK_CACHE_MAX_BYTES`, default 64 MiB) on top of its entry count, and full sequential reads consult it without filling it — one large download used to park every decoded block (256 × 1 MiB by default) and evict the range-read entries the cache exists for.

### Fixed

- **Scrub is trustworthy.** Light scrub reported healthy compressed objects as corrupt (it compared each block's logical length with the bytes left in the file; it now checks stored extents). Deep scrub decodes from disk rather than the decoded-block cache, which could mask on-disk corruption. A live row whose blob is missing is reported as corrupted (and healed from peers in replicated mode) instead of aborting the batch. One unreadable object no longer aborts blob migration either.
- **Conditional GET/HEAD follow RFC 9110.**
  - `If-None-Match` takes precedence over `If-Modified-Since`; a changed ETag could be answered `304`.
  - Any tag in an ETag list can match, and future `If-Modified-Since` dates are ignored.
  - `Last-Modified` is an IMF-fixdate (`… GMT`).
  - HEAD returns `404` whenever GET would. It answered `200` for rows whose blob was gone.
  - A HEAD `304` carries the object's `ETag` and `Last-Modified`.
  - `If-Range` is supported: a range is served only while the given ETag or date still matches, otherwise the full object.
- **Stale reads after overwrite.** The decoded-block cache was keyed by blob path and block number only, so overwriting a compressed object with same-size content could return the previous bytes under the new ETag. Entries are now keyed by each block's stored checksum. `NOS_BLOCK_CACHE_ENTRIES=0` now disables the cache as documented (it silently kept one entry).
- **Range GET.** `206` responses sent the full object size as `Content-Length`, so HTTP clients saw a truncated body. They now send the slice length. Unsatisfiable ranges return `416` with `Content-Range: bytes */<size>`. Malformed and multi-range headers return the full object with `200` instead of a mislabeled `206`.
- **Upload budget refused large uploads forever.** With the default `NOS_UPLOAD_MAX_IN_FLIGHT_BYTES` (32 MiB), every upload over ~30 MiB and every upload without `Content-Length` got `503 Retry-After: 1`, even on an idle node. An upload larger than the budget is now admitted when nothing else is uploading.
- **zstd dictionary (`NOS_ZSTD_DICT_ENABLED`).**
  - Once a dictionary was loaded, blocks that compressed better than 4:1 failed to decode.
  - Each training pass replaced the dictionary, which stranded every blob written with the previous one.
  - Dictionaries now get increasing ids (`.dict/{id}.zdict`) and are never replaced. Every zstd frame names its dictionary, and readers resolve it by that name. The periodic job trains the first dictionary once enough objects exist.
  - `POST /_nos/maintenance/train_dictionary` (admin) trains a newer dictionary when the data has changed. New writes use it, recompression moves blobs to it, and blobs written with older dictionaries keep decoding.
- CI: `cargo clippy --all-targets -- -D warnings` passes again. It had failed on every push to `master` since v0.1.4, so CI never reached the test step.
- **New keys whose on-disk filename would exceed 255 bytes** (each `/` is stored as `%2F`) are rejected with `400` instead of failing in the filesystem with `500`; existing objects with such keys in the legacy nested layout stay readable and deletable. Keys containing NUL are rejected.
- **Uploads that stall are cut off.** A request body that sends nothing for `NOS_UPLOAD_IDLE_TIMEOUT_SECS` (default `60`, `0` = off) is aborted with `408`, releasing its share of the upload budget. There were no timeouts before, so one silent client could hold the budget indefinitely.
- **Range requests on uncompressed objects seek.** A range was reached by reading and discarding every byte before it, so each seek near the end of a large video (videos are stored raw) read the whole file first. The spill file behind range reads of dedup (NOSD) objects is now also read from the requested offset.
- **A GET can no longer mix two blobs.** A GET detected the blob's format, then reopened the path to hash or stream it. An overwrite, or recompression swapping in a compressed copy, in between could return the new blob's container bytes as the object. Every read now goes through the handle it detected the format from.
- **Range requests on legacy NOS2/NOSZ objects** decode up to the range instead of decoding the whole object into memory and a spill file on every request. Migrating those blobs (`migrate_blobs`) makes ranges cheap.
- **Scrub doesn't report objects that change while being checked.** A write, delete or maintenance swap landing between reading an object's row and reading its blob looked like corruption. Suspect objects are now checked again with writers to that key locked out. Scrub also takes size and ETag from one fresh read of the row.
- **Slow downloads no longer stall the server.** Every download of a block-compressed or legacy zstd object held a blocking-pool thread until the client had read all of it. Clients that stop reading therefore used up the pool (512 threads by default) and froze every file operation in the process. With 520 stalled downloads, a 5-byte PUT took 12–17 seconds or timed out, and so did range GETs. Blocks are now decoded in steps that give the thread back whenever the client falls behind: in the same test, PUTs finish in 10–20 ms. Each download also reads at most two blocks ahead (it used to buffer nine), and full-GET throughput is unchanged. Corrupt blob headers now fail the request with `500` before any byte is sent, instead of cutting a `200` response short. A legacy zstd stream that ends early or runs long now ends the download with an error rather than a silently short body.
- **Schema upgrades fail loudly.** SQLite column migrations ignored every error, not just "column already exists". A locked or read-only metadata database therefore started with columns missing, and queries failed later with "no such column". Missing columns are now detected and added, and a failure stops start-up with the table and column named. Postgres schema setup runs in one transaction under an advisory lock; nodes starting together against a fresh database used to fail with `duplicate key value violates unique constraint "pg_type_typname_nsp_index"`. Applied schema versions are recorded in `nos_schema_migrations`, so restarts don't rerun DDL. That DDL would lock `nos_objects` and wait behind, or deadlock with, another node's index build.
- **Maintenance walks use an index.** Scrub, recompression, blob migration and replication backfill page through objects in key order.
  - Every page was a full table scan plus sort: 66 ms per page at 1M objects on SQLite, so a full `migrate_blobs` pass cost time quadratic in the object count. A partial index on active keys brings a page to 0.2 ms.
  - On SQLite, the first start after upgrading builds the index, about 0.7 s per million objects.
  - On Postgres, one node builds it in the background with `CREATE INDEX CONCURRENTLY`, so neither startup nor writes wait. A build that is interrupted leaves an invalid index, which is dropped and rebuilt at the next start.
- `docs/openapi.yaml` parses as YAML (unquoted backtick and comma values broke it) and documents the new `400`/`403`/`416`/`503` responses.
- **Background maintenance follows its configured intervals.** All jobs shared one 300-second loop, so recompression and scrub ran every five minutes regardless of `NOS_RECOMPRESS_INTERVAL_SECS` / `NOS_VERIFY_INTERVAL_SECS`. With `NOS_ORPHAN_GC_INTERVAL_SECS` set, the loop also waited for the orphan GC timer on every pass, so the soft-delete, multipart and `.tmp` purges ran only as often as orphan GC. Each job now has its own timer, and a run that overruns its interval delays the next one rather than setting off a burst of catch-up runs. With periodic recompression enabled, `NOS_RECOMPRESS_ON_STARTUP` no longer adds a second, overlapping pass at boot: the periodic job's first pass is the startup pass. Periodic reconciliation no longer repeats right after `NOS_RECONCILE_ON_STARTUP`.
- **Maintenance walks reach every object.** Recompression re-read the same `NOS_RECOMPRESS_BATCH_SIZE` oldest objects on every pass. The periodic scrub stopped for good after its first full pass, because its cursor stayed on the last key. Both now resume after the previous batch and start over after the last key. `POST /_cluster/replication/backfill` also returned the same first batch on every call; it now accepts `start_after` and returns `next_start_after` and `is_truncated`, like `verify_blobs` responses now do. Scrub, recompression, migration and backfill pages no longer skip objects when a page boundary falls inside a key that exists in several buckets. With `NOS_SCRUB_SAMPLE_DENOM` above 1, the periodic scrub also checked the same slice of keys on every pass (by default the same 1/1024), so the rest were never verified. The slice now moves on after each full pass, so every object is verified once every N passes.

### Changed (library API)

These affect crates that depend on `nebular_os` directly, so the next release should be a minor bump (it also ships the unreleased on-disk format changes):

- `GetObjectOutcome::Content` has a new `range: Option<(u64, u64)>` field.
- `StorageError::RangeNotSatisfiable` carries `{ size }`, and `StorageError::RequestTimeout` is new.
- `NosConfig` has new fields: `upload_idle_timeout_secs`, `header_read_timeout_secs`, `send_stall_timeout_secs`, `max_connections`, `presign_max_ttl_secs`, `fsync_writes`, `s3_access_key_role`, `legacy_access_key_auth`, `jwt_issuer` and `jwt_audience`. `AppState` has `auth_failures` and `storage_stats`.
- `storage::existing_blob_paths` and the exact-name check behind `first_existing_blob_path` guard fallback filenames; `cluster::forward::object_path` returns `None` for keys a URL can't address; `storage::RESERVED_BUCKETS` shrank to `_cluster` and `_nos`, with `storage::shadowed_by_system_route` for the rest; `cluster::http::upload_client` serves uploads.
- `server::serve_until(listener, app, options, shutdown)` stops gracefully when `shutdown` completes; `NosConfig` and `ServeOptions` have `shutdown_grace_secs` / `shutdown_grace`.
- `server::serve(listener, app, ServeOptions::from_config(&cfg))` replaces `axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>())` for embedders that want the same connection handling.
- **Commit hooks.** `WriteConditions` has a `hook: Option<&dyn CommitHook>` field. A `CommitHook` runs inside an object's write lock, before a change (it can veto it) and after it commits.
  - New engine methods take conditions or a hook: `copy_object_conditional`, `delete_object_conditional`, `complete_multipart_conditional`, `delete_objects_batch_hooked` and `delete_objects_by_prefix_hooked`.
- **Replication:**
  - `ReplicationEvent` has a `version` field.
  - `ReplicatedBackend` writes through `put_object_in_class`, `copy_object_in_class` and `complete_multipart_in_class`. `put_object_local`, `copy_object_local`, `complete_multipart_local` and `enqueue_replicated_put` are removed: they wrote without versioning.
  - `spawn_replication_worker` and `spawn_peer_health_checks` take a `CancellationToken`; `bump_worker_epoch` is removed in favour of `StorageBackend::shutdown`.
  - `drain_batch` returns the number of events handled. `heal_object_from_peers` takes the replication log.
  - `ClusterConfigSnapshot::replication_factor` is an `Option<u32>`.
- **Dictionaries:** `DictStore` shares its state between clones and has `train_next`, `current` and `next_id`. `DictTrainReport` has an `id`, and `StorageEngine::retrain_zstd_dictionary` is new.
- `storage::range::parse_content_range` and `routes::helpers::parse_range` are deprecated in favour of `storage::range::evaluate_range`.
- `BlockDecodeCache::get`/`insert` take a per-block content tag.
- `compression::IndexedBlobReader` reads a block-compressed blob one block per call; `pump_block_blob_full` / `pump_block_blob_range` are deprecated (they park a blocking thread until the receiver drains).
- `scrub::ScrubOptions` has a new `sample_epoch` field (`scrub::scrub_sample_selected_in` selects by slice).
- `StorageBackend::backfill_replication` takes a `start_after` cursor; `BackfillReport` and `VerifyBlobsReport` have new `next_start_after` / `is_truncated` fields. The binary's maintenance loop moved into the new `background_jobs` module.

## [0.1.4] — 2026-06-07

### Fixed

- **Flat encoded blob filenames**: object keys with `/` are stored as a single percent-encoded filename under the hash shard instead of nested directories. This fixes PUT failures when a parent key and a nested sidecar (e.g. `users/…/files/{uuid}` and `…/grid-thumbnail.jpg`) land in the same shard. Reads and deletes still fall back to the legacy nested on-disk layout until objects are overwritten.

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

[Unreleased]: https://github.com/AsP3X/nebular-os/compare/v0.1.4...HEAD
[0.1.4]: https://github.com/AsP3X/nebular-os/releases/tag/v0.1.4
[0.1.3]: https://github.com/AsP3X/nebular-os/releases/tag/v0.1.3
[0.1.2]: https://github.com/AsP3X/nebular-os/releases/tag/v0.1.2
[0.1.0]: https://github.com/AsP3X/nebular-os/releases/tag/v0.1.0
