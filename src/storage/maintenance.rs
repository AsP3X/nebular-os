use std::path::{Path, PathBuf};

use futures_util::StreamExt;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::blob_ops::{link_or_copy_blob, rename_into_place, sync_dir, sync_file, FileStamp};
use super::{blob_path, blob_path_variants, blob_rel_path};
use super::blocks::BlockStore;
use super::compressibility::CompressionContext;
use super::compression::{
    decompress_blob, encode_file_for_storage, is_zstd_blob,
    parse_dedup_manifest, read_blob_layout, read_blob_stored_zstd_level, read_indexed_dict_id,
    stored_blob_format, BlobFormat, BlobLayout, EncodeOptions, FileEncoding, IndexedFormat,
};
use super::dict_store::IdentifiedDict;
use super::engine::{StorageEngine, TempFileGuard};
use super::error::{internal, StorageError};
use super::streaming::{hash_file_xxh3_hex, open_object_body_stream};

/// maintenance_state key holding the last key recompression visited ("" = start over).
const RECOMPRESS_CURSOR: &str = "recompress_cursor";

#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct RecompressReport {
    pub scanned: u64,
    pub recompressed: u64,
    pub skipped: u64,
    pub bytes_saved: i64,
}

#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct MigrateBlobsReport {
    pub scanned: u64,
    pub migrated: u64,
    pub skipped: u64,
    pub failed: u64,
    pub next_start_after: Option<String>,
    pub is_truncated: bool,
}

#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct DictTrainReport {
    pub samples: u64,
    pub trained: bool,
    /// Id of the dictionary trained, which new writes now use.
    pub id: Option<u16>,
}

#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct VerifyBlobsReport {
    pub scanned: u64,
    pub verified: u64,
    pub corrupted: u64,
    pub recovered: u64,
    pub skipped: u64,
    pub sampled_out: u64,
    pub sample_denom: u64,
    pub mode: String,
    pub next_start_after: Option<String>,
    /// More objects follow `next_start_after`; false once the walk reached the last key.
    pub is_truncated: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub corrupted_keys: Vec<(String, String)>,
}

/// Outcome of scrubbing one object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScrubVerdict {
    Healthy,
    /// The row was deleted meanwhile.
    Gone,
    /// A live row whose blob file is gone.
    Missing,
    Corrupt,
}

/// What maintenance swaps in for an object.
enum Replacement<'a> {
    /// A new encoding already written to `staging`; `refs` are its (not yet counted) dedup blocks.
    Encoded {
        staging: &'a Path,
        refs: &'a [(u64, u32)],
    },
    /// Same bytes, new location (legacy nested path -> flat path).
    Relocate,
}

/// Outcome of re-encoding an object for maintenance.
enum Reencoded {
    Indexed {
        staging: TempFileGuard,
        refs: Vec<(u64, u32)>,
    },
    /// The encoder keeps these bytes raw: `decoded` holds a legacy container's content, None = already raw.
    Raw { decoded: Option<TempFileGuard> },
    Undecodable,
}

/// Leading bytes of a blob (enough for every format detector and header field) and its length.
async fn blob_head(path: &Path) -> std::io::Result<(Vec<u8>, u64)> {
    let file = fs::File::open(path).await?;
    let len = file.metadata().await?.len();
    let mut head = Vec::with_capacity(64);
    file.take(64).read_to_end(&mut head).await?;
    Ok((head, len))
}

/// True for ETags of the form Nebular computes — the xxh3 of the object's content as 16 lowercase hex digits —
/// which scrub can check content against.
fn is_content_etag(etag: &str) -> bool {
    etag.len() == 16 && etag.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Header + block index of an indexed blob, read without its blocks.
fn indexed_layout(path: &Path) -> Option<BlobLayout> {
    let file = std::fs::File::open(path).ok()?;
    read_blob_layout(std::io::BufReader::new(file)).ok()
}

fn is_legacy_format(format: BlobFormat) -> bool {
    matches!(
        format,
        BlobFormat::Nosd | BlobFormat::Nosb | BlobFormat::Nosz | BlobFormat::Nos2
    )
}

impl StorageEngine {
    /// Permanently removes soft-deleted metadata rows past TTL; removes blob files unless already dropped.
    pub async fn purge_soft_deleted(&self) -> Result<u64, StorageError> {
        if self.soft_delete_ttl_secs() <= 0 {
            return Ok(0);
        }
        let cutoff = chrono::Utc::now().timestamp() - self.soft_delete_ttl_secs();
        let rows = self
            .object_meta()
            .list_soft_deleted_before(cutoff)
            .await?;

        let mut purged = 0u64;
        for (bucket, key) in rows {
            // Human: Re-check under the key lock: a PUT may have revived this key after it was listed, and
            // then both its row and its blob (same path) belong to the live object.
            let _key_guard = self.key_locks().lock(&bucket, &key).await;
            if !self
                .object_meta()
                .purge_soft_deleted_row(&bucket, &key, cutoff)
                .await?
            {
                continue;
            }
            if !self.soft_delete_drop_blob() {
                for path in super::existing_blob_paths(&blob_path_variants(self.data_dir(), &bucket, &key)) {
                    BlockStore::release_blob(self.system_write_pool(), self.data_dir(), &path).await?;
                    let _ = fs::remove_file(&path).await;
                }
            }
            purged += 1;
        }
        Ok(purged)
    }

    pub(crate) fn decode_for_maintenance(
        &self,
        blob: &[u8],
        format: BlobFormat,
        size: i64,
        dict_bytes: Option<&[u8]>,
    ) -> Result<Vec<u8>, StorageError> {
        match format {
            BlobFormat::Raw => {
                if blob.len() as i64 != size {
                    return Err(internal(anyhow::anyhow!("raw blob size mismatch")));
                }
                Ok(blob.to_vec())
            }
            BlobFormat::Nosd => {
                let entries = parse_dedup_manifest(blob, size as u64)?;
                let store = self.block_store();
                let mut out = Vec::with_capacity(size as usize);
                for (hash, chunk_size) in entries {
                    let chunk = store.read_logical_block(hash, chunk_size as usize)?;
                    out.extend_from_slice(&chunk);
                }
                if out.len() as i64 != size {
                    return Err(internal(anyhow::anyhow!("dedup assemble size mismatch")));
                }
                Ok(out)
            }
            _ => decompress_blob(blob, size as u64, dict_bytes, Some(self.data_dir())),
        }
    }

    /// Human: Stream an object's logical bytes (any on-disk format) into `out`, so maintenance never holds a
    /// whole object in memory.
    async fn decode_blob_to_file(&self, blob: &Path, size: u64, out: &Path) -> Result<(), StorageError> {
        let mut content =
            open_object_body_stream(blob, size, 0, size, &self.uncached_read_context()).await?;
        let mut file = fs::File::create(out).await.map_err(internal)?;
        let mut written = 0u64;
        while let Some(chunk) = content.next().await {
            let chunk = chunk.map_err(internal)?;
            written += chunk.len() as u64;
            file.write_all(&chunk).await.map_err(internal)?;
        }
        file.flush().await.map_err(internal)?;
        if written != size {
            return Err(internal(anyhow::anyhow!("decoded {written} bytes, expected {size}")));
        }
        Ok(())
    }

    /// Human: Re-encode logical bytes in `source` into `staging` at the background level, off the runtime.
    async fn encode_for_maintenance(
        &self,
        source: &Path,
        staging: &Path,
        key: &str,
        size: u64,
        dict: Option<IdentifiedDict>,
    ) -> Result<FileEncoding, StorageError> {
        let (source, staging, key) = (source.to_path_buf(), staging.to_path_buf(), key.to_string());
        let exclude = self.compress_exclude_extensions().to_vec();
        let (min_size, level, block_size) =
            (self.compress_min_size(), self.zstd_level(), self.block_size());
        let store = self.dedup_enabled().then(|| self.block_store().clone());
        tokio::task::spawn_blocking(move || {
            let ctx = CompressionContext::new(Some(&key), None, size, min_size, &exclude);
            let opts = EncodeOptions {
                dict_id: dict.as_ref().map_or(0, |(id, _)| *id),
                dict: dict.as_ref().map(|(_, d)| d.as_slice()),
                dedup_store: store.as_ref(),
            };
            encode_file_for_storage(&source, &staging, size, level, block_size, ctx, opts)
        })
        .await
        .map_err(internal)?
    }

    /// Human: Read context that always decodes from disk — scrub and maintenance must see the bytes on disk,
    /// not decoded blocks cached from an earlier read.
    fn uncached_read_context(&self) -> super::streaming::ReadContext {
        let mut ctx = self.read_context();
        ctx.block_cache = None;
        ctx
    }

    /// Human: Check one object as it is now: its current row (read directly — HEAD reports a missing blob as
    /// NotFound) against the bytes on disk.
    async fn scrub_one(
        &self,
        bucket: &str,
        key: &str,
        mode: super::scrub::ScrubMode,
    ) -> Result<ScrubVerdict, StorageError> {
        let Some(meta) = self.object_meta().try_fetch_active_metadata(bucket, key).await? else {
            return Ok(ScrubVerdict::Gone);
        };
        let variants = blob_path_variants(self.data_dir(), bucket, key);
        let Some(path) = super::first_existing_blob_path(&variants)
            .await
            .map_err(internal)?
        else {
            // Human: A live row whose bytes are gone is corruption (and healable from peers), not a skip.
            return Ok(ScrubVerdict::Missing);
        };
        Ok(if self.scrub_blob_file(&path, meta.size, mode, meta.etag.as_deref()).await {
            ScrubVerdict::Healthy
        } else {
            ScrubVerdict::Corrupt
        })
    }

    /// Human: Scrub one blob from disk without loading it. Light checks sizes, headers and block extents;
    /// deep decodes every block (verifying checksums) and hashes raw blobs against their ETag.
    async fn scrub_blob_file(
        &self,
        path: &Path,
        size: i64,
        mode: super::scrub::ScrubMode,
        etag: Option<&str>,
    ) -> bool {
        use super::scrub::ScrubMode;
        let Ok((head, file_len)) = blob_head(path).await else {
            return false;
        };
        match (mode, stored_blob_format(&head, file_len, size.max(0) as u64)) {
            (_, BlobFormat::Raw) if file_len as i64 != size => false,
            (ScrubMode::Light, BlobFormat::Raw) => true,
            (ScrubMode::Light, BlobFormat::Nosb | BlobFormat::Nosi) => indexed_layout(path)
                .is_some_and(|layout| {
                    super::scrub::indexed_extents_fit(&layout, size as u64, file_len)
                }),
            (ScrubMode::Light, _) => file_len > 8,
            (ScrubMode::Deep, BlobFormat::Raw) => match etag.filter(|e| is_content_etag(e)) {
                Some(expected) => {
                    let path = path.to_path_buf();
                    tokio::task::spawn_blocking(move || hash_file_xxh3_hex(&path, 256 * 1024))
                        .await
                        .ok()
                        .and_then(Result::ok)
                        .is_some_and(|actual| actual == expected)
                }
                None => true,
            },
            (ScrubMode::Deep, _) => self.drain_blob(path, size as u64, etag).await.is_ok(),
        }
    }

    /// Human: Decode a blob end to end (block checksums included) and check the result against the ETag:
    /// block checksums prove each block is what was written, the ETag that the whole object is the version its
    /// metadata names.
    async fn drain_blob(&self, blob: &Path, size: u64, etag: Option<&str>) -> Result<(), StorageError> {
        let mut content =
            open_object_body_stream(blob, size, 0, size, &self.uncached_read_context()).await?;
        let mut seen = 0u64;
        let mut hasher = xxhash_rust::xxh3::Xxh3::new();
        while let Some(chunk) = content.next().await {
            let chunk = chunk.map_err(internal)?;
            seen += chunk.len() as u64;
            hasher.update(&chunk);
        }
        if seen != size {
            return Err(internal(anyhow::anyhow!("decoded {seen} bytes, expected {size}")));
        }
        if let Some(expected) = etag.filter(|e| is_content_etag(e))
            && format!("{:016x}", hasher.digest()) != expected
        {
            return Err(internal(anyhow::anyhow!("decoded content does not match its ETag")));
        }
        Ok(())
    }

    /// Human: Produce a fresh encoding of an object in `.tmp/` — decoding legacy containers to a scratch file
    /// first — without reading the object into memory.
    async fn reencode_to_staging(
        &self,
        path: &Path,
        format: BlobFormat,
        key: &str,
        size: u64,
        dict: Option<IdentifiedDict>,
    ) -> Result<Reencoded, StorageError> {
        let scratch = |suffix: &str| TempFileGuard {
            path: PathBuf::from(format!(
                "{}/.tmp/maintenance-{}.{suffix}",
                self.data_dir(),
                uuid::Uuid::new_v4()
            )),
        };
        let decoded = if format == BlobFormat::Raw {
            None
        } else {
            let decoded = scratch("decoded");
            if let Err(e) = self.decode_blob_to_file(path, size, &decoded.path).await {
                tracing::warn!(path = %path.display(), error = %e, "maintenance could not decode blob");
                return Ok(Reencoded::Undecodable);
            }
            Some(decoded)
        };
        let source = decoded.as_ref().map_or(path, |d| d.path.as_path());
        let staging = scratch("stage");
        Ok(
            match self
                .encode_for_maintenance(source, &staging.path, key, size, dict)
                .await
            {
                Ok(FileEncoding::Indexed(refs)) => Reencoded::Indexed { staging, refs },
                Ok(FileEncoding::Raw) => Reencoded::Raw { decoded },
                // Human: One bad object (e.g. a torn raw blob whose length disagrees with metadata) must
                // be reported and skipped, not abort the whole batch and pin its cursor.
                Err(e) => {
                    tracing::warn!(path = %path.display(), error = %e, "maintenance could not re-encode blob");
                    Reencoded::Undecodable
                }
            },
        )
    }

    fn indexed_needs_upgrade(
        &self,
        layout: Option<&BlobLayout>,
        format: BlobFormat,
        background_level: i32,
        dict_loaded: bool,
        target_dict_id: u16,
    ) -> bool {
        if format == BlobFormat::Nosb {
            return true;
        }
        if format != BlobFormat::Nosi {
            return false;
        }
        let Some(layout) = layout else {
            return true;
        };
        if layout.format == IndexedFormat::V0 {
            return true;
        }
        let stored_level = layout.zstd_level as i32;
        if stored_level == 0 || stored_level < background_level {
            return true;
        }
        // Human: Written without a dictionary, or with one older than the current: re-encode with the current.
        if dict_loaded && layout.dict_id < target_dict_id {
            return true;
        }
        false
    }

    fn should_replace_encoded(
        &self,
        old_len: u64,
        encoded_len: u64,
        upgrading_level: bool,
        migrating_legacy: bool,
    ) -> bool {
        migrating_legacy || upgrading_level || encoded_len < old_len
    }

    /// Scans active objects and rewrites blobs when stronger compression or NOSI migration helps.
    /// Human: Each call continues where the previous one stopped (cursor in maintenance_state) and wraps
    /// around after the last object — it used to rescan the same oldest `limit` rows on every pass.
    pub async fn recompress_blobs(&self, limit: usize) -> Result<RecompressReport, StorageError> {
        let limit = limit.max(1) as i64;
        let cursor = self
            .get_maintenance_state(RECOMPRESS_CURSOR)
            .await?
            .filter(|c| !c.is_empty());
        let page = self
            .object_meta()
            .list_key_page(limit, cursor.as_deref())
            .await?;
        let next_cursor = match page.last_key() {
            Some(key) if page.is_truncated => key.to_string(),
            _ => String::new(),
        };

        let background_level = self.zstd_level();
        let dict = self.write_dictionary();
        let dict_loaded = dict.is_some();
        let target_dict_id = dict.as_ref().map_or(0, |(id, _)| *id);

        let mut report = RecompressReport::default();
        for (bucket, key, size) in page.rows {
            report.scanned += 1;
            let variants = blob_path_variants(self.data_dir(), &bucket, &key);
            let Some(path) = super::first_existing_blob_path(&variants)
                .await
                .map_err(internal)?
            else {
                report.skipped += 1;
                continue;
            };
            let stamp = FileStamp::of(&path);
            let Ok((head, old_len)) = blob_head(&path).await else {
                report.skipped += 1;
                continue;
            };

            let format = stored_blob_format(&head, old_len, size.max(0) as u64);
            if format == BlobFormat::Raw && old_len as i64 != size {
                report.skipped += 1;
                continue;
            }

            let layout = matches!(format, BlobFormat::Nosi | BlobFormat::Nosb)
                .then(|| indexed_layout(&path))
                .flatten();
            let migrating_legacy = is_legacy_format(format);
            let upgrading_indexed = self.indexed_needs_upgrade(
                layout.as_ref(),
                format,
                background_level,
                dict_loaded,
                target_dict_id,
            );

            if format == BlobFormat::Nosi && !upgrading_indexed {
                report.skipped += 1;
                continue;
            }

            if is_zstd_blob(&head) && !migrating_legacy {
                let stored_level = read_blob_stored_zstd_level(&head).unwrap_or(0) as i32;
                let stored_dict = read_indexed_dict_id(&head)
                    .or_else(|| super::compression::read_stored_dict_id(&head))
                    .unwrap_or(0);
                let upgrading_level =
                    stored_level < background_level || format == BlobFormat::Nosz;
                let needs_dict = dict_loaded && stored_dict == 0 && format == BlobFormat::Nos2;
                if !upgrading_level && !needs_dict {
                    report.skipped += 1;
                    continue;
                }
            } else if !migrating_legacy && format != BlobFormat::Raw && !upgrading_indexed {
                report.skipped += 1;
                continue;
            }

            let Reencoded::Indexed { staging, refs } = self
                .reencode_to_staging(&path, format, &key, size as u64, dict.clone())
                .await?
            else {
                report.skipped += 1;
                continue;
            };
            let encoded_len = fs::metadata(&staging.path).await.map_err(internal)?.len();
            let upgrading_level = migrating_legacy
                || upgrading_indexed
                || (read_blob_stored_zstd_level(&head).unwrap_or(0) as i32) < background_level;

            if !self.should_replace_encoded(old_len, encoded_len, upgrading_level, migrating_legacy) {
                report.skipped += 1;
                continue;
            }

            if !self
                .replace_blob_if_unchanged(
                    &bucket,
                    &key,
                    &path,
                    stamp.as_ref(),
                    &path,
                    Replacement::Encoded {
                        staging: &staging.path,
                        refs: &refs,
                    },
                )
                .await?
            {
                report.skipped += 1;
                continue;
            }
            report.bytes_saved += (old_len as i64) - (encoded_len as i64);
            report.recompressed += 1;
        }

        self.set_maintenance_state(RECOMPRESS_CURSOR, &next_cursor).await?;
        if report.recompressed > 0 {
            tracing::info!(
                scanned = report.scanned,
                recompressed = report.recompressed,
                bytes_saved = report.bytes_saved,
                "storage::recompress_blobs completed"
            );
        }
        Ok(report)
    }

    /// Human: Move legacy nested blob paths to flat encoded filenames and upgrade old compression formats.
    /// Agent: READS list_migration_page; WRITES blob_path(); REMOVES legacy path; UPDATES Postgres blob_path.
    pub async fn migrate_blobs(
        &self,
        limit: usize,
        start_after: Option<&str>,
    ) -> Result<MigrateBlobsReport, StorageError> {
        let limit = limit.max(1) as i64;
        let page = self.object_meta().list_key_page(limit, start_after).await?;

        let mut report = MigrateBlobsReport {
            next_start_after: page.last_key().map(str::to_string),
            is_truncated: page.is_truncated,
            ..MigrateBlobsReport::default()
        };

        let background_level = self.zstd_level();
        let dict = self.write_dictionary();
        let dict_loaded = dict.is_some();
        let target_dict_id = dict.as_ref().map_or(0, |(id, _)| *id);

        for (bucket, key, size) in page.rows {
            report.scanned += 1;
            let variants = blob_path_variants(self.data_dir(), &bucket, &key);
            let Some(current) = super::first_existing_blob_path(&variants)
                .await
                .map_err(internal)?
            else {
                report.skipped += 1;
                continue;
            };
            let target = blob_path(self.data_dir(), &bucket, &key);
            let needs_relocate = current != target;

            let stamp = FileStamp::of(&current);
            let Ok((head, current_len)) = blob_head(&current).await else {
                report.failed += 1;
                continue;
            };

            let format = stored_blob_format(&head, current_len, size.max(0) as u64);
            let layout = matches!(format, BlobFormat::Nosi | BlobFormat::Nosb)
                .then(|| indexed_layout(&current))
                .flatten();
            let migrating_legacy = is_legacy_format(format);
            let upgrading_indexed = self.indexed_needs_upgrade(
                layout.as_ref(),
                format,
                background_level,
                dict_loaded,
                target_dict_id,
            );
            // Human: Raw blobs already on the encoded path are valid — only re-encode when relocating or upgrading.
            // Agent: AVOIDS re-migrating small raw files every batch; Raw+legacy path still upgrades on first pass.
            let needs_reencode = migrating_legacy
                || upgrading_indexed
                || (format == BlobFormat::Raw && needs_relocate);

            if !needs_relocate && !needs_reencode {
                report.skipped += 1;
                continue;
            }

            let reencoded = if needs_reencode {
                self.reencode_to_staging(&current, format, &key, size as u64, dict.clone())
                    .await?
            } else {
                Reencoded::Raw { decoded: None }
            };
            let replacement = match &reencoded {
                Reencoded::Indexed { staging, refs } => Replacement::Encoded {
                    staging: &staging.path,
                    refs,
                },
                // Human: Legacy container whose content is better stored raw: swap in the decoded bytes.
                Reencoded::Raw { decoded: Some(decoded) } => Replacement::Encoded {
                    staging: &decoded.path,
                    refs: &[],
                },
                Reencoded::Raw { decoded: None } if needs_relocate => Replacement::Relocate,
                Reencoded::Raw { decoded: None } => {
                    report.skipped += 1;
                    continue;
                }
                Reencoded::Undecodable => {
                    report.failed += 1;
                    continue;
                }
            };
            let write_ok = self
                .replace_blob_if_unchanged(&bucket, &key, &current, stamp.as_ref(), &target, replacement)
                .await?;

            if !write_ok {
                report.skipped += 1;
                continue;
            }
            if !self.metadata_mode().is_blob_only() {
                let rel = blob_rel_path(&bucket, &key);
                let _ = self
                    .object_meta()
                    .update_blob_path(&bucket, &key, &rel)
                    .await;
            }
            report.migrated += 1;
        }

        if report.migrated > 0 {
            tracing::info!(
                scanned = report.scanned,
                migrated = report.migrated,
                skipped = report.skipped,
                failed = report.failed,
                "storage::migrate_blobs completed"
            );
        }
        Ok(report)
    }

    /// Backward-compatible alias for legacy raw blob recompression.
    pub async fn recompress_legacy_blobs(
        &self,
        limit: usize,
    ) -> Result<RecompressReport, StorageError> {
        self.recompress_blobs(limit).await
    }

    /// Walk objects with optional hash sampling and light/deep scrub modes.
    pub async fn scrub_objects(
        &self,
        opts: super::scrub::ScrubOptions,
    ) -> Result<VerifyBlobsReport, StorageError> {
        let limit = opts.limit.max(1) as i64;
        let page = self
            .object_meta()
            .list_key_page(limit, opts.start_after.as_deref())
            .await?;

        let mut report = VerifyBlobsReport {
            sample_denom: opts.sample_denom.max(1),
            mode: match opts.mode {
                super::scrub::ScrubMode::Light => "light",
                super::scrub::ScrubMode::Deep => "deep",
            }
            .into(),
            next_start_after: page.last_key().map(str::to_string),
            is_truncated: page.is_truncated,
            ..VerifyBlobsReport::default()
        };

        for (bucket, key, _) in page.rows {
            if !super::scrub::scrub_sample_selected_in(&bucket, &key, opts.sample_denom, opts.sample_epoch) {
                report.sampled_out += 1;
                continue;
            }
            report.scanned += 1;

            let mut verdict = self.scrub_one(&bucket, &key, opts.mode).await?;
            if matches!(verdict, ScrubVerdict::Missing | ScrubVerdict::Corrupt) {
                // Human: A PUT, delete or maintenance swap landing between reading the row and reading the blob
                // looks like corruption. Check again with writers to this key excluded before reporting it —
                // in replicated mode a report makes peers "heal" the object with their copy.
                let _guard = self.key_locks().lock(&bucket, &key).await;
                verdict = self.scrub_one(&bucket, &key, opts.mode).await?;
            }
            match verdict {
                ScrubVerdict::Healthy => report.verified += 1,
                ScrubVerdict::Gone => report.skipped += 1,
                ScrubVerdict::Missing | ScrubVerdict::Corrupt => {
                    report.corrupted += 1;
                    report.corrupted_keys.push((bucket.clone(), key.clone()));
                    if verdict == ScrubVerdict::Missing {
                        tracing::warn!(bucket = %bucket, key = %key, "blob missing for live object");
                    } else {
                        tracing::warn!(bucket = %bucket, key = %key, mode = ?opts.mode, "blob scrub failed");
                    }
                }
            }
        }

        if report.corrupted > 0 {
            tracing::warn!(
                scanned = report.scanned,
                corrupted = report.corrupted,
                mode = %report.mode,
                "storage::scrub_objects found corruption"
            );
        }
        Ok(report)
    }

    /// Walk indexed blobs and verify block checksums without a client GET.
    pub async fn verify_blob_integrity(&self, limit: usize) -> Result<VerifyBlobsReport, StorageError> {
        self.scrub_objects(super::scrub::ScrubOptions {
            limit,
            ..super::scrub::ScrubOptions::default()
        })
        .await
    }

    /// Human: Train the first zstd dictionary once enough objects exist (NOS_ZSTD_DICT_ENABLED). Runs on the
    /// recompression schedule and never replaces a dictionary; `retrain_zstd_dictionary` trains a new one.
    /// Agent: NO-OP unless NOS_ZSTD_DICT_ENABLED and .dict/ is empty; else train_new_dictionary.
    pub async fn train_zstd_dictionary(&self) -> Result<DictTrainReport, StorageError> {
        if !self.zstd_dict_enabled() || self.dict_store().has_any_dictionary() {
            return Ok(DictTrainReport::default());
        }
        self.train_new_dictionary().await
    }

    /// Human: Train a new dictionary from current objects and make it the one new writes use. Blobs compressed
    /// with older dictionaries keep decoding — each frame names its dictionary — and recompression moves them
    /// to the new one over time.
    /// Agent: InvalidRequest (400) when NOS_ZSTD_DICT_ENABLED is off; RETURNS DictTrainReport { id }.
    pub async fn retrain_zstd_dictionary(&self) -> Result<DictTrainReport, StorageError> {
        if !self.zstd_dict_enabled() {
            return Err(StorageError::InvalidRequest(
                "zstd dictionaries are disabled (NOS_ZSTD_DICT_ENABLED)".into(),
            ));
        }
        self.train_new_dictionary().await
    }

    /// Human: Sample current objects and save a dictionary trained on them under the next id.
    /// Agent: CALLS dictionary_samples; DictStore::train_next on the blocking pool; `trained` false below 2 samples.
    async fn train_new_dictionary(&self) -> Result<DictTrainReport, StorageError> {
        let samples = self.dictionary_samples().await?;
        let mut report = DictTrainReport {
            samples: samples.len() as u64,
            ..DictTrainReport::default()
        };
        if samples.len() < 2 {
            return Ok(report);
        }
        let store = self.dict_store().clone();
        let max_bytes = self.zstd_dict_max_bytes();
        report.id = tokio::task::spawn_blocking(move || store.train_next(&samples, max_bytes))
            .await
            .map_err(internal)??;
        report.trained = report.id.is_some();
        if let Some(id) = report.id {
            tracing::info!(id, samples = report.samples, "zstd dictionary trained");
        }
        Ok(report)
    }

    /// Logical contents of up to NOS_ZSTD_DICT_TRAIN_BATCH small objects (≤ 256 KiB) to train a dictionary on.
    async fn dictionary_samples(&self) -> Result<Vec<Vec<u8>>, StorageError> {
        let batch = self.zstd_dict_train_batch();
        let rows = self.object_meta().list_recompress_candidates(batch as i64).await?;
        let max_sample: i64 = 256 * 1024;
        let mut samples: Vec<Vec<u8>> = Vec::new();
        for (bucket, key, size) in rows {
            if size <= 0 || size > max_sample {
                continue;
            }
            let variants = blob_path_variants(self.data_dir(), &bucket, &key);
            let Some(path) = super::first_existing_blob_path(&variants)
                .await
                .ok()
                .flatten()
            else {
                continue;
            };
            let Ok(blob) = fs::read(&path).await else {
                continue;
            };
            let format = stored_blob_format(&blob, blob.len() as u64, size as u64);
            if let Ok(logical) = self.decode_for_maintenance(&blob, format, size, None)
                && logical.len() >= 64
            {
                samples.push(logical);
            }
            if samples.len() >= batch {
                break;
            }
        }
        Ok(samples)
    }

    /// Human: Swap maintenance output in for an object — only if the object is still exactly what was read
    /// (a PUT/copy/delete since then wins) — moving dedup ref ownership from the old bytes to the new ones.
    /// Agent: Takes the key lock; RETURNS false when the object changed or the swap failed (caller skips it).
    async fn replace_blob_if_unchanged(
        &self,
        bucket: &str,
        key: &str,
        current: &Path,
        stamp: Option<&FileStamp>,
        target: &Path,
        replacement: Replacement<'_>,
    ) -> Result<bool, StorageError> {
        let _key_guard = self.key_locks().lock(bucket, key).await;
        if stamp.is_none() || FileStamp::of(current).as_ref() != stamp {
            return Ok(false);
        }

        let relocation = TempFileGuard {
            path: PathBuf::from(format!(
                "{}/.tmp/maintenance-{}.move",
                self.data_dir(),
                uuid::Uuid::new_v4()
            )),
        };
        let (staging, new_refs, old_refs) = match replacement {
            Replacement::Encoded { staging, refs } => {
                (staging, refs, BlockStore::manifest_entries(current)?)
            }
            // Human: Relocation keeps the same bytes, so the refs simply move with the file.
            Replacement::Relocate => {
                link_or_copy_blob(current, &relocation.path).await?;
                (relocation.path.as_path(), &[][..], Vec::new())
            }
        };
        if self.fsync_writes() {
            sync_file(staging).await?;
        }
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).await.map_err(internal)?;
        }
        if !new_refs.is_empty() {
            BlockStore::inc_refs(self.system_write_pool(), new_refs).await?;
        }
        if let Err(e) = rename_into_place(staging, target).await {
            if !new_refs.is_empty() {
                let _ = BlockStore::dec_refs(self.system_write_pool(), self.data_dir(), new_refs).await;
            }
            tracing::warn!(%bucket, %key, error = %e, "maintenance blob swap failed");
            return Ok(false);
        }
        if current != target {
            let _ = fs::remove_file(current).await;
        }
        if self.fsync_writes()
            && let Some(parent) = target.parent()
        {
            let _ = sync_dir(parent).await;
        }
        // Human: The swap is done; failing here would abort the batch and misreport committed work.
        if !old_refs.is_empty()
            && let Err(e) = BlockStore::dec_refs(self.system_write_pool(), self.data_dir(), &old_refs).await
        {
            tracing::warn!(%bucket, %key, error = %e, "releasing replaced blob refs failed");
        }
        Ok(true)
    }

    /// Human: Interrupted PUTs leave `{data_dir}/.tmp/*.tmp` — they are not objects and never GC'd.
    /// Agent: DELETES idle children of `.tmp` older than max_age; SKIPS missing dir.
    pub async fn purge_stale_tmp_files(
        &self,
        max_age: std::time::Duration,
    ) -> Result<u64, StorageError> {
        let tmp_dir = PathBuf::from(self.data_dir()).join(".tmp");
        purge_stale_tmp_dir(&tmp_dir, max_age)
            .await
            .map_err(internal)
    }
}

// Human: Remove leftover upload/decompress scratch files that outlived their PUT/GET.
// Agent: READS mtime; DELETES regular files idle >= max_age; NEVER deletes the .tmp directory itself.
pub async fn purge_stale_tmp_dir(
    tmp_dir: &Path,
    max_age: std::time::Duration,
) -> std::io::Result<u64> {
    let read_dir = match tokio::fs::read_dir(tmp_dir).await {
        Ok(dir) => dir,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error),
    };
    let mut read_dir = read_dir;
    let mut removed = 0u64;
    while let Some(entry) = read_dir.next_entry().await? {
        let path = entry.path();
        let meta = match entry.metadata().await {
            Ok(meta) => meta,
            Err(_) => continue,
        };
        if !meta.is_file() || is_swap_journal(&path) {
            continue;
        }
        let Some(modified) = last_touched(&meta) else {
            continue;
        };
        let idle = match modified.elapsed() {
            Ok(elapsed) => elapsed >= max_age,
            Err(_) => false,
        };
        if !idle {
            continue;
        }
        match tokio::fs::remove_file(&path).await {
            Ok(()) => removed += 1,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(removed)
}

/// Human: Files of the overwrite journal (`{id}.swap` and the `{id}.prev` backup it names) are left to
/// `recover_interrupted_swaps` at the next start; deleting them would keep an interrupted overwrite from being undone.
fn is_swap_journal(path: &Path) -> bool {
    match path.extension().and_then(|e| e.to_str()) {
        Some("swap") => true,
        Some("prev") => path.with_extension("swap").exists(),
        _ => false,
    }
}

/// Human: When a scratch file was last touched. Staging/backup files are hard links to existing blobs and
/// inherit their old mtime, so on Unix the inode change time (bumped by link/rename) counts too.
fn last_touched(meta: &std::fs::Metadata) -> Option<std::time::SystemTime> {
    let modified = meta.modified().ok()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let changed = std::time::UNIX_EPOCH
            + std::time::Duration::new(meta.ctime().max(0) as u64, meta.ctime_nsec().clamp(0, 999_999_999) as u32);
        return Some(modified.max(changed));
    }
    #[allow(unreachable_code)]
    Some(modified)
}

#[cfg(test)]
mod tmp_janitor_tests {
    use super::*;
    use std::time::Duration;
    use tempfile::TempDir;

    #[cfg(unix)]
    #[tokio::test]
    async fn purge_keeps_fresh_hard_links_to_old_blobs() {
        let root = TempDir::new().expect("tmp");
        let blob = root.path().join("blob.bin");
        std::fs::write(&blob, b"committed object bytes").unwrap();
        let two_hours_ago = std::time::SystemTime::now() - Duration::from_secs(7200);
        std::fs::File::options()
            .write(true)
            .open(&blob)
            .unwrap()
            .set_modified(two_hours_ago)
            .unwrap();
        let tmp_dir = root.path().join(".tmp");
        std::fs::create_dir_all(&tmp_dir).unwrap();
        // Human: A staging/backup link made just now inherits the blob's two-hour-old mtime.
        std::fs::hard_link(&blob, tmp_dir.join("x.prev")).unwrap();

        let removed = purge_stale_tmp_dir(&tmp_dir, Duration::from_secs(3600)).await.unwrap();
        assert_eq!(removed, 0, "an in-use link must not look idle");
        assert!(tmp_dir.join("x.prev").exists());
    }

    #[tokio::test]
    async fn purge_stale_tmp_dir_leaves_the_overwrite_journal() {
        let root = TempDir::new().expect("tmp");
        let dir = root.path();
        for name in ["open.swap", "open.prev", "orphan.prev", "upload.tmp"] {
            std::fs::write(dir.join(name), b"x").unwrap();
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
        let removed = purge_stale_tmp_dir(dir, Duration::from_millis(50)).await.unwrap();
        assert_eq!(removed, 2);
        let mut left: Vec<_> = std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        left.sort();
        assert_eq!(left, ["open.prev", "open.swap"]);
    }

    #[tokio::test]
    async fn purge_stale_tmp_dir_removes_only_idle_files() {
        let root = TempDir::new().expect("tmp");
        let dir = root.path();
        let stale = dir.join("upload-stale.tmp");
        let fresh = dir.join("upload-fresh.tmp");
        std::fs::write(&stale, vec![0u8; 1024]).unwrap();
        tokio::time::sleep(Duration::from_millis(250)).await;

        let removed = purge_stale_tmp_dir(dir, Duration::from_millis(50))
            .await
            .expect("purge stale");
        assert_eq!(removed, 1, "idle upload scratch must be deleted");
        assert!(!stale.exists());

        std::fs::write(&fresh, vec![0u8; 1024]).unwrap();
        let removed_fresh = purge_stale_tmp_dir(dir, Duration::from_secs(3600))
            .await
            .expect("purge fresh");
        assert_eq!(removed_fresh, 0, "in-flight tmp must not be deleted");
        assert!(fresh.exists());
    }
}
