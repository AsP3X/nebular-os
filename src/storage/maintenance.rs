use std::path::Path;

use tokio::fs;

use super::blob_ops::link_or_copy_blob;
use super::{blob_path, blob_path_variants, blob_rel_path};
use super::blocks::BlockStore;
use super::compressibility::CompressionContext;
use super::compression::{
    decompress_blob, detect_blob_format, encode_blob_for_storage, is_indexed_blob,
    is_zstd_blob, parse_dedup_manifest, parse_layout_bytes, read_blob_stored_zstd_level,
    read_indexed_dict_id, BlobFormat, EncodeOptions, IndexedFormat,
};
use super::engine::StorageEngine;
use super::error::{internal, StorageError};

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
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub corrupted_keys: Vec<(String, String)>,
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
            if !self.soft_delete_drop_blob() {
                for path in blob_path_variants(self.data_dir(), &bucket, &key) {
                    if path.exists() {
                        BlockStore::release_blob(self.system_write_pool(), self.data_dir(), &path)
                            .await?;
                        let _ = fs::remove_file(&path).await;
                    }
                }
            }
            self.object_meta()
                .delete_object_row(&bucket, &key)
                .await?;
            purged += 1;
        }
        Ok(purged)
    }

    fn maintenance_encode_opts<'a>(
        &'a self,
        dict_id: u16,
        dict_bytes: Option<&'a [u8]>,
    ) -> EncodeOptions<'a> {
        EncodeOptions {
            dict_id,
            dict: dict_bytes,
            dedup_store: if self.dedup_enabled() {
                Some(self.block_store())
            } else {
                None
            },
        }
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

    fn indexed_needs_upgrade(
        &self,
        blob: &[u8],
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
        let layout = match parse_layout_bytes(blob) {
            Ok(l) => l,
            Err(_) => return true,
        };
        if layout.format == IndexedFormat::V0 {
            return true;
        }
        let stored_level = layout.zstd_level as i32;
        if stored_level == 0 || stored_level < background_level {
            return true;
        }
        if dict_loaded && layout.dict_id == 0 && target_dict_id > 0 {
            return true;
        }
        false
    }

    fn should_replace_encoded(
        &self,
        old_len: usize,
        encoded: &[u8],
        upgrading_level: bool,
        migrating_legacy: bool,
    ) -> bool {
        if !is_indexed_blob(encoded) {
            return false;
        }
        if migrating_legacy || upgrading_level {
            return true;
        }
        encoded.len() < old_len
    }

    /// Scans active objects and rewrites blobs when stronger compression or NOSI migration helps.
    pub async fn recompress_blobs(&self, limit: usize) -> Result<RecompressReport, StorageError> {
        let limit = limit.max(1) as i64;
        let rows = self
            .object_meta()
            .list_recompress_candidates(limit)
            .await?;

        let background_level = self.zstd_level();
        let dict = if self.zstd_dict_enabled() {
            self.dict_store().global_dict()
        } else {
            None
        };
        let dict_bytes = dict.as_deref().map(|v| v.as_slice());
        let dict_loaded = dict.is_some();
        let target_dict_id = if dict_loaded { 0 } else { 0 };

        let mut report = RecompressReport::default();
        for (bucket, key, size) in rows {
            report.scanned += 1;
            let variants = blob_path_variants(self.data_dir(), &bucket, &key);
            let Some(path) = super::first_existing_blob_path(&variants)
                .await
                .map_err(internal)?
            else {
                report.skipped += 1;
                continue;
            };
            let Ok(blob) = fs::read(&path).await else {
                report.skipped += 1;
                continue;
            };

            let format = detect_blob_format(&blob);
            if format == BlobFormat::Raw {
                if blob.len() as i64 != size {
                    report.skipped += 1;
                    continue;
                }
            }

            let migrating_legacy = is_legacy_format(format);
            let upgrading_indexed = self.indexed_needs_upgrade(
                &blob,
                format,
                background_level,
                dict_loaded,
                target_dict_id,
            );

            if format == BlobFormat::Nosi && !upgrading_indexed {
                report.skipped += 1;
                continue;
            }

            if is_zstd_blob(&blob) && !migrating_legacy {
                let stored_level = read_blob_stored_zstd_level(&blob).unwrap_or(0) as i32;
                let stored_dict = read_indexed_dict_id(&blob)
                    .or_else(|| super::compression::read_stored_dict_id(&blob))
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

            let logical = match self.decode_for_maintenance(&blob, format, size, dict_bytes) {
                Ok(v) => v,
                Err(_) => {
                    report.skipped += 1;
                    continue;
                }
            };

            let ctx = CompressionContext::new(
                Some(key.as_str()),
                None,
                size as u64,
                self.compress_min_size(),
                self.compress_exclude_extensions(),
            );
            let encode_opts = self.maintenance_encode_opts(target_dict_id, dict_bytes);
            let encoded = encode_blob_for_storage(
                &logical,
                background_level,
                self.block_size(),
                ctx,
                encode_opts,
            )?;

            let old_len = blob.len();
            let upgrading_level = migrating_legacy
                || upgrading_indexed
                || (read_blob_stored_zstd_level(&blob).unwrap_or(0) as i32) < background_level;

            if !self.should_replace_encoded(old_len, &encoded, upgrading_level, migrating_legacy) {
                report.skipped += 1;
                continue;
            }

            if !self.atomic_replace_blob(&path, &encoded).await? {
                report.skipped += 1;
                continue;
            }
            report.bytes_saved += (old_len as i64) - (encoded.len() as i64);
            report.recompressed += 1;
        }

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
        let rows = self
            .object_meta()
            .list_migration_page(limit + 1, start_after)
            .await?;

        let mut report = MigrateBlobsReport::default();
        let truncated = rows.len() > limit as usize;
        let page: Vec<_> = rows.into_iter().take(limit as usize).collect();
        if let Some((_, last_key, _)) = page.last() {
            report.next_start_after = Some(last_key.clone());
        }
        report.is_truncated = truncated;

        let background_level = self.zstd_level();
        let dict = if self.zstd_dict_enabled() {
            self.dict_store().global_dict()
        } else {
            None
        };
        let dict_bytes = dict.as_deref().map(|v| v.as_slice());
        let dict_loaded = dict.is_some();
        let target_dict_id = if dict_loaded { 0 } else { 0 };

        for (bucket, key, size) in page {
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

            let Ok(blob) = fs::read(&current).await else {
                report.failed += 1;
                continue;
            };

            let format = detect_blob_format(&blob);
            let migrating_legacy = is_legacy_format(format);
            let upgrading_indexed = self.indexed_needs_upgrade(
                &blob,
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

            let write_ok = if needs_reencode {
                let logical = match self.decode_for_maintenance(&blob, format, size, dict_bytes) {
                    Ok(v) => v,
                    Err(_) => {
                        report.failed += 1;
                        continue;
                    }
                };
                let ctx = CompressionContext::new(
                    Some(key.as_str()),
                    None,
                    size as u64,
                    self.compress_min_size(),
                    self.compress_exclude_extensions(),
                );
                let encode_opts = self.maintenance_encode_opts(target_dict_id, dict_bytes);
                let encoded = encode_blob_for_storage(
                    &logical,
                    background_level,
                    self.block_size(),
                    ctx,
                    encode_opts,
                )?;
                self.atomic_replace_blob(&target, &encoded).await?
            } else {
                link_or_copy_blob(&current, &target).await?;
                true
            };

            if !write_ok {
                report.failed += 1;
                continue;
            }

            if current != target && current.exists() {
                BlockStore::release_blob(self.system_write_pool(), self.data_dir(), &current)
                    .await?;
                let _ = fs::remove_file(&current).await;
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
        let rows = self
            .object_meta()
            .list_migration_page(limit, opts.start_after.as_deref())
            .await?;

        let dict = if self.zstd_dict_enabled() {
            self.dict_store().global_dict()
        } else {
            None
        };
        let dict_bytes = dict.as_deref().map(|v| v.as_slice());

        let mut report = VerifyBlobsReport {
            sample_denom: opts.sample_denom.max(1),
            mode: match opts.mode {
                super::scrub::ScrubMode::Light => "light",
                super::scrub::ScrubMode::Deep => "deep",
            }
            .into(),
            ..VerifyBlobsReport::default()
        };

        let mut last_key: Option<String> = None;
        for (bucket, key, size) in rows {
            last_key = Some(key.clone());
            if !super::scrub::scrub_sample_selected(&bucket, &key, opts.sample_denom) {
                report.sampled_out += 1;
                continue;
            }
            report.scanned += 1;

            let meta = match self.head_object(&bucket, &key, None, None).await? {
                Some(m) => m,
                None => {
                    report.skipped += 1;
                    continue;
                }
            };

            let variants = blob_path_variants(self.data_dir(), &bucket, &key);
            let Some(path) = super::first_existing_blob_path(&variants)
                .await
                .map_err(internal)?
            else {
                report.skipped += 1;
                continue;
            };
            let Ok(blob) = fs::read(&path).await else {
                report.skipped += 1;
                continue;
            };

            let format = detect_blob_format(&blob);
            let ok = super::scrub::verify_blob_for_scrub(
                &blob,
                format,
                size,
                opts.mode,
                path.as_path(),
                meta.etag.as_deref(),
                dict_bytes,
                Some(self.data_dir()),
                |b, f, s, d| self.decode_for_maintenance(b, f, s, d),
            )
            .await;

            if ok {
                report.verified += 1;
            } else {
                report.corrupted += 1;
                report.corrupted_keys.push((bucket.clone(), key.clone()));
                tracing::warn!(bucket = %bucket, key = %key, mode = ?opts.mode, "blob scrub failed");
            }
        }

        if let Some(k) = last_key {
            report.next_start_after = Some(k);
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
            sample_denom: 1,
            mode: super::scrub::ScrubMode::Deep,
            start_after: None,
        })
        .await
    }

    /// Sample recent objects and train a global zstd dictionary when enabled.
    pub async fn train_zstd_dictionary(&self) -> Result<DictTrainReport, StorageError> {
        let mut report = DictTrainReport::default();
        if !self.zstd_dict_enabled() {
            return Ok(report);
        }
        let batch = self.zstd_dict_train_batch() as i64;
        let rows = self.object_meta().list_recompress_candidates(batch).await?;
        let max_sample = 256 * 1024;
        let mut samples: Vec<Vec<u8>> = Vec::new();

        for (bucket, key, size) in rows {
            if size <= 0 || size as u64 > max_sample as u64 {
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
            let format = detect_blob_format(&blob);
            let logical = match self.decode_for_maintenance(&blob, format, size, None) {
                Ok(v) => v,
                Err(_) => continue,
            };
            if logical.len() >= 64 {
                samples.push(logical);
                report.samples += 1;
            }
            if samples.len() >= self.zstd_dict_train_batch() {
                break;
            }
        }

        if samples.len() < 2 {
            return Ok(report);
        }
        report.trained = self.dict_store().train_and_save(
            0,
            &samples,
            self.zstd_dict_max_bytes(),
            self.zstd_level(),
        )?;
        if report.trained {
            tracing::info!(samples = report.samples, "Global zstd dictionary trained");
        }
        Ok(report)
    }

    async fn atomic_replace_blob(&self, path: &Path, encoded: &[u8]) -> Result<bool, StorageError> {
        let tmp_path = format!(
            "{}/.tmp/recompress-{}.tmp",
            self.data_dir(),
            uuid::Uuid::new_v4()
        );
        fs::write(&tmp_path, encoded).await.map_err(internal)?;
        if fs::rename(&tmp_path, path).await.is_err() {
            let _ = fs::remove_file(&tmp_path).await;
            return Ok(false);
        }
        Ok(true)
    }
}
