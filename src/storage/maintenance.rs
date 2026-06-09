use std::path::Path;

use tokio::fs;

use super::blob_path_variants;
use super::blocks::BlockStore;
use super::compressibility::CompressionContext;
use super::compression::{
    decompress_blob, detect_blob_format, encode_blob_for_storage, is_nosb_blob, is_zstd_blob,
    read_stored_dict_id, read_stored_zstd_level, BlobFormat,
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
pub struct DictTrainReport {
    pub samples: u64,
    pub trained: bool,
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

    /// Scans active objects and rewrites blobs when stronger NOSB compression would shrink them.
    /// Handles legacy raw blobs, NOSZ/NOS2, and upgrades existing NOSB when level improves.
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
            if format == BlobFormat::Nosd {
                report.skipped += 1;
                continue;
            }

            if format == BlobFormat::Raw {
                if blob.len() as i64 != size {
                    report.skipped += 1;
                    continue;
                }
                let ctx = CompressionContext::new(
                    Some(key.as_str()),
                    None,
                    size as u64,
                    self.compress_min_size(),
                );
                let encoded = encode_blob_for_storage(
                    &blob,
                    background_level,
                    self.compress_block_size(),
                    ctx,
                )?;
                if encoded.len() >= blob.len() {
                    report.skipped += 1;
                    continue;
                }
                if !self.atomic_replace_blob(&path, &encoded).await? {
                    report.skipped += 1;
                    continue;
                }
                report.bytes_saved += (blob.len() as i64) - (encoded.len() as i64);
                report.recompressed += 1;
                continue;
            }

            if is_nosb_blob(&blob) {
                report.skipped += 1;
                continue;
            }

            if !is_zstd_blob(&blob) {
                report.skipped += 1;
                continue;
            }

            let stored_level = read_stored_zstd_level(&blob).unwrap_or(0) as i32;
            let stored_dict = read_stored_dict_id(&blob).unwrap_or(0);
            let dict_loaded = dict.is_some();
            let upgrading_level =
                stored_level < background_level || format == BlobFormat::Nosz;
            let needs_dict = dict_loaded && stored_dict == 0 && format == BlobFormat::Nos2;
            if !upgrading_level && !needs_dict && format == BlobFormat::Nos2 {
                report.skipped += 1;
                continue;
            }

            let logical = match decompress_blob(&blob, size as u64, dict_bytes) {
                Ok(v) => v,
                Err(_) => {
                    report.skipped += 1;
                    continue;
                }
            };

            let old_len = blob.len();
            let ctx = CompressionContext::new(
                Some(key.as_str()),
                None,
                size as u64,
                self.compress_min_size(),
            );
            let encoded = encode_blob_for_storage(
                &logical,
                background_level,
                self.compress_block_size(),
                ctx,
            )?;
            if upgrading_level {
                if !is_nosb_blob(&encoded) && encoded.len() >= old_len {
                    report.skipped += 1;
                    continue;
                }
            } else {
                if encoded.len() > old_len {
                    report.skipped += 1;
                    continue;
                }
                if !is_nosb_blob(&encoded) {
                    report.skipped += 1;
                    continue;
                }
            }
            if !upgrading_level && encoded.len() == old_len {
                report.skipped += 1;
                continue;
            }
            if !self.atomic_replace_blob(&path, &encoded).await? {
                report.skipped += 1;
                continue;
            }
            let new_blob = encoded;
            report.bytes_saved += (old_len as i64) - (new_blob.len() as i64);
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

    /// Backward-compatible alias for legacy raw blob recompression.
    pub async fn recompress_legacy_blobs(
        &self,
        limit: usize,
    ) -> Result<RecompressReport, StorageError> {
        self.recompress_blobs(limit).await
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
            if format == BlobFormat::Nosd {
                continue;
            }
            let logical = if format == BlobFormat::Raw {
                if blob.len() as i64 != size {
                    continue;
                }
                blob
            } else {
                match decompress_blob(&blob, size as u64, None) {
                    Ok(v) => v,
                    Err(_) => continue,
                }
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
