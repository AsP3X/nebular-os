use tokio::fs;

use super::blob_path;
use super::compressibility::CompressionContext;
use super::compression::{encode_blob_for_storage, is_compressed_blob};
use super::engine::StorageEngine;
use super::error::{internal, StorageError};

#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct RecompressReport {
    pub scanned: u64,
    pub recompressed: u64,
    pub skipped: u64,
    pub bytes_saved: i64,
}

impl StorageEngine {
    /// Permanently removes soft-deleted metadata rows past TTL; removes blob files unless already dropped.
    pub async fn purge_soft_deleted(&self) -> Result<u64, StorageError> {
        if self.soft_delete_ttl_secs() <= 0 {
            return Ok(0);
        }
        let cutoff = chrono::Utc::now().timestamp() - self.soft_delete_ttl_secs();
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT bucket, key FROM objects WHERE deleted_at IS NOT NULL AND deleted_at < ?",
        )
        .bind(cutoff)
        .fetch_all(self.read_pool())
        .await
        .map_err(internal)?;

        let mut purged = 0u64;
        for (bucket, key) in rows {
            if !self.soft_delete_drop_blob() {
                let path = blob_path(self.data_dir(), &bucket, &key);
                let _ = fs::remove_file(&path).await;
            }
            sqlx::query("DELETE FROM objects WHERE bucket = ? AND key = ?")
                .bind(&bucket)
                .bind(&key)
                .execute(self.write_pool())
                .await
                .map_err(internal)?;
            purged += 1;
        }
        Ok(purged)
    }

    /// Scans active objects and rewrites raw on-disk blobs when NOSB block compression would shrink them.
    pub async fn recompress_legacy_blobs(
        &self,
        limit: usize,
    ) -> Result<RecompressReport, StorageError> {
        let limit = limit.max(1) as i64;
        let rows: Vec<(String, String, i64, Option<String>)> = sqlx::query_as(
            "SELECT bucket, key, size, mime_type FROM objects WHERE deleted_at IS NULL ORDER BY updated_at LIMIT ?",
        )
        .bind(limit)
        .fetch_all(self.read_pool())
        .await
        .map_err(internal)?;

        let mut report = RecompressReport::default();
        for (bucket, key, size, mime_type) in rows {
            report.scanned += 1;
            let path = blob_path(self.data_dir(), &bucket, &key);
            let Ok(blob) = fs::read(&path).await else {
                report.skipped += 1;
                continue;
            };
            if is_compressed_blob(&blob) {
                report.skipped += 1;
                continue;
            }
            if blob.len() as i64 != size {
                report.skipped += 1;
                continue;
            }
            let ctx = CompressionContext::new(
                Some(key.as_str()),
                mime_type.as_deref(),
                size as u64,
                self.compress_min_size(),
            );
            let encoded = encode_blob_for_storage(
                &blob,
                self.zstd_level(),
                self.compress_block_size(),
                ctx,
            )?;
            if encoded.len() >= blob.len() {
                report.skipped += 1;
                continue;
            }

            let tmp_path = format!(
                "{}/.tmp/recompress-{}.tmp",
                self.data_dir(),
                uuid::Uuid::new_v4()
            );
            fs::write(&tmp_path, &encoded).await.map_err(internal)?;
            if fs::rename(&tmp_path, &path).await.is_err() {
                let _ = fs::remove_file(&tmp_path).await;
                report.skipped += 1;
                continue;
            }
            report.bytes_saved += (blob.len() as i64) - (encoded.len() as i64);
            report.recompressed += 1;
        }

        if report.recompressed > 0 {
            tracing::info!(
                scanned = report.scanned,
                recompressed = report.recompressed,
                bytes_saved = report.bytes_saved,
                "storage::recompress_legacy_blobs completed"
            );
        }
        Ok(report)
    }
}
