use std::collections::HashSet;
use tokio::fs;

use super::engine::StorageEngine;
use super::error::{internal, StorageError};
use super::{blob_path, sanitize_bucket};

#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct ReconcileReport {
    pub orphan_blobs_removed: u64,
    pub stale_rows_removed: u64,
    /// Pending replication_log rows (informational when cluster replication is enabled).
    pub replication_pending_events: u64,
    /// Pending put events whose payload blob file is missing on disk.
    pub replication_pending_missing_blob: u64,
}

impl StorageEngine {
    /// Compares SQLite metadata with on-disk blobs and repairs drift in both directions.
    pub async fn reconcile(&self) -> Result<ReconcileReport, StorageError> {
        let mut report = ReconcileReport::default();

        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT bucket, key FROM objects WHERE deleted_at IS NULL",
        )
        .fetch_all(self.write_pool())
        .await
        .map_err(internal)?;

        let mut db_keys: HashSet<(String, String)> = HashSet::new();
        for (bucket, key) in &rows {
            db_keys.insert((bucket.clone(), key.clone()));
            let path = blob_path(self.data_dir(), bucket, key);
            if !path.exists() {
                sqlx::query("DELETE FROM objects WHERE bucket = ? AND key = ?")
                    .bind(bucket)
                    .bind(key)
                    .execute(self.write_pool())
                    .await
                    .map_err(internal)?;
                report.stale_rows_removed += 1;
            }
        }

        let data_dir = self.data_dir();
        let mut entries = fs::read_dir(data_dir).await.map_err(internal)?;
        while let Some(entry) = entries.next_entry().await.map_err(internal)? {
            let file_type = entry.file_type().await.map_err(internal)?;
            if !file_type.is_dir() {
                continue;
            }
            let bucket_name = entry.file_name().to_string_lossy().to_string();
            if bucket_name.starts_with('.') {
                continue;
            }
            let bucket = match sanitize_bucket(&bucket_name) {
                Ok(b) => b,
                Err(_) => continue,
            };
            Self::scan_bucket_blobs(entry.path(), &bucket, &db_keys, &mut report).await?;
        }

        if let Ok(hints) = self.replication_reconcile_hints().await {
            report.replication_pending_events = hints.pending;
            report.replication_pending_missing_blob = hints.missing_blob;
        }

        tracing::info!(
            orphan_blobs_removed = report.orphan_blobs_removed,
            stale_rows_removed = report.stale_rows_removed,
            replication_pending_events = report.replication_pending_events,
            replication_pending_missing_blob = report.replication_pending_missing_blob,
            "storage::reconcile completed"
        );
        Ok(report)
    }

    async fn replication_reconcile_hints(&self) -> Result<ReplicationHints, StorageError> {
        let table: Option<(i64,)> = sqlx::query_as(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'replication_log'",
        )
        .fetch_optional(self.write_pool())
        .await
        .map_err(internal)?;
        if table.is_none() {
            return Ok(ReplicationHints::default());
        }

        let pending: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM replication_log WHERE status = 'pending'",
        )
        .fetch_one(self.write_pool())
        .await
        .map_err(internal)?;

        let paths: Vec<(String,)> = sqlx::query_as(
            "SELECT payload_path FROM replication_log WHERE status = 'pending' AND op = 'put' AND payload_path IS NOT NULL",
        )
        .fetch_all(self.write_pool())
        .await
        .map_err(internal)?;

        let mut missing_blob = 0u64;
        for (rel,) in paths {
            let path = std::path::Path::new(self.data_dir()).join(&rel);
            if !path.exists() {
                missing_blob += 1;
            }
        }

        Ok(ReplicationHints {
            pending: pending.0.max(0) as u64,
            missing_blob,
        })
    }

    async fn scan_bucket_blobs(
        bucket_dir: std::path::PathBuf,
        bucket: &str,
        db_keys: &HashSet<(String, String)>,
        report: &mut ReconcileReport,
    ) -> Result<(), StorageError> {
        let mut stack = vec![bucket_dir.clone()];
        while let Some(dir) = stack.pop() {
            let mut rd = fs::read_dir(&dir).await.map_err(internal)?;
            while let Some(ent) = rd.next_entry().await.map_err(internal)? {
                let ft = ent.file_type().await.map_err(internal)?;
                if ft.is_dir() {
                    stack.push(ent.path());
                    continue;
                }
                if !ft.is_file() {
                    continue;
                }
                let path = ent.path();
                let rel = path
                    .strip_prefix(&bucket_dir)
                    .map_err(internal)?
                    .to_string_lossy()
                    .replace('\\', "/");
                let key = rel
                    .split_once('/')
                    .map(|(_, k)| k.to_string())
                    .unwrap_or(rel);
                if key.is_empty() {
                    continue;
                }
                if !db_keys.contains(&(bucket.to_string(), key)) {
                    let _ = fs::remove_file(path).await;
                    report.orphan_blobs_removed += 1;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
struct ReplicationHints {
    pending: u64,
    missing_blob: u64,
}
