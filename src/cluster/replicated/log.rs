use chrono::Utc;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use uuid::Uuid;

use crate::storage::blob_path;
use crate::storage::error::{internal, StorageError};
use crate::storage::types::ObjectMetadata;

/// Human: Mutation types replicated to peers (copy is applied as a put on the destination key).
/// Agent: Serialized to replication_log.op; Copy enqueued as Put on dst for v1 apply path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReplicationOp {
    Put,
    Delete,
}

impl ReplicationOp {
    fn as_str(self) -> &'static str {
        match self {
            Self::Put => "put",
            Self::Delete => "delete",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "put" | "copy" => Some(Self::Put),
            "delete" => Some(Self::Delete),
            _ => None,
        }
    }
}

/// Human: One durable replication unit identified by event_id for idempotent peer apply.
/// Agent: Maps to replication_log row; payload_path relative to NOS_DATA_DIR for blob transfer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationEvent {
    pub event_id: String,
    pub origin_node: String,
    pub op: ReplicationOp,
    pub bucket: String,
    pub key: String,
    pub etag: Option<String>,
    pub size: Option<i64>,
    pub payload_path: Option<String>,
    pub storage_class: String,
    pub replication_group: String,
    #[serde(default)]
    pub content_type: Option<String>,
    #[serde(default)]
    pub custom_meta: Option<String>,
    #[serde(default)]
    pub wire_checksum: Option<String>,
    pub created_at: i64,
}

#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct ReplicationStatusReport {
    pub pending: u64,
    pub failed: u64,
    pub dead_letter: u64,
    pub sent: u64,
    pub applied: u64,
    pub oldest_pending_age_secs: Option<i64>,
}

#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct BackfillReport {
    pub scanned: u64,
    pub enqueued: u64,
    pub skipped: u64,
}

#[derive(Clone)]
pub struct ReplicationLog {
    pool: SqlitePool,
    data_dir: String,
    origin_node: String,
}

impl ReplicationLog {
    pub fn new(pool: SqlitePool, data_dir: String, origin_node: String) -> Self {
        Self {
            pool,
            data_dir,
            origin_node,
        }
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub fn data_dir(&self) -> &str {
        &self.data_dir
    }

    fn relative_blob_path(&self, bucket: &str, key: &str) -> String {
        let full = blob_path(&self.data_dir, bucket, key);
        full.strip_prefix(&self.data_dir)
            .unwrap_or(&full)
            .to_string_lossy()
            .trim_start_matches(['/', '\\'])
            .to_string()
    }

    /// Human: Record a successful local write so the worker can push to peers.
    /// Agent: INSERT replication_log status=pending; event_id UUID v4.
    pub async fn enqueue_put(
        &self,
        meta: &ObjectMetadata,
        storage_class: &str,
        replication_group: &str,
    ) -> Result<ReplicationEvent, StorageError> {
        let rel = self.relative_blob_path(&meta.bucket, &meta.key);
        let blob_path = std::path::Path::new(&self.data_dir).join(&rel);
        let wire_checksum = if blob_path.exists() {
            let path = blob_path.clone();
            Some(
                tokio::task::spawn_blocking(move || {
                    crate::storage::streaming::hash_file_xxh3_hex(&path, 256 * 1024)
                })
                .await
                .map_err(internal)??,
            )
        } else {
            None
        };
        let event = ReplicationEvent {
            event_id: Uuid::new_v4().to_string(),
            origin_node: self.origin_node.clone(),
            op: ReplicationOp::Put,
            bucket: meta.bucket.clone(),
            key: meta.key.clone(),
            etag: meta.etag.clone(),
            size: Some(meta.size),
            payload_path: Some(rel),
            storage_class: storage_class.to_string(),
            replication_group: replication_group.to_string(),
            content_type: meta.mime_type.clone(),
            custom_meta: meta.custom_meta.clone(),
            wire_checksum,
            created_at: Utc::now().timestamp(),
        };
        self.insert_pending(&event).await?;
        Ok(event)
    }

    pub async fn enqueue_delete(
        &self,
        bucket: &str,
        key: &str,
        storage_class: &str,
        replication_group: &str,
    ) -> Result<ReplicationEvent, StorageError> {
        let event = ReplicationEvent {
            event_id: Uuid::new_v4().to_string(),
            origin_node: self.origin_node.clone(),
            op: ReplicationOp::Delete,
            bucket: bucket.to_string(),
            key: key.to_string(),
            etag: None,
            size: None,
            payload_path: None,
            storage_class: storage_class.to_string(),
            replication_group: replication_group.to_string(),
            content_type: None,
            custom_meta: None,
            wire_checksum: None,
            created_at: Utc::now().timestamp(),
        };
        self.insert_pending(&event).await?;
        Ok(event)
    }

    async fn insert_pending(&self, event: &ReplicationEvent) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO replication_log (event_id, origin_node, op, bucket, key, etag, size, payload_path, storage_class, replication_group, content_type, custom_meta, wire_checksum, created_at, status, attempts, next_retry_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, NULL)",
        )
        .bind(&event.event_id)
        .bind(&event.origin_node)
        .bind(event.op.as_str())
        .bind(&event.bucket)
        .bind(&event.key)
        .bind(&event.etag)
        .bind(event.size)
        .bind(&event.payload_path)
        .bind(&event.storage_class)
        .bind(&event.replication_group)
        .bind(&event.content_type)
        .bind(&event.custom_meta)
        .bind(&event.wire_checksum)
        .bind(event.created_at)
        .execute(&self.pool)
        .await
        .map_err(internal)?;
        Ok(())
    }

    pub async fn list_pending(&self, limit: i64) -> Result<Vec<ReplicationEvent>, StorageError> {
        let now = Utc::now().timestamp();
        let rows = sqlx::query_as::<_, ReplicationRow>(
            "SELECT event_id, origin_node, op, bucket, key, etag, size, payload_path, storage_class, COALESCE(replication_group, 'default') AS replication_group, content_type, custom_meta, wire_checksum, created_at, status
             FROM replication_log
             WHERE status = 'pending'
                OR (status = 'failed' AND (next_retry_at IS NULL OR next_retry_at <= ?))
             ORDER BY created_at ASC
             LIMIT ?",
        )
        .bind(now)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(internal)?;

        for row in &rows {
            if row.status.as_deref() == Some("failed") {
                sqlx::query("UPDATE replication_log SET status = 'pending' WHERE event_id = ?")
                    .bind(&row.event_id)
                    .execute(&self.pool)
                    .await
                    .map_err(internal)?;
            }
        }

        rows.into_iter()
            .map(ReplicationRow::into_event)
            .collect::<Result<Vec<_>, _>>()
    }

    pub async fn count_pending(&self) -> Result<u64, StorageError> {
        let now = Utc::now().timestamp();
        let row: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM replication_log
             WHERE status = 'pending'
                OR (status = 'failed' AND (next_retry_at IS NULL OR next_retry_at <= ?))",
        )
        .bind(now)
        .fetch_one(&self.pool)
        .await
        .map_err(internal)?;
        Ok(row.0.max(0) as u64)
    }

    pub async fn status_report(&self) -> Result<ReplicationStatusReport, StorageError> {
        let now = Utc::now().timestamp();
        let rows: Vec<(String, i64)> = sqlx::query_as(
            "SELECT status, COUNT(*) FROM replication_log GROUP BY status",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(internal)?;

        let mut report = ReplicationStatusReport::default();
        for (status, count) in rows {
            let n = count.max(0) as u64;
            match status.as_str() {
                "pending" => report.pending = n,
                "failed" => report.failed = n,
                "dead_letter" => report.dead_letter = n,
                "sent" => report.sent = n,
                "applied" => report.applied = n,
                _ => {}
            }
        }

        let oldest: Option<(i64,)> = sqlx::query_as(
            "SELECT MIN(created_at) FROM replication_log WHERE status = 'pending'",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(internal)?;
        if let Some((ts,)) = oldest {
            report.oldest_pending_age_secs = Some(now.saturating_sub(ts));
        }
        Ok(report)
    }

    pub async fn mark_sent(&self, event_id: &str) -> Result<(), StorageError> {
        sqlx::query("UPDATE replication_log SET status = 'sent' WHERE event_id = ?")
            .bind(event_id)
            .execute(&self.pool)
            .await
            .map_err(internal)?;
        Ok(())
    }

    pub async fn mark_failed(&self, event_id: &str, max_attempts: u32) -> Result<(), StorageError> {
        let row: Option<(i64,)> = sqlx::query_as(
            "SELECT attempts FROM replication_log WHERE event_id = ?",
        )
        .bind(event_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(internal)?;
        let attempts = row.map(|(a,)| a).unwrap_or(0) + 1;
        if attempts >= max_attempts.max(1) as i64 {
            sqlx::query(
                "UPDATE replication_log SET status = 'dead_letter', attempts = ? WHERE event_id = ?",
            )
            .bind(attempts)
            .bind(event_id)
            .execute(&self.pool)
            .await
            .map_err(internal)?;
            return Ok(());
        }
        let backoff = (1i64 << attempts.min(10)).min(3600);
        let next_retry_at = Utc::now().timestamp() + backoff;
        sqlx::query(
            "UPDATE replication_log SET status = 'failed', attempts = ?, next_retry_at = ? WHERE event_id = ?",
        )
        .bind(attempts)
        .bind(next_retry_at)
        .bind(event_id)
        .execute(&self.pool)
        .await
        .map_err(internal)?;
        Ok(())
    }

    pub async fn record_applied(&self, event: &ReplicationEvent) -> Result<bool, StorageError> {
        let now = Utc::now().timestamp();
        let result = sqlx::query(
            "INSERT INTO replication_log (event_id, origin_node, op, bucket, key, etag, size, payload_path, storage_class, replication_group, content_type, custom_meta, wire_checksum, created_at, applied_at, status)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'applied')
             ON CONFLICT(event_id) DO NOTHING",
        )
        .bind(&event.event_id)
        .bind(&event.origin_node)
        .bind(event.op.as_str())
        .bind(&event.bucket)
        .bind(&event.key)
        .bind(&event.etag)
        .bind(event.size)
        .bind(&event.payload_path)
        .bind(&event.storage_class)
        .bind(&event.replication_group)
        .bind(&event.content_type)
        .bind(&event.custom_meta)
        .bind(&event.wire_checksum)
        .bind(event.created_at)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(internal)?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn replay_dead_letter(&self, event_id: &str) -> Result<bool, StorageError> {
        let result = sqlx::query(
            "UPDATE replication_log SET status = 'pending', attempts = 0, next_retry_at = NULL
             WHERE event_id = ? AND status = 'dead_letter'",
        )
        .bind(event_id)
        .execute(&self.pool)
        .await
        .map_err(internal)?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn has_event(&self, event_id: &str) -> Result<bool, StorageError> {
        let row: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM replication_log WHERE event_id = ?",
        )
        .bind(event_id)
        .fetch_one(&self.pool)
        .await
        .map_err(internal)?;
        Ok(row.0 > 0)
    }
}

#[derive(sqlx::FromRow)]
struct ReplicationRow {
    event_id: String,
    origin_node: String,
    op: String,
    bucket: String,
    key: String,
    etag: Option<String>,
    size: Option<i64>,
    payload_path: Option<String>,
    storage_class: String,
    replication_group: String,
    content_type: Option<String>,
    custom_meta: Option<String>,
    wire_checksum: Option<String>,
    created_at: i64,
    status: Option<String>,
}

impl ReplicationRow {
    fn into_event(self) -> Result<ReplicationEvent, StorageError> {
        let op = ReplicationOp::parse(&self.op).ok_or_else(|| {
            internal(anyhow::anyhow!("unknown replication op: {}", self.op))
        })?;
        Ok(ReplicationEvent {
            event_id: self.event_id,
            origin_node: self.origin_node,
            op,
            bucket: self.bucket,
            key: self.key,
            etag: self.etag,
            size: self.size,
            payload_path: self.payload_path,
            storage_class: self.storage_class,
            replication_group: self.replication_group,
            content_type: self.content_type,
            custom_meta: self.custom_meta,
            wire_checksum: self.wire_checksum,
            created_at: self.created_at,
        })
    }
}
