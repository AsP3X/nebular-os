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
    pub created_at: i64,
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
    ) -> Result<ReplicationEvent, StorageError> {
        let event = ReplicationEvent {
            event_id: Uuid::new_v4().to_string(),
            origin_node: self.origin_node.clone(),
            op: ReplicationOp::Put,
            bucket: meta.bucket.clone(),
            key: meta.key.clone(),
            etag: meta.etag.clone(),
            size: Some(meta.size),
            payload_path: Some(self.relative_blob_path(&meta.bucket, &meta.key)),
            storage_class: storage_class.to_string(),
            created_at: Utc::now().timestamp(),
        };
        self.insert_pending(&event).await?;
        Ok(event)
    }

    pub async fn enqueue_delete(
        &self,
        bucket: &str,
        key: &str,
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
            storage_class: "default".into(),
            created_at: Utc::now().timestamp(),
        };
        self.insert_pending(&event).await?;
        Ok(event)
    }

    async fn insert_pending(&self, event: &ReplicationEvent) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO replication_log (event_id, origin_node, op, bucket, key, etag, size, payload_path, storage_class, created_at, status)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')",
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
        .bind(event.created_at)
        .execute(&self.pool)
        .await
        .map_err(internal)?;
        Ok(())
    }

    pub async fn list_pending(&self, limit: i64) -> Result<Vec<ReplicationEvent>, StorageError> {
        let rows = sqlx::query_as::<_, ReplicationRow>(
            "SELECT event_id, origin_node, op, bucket, key, etag, size, payload_path, storage_class, created_at
             FROM replication_log
             WHERE status = 'pending'
             ORDER BY created_at ASC
             LIMIT ?",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(internal)?;

        rows.into_iter()
            .map(ReplicationRow::into_event)
            .collect::<Result<Vec<_>, _>>()
    }

    pub async fn count_pending(&self) -> Result<u64, StorageError> {
        let row: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM replication_log WHERE status = 'pending'")
                .fetch_one(&self.pool)
                .await
                .map_err(internal)?;
        Ok(row.0.max(0) as u64)
    }

    pub async fn mark_sent(&self, event_id: &str) -> Result<(), StorageError> {
        sqlx::query("UPDATE replication_log SET status = 'sent' WHERE event_id = ?")
            .bind(event_id)
            .execute(&self.pool)
            .await
            .map_err(internal)?;
        Ok(())
    }

    pub async fn mark_failed(&self, event_id: &str) -> Result<(), StorageError> {
        sqlx::query("UPDATE replication_log SET status = 'failed' WHERE event_id = ?")
            .bind(event_id)
            .execute(&self.pool)
            .await
            .map_err(internal)?;
        Ok(())
    }

    pub async fn record_applied(&self, event: &ReplicationEvent) -> Result<bool, StorageError> {
        let now = Utc::now().timestamp();
        let result = sqlx::query(
            "INSERT INTO replication_log (event_id, origin_node, op, bucket, key, etag, size, payload_path, storage_class, created_at, applied_at, status)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'applied')
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
        .bind(event.created_at)
        .bind(now)
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
    created_at: i64,
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
            created_at: self.created_at,
        })
    }
}
