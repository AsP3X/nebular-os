use chrono::Utc;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use uuid::Uuid;

use crate::storage::blob_path;
use crate::storage::error::{internal, StorageError};
use crate::storage::types::ObjectMetadata;
use crate::storage::Committed;

use super::versions::{self, KeyVersion, Version};

/// Rows handled per statement when pruning the log.
const PRUNE_BATCH: i64 = 5_000;

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
    /// When the change was made on `origin_node`, in microseconds (see `versions`); 0 from nodes that predate
    /// versions, whose events count as made at the start of `created_at`'s second.
    #[serde(default)]
    pub version: i64,
}

impl ReplicationEvent {
    /// The change's version, ordered against the versions this node has recorded.
    pub fn effective_version(&self) -> Version {
        Version {
            micros: if self.version > 0 {
                self.version
            } else {
                self.created_at.saturating_mul(1_000_000)
            },
            origin: self.origin_node.clone(),
        }
    }
}

/// An event waiting for delivery and the peers that already have it.
pub(crate) struct PendingDelivery {
    pub event: ReplicationEvent,
    pub delivered_to: Vec<String>,
}

#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct ReplicationStatusReport {
    pub pending: u64,
    pub failed: u64,
    pub dead_letter: u64,
    pub sent: u64,
    pub applied: u64,
    /// Not sent because a newer change to the same key replaced it.
    pub superseded: u64,
    pub oldest_pending_age_secs: Option<i64>,
}

/// What `ReplicationLog::prune` removed.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PruneReport {
    pub events: u64,
    pub tombstones: u64,
}

#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct BackfillReport {
    pub scanned: u64,
    pub enqueued: u64,
    pub skipped: u64,
    /// Pass as `start_after` to continue with the next batch.
    pub next_start_after: Option<String>,
    /// More objects follow `next_start_after`.
    pub is_truncated: bool,
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

    /// Human: Queue an object's current state for peers under the version this node has for it — or, for an
    /// object that has none yet (stored before versions existed), its last-modified time (backfill).
    /// Agent: INSERT replication_log status=pending; event_id UUID v4. Writes queue through `record_local_change`.
    pub async fn enqueue_put(
        &self,
        meta: &ObjectMetadata,
        storage_class: &str,
        replication_group: &str,
    ) -> Result<ReplicationEvent, StorageError> {
        let implied = KeyVersion {
            version: Version {
                micros: meta.updated_at.timestamp_micros(),
                origin: self.origin_node.clone(),
            },
            deleted: false,
        };
        let version = versions::store_if_absent(&self.pool, &meta.bucket, &meta.key, &implied).await?;
        let event = self.put_event(meta, storage_class, replication_group, version.micros);
        self.insert_pending(&self.pool, &event).await?;
        Ok(event)
    }

    fn put_event(
        &self,
        meta: &ObjectMetadata,
        storage_class: &str,
        replication_group: &str,
        version: i64,
    ) -> ReplicationEvent {
        let rel = self.relative_blob_path(&meta.bucket, &meta.key);
        // Human: Peers receive the object's logical bytes (not Nebular's on-disk container), and the
        // object ETag is already xxh3 of exactly those bytes — no need to hash the blob file.
        let wire_checksum = meta.etag.clone().filter(|e| !e.is_empty());
        ReplicationEvent {
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
            version,
        }
    }

    /// Human: Queue a delete of `bucket/key` for peers as a new change made here now (tombstone included).
    /// Agent: ONE transaction: store_local(deleted) + INSERT pending delete event; no key lock (callers own ordering).
    pub async fn enqueue_delete(
        &self,
        bucket: &str,
        key: &str,
        storage_class: &str,
        replication_group: &str,
    ) -> Result<ReplicationEvent, StorageError> {
        let mut tx = self.begin_write().await?;
        let version = versions::store_local(&mut *tx, bucket, key, &self.origin_node, true).await?;
        let event = self.delete_event(bucket, key, storage_class, replication_group, version);
        self.insert_pending(&mut *tx, &event).await?;
        tx.commit().await.map_err(internal)?;
        Ok(event)
    }

    fn delete_event(
        &self,
        bucket: &str,
        key: &str,
        storage_class: &str,
        replication_group: &str,
        version: i64,
    ) -> ReplicationEvent {
        ReplicationEvent {
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
            version,
        }
    }

    /// Human: Version a change just committed on this node and queue it for peers, atomically — called under
    /// the key's write lock (`CommitHook::after`), so versions follow the order changes were committed in.
    /// Agent: BEGIN IMMEDIATE; store_local + INSERT pending event carrying that version; COMMIT.
    pub(crate) async fn record_local_change(
        &self,
        bucket: &str,
        key: &str,
        committed: Committed<'_>,
        write_storage_class: &str,
        replication_group: &str,
    ) -> Result<(), StorageError> {
        let deleted = matches!(committed, Committed::Deleted { .. });
        let mut tx = self.begin_write().await?;
        let version = versions::store_local(&mut *tx, bucket, key, &self.origin_node, deleted).await?;
        let event = match committed {
            Committed::Written(meta) => self.put_event(meta, write_storage_class, replication_group, version),
            Committed::Deleted { storage_class } => self.delete_event(
                bucket,
                key,
                storage_class.unwrap_or(write_storage_class),
                replication_group,
                version,
            ),
        };
        self.insert_pending(&mut *tx, &event).await?;
        tx.commit().await.map_err(internal)
    }

    /// Human: Record a peer's change as applied here: the key takes its version, and its event id is kept so a
    /// redelivery is recognized. Called under the key's write lock once the change is committed.
    /// Agent: BEGIN IMMEDIATE; UPSERT replication_versions (event version, deleted for deletes) + INSERT applied row.
    pub(crate) async fn record_remote_change(
        &self,
        bucket: &str,
        key: &str,
        event: &ReplicationEvent,
    ) -> Result<(), StorageError> {
        let state = KeyVersion {
            version: event.effective_version(),
            deleted: event.op == ReplicationOp::Delete,
        };
        let mut tx = self.begin_write().await?;
        versions::store(&mut *tx, bucket, key, &state).await?;
        self.insert_applied(&mut *tx, event).await?;
        tx.commit().await.map_err(internal)
    }

    /// The version of `bucket/key`'s current state on this node, if recorded (keys as the engine stores them).
    pub async fn key_version(&self, bucket: &str, key: &str) -> Result<Option<KeyVersion>, StorageError> {
        let Ok(key) = crate::storage::sanitize_key(key) else {
            return Ok(None);
        };
        versions::load(&self.pool, bucket, &key).await
    }

    /// Record `version` for `bucket/key`, whose object was just restored from a peer copy of that version.
    pub(crate) async fn record_restored(&self, bucket: &str, key: &str, version: Version) -> Result<(), StorageError> {
        let state = KeyVersion {
            version,
            deleted: false,
        };
        versions::store(&self.pool, bucket, key, &state).await
    }

    /// A write transaction that takes SQLite's write lock up front (a read-then-write transaction can fail with
    /// "database is locked" instead of waiting when another connection writes meanwhile).
    async fn begin_write(&self) -> Result<sqlx::Transaction<'static, sqlx::Sqlite>, StorageError> {
        self.pool.begin_with("BEGIN IMMEDIATE").await.map_err(internal)
    }

    async fn insert_pending<'e, E>(&self, executor: E, event: &ReplicationEvent) -> Result<(), StorageError>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        sqlx::query(
            "INSERT INTO replication_log (event_id, origin_node, op, bucket, key, etag, size, payload_path, storage_class, replication_group, content_type, custom_meta, wire_checksum, created_at, version, status, attempts, next_retry_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, NULL)",
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
        .bind(event.version)
        .execute(executor)
        .await
        .map_err(internal)?;
        Ok(())
    }

    pub async fn list_pending(&self, limit: i64) -> Result<Vec<ReplicationEvent>, StorageError> {
        Ok(self
            .list_pending_deliveries(limit)
            .await?
            .into_iter()
            .map(|pending| pending.event)
            .collect())
    }

    /// Human: Events due for delivery — new ones and failed ones whose retry time has come, oldest first — with
    /// the peers each has reached so far.
    /// Agent: SELECT status pending | failed with next_retry_at due, ORDER BY created_at; FLIPS listed failed rows to pending.
    pub(crate) async fn list_pending_deliveries(&self, limit: i64) -> Result<Vec<PendingDelivery>, StorageError> {
        let now = Utc::now().timestamp();
        let rows = sqlx::query_as::<_, ReplicationRow>(
            "SELECT event_id, origin_node, op, bucket, key, etag, size, payload_path, storage_class, COALESCE(replication_group, 'default') AS replication_group, content_type, custom_meta, wire_checksum, created_at, version, delivered_to, status
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
            .map(|row| {
                let delivered_to = row
                    .delivered_to
                    .as_deref()
                    .unwrap_or_default()
                    .split(',')
                    .filter(|peer| !peer.is_empty())
                    .map(str::to_string)
                    .collect();
                Ok(PendingDelivery {
                    event: row.into_event()?,
                    delivered_to,
                })
            })
            .collect()
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
                "superseded" => report.superseded = n,
                _ => {}
            }
        }

        // Human: Failed events are waiting for their retry too. MIN over no rows is NULL — it used to be read as 0,
        // reporting an age of ~56 years whenever nothing was waiting.
        let oldest: Option<i64> = sqlx::query_scalar(
            "SELECT MIN(created_at) FROM replication_log WHERE status IN ('pending', 'failed')",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(internal)?;
        report.oldest_pending_age_secs = oldest.map(|ts| now.saturating_sub(ts).max(0));
        Ok(report)
    }

    pub async fn mark_sent(&self, event_id: &str) -> Result<(), StorageError> {
        self.set_status(event_id, "sent").await
    }

    /// The event needn't be sent: a newer change to its key replaced it.
    pub(crate) async fn mark_superseded(&self, event_id: &str) -> Result<(), StorageError> {
        self.set_status(event_id, "superseded").await
    }

    async fn set_status(&self, event_id: &str, status: &str) -> Result<(), StorageError> {
        sqlx::query("UPDATE replication_log SET status = ? WHERE event_id = ?")
            .bind(status)
            .bind(event_id)
            .execute(&self.pool)
            .await
            .map_err(internal)?;
        Ok(())
    }

    /// Human: Remember that `peer_id` has the event, so retries go only to the peers still missing it.
    /// Agent: APPENDS peer_id to the comma-separated delivered_to column.
    pub(crate) async fn mark_delivered(&self, event_id: &str, peer_id: &str) -> Result<(), StorageError> {
        sqlx::query(
            "UPDATE replication_log SET delivered_to = COALESCE(delivered_to || ',', '') || ? WHERE event_id = ?",
        )
        .bind(peer_id)
        .bind(event_id)
        .execute(&self.pool)
        .await
        .map_err(internal)?;
        Ok(())
    }

    /// Drop finished history (see `prune_history`).
    pub async fn prune(&self, retention_secs: i64) -> Result<PruneReport, StorageError> {
        prune_history(&self.pool, retention_secs).await
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
        self.insert_applied(&self.pool, event).await
    }

    async fn insert_applied<'e, E>(&self, executor: E, event: &ReplicationEvent) -> Result<bool, StorageError>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let now = Utc::now().timestamp();
        let result = sqlx::query(
            "INSERT INTO replication_log (event_id, origin_node, op, bucket, key, etag, size, payload_path, storage_class, replication_group, content_type, custom_meta, wire_checksum, created_at, version, applied_at, status)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'applied')
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
        .bind(event.version)
        .bind(now)
        .execute(executor)
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

/// Human: Drop finished replication history — events sent, superseded or applied — and tombstones recorded more
/// than `retention_secs` ago. Pending, failed and dead-letter events are kept.
/// Agent: DELETES in batches of PRUNE_BATCH until a batch comes back short; RETURNS the counts removed.
pub async fn prune_history(pool: &SqlitePool, retention_secs: i64) -> Result<PruneReport, StorageError> {
    let before = Utc::now().timestamp().saturating_sub(retention_secs);
    let mut report = PruneReport::default();
    loop {
        let removed = sqlx::query(
            "DELETE FROM replication_log WHERE rowid IN (
                 SELECT rowid FROM replication_log
                 WHERE status IN ('sent', 'applied', 'superseded') AND created_at < ? LIMIT ?
             )",
        )
        .bind(before)
        .bind(PRUNE_BATCH)
        .execute(pool)
        .await
        .map_err(internal)?
        .rows_affected();
        report.events += removed;
        if removed < PRUNE_BATCH as u64 {
            break;
        }
    }
    loop {
        let removed = versions::prune_tombstones(pool, before, PRUNE_BATCH).await?;
        report.tombstones += removed;
        if removed < PRUNE_BATCH as u64 {
            break;
        }
    }
    Ok(report)
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
    version: Option<i64>,
    delivered_to: Option<String>,
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
            version: self.version.unwrap_or(0),
        })
    }
}
