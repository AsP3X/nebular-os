use std::path::PathBuf;

use chrono::{DateTime, Utc};
use sqlx::postgres::PgPoolOptions;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Pool, Postgres, Sqlite, SqlitePool};
use tokio::fs;

use super::error::{internal, StorageError};
use super::metadata_backend::MetadataBackendKind;
use super::types::ObjectMetadata;
use super::blob_rel_path;

const META_SELECT_SQLITE: &str =
    "bucket, key, size, mime_type, etag, created_at, updated_at, custom_meta, deleted_at, storage_class, origin_node";
const ACTIVE_WHERE_SQLITE: &str = "deleted_at IS NULL";

const META_SELECT_PG: &str = "bucket, object_key AS key, size_bytes AS size, content_type AS mime_type, etag, \
    created_at, updated_at, custom_meta, deleted_at, storage_class, origin_node";
const ACTIVE_WHERE_PG: &str = "deleted_at IS NULL";
const OBJECTS_TABLE_PG: &str = "nos_objects";
const OBJECTS_TABLE_SQLITE: &str = "objects";
/// Human: Case-sensitive "key starts with ?" for SQLite, whose LIKE ignores ASCII case.
/// Agent: BINDS prefix twice; `key >= prefix` lets the (bucket, key) index seek, instr() = 1 is the exact test.
const PREFIX_WHERE_SQLITE: &str = "key >= ? AND instr(key, ?) = 1";

// Human: Maintenance walks in key order. INDEXED BY pins the plan: with both the cursor and the limit bound,
// SQLite 3.46 picks a full scan plus sort over the partial index (66 ms vs 0.2 ms per page at 1M objects).
const SQLITE_KEY_PAGE_FIRST: &str = "SELECT bucket, key, size FROM objects INDEXED BY idx_objects_active_key \
     WHERE deleted_at IS NULL ORDER BY key LIMIT ?";
const SQLITE_KEY_PAGE_AFTER: &str = "SELECT bucket, key, size FROM objects INDEXED BY idx_objects_active_key \
     WHERE deleted_at IS NULL AND key > ? ORDER BY key LIMIT ?";
const SQLITE_ACTIVE_WITH_KEY: &str = "SELECT bucket, key, size FROM objects INDEXED BY idx_objects_active_key \
     WHERE deleted_at IS NULL AND key = ? ORDER BY bucket";

/// `LIKE` pattern matching keys that start with `prefix` (Postgres LIKE is case-sensitive).
fn like_prefix_pattern(prefix: &str) -> String {
    let escaped = prefix
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_");
    format!("{escaped}%")
}

/// Human: A page of `list_key_page` — (bucket, key, size) rows, and whether more objects follow.
/// Agent: RESUME with start_after = `last_key()`; `is_truncated == false` means the walk is complete.
#[derive(Debug, Default)]
pub struct KeyPage {
    pub rows: Vec<(String, String, i64)>,
    pub is_truncated: bool,
    /// The key this page ends at (usually the last row's key).
    cursor: Option<String>,
}

impl KeyPage {
    /// Where the next page starts: the key this page ends at.
    pub fn last_key(&self) -> Option<&str> {
        self.cursor.as_deref()
    }

    fn ending_at_last_row(rows: Vec<(String, String, i64)>, is_truncated: bool) -> Self {
        let cursor = rows.last().map(|(_, key, _)| key.clone());
        Self {
            rows,
            is_truncated,
            cursor,
        }
    }
}

#[derive(Clone)]
pub struct ObjectMetaStore {
    inner: ObjectMetaInner,
}

#[derive(Clone)]
enum ObjectMetaInner {
    Sqlite {
        write: Pool<Sqlite>,
        read: Pool<Sqlite>,
    },
    Postgres {
        write: Pool<Postgres>,
        read: Pool<Postgres>,
    },
}

pub struct ObjectMetaConnect {
    pub backend: MetadataBackendKind,
    pub sqlite_path: String,
    pub postgres_url: Option<String>,
    pub read_pool_size: u32,
}

impl ObjectMetaStore {
    pub fn backend(&self) -> MetadataBackendKind {
        match &self.inner {
            ObjectMetaInner::Sqlite { .. } => MetadataBackendKind::Sqlite,
            ObjectMetaInner::Postgres { .. } => MetadataBackendKind::Postgres,
        }
    }

    pub async fn connect(cfg: ObjectMetaConnect) -> Result<Self, StorageError> {
        match cfg.backend {
            MetadataBackendKind::Sqlite => {
                let conn_str = resolve_sqlite_conn_str(&cfg.sqlite_path).await?;
                let write = SqlitePool::connect(&conn_str).await.map_err(internal)?;
                let read_pool_size = cfg.read_pool_size.max(1);
                let read = SqlitePoolOptions::new()
                    .max_connections(read_pool_size)
                    .connect(&conn_str)
                    .await
                    .map_err(internal)?;
                init_sqlite_object_schema(&write).await?;
                init_system_sqlite_schema(&write).await?;
                Ok(Self {
                    inner: ObjectMetaInner::Sqlite { write, read },
                })
            }
            MetadataBackendKind::Postgres => {
                let url = cfg
                    .postgres_url
                    .as_deref()
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| {
                        StorageError::Internal(anyhow::anyhow!(
                            "NOS_METADATA_DATABASE_URL is required when NOS_METADATA_BACKEND=postgres"
                        ))
                    })?;
                let write = PgPoolOptions::new()
                    .max_connections(4)
                    .connect(url)
                    .await
                    .map_err(internal)?;
                let read = PgPoolOptions::new()
                    .max_connections(cfg.read_pool_size.max(1))
                    .connect(url)
                    .await
                    .map_err(internal)?;
                run_postgres_migrations(&write).await?;
                let indexes = write.clone();
                tokio::spawn(async move {
                    if let Err(e) = build_postgres_online_indexes(&indexes).await {
                        tracing::warn!(error = %e, "building metadata indexes failed; retried at next start");
                    }
                });
                Ok(Self {
                    inner: ObjectMetaInner::Postgres { write, read },
                })
            }
        }
    }

    pub fn sqlite_write_pool(&self) -> Option<&Pool<Sqlite>> {
        match &self.inner {
            ObjectMetaInner::Sqlite { write, .. } => Some(write),
            ObjectMetaInner::Postgres { .. } => None,
        }
    }

    pub fn sqlite_read_pool(&self) -> Option<&Pool<Sqlite>> {
        match &self.inner {
            ObjectMetaInner::Sqlite { read, .. } => Some(read),
            ObjectMetaInner::Postgres { .. } => None,
        }
    }

    pub async fn probe(&self) -> (bool, bool) {
        match &self.inner {
            ObjectMetaInner::Sqlite { write, read } => {
                let w = sqlx::query("SELECT 1")
                    .fetch_one(write)
                    .await
                    .is_ok();
                let r = sqlx::query("SELECT 1").fetch_one(read).await.is_ok();
                (w, r)
            }
            ObjectMetaInner::Postgres { write, read } => {
                let w = sqlx::query("SELECT 1")
                    .fetch_one(write)
                    .await
                    .is_ok();
                let r = sqlx::query("SELECT 1").fetch_one(read).await.is_ok();
                (w, r)
            }
        }
    }

    pub async fn try_fetch_active_metadata(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<ObjectMetadata>, StorageError> {
        match &self.inner {
            ObjectMetaInner::Sqlite { read, .. } => {
                let q = format!(
                    "SELECT {META_SELECT_SQLITE} FROM {OBJECTS_TABLE_SQLITE} WHERE bucket = ? AND key = ? AND {ACTIVE_WHERE_SQLITE}"
                );
                sqlx::query_as(&q)
                    .bind(bucket)
                    .bind(key)
                    .fetch_optional(read)
                    .await
                    .map_err(internal)
            }
            ObjectMetaInner::Postgres { read, .. } => {
                let q = format!(
                    "SELECT {META_SELECT_PG} FROM {OBJECTS_TABLE_PG} WHERE bucket = $1 AND object_key = $2 AND {ACTIVE_WHERE_PG}"
                );
                sqlx::query_as(&q)
                    .bind(bucket)
                    .bind(key)
                    .fetch_optional(read)
                    .await
                    .map_err(internal)
            }
        }
    }

    pub async fn fetch_active_metadata(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<ObjectMetadata, StorageError> {
        self.try_fetch_active_metadata(bucket, key)
            .await?
            .ok_or(StorageError::NotFound)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn upsert_object(
        &self,
        data_dir: &str,
        bucket: &str,
        key: &str,
        size: i64,
        content_type: Option<&str>,
        etag: &str,
        custom_meta: Option<&str>,
        storage_class: Option<&str>,
        origin_node: Option<&str>,
    ) -> Result<ObjectMetadata, StorageError> {
        let now = Utc::now();
        match &self.inner {
            ObjectMetaInner::Sqlite { write, .. } => {
                let unix_now = now.timestamp();
                sqlx::query(
                    "INSERT INTO objects (bucket, key, size, mime_type, etag, created_at, updated_at, custom_meta, deleted_at, storage_class, origin_node)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)
                     ON CONFLICT(bucket, key) DO UPDATE SET
                         size = excluded.size,
                         mime_type = excluded.mime_type,
                         etag = excluded.etag,
                         updated_at = excluded.updated_at,
                         custom_meta = excluded.custom_meta,
                         deleted_at = NULL,
                         storage_class = COALESCE(excluded.storage_class, objects.storage_class),
                         origin_node = COALESCE(excluded.origin_node, objects.origin_node)",
                )
                .bind(bucket)
                .bind(key)
                .bind(size)
                .bind(content_type)
                .bind(etag)
                .bind(unix_now)
                .bind(unix_now)
                .bind(custom_meta)
                .bind(storage_class)
                .bind(origin_node)
                .execute(write)
                .await
                .map_err(internal)?;
            }
            ObjectMetaInner::Postgres { write, .. } => {
                let blob_path = blob_rel_path(bucket, key);
                sqlx::query(
                    "INSERT INTO nos_objects (bucket, object_key, blob_path, size_bytes, content_type, etag, custom_meta, storage_class, origin_node, deleted_at)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULL)
                     ON CONFLICT (bucket, object_key) DO UPDATE SET
                         blob_path = EXCLUDED.blob_path,
                         size_bytes = EXCLUDED.size_bytes,
                         content_type = EXCLUDED.content_type,
                         etag = EXCLUDED.etag,
                         updated_at = now(),
                         custom_meta = EXCLUDED.custom_meta,
                         deleted_at = NULL,
                         storage_class = COALESCE(EXCLUDED.storage_class, nos_objects.storage_class),
                         origin_node = COALESCE(EXCLUDED.origin_node, nos_objects.origin_node)",
                )
                .bind(bucket)
                .bind(key)
                .bind(&blob_path)
                .bind(size)
                .bind(content_type)
                .bind(etag)
                .bind(custom_meta)
                .bind(storage_class.unwrap_or("default"))
                .bind(origin_node)
                .execute(write)
                .await
                .map_err(internal)?;
                let _ = data_dir;
            }
        }
        Ok(ObjectMetadata {
            bucket: bucket.to_string(),
            key: key.to_string(),
            size,
            mime_type: content_type.map(|s| s.to_string()),
            etag: Some(etag.to_string()),
            created_at: now,
            updated_at: now,
            custom_meta: custom_meta.map(|s| s.to_string()),
            deleted_at: None,
            storage_class: storage_class.map(|s| s.to_string()),
            origin_node: origin_node.map(|s| s.to_string()),
        })
    }

    pub async fn copy_object_metadata(
        &self,
        src: &ObjectMetadata,
        dst_bucket: &str,
        dst_key: &str,
    ) -> Result<ObjectMetadata, StorageError> {
        self.upsert_object(
            "",
            dst_bucket,
            dst_key,
            src.size,
            src.mime_type.as_deref(),
            src.etag.as_deref().unwrap_or(""),
            src.custom_meta.as_deref(),
            src.storage_class.as_deref(),
            src.origin_node.as_deref(),
        )
        .await
    }

    pub async fn active_row_count(&self, bucket: &str, key: &str) -> Result<i64, StorageError> {
        match &self.inner {
            ObjectMetaInner::Sqlite { write, .. } => {
                let q = format!(
                    "SELECT COUNT(*) FROM {OBJECTS_TABLE_SQLITE} WHERE bucket = ? AND key = ? AND {ACTIVE_WHERE_SQLITE}"
                );
                sqlx::query_scalar(&q)
                    .bind(bucket)
                    .bind(key)
                    .fetch_one(write)
                    .await
                    .map_err(internal)
            }
            ObjectMetaInner::Postgres { write, .. } => {
                let q = format!(
                    "SELECT COUNT(*)::bigint FROM {OBJECTS_TABLE_PG} WHERE bucket = $1 AND object_key = $2 AND {ACTIVE_WHERE_PG}"
                );
                sqlx::query_scalar(&q)
                    .bind(bucket)
                    .bind(key)
                    .fetch_one(write)
                    .await
                    .map_err(internal)
            }
        }
    }

    pub async fn hard_delete_object(&self, bucket: &str, key: &str) -> Result<(), StorageError> {
        match &self.inner {
            ObjectMetaInner::Sqlite { write, .. } => {
                sqlx::query("DELETE FROM objects WHERE bucket = ? AND key = ?")
                    .bind(bucket)
                    .bind(key)
                    .execute(write)
                    .await
                    .map_err(internal)?;
            }
            ObjectMetaInner::Postgres { write, .. } => {
                sqlx::query("DELETE FROM nos_objects WHERE bucket = $1 AND object_key = $2")
                    .bind(bucket)
                    .bind(key)
                    .execute(write)
                    .await
                    .map_err(internal)?;
            }
        }
        Ok(())
    }

    pub async fn soft_delete_object(&self, bucket: &str, key: &str) -> Result<(), StorageError> {
        match &self.inner {
            ObjectMetaInner::Sqlite { write, .. } => {
                let now = Utc::now().timestamp();
                let q = format!(
                    "UPDATE objects SET deleted_at = ? WHERE bucket = ? AND key = ? AND {ACTIVE_WHERE_SQLITE}"
                );
                sqlx::query(&q)
                    .bind(now)
                    .bind(bucket)
                    .bind(key)
                    .execute(write)
                    .await
                    .map_err(internal)?;
            }
            ObjectMetaInner::Postgres { write, .. } => {
                let q = format!(
                    "UPDATE nos_objects SET deleted_at = now() WHERE bucket = $1 AND object_key = $2 AND {ACTIVE_WHERE_PG}"
                );
                sqlx::query(&q)
                    .bind(bucket)
                    .bind(key)
                    .execute(write)
                    .await
                    .map_err(internal)?;
            }
        }
        Ok(())
    }

    /// Soft-deletes many active objects in one metadata transaction.
    pub async fn soft_delete_objects(
        &self,
        bucket: &str,
        keys: &[String],
    ) -> Result<u64, StorageError> {
        if keys.is_empty() {
            return Ok(0);
        }
        match &self.inner {
            ObjectMetaInner::Sqlite { write, .. } => {
                let now = Utc::now().timestamp();
                let q = format!(
                    "UPDATE objects SET deleted_at = ? WHERE bucket = ? AND key = ? AND {ACTIVE_WHERE_SQLITE}"
                );
                let mut tx = write.begin().await.map_err(internal)?;
                for key in keys {
                    sqlx::query(&q)
                        .bind(now)
                        .bind(bucket)
                        .bind(key)
                        .execute(&mut *tx)
                        .await
                        .map_err(internal)?;
                }
                tx.commit().await.map_err(internal)?;
            }
            ObjectMetaInner::Postgres { write, .. } => {
                let q = format!(
                    "UPDATE nos_objects SET deleted_at = now() WHERE bucket = $1 AND object_key = $2 AND {ACTIVE_WHERE_PG}"
                );
                let mut tx = write.begin().await.map_err(internal)?;
                for key in keys {
                    sqlx::query(&q)
                        .bind(bucket)
                        .bind(key)
                        .execute(&mut *tx)
                        .await
                        .map_err(internal)?;
                }
                tx.commit().await.map_err(internal)?;
            }
        }
        Ok(keys.len() as u64)
    }

    /// Hard-deletes metadata rows for many objects in one transaction.
    pub async fn hard_delete_objects(
        &self,
        bucket: &str,
        keys: &[String],
    ) -> Result<u64, StorageError> {
        if keys.is_empty() {
            return Ok(0);
        }
        match &self.inner {
            ObjectMetaInner::Sqlite { write, .. } => {
                let mut tx = write.begin().await.map_err(internal)?;
                for key in keys {
                    sqlx::query("DELETE FROM objects WHERE bucket = ? AND key = ?")
                        .bind(bucket)
                        .bind(key)
                        .execute(&mut *tx)
                        .await
                        .map_err(internal)?;
                }
                tx.commit().await.map_err(internal)?;
            }
            ObjectMetaInner::Postgres { write, .. } => {
                let mut tx = write.begin().await.map_err(internal)?;
                for key in keys {
                    sqlx::query("DELETE FROM nos_objects WHERE bucket = $1 AND object_key = $2")
                        .bind(bucket)
                        .bind(key)
                        .execute(&mut *tx)
                        .await
                        .map_err(internal)?;
                }
                tx.commit().await.map_err(internal)?;
            }
        }
        Ok(keys.len() as u64)
    }

    /// Active rows whose key starts with `prefix` (case-sensitive), ordered by key, after `start_after`.
    pub async fn list_active_rows(
        &self,
        bucket: &str,
        start_after: &str,
        prefix: &str,
        limit: i64,
    ) -> Result<Vec<ObjectMetadata>, StorageError> {
        match &self.inner {
            ObjectMetaInner::Sqlite { read, .. } => {
                let q = format!(
                    "SELECT {META_SELECT_SQLITE} FROM {OBJECTS_TABLE_SQLITE}
                     WHERE bucket = ? AND key > ? AND {PREFIX_WHERE_SQLITE} AND {ACTIVE_WHERE_SQLITE}
                     ORDER BY key LIMIT ?"
                );
                sqlx::query_as(&q)
                    .bind(bucket)
                    .bind(start_after)
                    .bind(prefix)
                    .bind(prefix)
                    .bind(limit)
                    .fetch_all(read)
                    .await
                    .map_err(internal)
            }
            ObjectMetaInner::Postgres { read, .. } => {
                let q = format!(
                    "SELECT {META_SELECT_PG} FROM {OBJECTS_TABLE_PG}
                     WHERE bucket = $1 AND object_key > $2 AND object_key LIKE $3 ESCAPE '\\' AND {ACTIVE_WHERE_PG}
                     ORDER BY object_key LIMIT $4"
                );
                sqlx::query_as(&q)
                    .bind(bucket)
                    .bind(start_after)
                    .bind(like_prefix_pattern(prefix))
                    .bind(limit)
                    .fetch_all(read)
                    .await
                    .map_err(internal)
            }
        }
    }

    pub async fn count_keys_after(
        &self,
        bucket: &str,
        last_key: &str,
        prefix: &str,
    ) -> Result<i64, StorageError> {
        match &self.inner {
            ObjectMetaInner::Sqlite { read, .. } => {
                let q = format!(
                    "SELECT COUNT(*) FROM {OBJECTS_TABLE_SQLITE}
                     WHERE bucket = ? AND key > ? AND {PREFIX_WHERE_SQLITE} AND {ACTIVE_WHERE_SQLITE}"
                );
                sqlx::query_scalar(&q)
                    .bind(bucket)
                    .bind(last_key)
                    .bind(prefix)
                    .bind(prefix)
                    .fetch_one(read)
                    .await
                    .map_err(internal)
            }
            ObjectMetaInner::Postgres { read, .. } => {
                let q = format!(
                    "SELECT COUNT(*)::bigint FROM {OBJECTS_TABLE_PG}
                     WHERE bucket = $1 AND object_key > $2 AND object_key LIKE $3 ESCAPE '\\' AND {ACTIVE_WHERE_PG}"
                );
                sqlx::query_scalar(&q)
                    .bind(bucket)
                    .bind(last_key)
                    .bind(like_prefix_pattern(prefix))
                    .fetch_one(read)
                    .await
                    .map_err(internal)
            }
        }
    }

    pub async fn count_active_with_prefix(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> Result<i64, StorageError> {
        match &self.inner {
            ObjectMetaInner::Sqlite { read, .. } => {
                let q = format!(
                    "SELECT COUNT(*) FROM {OBJECTS_TABLE_SQLITE}
                     WHERE bucket = ? AND {PREFIX_WHERE_SQLITE} AND {ACTIVE_WHERE_SQLITE}"
                );
                sqlx::query_scalar(&q)
                    .bind(bucket)
                    .bind(prefix)
                    .bind(prefix)
                    .fetch_one(read)
                    .await
                    .map_err(internal)
            }
            ObjectMetaInner::Postgres { read, .. } => {
                let q = format!(
                    "SELECT COUNT(*)::bigint FROM {OBJECTS_TABLE_PG}
                     WHERE bucket = $1 AND object_key LIKE $2 ESCAPE '\\' AND {ACTIVE_WHERE_PG}"
                );
                sqlx::query_scalar(&q)
                    .bind(bucket)
                    .bind(like_prefix_pattern(prefix))
                    .fetch_one(read)
                    .await
                    .map_err(internal)
            }
        }
    }

    pub async fn object_exists(&self, bucket: &str, key: &str) -> Result<bool, StorageError> {
        Ok(self.active_row_count(bucket, key).await? > 0)
    }

    pub async fn object_count(&self) -> Result<i64, StorageError> {
        match &self.inner {
            ObjectMetaInner::Sqlite { read, .. } => {
                let q = format!("SELECT COUNT(*) FROM {OBJECTS_TABLE_SQLITE} WHERE {ACTIVE_WHERE_SQLITE}");
                sqlx::query_scalar(&q).fetch_one(read).await.map_err(internal)
            }
            ObjectMetaInner::Postgres { read, .. } => {
                let q =
                    format!("SELECT COUNT(*)::bigint FROM {OBJECTS_TABLE_PG} WHERE {ACTIVE_WHERE_PG}");
                sqlx::query_scalar(&q).fetch_one(read).await.map_err(internal)
            }
        }
    }

    pub async fn total_bytes(&self) -> Result<i64, StorageError> {
        match &self.inner {
            ObjectMetaInner::Sqlite { read, .. } => {
                let q = format!(
                    "SELECT COALESCE(SUM(size), 0) FROM {OBJECTS_TABLE_SQLITE} WHERE {ACTIVE_WHERE_SQLITE}"
                );
                sqlx::query_scalar(&q).fetch_one(read).await.map_err(internal)
            }
            ObjectMetaInner::Postgres { read, .. } => {
                let q = format!(
                    "SELECT COALESCE(SUM(size_bytes), 0)::bigint FROM {OBJECTS_TABLE_PG} WHERE {ACTIVE_WHERE_PG}"
                );
                sqlx::query_scalar(&q).fetch_one(read).await.map_err(internal)
            }
        }
    }

    pub async fn set_object_placement(
        &self,
        bucket: &str,
        key: &str,
        storage_class: &str,
        origin_node: &str,
    ) -> Result<(), StorageError> {
        match &self.inner {
            ObjectMetaInner::Sqlite { write, .. } => {
                sqlx::query(
                    "UPDATE objects SET storage_class = ?, origin_node = ? WHERE bucket = ? AND key = ? AND deleted_at IS NULL",
                )
                .bind(storage_class)
                .bind(origin_node)
                .bind(bucket)
                .bind(key)
                .execute(write)
                .await
                .map_err(internal)?;
            }
            ObjectMetaInner::Postgres { write, .. } => {
                sqlx::query(
                    "UPDATE nos_objects SET storage_class = $1, origin_node = $2 WHERE bucket = $3 AND object_key = $4 AND deleted_at IS NULL",
                )
                .bind(storage_class)
                .bind(origin_node)
                .bind(bucket)
                .bind(key)
                .execute(write)
                .await
                .map_err(internal)?;
            }
        }
        Ok(())
    }

    pub async fn active_storage_class(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<String>, StorageError> {
        match &self.inner {
            ObjectMetaInner::Sqlite { read, .. } => {
                let row: Option<(String,)> = sqlx::query_as(
                    "SELECT COALESCE(storage_class, 'default') FROM objects WHERE bucket = ? AND key = ? AND deleted_at IS NULL",
                )
                .bind(bucket)
                .bind(key)
                .fetch_optional(read)
                .await
                .map_err(internal)?;
                Ok(row.map(|(c,)| c))
            }
            ObjectMetaInner::Postgres { read, .. } => {
                let row: Option<(String,)> = sqlx::query_as(
                    "SELECT COALESCE(storage_class, 'default') FROM nos_objects WHERE bucket = $1 AND object_key = $2 AND deleted_at IS NULL",
                )
                .bind(bucket)
                .bind(key)
                .fetch_optional(read)
                .await
                .map_err(internal)?;
                Ok(row.map(|(c,)| c))
            }
        }
    }

    pub async fn objects_by_storage_class(&self) -> Result<Vec<(String, i64)>, StorageError> {
        match &self.inner {
            ObjectMetaInner::Sqlite { read, .. } => {
                sqlx::query_as(
                    "SELECT COALESCE(storage_class, 'default'), COUNT(*) FROM objects WHERE deleted_at IS NULL GROUP BY storage_class",
                )
                .fetch_all(read)
                .await
                .map_err(internal)
            }
            ObjectMetaInner::Postgres { read, .. } => {
                sqlx::query_as(
                    "SELECT COALESCE(storage_class, 'default'), COUNT(*)::bigint FROM nos_objects WHERE deleted_at IS NULL GROUP BY storage_class",
                )
                .fetch_all(read)
                .await
                .map_err(internal)
            }
        }
    }

    pub async fn list_active_bucket_keys(&self) -> Result<Vec<(String, String)>, StorageError> {
        match &self.inner {
            ObjectMetaInner::Sqlite { write, .. } => {
                sqlx::query_as(
                    "SELECT bucket, key FROM objects WHERE deleted_at IS NULL",
                )
                .fetch_all(write)
                .await
                .map_err(internal)
            }
            ObjectMetaInner::Postgres { write, .. } => {
                sqlx::query_as(
                    "SELECT bucket, object_key AS key FROM nos_objects WHERE deleted_at IS NULL",
                )
                .fetch_all(write)
                .await
                .map_err(internal)
            }
        }
    }

    pub async fn delete_object_row(&self, bucket: &str, key: &str) -> Result<(), StorageError> {
        self.hard_delete_object(bucket, key).await
    }

    /// Human: Refresh stored relative blob path after on-disk layout migration (Postgres metadata mode).
    /// Agent: UPDATE nos_objects.blob_path; NO-OP for SQLite where paths are derived from keys.
    pub async fn update_blob_path(
        &self,
        bucket: &str,
        key: &str,
        blob_path: &str,
    ) -> Result<(), StorageError> {
        match &self.inner {
            ObjectMetaInner::Sqlite { .. } => {}
            ObjectMetaInner::Postgres { write, .. } => {
                sqlx::query(
                    "UPDATE nos_objects SET blob_path = $1, updated_at = now() WHERE bucket = $2 AND object_key = $3",
                )
                .bind(blob_path)
                .bind(bucket)
                .bind(key)
                .execute(write)
                .await
                .map_err(internal)?;
            }
        }
        Ok(())
    }

    /// Human: Permanently remove a row only if it is still soft-deleted before `cutoff_ts` — a key that was
    /// re-created since it was listed for purge must survive. RETURNS whether a row was removed.
    pub async fn purge_soft_deleted_row(
        &self,
        bucket: &str,
        key: &str,
        cutoff_ts: i64,
    ) -> Result<bool, StorageError> {
        let affected = match &self.inner {
            ObjectMetaInner::Sqlite { write, .. } => sqlx::query(
                "DELETE FROM objects WHERE bucket = ? AND key = ? AND deleted_at IS NOT NULL AND deleted_at < ?",
            )
            .bind(bucket)
            .bind(key)
            .bind(cutoff_ts)
            .execute(write)
            .await
            .map_err(internal)?
            .rows_affected(),
            ObjectMetaInner::Postgres { write, .. } => {
                let cutoff = DateTime::from_timestamp(cutoff_ts, 0).unwrap_or_else(Utc::now);
                sqlx::query(
                    "DELETE FROM nos_objects WHERE bucket = $1 AND object_key = $2 AND deleted_at IS NOT NULL AND deleted_at < $3",
                )
                .bind(bucket)
                .bind(key)
                .bind(cutoff)
                .execute(write)
                .await
                .map_err(internal)?
                .rows_affected()
            }
        };
        Ok(affected > 0)
    }

    pub async fn list_soft_deleted_before(
        &self,
        cutoff_ts: i64,
    ) -> Result<Vec<(String, String)>, StorageError> {
        match &self.inner {
            ObjectMetaInner::Sqlite { read, .. } => {
                sqlx::query_as(
                    "SELECT bucket, key FROM objects WHERE deleted_at IS NOT NULL AND deleted_at < ?",
                )
                .bind(cutoff_ts)
                .fetch_all(read)
                .await
                .map_err(internal)
            }
            ObjectMetaInner::Postgres { read, .. } => {
                let cutoff = DateTime::from_timestamp(cutoff_ts, 0).unwrap_or_else(Utc::now);
                sqlx::query_as(
                    "SELECT bucket, object_key AS key FROM nos_objects WHERE deleted_at IS NOT NULL AND deleted_at < $1",
                )
                .bind(cutoff)
                .fetch_all(read)
                .await
                .map_err(internal)
            }
        }
    }

    pub async fn list_recompress_candidates(
        &self,
        limit: i64,
    ) -> Result<Vec<(String, String, i64)>, StorageError> {
        match &self.inner {
            ObjectMetaInner::Sqlite { read, .. } => {
                sqlx::query_as(
                    "SELECT bucket, key, size FROM objects WHERE deleted_at IS NULL ORDER BY updated_at LIMIT ?",
                )
                .bind(limit)
                .fetch_all(read)
                .await
                .map_err(internal)
            }
            ObjectMetaInner::Postgres { read, .. } => {
                sqlx::query_as(
                    "SELECT bucket, object_key AS key, size_bytes AS size FROM nos_objects WHERE deleted_at IS NULL ORDER BY updated_at LIMIT $1",
                )
                .bind(limit)
                .fetch_all(read)
                .await
                .map_err(internal)
            }
        }
    }

    /// Human: Paginated active objects for layout/format migration batches.
    /// Agent: ORDER BY key; READS start_after as exclusive cursor; RETURNS (bucket, key, size).
    pub async fn list_migration_page(
        &self,
        limit: i64,
        start_after: Option<&str>,
    ) -> Result<Vec<(String, String, i64)>, StorageError> {
        match &self.inner {
            ObjectMetaInner::Sqlite { read, .. } => {
                if let Some(after) = start_after {
                    sqlx::query_as(SQLITE_KEY_PAGE_AFTER)
                        .bind(after)
                        .bind(limit)
                        .fetch_all(read)
                        .await
                        .map_err(internal)
                } else {
                    sqlx::query_as(SQLITE_KEY_PAGE_FIRST)
                        .bind(limit)
                        .fetch_all(read)
                        .await
                        .map_err(internal)
                }
            }
            // Human: Two statements rather than `$1 IS NULL OR object_key > $1`, which the planner can't turn
            // into an index range — every page would scan the index from the first key.
            ObjectMetaInner::Postgres { read, .. } => {
                if let Some(after) = start_after {
                    sqlx::query_as(
                        "SELECT bucket, object_key AS key, size_bytes AS size FROM nos_objects \
                         WHERE deleted_at IS NULL AND object_key > $1 ORDER BY object_key LIMIT $2",
                    )
                    .bind(after)
                    .bind(limit)
                    .fetch_all(read)
                    .await
                    .map_err(internal)
                } else {
                    sqlx::query_as(
                        "SELECT bucket, object_key AS key, size_bytes AS size FROM nos_objects \
                         WHERE deleted_at IS NULL ORDER BY object_key LIMIT $1",
                    )
                    .bind(limit)
                    .fetch_all(read)
                    .await
                    .map_err(internal)
                }
            }
        }
    }

    /// Human: One page of a maintenance walk over active objects in key order, resumed after the previous
    /// page's last key. A page never ends partway through a run of equal keys (one key in several buckets):
    /// the cursor is only the key, so a split run would have its remaining buckets skipped.
    /// Agent: FETCHES limit+1 to detect more; DROPS a trailing equal-key run that continues past the page, or,
    /// when the whole page is that run, TAKES the complete run (so the page can exceed `limit` by #buckets).
    pub async fn list_key_page(
        &self,
        limit: i64,
        start_after: Option<&str>,
    ) -> Result<KeyPage, StorageError> {
        let limit = limit.max(1);
        let mut rows = self.list_migration_page(limit + 1, start_after).await?;
        if rows.len() as i64 <= limit {
            return Ok(KeyPage::ending_at_last_row(rows, false));
        }
        let boundary = rows.pop().map(|(_, key, _)| key).unwrap_or_default();
        while rows.last().is_some_and(|(_, key, _)| *key == boundary) {
            rows.pop();
        }
        if rows.is_empty() {
            // Human: The cursor is the run's key even if its rows vanished meanwhile, so the walk still advances.
            let run = self.list_active_with_key(&boundary).await?;
            let is_truncated = !self
                .list_migration_page(1, Some(&boundary))
                .await?
                .is_empty();
            return Ok(KeyPage {
                rows: run,
                is_truncated,
                cursor: Some(boundary),
            });
        }
        Ok(KeyPage::ending_at_last_row(rows, true))
    }

    /// Every active object stored under `key`, across buckets.
    async fn list_active_with_key(
        &self,
        key: &str,
    ) -> Result<Vec<(String, String, i64)>, StorageError> {
        match &self.inner {
            ObjectMetaInner::Sqlite { read, .. } => sqlx::query_as(SQLITE_ACTIVE_WITH_KEY)
            .bind(key)
            .fetch_all(read)
            .await
            .map_err(internal),
            ObjectMetaInner::Postgres { read, .. } => sqlx::query_as(
                "SELECT bucket, object_key AS key, size_bytes AS size FROM nos_objects \
                 WHERE deleted_at IS NULL AND object_key = $1 ORDER BY bucket",
            )
            .bind(key)
            .fetch_all(read)
            .await
            .map_err(internal),
        }
    }

    // --- Multipart ---

    pub async fn init_multipart(
        &self,
        upload_id: &str,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
        ttl_secs: i64,
    ) -> Result<(), StorageError> {
        match &self.inner {
            ObjectMetaInner::Sqlite { write, .. } => {
                let now = Utc::now().timestamp();
                sqlx::query(
                    "INSERT INTO multipart_uploads (upload_id, bucket, key, content_type, created_at)
                     VALUES (?, ?, ?, ?, ?)",
                )
                .bind(upload_id)
                .bind(bucket)
                .bind(key)
                .bind(content_type)
                .bind(now)
                .execute(write)
                .await
                .map_err(internal)?;
            }
            ObjectMetaInner::Postgres { write, .. } => {
                let expires = Utc::now() + chrono::Duration::seconds(ttl_secs.max(60));
                sqlx::query(
                    "INSERT INTO nos_multipart_uploads (upload_id, bucket, object_key, content_type, expires_at)
                     VALUES ($1, $2, $3, $4, $5)",
                )
                .bind(upload_id)
                .bind(bucket)
                .bind(key)
                .bind(content_type)
                .bind(expires)
                .execute(write)
                .await
                .map_err(internal)?;
            }
        }
        Ok(())
    }

    pub async fn upsert_multipart_part(
        &self,
        upload_id: &str,
        part_number: i32,
        size: i64,
        etag: &str,
        part_blob_path: Option<&str>,
    ) -> Result<(), StorageError> {
        match &self.inner {
            ObjectMetaInner::Sqlite { write, .. } => {
                sqlx::query(
                    "INSERT INTO multipart_parts (upload_id, part_number, size, etag)
                     VALUES (?, ?, ?, ?)
                     ON CONFLICT(upload_id, part_number) DO UPDATE SET
                        size = excluded.size,
                        etag = excluded.etag",
                )
                .bind(upload_id)
                .bind(part_number)
                .bind(size)
                .bind(etag)
                .execute(write)
                .await
                .map_err(internal)?;
            }
            ObjectMetaInner::Postgres { write, .. } => {
                let blob_path = part_blob_path.unwrap_or("");
                sqlx::query(
                    "INSERT INTO nos_multipart_parts (upload_id, part_number, blob_path, size_bytes, etag)
                     VALUES ($1, $2, $3, $4, $5)
                     ON CONFLICT (upload_id, part_number) DO UPDATE SET
                        blob_path = EXCLUDED.blob_path,
                        size_bytes = EXCLUDED.size_bytes,
                        etag = EXCLUDED.etag",
                )
                .bind(upload_id)
                .bind(part_number)
                .bind(blob_path)
                .bind(size)
                .bind(etag)
                .execute(write)
                .await
                .map_err(internal)?;
            }
        }
        Ok(())
    }

    /// Recorded parts of an upload as (part_number, size, etag), ordered by part number.
    pub async fn list_multipart_parts(
        &self,
        upload_id: &str,
    ) -> Result<Vec<(i32, i64, String)>, StorageError> {
        match &self.inner {
            ObjectMetaInner::Sqlite { read, .. } => sqlx::query_as(
                "SELECT part_number, size, etag FROM multipart_parts WHERE upload_id = ? ORDER BY part_number",
            )
            .bind(upload_id)
            .fetch_all(read)
            .await
            .map_err(internal),
            ObjectMetaInner::Postgres { read, .. } => sqlx::query_as(
                "SELECT part_number, size_bytes, etag FROM nos_multipart_parts WHERE upload_id = $1 ORDER BY part_number",
            )
            .bind(upload_id)
            .fetch_all(read)
            .await
            .map_err(internal),
        }
    }

    pub async fn list_multipart_part_numbers(
        &self,
        upload_id: &str,
    ) -> Result<Vec<i32>, StorageError> {
        match &self.inner {
            ObjectMetaInner::Sqlite { read, .. } => {
                let rows: Vec<(i32,)> = sqlx::query_as(
                    "SELECT part_number FROM multipart_parts WHERE upload_id = ? ORDER BY part_number",
                )
                .bind(upload_id)
                .fetch_all(read)
                .await
                .map_err(internal)?;
                Ok(rows.into_iter().map(|(n,)| n).collect())
            }
            ObjectMetaInner::Postgres { read, .. } => {
                let rows: Vec<(i32,)> = sqlx::query_as(
                    "SELECT part_number FROM nos_multipart_parts WHERE upload_id = $1 ORDER BY part_number",
                )
                .bind(upload_id)
                .fetch_all(read)
                .await
                .map_err(internal)?;
                Ok(rows.into_iter().map(|(n,)| n).collect())
            }
        }
    }

    pub async fn sum_multipart_parts_size(&self, upload_id: &str) -> Result<i64, StorageError> {
        match &self.inner {
            ObjectMetaInner::Sqlite { read, .. } => {
                let total: (i64,) = sqlx::query_as(
                    "SELECT COALESCE(SUM(size), 0) FROM multipart_parts WHERE upload_id = ?",
                )
                .bind(upload_id)
                .fetch_one(read)
                .await
                .map_err(internal)?;
                Ok(total.0)
            }
            ObjectMetaInner::Postgres { read, .. } => {
                let total: (i64,) = sqlx::query_as(
                    "SELECT COALESCE(SUM(size_bytes), 0)::bigint FROM nos_multipart_parts WHERE upload_id = $1",
                )
                .bind(upload_id)
                .fetch_one(read)
                .await
                .map_err(internal)?;
                Ok(total.0)
            }
        }
    }

    /// Returns `Ok(None)` when the upload id does not exist; `Ok(Some(content_type))` when it matches bucket/key.
    pub async fn fetch_multipart_session(
        &self,
        upload_id: &str,
        bucket: &str,
        key: &str,
    ) -> Result<Option<Option<String>>, StorageError> {
        match &self.inner {
            ObjectMetaInner::Sqlite { read, .. } => {
                let row: Option<(String, String, Option<String>)> = sqlx::query_as(
                    "SELECT bucket, key, content_type FROM multipart_uploads WHERE upload_id = ?",
                )
                .bind(upload_id)
                .fetch_optional(read)
                .await
                .map_err(internal)?;
                let Some((b, k, ct)) = row else {
                    return Ok(None);
                };
                if b != bucket || k != key {
                    return Err(StorageError::NotFound);
                }
                Ok(Some(ct))
            }
            ObjectMetaInner::Postgres { read, .. } => {
                let row: Option<(String, String, Option<String>)> = sqlx::query_as(
                    "SELECT bucket, object_key, content_type FROM nos_multipart_uploads WHERE upload_id = $1",
                )
                .bind(upload_id)
                .fetch_optional(read)
                .await
                .map_err(internal)?;
                let Some((b, k, ct)) = row else {
                    return Ok(None);
                };
                if b != bucket || k != key {
                    return Err(StorageError::NotFound);
                }
                Ok(Some(ct))
            }
        }
    }

    pub async fn multipart_object_key(&self, upload_id: &str) -> Result<Option<String>, StorageError> {
        match &self.inner {
            ObjectMetaInner::Sqlite { read, .. } => {
                let row: Option<(String,)> = sqlx::query_as(
                    "SELECT key FROM multipart_uploads WHERE upload_id = ?",
                )
                .bind(upload_id)
                .fetch_optional(read)
                .await
                .map_err(internal)?;
                Ok(row.map(|(k,)| k))
            }
            ObjectMetaInner::Postgres { read, .. } => {
                let row: Option<(String,)> = sqlx::query_as(
                    "SELECT object_key FROM nos_multipart_uploads WHERE upload_id = $1",
                )
                .bind(upload_id)
                .fetch_optional(read)
                .await
                .map_err(internal)?;
                Ok(row.map(|(k,)| k))
            }
        }
    }

    pub async fn cleanup_multipart(&self, upload_id: &str) -> Result<(), StorageError> {
        match &self.inner {
            ObjectMetaInner::Sqlite { write, .. } => {
                sqlx::query("DELETE FROM multipart_parts WHERE upload_id = ?")
                    .bind(upload_id)
                    .execute(write)
                    .await
                    .map_err(internal)?;
                sqlx::query("DELETE FROM multipart_uploads WHERE upload_id = ?")
                    .bind(upload_id)
                    .execute(write)
                    .await
                    .map_err(internal)?;
            }
            ObjectMetaInner::Postgres { write, .. } => {
                sqlx::query("DELETE FROM nos_multipart_parts WHERE upload_id = $1")
                    .bind(upload_id)
                    .execute(write)
                    .await
                    .map_err(internal)?;
                sqlx::query("DELETE FROM nos_multipart_uploads WHERE upload_id = $1")
                    .bind(upload_id)
                    .execute(write)
                    .await
                    .map_err(internal)?;
            }
        }
        Ok(())
    }

    pub async fn list_stale_multipart_upload_ids(
        &self,
        cutoff_ts: i64,
    ) -> Result<Vec<String>, StorageError> {
        match &self.inner {
            ObjectMetaInner::Sqlite { read, .. } => {
                let rows: Vec<(String,)> = sqlx::query_as(
                    "SELECT upload_id FROM multipart_uploads WHERE created_at < ?",
                )
                .bind(cutoff_ts)
                .fetch_all(read)
                .await
                .map_err(internal)?;
                Ok(rows.into_iter().map(|(id,)| id).collect())
            }
            ObjectMetaInner::Postgres { read, .. } => {
                let cutoff = DateTime::from_timestamp(cutoff_ts, 0).unwrap_or_else(Utc::now);
                let rows: Vec<(String,)> = sqlx::query_as(
                    "SELECT upload_id FROM nos_multipart_uploads WHERE expires_at < $1",
                )
                .bind(cutoff)
                .fetch_all(read)
                .await
                .map_err(internal)?;
                Ok(rows.into_iter().map(|(id,)| id).collect())
            }
        }
    }

    pub async fn load_cluster_config_json(&self) -> Result<Option<String>, StorageError> {
        match &self.inner {
            ObjectMetaInner::Sqlite { read, .. } => {
                let row: Option<(String,)> =
                    sqlx::query_as("SELECT json FROM cluster_runtime_config WHERE id = ?")
                        .bind(1_i32)
                        .fetch_optional(read)
                        .await
                        .map_err(internal)?;
                Ok(row.map(|(j,)| j))
            }
            ObjectMetaInner::Postgres { read, .. } => {
                let row: Option<(String,)> =
                    sqlx::query_as("SELECT json FROM nos_cluster_runtime_config WHERE id = $1")
                        .bind(1_i32)
                        .fetch_optional(read)
                        .await
                        .map_err(internal)?;
                Ok(row.map(|(j,)| j))
            }
        }
    }

    pub async fn save_cluster_config_json(&self, json: &str) -> Result<(), StorageError> {
        match &self.inner {
            ObjectMetaInner::Sqlite { write, .. } => {
                sqlx::query(
                    "INSERT INTO cluster_runtime_config (id, json) VALUES (?, ?)
                     ON CONFLICT(id) DO UPDATE SET json = excluded.json",
                )
                .bind(1_i32)
                .bind(json)
                .execute(write)
                .await
                .map_err(internal)?;
            }
            ObjectMetaInner::Postgres { write, .. } => {
                sqlx::query(
                    "INSERT INTO nos_cluster_runtime_config (id, json) VALUES ($1, $2)
                     ON CONFLICT(id) DO UPDATE SET json = EXCLUDED.json",
                )
                .bind(1_i32)
                .bind(json)
                .execute(write)
                .await
                .map_err(internal)?;
            }
        }
        Ok(())
    }
}

async fn resolve_sqlite_conn_str(meta_path: &str) -> Result<String, StorageError> {
    if meta_path.starts_with("file:") {
        return Ok(meta_path.to_string());
    }
    let meta_path = meta_path.strip_prefix("./").unwrap_or(meta_path);
    let meta_path_buf = PathBuf::from(meta_path);
    let meta_path_buf = if meta_path_buf.is_absolute() {
        meta_path_buf
    } else {
        std::env::current_dir()
            .map_err(internal)?
            .join(meta_path_buf)
    };
    if let Some(parent) = meta_path_buf.parent() {
        fs::create_dir_all(parent).await.map_err(internal)?;
    }
    if !meta_path_buf.exists() {
        fs::File::create(&meta_path_buf)
            .await
            .map_err(internal)?;
    }
    Ok(meta_path_buf.to_string_lossy().to_string())
}

async fn init_sqlite_object_schema(pool: &Pool<Sqlite>) -> Result<(), StorageError> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS objects (
            bucket      TEXT NOT NULL,
            key         TEXT NOT NULL,
            size        INTEGER NOT NULL,
            mime_type   TEXT,
            etag        TEXT,
            created_at  INTEGER NOT NULL,
            updated_at  INTEGER NOT NULL,
            custom_meta TEXT,
            deleted_at  INTEGER,
            PRIMARY KEY (bucket, key)
        )",
    )
    .execute(pool)
    .await
    .map_err(internal)?;

    sqlx::query("CREATE INDEX IF NOT EXISTS idx_prefix ON objects(bucket, key)")
        .execute(pool)
        .await
        .map_err(internal)?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS multipart_uploads (
            upload_id    TEXT PRIMARY KEY,
            bucket       TEXT NOT NULL,
            key          TEXT NOT NULL,
            content_type TEXT,
            created_at   INTEGER NOT NULL
        )",
    )
    .execute(pool)
    .await
    .map_err(internal)?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS multipart_parts (
            upload_id    TEXT NOT NULL,
            part_number  INTEGER NOT NULL,
            size         INTEGER NOT NULL,
            etag         TEXT NOT NULL,
            PRIMARY KEY (upload_id, part_number)
        )",
    )
    .execute(pool)
    .await
    .map_err(internal)?;

    add_column_if_missing(pool, "objects", "deleted_at", "INTEGER").await?;
    add_column_if_missing(pool, "objects", "storage_class", "TEXT DEFAULT 'default'").await?;
    add_column_if_missing(pool, "objects", "origin_node", "TEXT").await?;

    // Human: Maintenance walks (scrub, recompression, migration, backfill) page through active objects in key
    // order; without this index every page was a full scan plus sort (66 ms per page at 1M objects vs 0.2 ms).
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_objects_active_key ON objects(key) WHERE deleted_at IS NULL",
    )
    .execute(pool)
    .await
    .map_err(internal)?;

    sqlx::query("PRAGMA foreign_keys = ON")
        .execute(pool)
        .await
        .map_err(internal)?;
    sqlx::query("PRAGMA journal_mode = WAL")
        .execute(pool)
        .await
        .map_err(internal)?;

    Ok(())
}

/// Human: Add a column to an existing table unless it is already there. Failures propagate — the old
/// `let _ = ALTER TABLE …` also swallowed "database is locked" or read-only errors, which left columns missing
/// and made queries fail later with "no such column".
/// Agent: `table`, `column` and `definition` are trusted literals (interpolated into DDL).
pub(crate) async fn add_column_if_missing(
    pool: &Pool<Sqlite>,
    table: &str,
    column: &str,
    definition: &str,
) -> Result<(), StorageError> {
    let has_column = || async {
        sqlx::query_scalar::<_, String>("SELECT name FROM pragma_table_info(?) WHERE name = ?")
            .bind(table)
            .bind(column)
            .fetch_optional(pool)
            .await
            .map(|found| found.is_some())
            .map_err(internal)
    };
    if has_column().await? {
        return Ok(());
    }
    let added = sqlx::query(&format!("ALTER TABLE {table} ADD COLUMN {column} {definition}"))
        .execute(pool)
        .await;
    match added {
        Ok(_) => Ok(()),
        // Human: Another process opening the same database may have added it in the meantime.
        Err(_) if has_column().await? => Ok(()),
        Err(e) => Err(internal(anyhow::anyhow!(
            "adding column {table}.{column} failed: {e}"
        ))),
    }
}

/// Schema files by version, applied in order once each (recorded in `nos_schema_migrations`). Statements must
/// still be idempotent (`IF NOT EXISTS`): databases created before the version table re-run version 1.
const POSTGRES_MIGRATIONS: [(i32, &str); 1] = [(1, include_str!("../../migrations/001_nos_object_index.sql"))];

/// Human: Indexes added once tables may already be large, by name. They are built with `CREATE INDEX
/// CONCURRENTLY` after startup, so neither startup nor writes wait for them; queries work without them.
const POSTGRES_ONLINE_INDEXES: [(&str, &str); 1] = [(
    "idx_nos_objects_active_key",
    include_str!("../../migrations/002_nos_object_key_index.sql"),
)];

/// Advisory lock id held while migrating ("nosmigr" in ASCII).
const POSTGRES_MIGRATION_LOCK: i64 = 0x006e_6f73_6d69_6772;

/// Advisory lock id held while building online indexes ("nosindx" in ASCII).
const POSTGRES_INDEX_LOCK: i64 = 0x006e_6f73_696e_6478;

/// Human: Apply the schema versions not yet recorded, in one transaction under an advisory lock, so nodes
/// starting at the same time don't collide in the catalog (concurrent `CREATE TABLE IF NOT EXISTS` can still
/// fail) and a failure leaves no half-applied schema behind. Applied versions aren't re-run: their DDL locks
/// `nos_objects`, which would make every start wait behind another node's online index build.
/// Agent: statements are split on `;` — migration files must not contain semicolons inside literals or bodies.
async fn run_postgres_migrations(pool: &Pool<Postgres>) -> Result<(), StorageError> {
    let mut tx = pool.begin().await.map_err(internal)?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(POSTGRES_MIGRATION_LOCK)
        .execute(&mut *tx)
        .await
        .map_err(internal)?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS nos_schema_migrations (
            version INTEGER PRIMARY KEY,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )",
    )
    .execute(&mut *tx)
    .await
    .map_err(internal)?;
    let applied: Vec<i32> = sqlx::query_scalar("SELECT version FROM nos_schema_migrations")
        .fetch_all(&mut *tx)
        .await
        .map_err(internal)?;
    for (version, sql) in POSTGRES_MIGRATIONS {
        if applied.contains(&version) {
            continue;
        }
        for statement in sql.split(';').map(str::trim).filter(|s| !s.is_empty()) {
            sqlx::query(statement)
                .execute(&mut *tx)
                .await
                .map_err(internal)?;
        }
        sqlx::query("INSERT INTO nos_schema_migrations (version) VALUES ($1)")
            .bind(version)
            .execute(&mut *tx)
            .await
            .map_err(internal)?;
    }
    tx.commit().await.map_err(internal)
}

/// Human: Build the `POSTGRES_ONLINE_INDEXES` that don't exist yet. One node at a time (the others skip); an
/// interrupted build leaves an invalid index behind, which is dropped and rebuilt.
/// Agent: uses a connection detached from the pool, so the session lock dies with it whatever happens here.
async fn build_postgres_online_indexes(pool: &Pool<Postgres>) -> Result<(), StorageError> {
    let mut conn = pool.acquire().await.map_err(internal)?.detach();
    let locked: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(POSTGRES_INDEX_LOCK)
        .fetch_one(&mut conn)
        .await
        .map_err(internal)?;
    if !locked {
        return Ok(());
    }
    // Human: A server-wide statement_timeout would cancel long builds on every start, forever.
    sqlx::query("SET statement_timeout = 0")
        .execute(&mut conn)
        .await
        .map_err(internal)?;
    for (name, sql) in POSTGRES_ONLINE_INDEXES {
        let valid: Option<bool> =
            sqlx::query_scalar("SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass($1)")
                .bind(name)
                .fetch_optional(&mut conn)
                .await
                .map_err(internal)?;
        match valid {
            Some(true) => continue,
            Some(false) => {
                sqlx::query(&format!("DROP INDEX CONCURRENTLY IF EXISTS {name}"))
                    .execute(&mut conn)
                    .await
                    .map_err(internal)?;
            }
            None => {}
        }
        tracing::info!(index = name, "building metadata index");
        sqlx::query(sql).execute(&mut conn).await.map_err(internal)?;
    }
    sqlx::Connection::close(conn).await.map_err(internal)
}

/// Sidecar SQLite for replication_log when using postgres object metadata.
pub async fn connect_system_sqlite(meta_path: &str, read_pool_size: u32) -> Result<(Pool<Sqlite>, Pool<Sqlite>), StorageError> {
    let conn_str = resolve_sqlite_conn_str(meta_path).await?;
    let write = SqlitePool::connect(&conn_str).await.map_err(internal)?;
    let read = SqlitePoolOptions::new()
        .max_connections(read_pool_size.max(1))
        .connect(&conn_str)
        .await
        .map_err(internal)?;
    init_system_sqlite_schema(&write).await?;
    Ok((write, read))
}

pub(crate) async fn init_system_sqlite_schema(pool: &Pool<Sqlite>) -> Result<(), StorageError> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS replication_log (
            event_id     TEXT PRIMARY KEY,
            origin_node  TEXT NOT NULL,
            op           TEXT NOT NULL,
            bucket       TEXT NOT NULL,
            key          TEXT NOT NULL,
            etag         TEXT,
            size         INTEGER,
            payload_path TEXT,
            created_at   INTEGER NOT NULL,
            applied_at   INTEGER,
            status       TEXT NOT NULL DEFAULT 'pending'
        )",
    )
    .execute(pool)
    .await
    .map_err(internal)?;

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_repl_status ON replication_log(status, created_at)",
    )
    .execute(pool)
    .await
    .map_err(internal)?;

    for (column, definition) in [
        ("storage_class", "TEXT DEFAULT 'default'"),
        ("replication_group", "TEXT DEFAULT 'default'"),
        ("attempts", "INTEGER DEFAULT 0"),
        ("next_retry_at", "INTEGER"),
        ("content_type", "TEXT"),
        ("custom_meta", "TEXT"),
        ("wire_checksum", "TEXT"),
        ("version", "INTEGER"),
        ("delivered_to", "TEXT"),
    ] {
        add_column_if_missing(pool, "replication_log", column, definition).await?;
    }

    // Human: Each replicated key's current version (see cluster::replicated::versions).
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS replication_versions (
            bucket      TEXT NOT NULL,
            key         TEXT NOT NULL,
            version     INTEGER NOT NULL,
            origin      TEXT NOT NULL,
            deleted     INTEGER NOT NULL DEFAULT 0,
            recorded_at INTEGER NOT NULL,
            PRIMARY KEY (bucket, key)
        )",
    )
    .execute(pool)
    .await
    .map_err(internal)?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_repl_versions_tombstones ON replication_versions(recorded_at) WHERE deleted = 1",
    )
    .execute(pool)
    .await
    .map_err(internal)?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS maintenance_state (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )",
    )
    .execute(pool)
    .await
    .map_err(internal)?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS cluster_runtime_config (
            id   INTEGER PRIMARY KEY,
            json TEXT NOT NULL
        )",
    )
    .execute(pool)
    .await
    .map_err(internal)?;

    sqlx::query("PRAGMA foreign_keys = ON")
        .execute(pool)
        .await
        .map_err(internal)?;
    sqlx::query("PRAGMA journal_mode = WAL")
        .execute(pool)
        .await
        .map_err(internal)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use sqlx::sqlite::SqliteConnectOptions;

    use super::*;

    async fn columns(pool: &Pool<Sqlite>, table: &str) -> Vec<String> {
        sqlx::query_scalar("SELECT name FROM pragma_table_info(?)")
            .bind(table)
            .fetch_all(pool)
            .await
            .unwrap()
    }

    /// A metadata file in the shape an early release created (no soft deletes, classes or retries).
    async fn legacy_database(dir: &std::path::Path) -> (String, Pool<Sqlite>) {
        let url = format!("sqlite:{}?mode=rwc", dir.join("legacy.db").display());
        let pool = SqlitePool::connect(&url).await.unwrap();
        for ddl in [
            "CREATE TABLE objects (bucket TEXT NOT NULL, key TEXT NOT NULL, size INTEGER NOT NULL, \
             mime_type TEXT, etag TEXT, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, \
             custom_meta TEXT, PRIMARY KEY (bucket, key))",
            "CREATE INDEX idx_prefix ON objects(bucket, key)",
            "CREATE TABLE multipart_uploads (upload_id TEXT PRIMARY KEY, bucket TEXT NOT NULL, \
             key TEXT NOT NULL, content_type TEXT, created_at INTEGER NOT NULL)",
            "CREATE TABLE multipart_parts (upload_id TEXT NOT NULL, part_number INTEGER NOT NULL, \
             size INTEGER NOT NULL, etag TEXT NOT NULL, PRIMARY KEY (upload_id, part_number))",
            "CREATE TABLE replication_log (event_id TEXT PRIMARY KEY, origin_node TEXT NOT NULL, \
             op TEXT NOT NULL, bucket TEXT NOT NULL, key TEXT NOT NULL, etag TEXT, size INTEGER, \
             payload_path TEXT, created_at INTEGER NOT NULL, applied_at INTEGER, \
             status TEXT NOT NULL DEFAULT 'pending')",
            "PRAGMA journal_mode = WAL",
        ] {
            sqlx::query(ddl).execute(&pool).await.unwrap();
        }
        (url, pool)
    }

    #[tokio::test]
    async fn legacy_sqlite_schema_is_upgraded_in_place() {
        let dir = tempfile::tempdir().unwrap();
        let (_, pool) = legacy_database(dir.path()).await;
        for _ in 0..2 {
            init_sqlite_object_schema(&pool).await.unwrap();
            init_system_sqlite_schema(&pool).await.unwrap();
        }
        let objects = columns(&pool, "objects").await;
        for column in ["deleted_at", "storage_class", "origin_node"] {
            assert!(objects.iter().any(|c| c == column), "objects.{column} missing: {objects:?}");
        }
        let log = columns(&pool, "replication_log").await;
        for column in ["storage_class", "replication_group", "attempts", "next_retry_at", "wire_checksum"] {
            assert!(log.iter().any(|c| c == column), "replication_log.{column} missing: {log:?}");
        }
    }

    #[tokio::test]
    async fn a_failed_column_migration_is_reported() {
        let dir = tempfile::tempdir().unwrap();
        let (url, pool) = legacy_database(dir.path()).await;
        pool.close().await;
        let read_only = SqlitePool::connect_with(
            SqliteConnectOptions::from_str(&url).unwrap().read_only(true),
        )
        .await
        .unwrap();
        // Human: Start-up used to succeed here with the columns still missing; queries then failed at runtime.
        let err = init_sqlite_object_schema(&read_only)
            .await
            .expect_err("a schema upgrade that could not be applied must fail start-up");
        assert!(format!("{err:?}").contains("objects.deleted_at"), "{err:?}");
    }

    #[tokio::test]
    async fn maintenance_pages_use_the_active_key_index() {
        let dir = tempfile::tempdir().unwrap();
        let (_, pool) = legacy_database(dir.path()).await;
        init_sqlite_object_schema(&pool).await.unwrap();
        for i in 0..2_000 {
            sqlx::query("INSERT INTO objects (bucket, key, size, created_at, updated_at) VALUES ('b', ?, 1, 0, 0)")
                .bind(format!("key-{i:05}"))
                .execute(&pool)
                .await
                .unwrap();
        }
        // Human: (statement, bound values, may sort) — the equal-key run is a handful of rows sorted by bucket.
        for (sql, binds, sorts) in [
            (SQLITE_KEY_PAGE_FIRST, 1, false),
            (SQLITE_KEY_PAGE_AFTER, 2, false),
            (SQLITE_ACTIVE_WITH_KEY, 1, true),
        ] {
            let explain = format!("EXPLAIN QUERY PLAN {sql}");
            // Human: EXPLAIN QUERY PLAN rows are (id, parent, notused, detail).
            let mut query = sqlx::query_as::<_, (i64, i64, i64, String)>(&explain);
            query = if binds == 2 { query.bind("key-01000").bind(10) } else { query.bind("key-01000") };
            let plan: Vec<String> = query
                .fetch_all(&pool)
                .await
                .unwrap()
                .into_iter()
                .map(|row| row.3)
                .collect();
            assert!(
                plan.iter().any(|step| step.contains("idx_objects_active_key"))
                    && (sorts || !plan.iter().any(|step| step.contains("TEMP B-TREE"))),
                "{sql}: {plan:?}"
            );
        }
    }
}
