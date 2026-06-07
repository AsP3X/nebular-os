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

    pub async fn list_active_rows(
        &self,
        bucket: &str,
        start_after: &str,
        prefix_pattern: &str,
        limit: i64,
    ) -> Result<Vec<ObjectMetadata>, StorageError> {
        match &self.inner {
            ObjectMetaInner::Sqlite { read, .. } => {
                let q = format!(
                    "SELECT {META_SELECT_SQLITE} FROM {OBJECTS_TABLE_SQLITE}
                     WHERE bucket = ? AND key > ? AND key LIKE ? ESCAPE '\\' AND {ACTIVE_WHERE_SQLITE}
                     ORDER BY key LIMIT ?"
                );
                sqlx::query_as(&q)
                    .bind(bucket)
                    .bind(start_after)
                    .bind(prefix_pattern)
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
                    .bind(prefix_pattern)
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
        prefix_pattern: &str,
    ) -> Result<i64, StorageError> {
        match &self.inner {
            ObjectMetaInner::Sqlite { read, .. } => {
                let q = format!(
                    "SELECT COUNT(*) FROM {OBJECTS_TABLE_SQLITE}
                     WHERE bucket = ? AND key > ? AND key LIKE ? ESCAPE '\\' AND {ACTIVE_WHERE_SQLITE}"
                );
                sqlx::query_scalar(&q)
                    .bind(bucket)
                    .bind(last_key)
                    .bind(prefix_pattern)
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
                    .bind(prefix_pattern)
                    .fetch_one(read)
                    .await
                    .map_err(internal)
            }
        }
    }

    pub async fn count_active_with_prefix(
        &self,
        bucket: &str,
        prefix_pattern: &str,
    ) -> Result<i64, StorageError> {
        match &self.inner {
            ObjectMetaInner::Sqlite { read, .. } => {
                let q = format!(
                    "SELECT COUNT(*) FROM {OBJECTS_TABLE_SQLITE}
                     WHERE bucket = ? AND key LIKE ? ESCAPE '\\' AND {ACTIVE_WHERE_SQLITE}"
                );
                sqlx::query_scalar(&q)
                    .bind(bucket)
                    .bind(prefix_pattern)
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
                    .bind(prefix_pattern)
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

    let _ = sqlx::query("ALTER TABLE objects ADD COLUMN deleted_at INTEGER")
        .execute(pool)
        .await;
    let _ = sqlx::query("ALTER TABLE objects ADD COLUMN storage_class TEXT DEFAULT 'default'")
        .execute(pool)
        .await;
    let _ = sqlx::query("ALTER TABLE objects ADD COLUMN origin_node TEXT")
        .execute(pool)
        .await;

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

async fn run_postgres_migrations(pool: &Pool<Postgres>) -> Result<(), StorageError> {
    let sql = include_str!("../../migrations/001_nos_object_index.sql");
    for statement in sql.split(';') {
        let stmt = statement.trim();
        if stmt.is_empty() {
            continue;
        }
        sqlx::query(stmt).execute(pool).await.map_err(internal)?;
    }
    Ok(())
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

    let _ = sqlx::query("ALTER TABLE replication_log ADD COLUMN storage_class TEXT DEFAULT 'default'")
        .execute(pool)
        .await;
    let _ = sqlx::query(
        "ALTER TABLE replication_log ADD COLUMN replication_group TEXT DEFAULT 'default'",
    )
    .execute(pool)
    .await;
    let _ = sqlx::query("ALTER TABLE replication_log ADD COLUMN attempts INTEGER DEFAULT 0")
        .execute(pool)
        .await;
    let _ = sqlx::query("ALTER TABLE replication_log ADD COLUMN next_retry_at INTEGER")
        .execute(pool)
        .await;

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
