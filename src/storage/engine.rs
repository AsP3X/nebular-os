use std::collections::BTreeSet;
use std::path::PathBuf;

use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Pool, Sqlite, SqlitePool};
use tokio::fs;

use super::blob_ops::link_or_copy_blob;
use super::compression::{self, DEFAULT_ZSTD_LEVEL};
use super::error::{internal, StorageError};
use super::range::parse_content_range;
use super::streaming::{
    finalize_temp_to_blob, open_object_body_stream, stream_body_to_temp, GuardedObjectBodyStream,
};
use super::precondition::{check_write_preconditions, etag_matches};
use super::types::{ListItem, ListResult, ObjectMetadata};
use super::{blob_path, sanitize_bucket, sanitize_key};

pub(crate) const DEFAULT_UPLOAD_BUFFER: usize = 256 * 1024;
const DEFAULT_LIST_SCAN_CAP: i64 = 4096;

const META_SELECT: &str = "bucket, key, size, mime_type, etag, created_at, updated_at, custom_meta, deleted_at, storage_class, origin_node";
const ACTIVE_WHERE: &str = "deleted_at IS NULL";

fn escape_like_pattern(s: &str) -> String {
    s.replace("\\", "\\\\")
        .replace("%", "\\%")
        .replace("_", "\\_")
}

/// Outcome of GET after conditional header checks against stored metadata.
/// Per-check results for `GET /health/ready`.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ReadinessChecks {
    pub sqlite_write: bool,
    pub sqlite_read: bool,
    pub data_dir_writable: bool,
}

impl ReadinessChecks {
    pub fn ready(&self) -> bool {
        self.sqlite_write && self.sqlite_read && self.data_dir_writable
    }
}

pub enum GetObjectOutcome {
    NotModified(ObjectMetadata),
    Content {
        stream: GuardedObjectBodyStream,
        content_length: u64,
        total_size: u64,
        meta: ObjectMetadata,
    },
}

pub struct EngineOptions {
    pub upload_buffer_size: usize,
    pub list_scan_cap: i64,
    pub multipart_part_size: usize,
    pub soft_delete_ttl_secs: i64,
    pub soft_delete_drop_blob: bool,
    pub multipart_upload_ttl_secs: i64,
    pub recompress_batch_size: usize,
    pub read_pool_size: u32,
    pub zstd_level: i32,
}

impl Default for EngineOptions {
    fn default() -> Self {
        Self {
            upload_buffer_size: DEFAULT_UPLOAD_BUFFER,
            list_scan_cap: DEFAULT_LIST_SCAN_CAP,
            multipart_part_size: 8 * 1024 * 1024,
            soft_delete_ttl_secs: 86_400,
            soft_delete_drop_blob: false,
            multipart_upload_ttl_secs: 86_400,
            recompress_batch_size: 100,
            read_pool_size: 4,
            zstd_level: DEFAULT_ZSTD_LEVEL,
        }
    }
}

#[derive(Clone)]
pub struct StorageEngine {
    write_pool: Pool<Sqlite>,
    read_pool: Pool<Sqlite>,
    data_dir: String,
    upload_buffer_size: usize,
    list_scan_cap: i64,
    multipart_part_size: usize,
    soft_delete_ttl_secs: i64,
    soft_delete_drop_blob: bool,
    multipart_upload_ttl_secs: i64,
    recompress_batch_size: usize,
    zstd_level: i32,
}

pub(crate) struct TempFileGuard {
    pub path: PathBuf,
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        if self.path.exists() {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

impl StorageEngine {
    pub async fn new(meta_path: &str, data_dir: &str) -> Result<Self, StorageError> {
        Self::with_options(meta_path, data_dir, DEFAULT_UPLOAD_BUFFER).await
    }

    pub async fn with_options(
        meta_path: &str,
        data_dir: &str,
        upload_buffer_size: usize,
    ) -> Result<Self, StorageError> {
        Self::with_full_options(
            meta_path,
            data_dir,
            EngineOptions {
                upload_buffer_size,
                ..EngineOptions::default()
            },
        )
        .await
    }

    pub async fn with_full_options(
        meta_path: &str,
        data_dir: &str,
        opts: EngineOptions,
    ) -> Result<Self, StorageError> {
        let conn_str = Self::resolve_conn_str(meta_path).await?;
        let write_pool = SqlitePool::connect(&conn_str).await.map_err(internal)?;
        // Human: Size the read pool from NOS_READ_POOL_SIZE so list/GET metadata queries do not share one connection.
        // Agent: READS opts.read_pool_size; SqlitePoolOptions::max_connections; separate pool from write_pool.
        let read_pool_size = opts.read_pool_size.max(1);
        let read_pool = SqlitePoolOptions::new()
            .max_connections(read_pool_size)
            .connect(&conn_str)
            .await
            .map_err(internal)?;

        Self::init_schema(&write_pool).await?;

        fs::create_dir_all(data_dir).await.map_err(internal)?;
        fs::create_dir_all(format!("{}/.tmp", data_dir))
            .await
            .map_err(internal)?;
        fs::create_dir_all(format!("{}/.multipart", data_dir))
            .await
            .map_err(internal)?;

        Ok(Self {
            write_pool,
            read_pool,
            data_dir: data_dir.to_string(),
            upload_buffer_size: opts.upload_buffer_size.max(4096),
            list_scan_cap: opts.list_scan_cap.max(100),
            multipart_part_size: opts.multipart_part_size.max(1024 * 1024),
            soft_delete_ttl_secs: opts.soft_delete_ttl_secs.max(0),
            soft_delete_drop_blob: opts.soft_delete_drop_blob,
            multipart_upload_ttl_secs: opts.multipart_upload_ttl_secs.max(0),
            recompress_batch_size: opts.recompress_batch_size.max(1),
            zstd_level: compression::clamp_zstd_level(opts.zstd_level),
        })
    }

    async fn resolve_conn_str(meta_path: &str) -> Result<String, StorageError> {
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

    async fn init_schema(pool: &Pool<Sqlite>) -> Result<(), StorageError> {
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

        let _ = sqlx::query("ALTER TABLE objects ADD COLUMN storage_class TEXT DEFAULT 'default'")
            .execute(pool)
            .await;
        let _ = sqlx::query("ALTER TABLE objects ADD COLUMN origin_node TEXT")
            .execute(pool)
            .await;
        let _ = sqlx::query(
            "ALTER TABLE replication_log ADD COLUMN storage_class TEXT DEFAULT 'default'",
        )
        .execute(pool)
        .await;
        let _ = sqlx::query(
            "ALTER TABLE replication_log ADD COLUMN replication_group TEXT DEFAULT 'default'",
        )
        .execute(pool)
        .await;
        let _ = sqlx::query(
            "ALTER TABLE replication_log ADD COLUMN attempts INTEGER DEFAULT 0",
        )
        .execute(pool)
        .await;
        let _ = sqlx::query("ALTER TABLE replication_log ADD COLUMN next_retry_at INTEGER")
            .execute(pool)
            .await;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS cluster_runtime_config (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                json TEXT NOT NULL,
                applied_at INTEGER NOT NULL
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

    pub fn write_pool(&self) -> &Pool<Sqlite> {
        &self.write_pool
    }

    pub fn read_pool(&self) -> &Pool<Sqlite> {
        &self.read_pool
    }

    pub fn data_dir(&self) -> &str {
        &self.data_dir
    }

    pub fn upload_buffer_size(&self) -> usize {
        self.upload_buffer_size
    }

    pub fn multipart_part_size(&self) -> usize {
        self.multipart_part_size
    }

    pub fn soft_delete_ttl_secs(&self) -> i64 {
        self.soft_delete_ttl_secs
    }

    pub fn soft_delete_drop_blob(&self) -> bool {
        self.soft_delete_drop_blob
    }

    pub fn multipart_upload_ttl_secs(&self) -> i64 {
        self.multipart_upload_ttl_secs
    }

    pub fn recompress_batch_size(&self) -> usize {
        self.recompress_batch_size
    }

    pub fn zstd_level(&self) -> i32 {
        self.zstd_level
    }

    /// Human: Loads active object metadata when present, without treating a miss as an error.
    /// Agent: SELECT objects WHERE deleted_at IS NULL; RETURNS Option (None = no live row).
    pub async fn try_fetch_active_metadata(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<ObjectMetadata>, StorageError> {
        let bucket = sanitize_bucket(bucket).map_err(|_| StorageError::InvalidBucket)?;
        let safe_key = sanitize_key(key).map_err(|_| StorageError::InvalidKey)?;
        let q = format!(
            "SELECT {META_SELECT} FROM objects WHERE bucket = ? AND key = ? AND {ACTIVE_WHERE}"
        );
        sqlx::query_as(&q)
            .bind(&bucket)
            .bind(&safe_key)
            .fetch_optional(&self.read_pool)
            .await
            .map_err(internal)
    }

    /// Human: Validates If-Match / If-None-Match against the current object before a write or delete.
    /// Agent: READS try_fetch_active_metadata; CALLS precondition::check_write_preconditions.
    pub async fn ensure_write_preconditions(
        &self,
        bucket: &str,
        key: &str,
        if_match: Option<&str>,
        if_none_match: Option<&str>,
    ) -> Result<(), StorageError> {
        let existing = self.try_fetch_active_metadata(bucket, key).await?;
        check_write_preconditions(existing.as_ref(), if_match, if_none_match)
    }

    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
        custom_meta: Option<&str>,
        mut body: impl tokio::io::AsyncRead + Unpin,
    ) -> Result<ObjectMetadata, StorageError> {
        let bucket = sanitize_bucket(bucket).map_err(|_| StorageError::InvalidBucket)?;
        let safe_key = sanitize_key(key).map_err(|_| StorageError::InvalidKey)?;
        let (meta, _) = self
            .write_object_stream(&bucket, &safe_key, content_type, custom_meta, &mut body)
            .await?;
        Ok(meta)
    }

    /// Server-side copy using kernel copy when available, otherwise async file copy.
    pub async fn copy_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
        if_match: Option<&str>,
        if_none_match: Option<&str>,
    ) -> Result<ObjectMetadata, StorageError> {
        let src_bucket = sanitize_bucket(src_bucket).map_err(|_| StorageError::InvalidBucket)?;
        let src_key = sanitize_key(src_key).map_err(|_| StorageError::InvalidKey)?;
        let dst_bucket = sanitize_bucket(dst_bucket).map_err(|_| StorageError::InvalidBucket)?;
        let dst_key = sanitize_key(dst_key).map_err(|_| StorageError::InvalidKey)?;

        if if_match.is_some() || if_none_match.is_some() {
            self.ensure_write_preconditions(&dst_bucket, &dst_key, if_match, if_none_match)
                .await?;
        }

        let src_meta = self.fetch_active_metadata(&src_bucket, &src_key).await?;
        let src_path = blob_path(&self.data_dir, &src_bucket, &src_key);
        let dst_path = blob_path(&self.data_dir, &dst_bucket, &dst_key);

        // Human: Hard-link the on-disk blob when possible so copies share storage on the same volume.
        // Agent: CALLS link_or_copy_blob(src,dst); fallback fs::copy on EXDEV; metadata row for dst only.
        link_or_copy_blob(&src_path, &dst_path).await?;

        let now = chrono::Utc::now();
        let unix_now = now.timestamp();
        sqlx::query(
            "INSERT INTO objects (bucket, key, size, mime_type, etag, created_at, updated_at, custom_meta, deleted_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)
             ON CONFLICT(bucket, key) DO UPDATE SET
                 size = excluded.size,
                 mime_type = excluded.mime_type,
                 etag = excluded.etag,
                 updated_at = excluded.updated_at,
                 custom_meta = excluded.custom_meta,
                 deleted_at = NULL",
        )
        .bind(&dst_bucket)
        .bind(&dst_key)
        .bind(src_meta.size)
        .bind(&src_meta.mime_type)
        .bind(&src_meta.etag)
        .bind(unix_now)
        .bind(unix_now)
        .bind(&src_meta.custom_meta)
        .execute(&self.write_pool)
        .await
        .map_err(internal)?;

        Ok(ObjectMetadata {
            bucket: dst_bucket,
            key: dst_key,
            size: src_meta.size,
            mime_type: src_meta.mime_type,
            etag: src_meta.etag,
            created_at: now,
            updated_at: now,
            custom_meta: src_meta.custom_meta,
            deleted_at: None,
            storage_class: src_meta.storage_class,
            origin_node: src_meta.origin_node,
        })
    }

    async fn write_object_stream(
        &self,
        bucket: &str,
        safe_key: &str,
        content_type: Option<&str>,
        custom_meta: Option<&str>,
        body: &mut (impl tokio::io::AsyncRead + Unpin),
    ) -> Result<(ObjectMetadata, String), StorageError> {
        let tmp_path = format!("{}/.tmp/{}.tmp", self.data_dir, uuid::Uuid::new_v4());
        let final_path = blob_path(&self.data_dir, bucket, safe_key);
        let _tmp_guard = TempFileGuard {
            path: PathBuf::from(&tmp_path),
        };

        // Human: Stream upload to a temp file, hash on the fly, then compress to the final blob without buffering the whole object in RAM.
        // Agent: CALLS stream_body_to_temp; finalize_temp_to_blob(zstd_level); metadata size=logical bytes; TempFileGuard cleans tmp.
        let (size, etag) =
            stream_body_to_temp(body, PathBuf::from(&tmp_path).as_path(), self.upload_buffer_size)
                .await?;

        finalize_temp_to_blob(
            PathBuf::from(&tmp_path).as_path(),
            &final_path,
            size,
            self.zstd_level(),
        )
        .await?;

        let now = chrono::Utc::now();
        let unix_now = now.timestamp();
        if let Err(e) = sqlx::query(
            "INSERT INTO objects (bucket, key, size, mime_type, etag, created_at, updated_at, custom_meta, deleted_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)
             ON CONFLICT(bucket, key) DO UPDATE SET
                 size = excluded.size,
                 mime_type = excluded.mime_type,
                 etag = excluded.etag,
                 updated_at = excluded.updated_at,
                 custom_meta = excluded.custom_meta,
                 deleted_at = NULL",
        )
        .bind(bucket)
        .bind(safe_key)
        .bind(size as i64)
        .bind(content_type)
        .bind(&etag)
        .bind(unix_now)
        .bind(unix_now)
        .bind(custom_meta)
        .execute(&self.write_pool)
        .await
        {
            let _ = fs::remove_file(&final_path).await;
            return Err(StorageError::Internal(e.into()));
        }

        let meta = ObjectMetadata {
            bucket: bucket.to_string(),
            key: safe_key.to_string(),
            size: size as i64,
            mime_type: content_type.map(|s| s.to_string()),
            etag: Some(etag.clone()),
            created_at: now,
            updated_at: now,
            custom_meta: custom_meta.map(|s| s.to_string()),
            deleted_at: None,
            storage_class: None,
            origin_node: None,
        };
        Ok((meta, etag))
    }

    pub async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        range_header: Option<&str>,
        if_none_match: Option<&str>,
        if_modified_since: Option<i64>,
    ) -> Result<GetObjectOutcome, StorageError> {
        let meta = self
            .fetch_active_metadata(
                &sanitize_bucket(bucket).map_err(|_| StorageError::InvalidBucket)?,
                &sanitize_key(key).map_err(|_| StorageError::InvalidKey)?,
            )
            .await?;

        if self.is_not_modified(&meta, if_none_match, if_modified_since) {
            return Ok(GetObjectOutcome::NotModified(meta));
        }

        let total_size = meta.size as u64;
        let range = range_header.and_then(|h| parse_content_range(h, total_size));

        let path = blob_path(&self.data_dir, &meta.bucket, &meta.key);
        let (start, _end, content_length) = Self::resolve_range(range, total_size)?;

        // Human: Stream object bytes from disk, decompressing via spill file or channel when the blob is zstd-wrapped.
        // Agent: CALLS open_object_body_stream(path, logical_size, range_start, content_length, data_dir); no full-blob RAM buffer.
        let stream = open_object_body_stream(
            path.as_path(),
            total_size,
            start,
            content_length,
            &self.data_dir,
        )
        .await?;

        Ok(GetObjectOutcome::Content {
            stream,
            content_length,
            total_size,
            meta,
        })
    }

    pub async fn head_object(
        &self,
        bucket: &str,
        key: &str,
        if_none_match: Option<&str>,
        if_modified_since: Option<i64>,
    ) -> Result<Option<ObjectMetadata>, StorageError> {
        let meta = self
            .fetch_active_metadata(
                &sanitize_bucket(bucket).map_err(|_| StorageError::InvalidBucket)?,
                &sanitize_key(key).map_err(|_| StorageError::InvalidKey)?,
            )
            .await?;
        if self.is_not_modified(&meta, if_none_match, if_modified_since) {
            return Ok(None);
        }
        Ok(Some(meta))
    }

    fn is_not_modified(
        &self,
        meta: &ObjectMetadata,
        if_none_match: Option<&str>,
        if_modified_since: Option<i64>,
    ) -> bool {
        if let Some(etag) = if_none_match {
            if etag == "*" {
                return true;
            }
            if let Some(stored) = &meta.etag
                && etag_matches(stored, etag) {
                    return true;
                }
        }
        if let Some(since) = if_modified_since
            && meta.updated_at.timestamp() <= since {
                return true;
            }
        false
    }

    pub async fn delete_object(
        &self,
        bucket: &str,
        key: &str,
        if_match: Option<&str>,
    ) -> Result<(), StorageError> {
        let bucket = sanitize_bucket(bucket).map_err(|_| StorageError::InvalidBucket)?;
        let safe_key = sanitize_key(key).map_err(|_| StorageError::InvalidKey)?;

        if if_match.is_some() {
            self.ensure_write_preconditions(&bucket, &safe_key, if_match, None)
                .await?;
        }

        let exists: i64 = sqlx::query_scalar(&format!(
            "SELECT COUNT(*) FROM objects WHERE bucket = ? AND key = ? AND {ACTIVE_WHERE}"
        ))
        .bind(&bucket)
        .bind(&safe_key)
        .fetch_one(&self.write_pool)
        .await
        .map_err(internal)?;

        if exists == 0 {
            return Ok(());
        }

        let path = blob_path(&self.data_dir, &bucket, &safe_key);

        // Human: TTL 0 hard-deletes metadata and blob; otherwise soft-delete with optional immediate blob drop.
        // Agent: IF soft_delete_ttl_secs==0 THEN DELETE row + remove_file; ELIF drop_blob THEN UPDATE deleted_at + remove_file; ELSE UPDATE deleted_at only.
        if self.soft_delete_ttl_secs <= 0 {
            let _ = fs::remove_file(&path).await;
            sqlx::query("DELETE FROM objects WHERE bucket = ? AND key = ?")
                .bind(&bucket)
                .bind(&safe_key)
                .execute(&self.write_pool)
                .await
                .map_err(internal)?;
            return Ok(());
        }

        if self.soft_delete_drop_blob {
            let _ = fs::remove_file(&path).await;
        }

        let now = chrono::Utc::now().timestamp();
        sqlx::query(&format!(
            "UPDATE objects SET deleted_at = ? WHERE bucket = ? AND key = ? AND {ACTIVE_WHERE}"
        ))
        .bind(now)
        .bind(&bucket)
        .bind(&safe_key)
        .execute(&self.write_pool)
        .await
        .map_err(internal)?;

        Ok(())
    }

    /// Human: Probes SQLite pools and blob directory writability for orchestrator readiness checks.
    /// Agent: SELECT 1 on write+read pools; WRITE+DELETE probe file under NOS_DATA_DIR/.nos-ready-probe.
    pub async fn probe_readiness(&self) -> ReadinessChecks {
        let sqlite_write = sqlx::query("SELECT 1")
            .fetch_one(&self.write_pool)
            .await
            .is_ok();
        let sqlite_read = sqlx::query("SELECT 1")
            .fetch_one(&self.read_pool)
            .await
            .is_ok();
        let data_dir_writable = Self::probe_data_dir_writable(&self.data_dir).await;
        ReadinessChecks {
            sqlite_write,
            sqlite_read,
            data_dir_writable,
        }
    }

    async fn probe_data_dir_writable(data_dir: &str) -> bool {
        let probe = PathBuf::from(data_dir).join(".nos-ready-probe");
        if fs::create_dir_all(data_dir).await.is_err() {
            return false;
        }
        if fs::write(&probe, b"1").await.is_err() {
            return false;
        }
        fs::remove_file(&probe).await.is_ok()
    }

    async fn fetch_active_metadata(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<ObjectMetadata, StorageError> {
        let q = format!(
            "SELECT {META_SELECT} FROM objects WHERE bucket = ? AND key = ? AND {ACTIVE_WHERE}"
        );
        sqlx::query_as(&q)
            .bind(bucket)
            .bind(key)
            .fetch_optional(&self.read_pool)
            .await
            .map_err(internal)?
            .ok_or(StorageError::NotFound)
    }

    fn resolve_range(
        range: Option<(u64, u64)>,
        total_size: u64,
    ) -> Result<(u64, u64, u64), StorageError> {
        match range {
            Some((_s, _e)) if total_size == 0 => Err(StorageError::RangeNotSatisfiable),
            Some((s, e)) => {
                if s >= total_size {
                    return Err(StorageError::RangeNotSatisfiable);
                }
                let end = e.min(total_size - 1);
                Ok((s, end, end - s + 1))
            }
            None => {
                if total_size == 0 {
                    Ok((0, 0, 0))
                } else {
                    Ok((0, total_size - 1, total_size))
                }
            }
        }
    }

    pub async fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        limit: Option<u64>,
        start_after: Option<&str>,
    ) -> Result<ListResult, StorageError> {
        let bucket = sanitize_bucket(bucket).map_err(|_| StorageError::InvalidBucket)?;
        let limit = limit.unwrap_or(100).min(1000) as usize;
        let prefix = prefix.unwrap_or("");
        let start_after = start_after.unwrap_or("");
        let prefix_pattern = format!("{}%", escape_like_pattern(prefix));

        let scan_limit = if delimiter.is_some() {
            self.list_scan_cap
        } else {
            (limit as i64).saturating_add(1)
        };

        let q = format!(
            "SELECT {META_SELECT} FROM objects
             WHERE bucket = ? AND key > ? AND key LIKE ? ESCAPE '\\' AND {ACTIVE_WHERE}
             ORDER BY key LIMIT ?"
        );
        let rows: Vec<ObjectMetadata> = sqlx::query_as(&q)
            .bind(&bucket)
            .bind(start_after)
            .bind(prefix_pattern.clone())
            .bind(scan_limit)
            .fetch_all(&self.read_pool)
            .await
            .map_err(internal)?;

        if delimiter.is_none() {
            let is_truncated = rows.len() > limit;
            let page: Vec<_> = rows.into_iter().take(limit).collect();
            let next_start_after = if is_truncated {
                page.last().map(|r| r.key.clone())
            } else {
                None
            };
            let items = page
                .into_iter()
                .map(|r| ListItem {
                    key: r.key,
                    size: r.size,
                    mime_type: r.mime_type,
                    etag: r.etag,
                    last_modified: r.updated_at,
                    storage_class: r.storage_class.clone(),
                    origin_node: r.origin_node.clone(),
                })
                .collect();
            return Ok(ListResult {
                items,
                common_prefixes: Vec::new(),
                prefix: Some(prefix.to_string()),
                delimiter: None,
                is_truncated,
                next_start_after,
            });
        }

        let delimiter = delimiter.unwrap();
        let mut items = Vec::new();
        let mut common_prefixes = BTreeSet::new();
        let mut last_scanned: Option<String> = None;
        let mut is_truncated = false;
        let scanned_len = rows.len();

        for row in rows {
            last_scanned = Some(row.key.clone());
            let key = &row.key;
            let remainder = key.strip_prefix(prefix).unwrap_or(key.as_str());
            if let Some(pos) = remainder.find(delimiter) {
                let prefix_end = prefix.len() + pos + delimiter.len();
                let folder = key[..prefix_end].to_string();
                if common_prefixes.contains(&folder) {
                    continue;
                }
                if items.len() + common_prefixes.len() >= limit {
                    is_truncated = true;
                    break;
                }
                common_prefixes.insert(folder);
                continue;
            }
            if items.len() + common_prefixes.len() >= limit {
                is_truncated = true;
                break;
            }
            items.push(ListItem {
                key: row.key,
                size: row.size,
                mime_type: row.mime_type,
                etag: row.etag,
                last_modified: row.updated_at,
                storage_class: row.storage_class.clone(),
                origin_node: row.origin_node.clone(),
            });
        }

        if !is_truncated {
            if scanned_len as i64 >= self.list_scan_cap {
                is_truncated = true;
            } else if let Some(ref last) = last_scanned {
                let count_q = format!(
                    "SELECT COUNT(*) FROM objects
                     WHERE bucket = ? AND key > ? AND key LIKE ? ESCAPE '\\' AND {ACTIVE_WHERE}"
                );
                let count: i64 = sqlx::query_scalar(&count_q)
                    .bind(&bucket)
                    .bind(last)
                    .bind(prefix_pattern)
                    .fetch_one(&self.read_pool)
                    .await
                    .map_err(internal)?;
                is_truncated = count > 0;
            }
        }

        Ok(ListResult {
            items,
            common_prefixes: common_prefixes.into_iter().collect(),
            prefix: Some(prefix.to_string()),
            delimiter: Some(delimiter.to_string()),
            is_truncated,
            next_start_after: if is_truncated { last_scanned } else { None },
        })
    }

    pub async fn object_exists(&self, bucket: &str, key: &str) -> Result<bool, StorageError> {
        let bucket = sanitize_bucket(bucket).map_err(|_| StorageError::InvalidBucket)?;
        let safe_key = sanitize_key(key).map_err(|_| StorageError::InvalidKey)?;
        let q = format!(
            "SELECT COUNT(*) FROM objects WHERE bucket = ? AND key = ? AND {ACTIVE_WHERE}"
        );
        let count: i64 = sqlx::query_scalar(&q)
            .bind(&bucket)
            .bind(&safe_key)
            .fetch_one(&self.read_pool)
            .await
            .map_err(internal)?;
        Ok(count > 0)
    }

    pub async fn object_count(&self) -> Result<i64, StorageError> {
        let q = format!("SELECT COUNT(*) FROM objects WHERE {ACTIVE_WHERE}");
        let count: i64 = sqlx::query_scalar(&q)
            .fetch_one(&self.read_pool)
            .await
            .map_err(internal)?;
        Ok(count)
    }

    pub async fn total_bytes(&self) -> Result<i64, StorageError> {
        let q = format!("SELECT COALESCE(SUM(size), 0) FROM objects WHERE {ACTIVE_WHERE}");
        let total: i64 = sqlx::query_scalar(&q)
            .fetch_one(&self.read_pool)
            .await
            .map_err(internal)?;
        Ok(total)
    }

    /// Human: Records which storage class and node own an object after assignment accepts a write.
    /// Agent: UPDATE objects SET storage_class, origin_node for active row matching bucket/key.
    pub async fn set_object_placement(
        &self,
        bucket: &str,
        key: &str,
        storage_class: &str,
        origin_node: &str,
    ) -> Result<(), StorageError> {
        let bucket = sanitize_bucket(bucket).map_err(|_| StorageError::InvalidBucket)?;
        let safe_key = sanitize_key(key).map_err(|_| StorageError::InvalidKey)?;
        sqlx::query(
            "UPDATE objects SET storage_class = ?, origin_node = ? WHERE bucket = ? AND key = ? AND deleted_at IS NULL",
        )
        .bind(storage_class)
        .bind(origin_node)
        .bind(&bucket)
        .bind(&safe_key)
        .execute(&self.write_pool)
        .await
        .map_err(internal)?;
        Ok(())
    }

    /// Human: Lookup storage class before delete replication enqueue.
    /// Agent: SELECT storage_class FROM objects WHERE active row; None if missing.
    pub async fn active_storage_class(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<String>, StorageError> {
        let bucket = sanitize_bucket(bucket).map_err(|_| StorageError::InvalidBucket)?;
        let safe_key = sanitize_key(key).map_err(|_| StorageError::InvalidKey)?;
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT COALESCE(storage_class, 'default') FROM objects WHERE bucket = ? AND key = ? AND deleted_at IS NULL",
        )
        .bind(&bucket)
        .bind(&safe_key)
        .fetch_optional(&self.read_pool)
        .await
        .map_err(internal)?;
        Ok(row.map(|(c,)| c))
    }

    /// Human: Per-class object counts for Prometheus metrics.
    /// Agent: GROUP BY storage_class on active objects.
    pub async fn objects_by_storage_class(
        &self,
    ) -> Result<Vec<(String, i64)>, StorageError> {
        let rows: Vec<(String, i64)> = sqlx::query_as(
            "SELECT COALESCE(storage_class, 'default'), COUNT(*) FROM objects WHERE deleted_at IS NULL GROUP BY storage_class",
        )
        .fetch_all(&self.read_pool)
        .await
        .map_err(internal)?;
        Ok(rows)
    }

    /// Human: Load Ownly-applied runtime cluster JSON persisted across restarts.
    /// Agent: READS cluster_runtime_config row id=1; RETURNS None when unset.
    pub async fn load_cluster_runtime_config(&self) -> Result<Option<String>, StorageError> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT json FROM cluster_runtime_config WHERE id = 1")
                .fetch_optional(&self.read_pool)
                .await
                .map_err(internal)?;
        Ok(row.map(|(json,)| json))
    }

    /// Human: Persist runtime cluster topology after PUT /_cluster/config succeeds.
    /// Agent: UPSERT cluster_runtime_config id=1; WRITES applied_at unix timestamp.
    pub async fn save_cluster_runtime_config(&self, json: &str) -> Result<(), StorageError> {
        let applied_at = chrono::Utc::now().timestamp();
        sqlx::query(
            "INSERT INTO cluster_runtime_config (id, json, applied_at) VALUES (1, $1, $2) \
             ON CONFLICT(id) DO UPDATE SET json = excluded.json, applied_at = excluded.applied_at",
        )
        .bind(json)
        .bind(applied_at)
        .execute(&self.write_pool)
        .await
        .map_err(internal)?;
        Ok(())
    }
}
