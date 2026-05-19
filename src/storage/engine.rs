use std::path::PathBuf;
use anyhow::{Context, Result};
use sqlx::{Pool, Sqlite, SqlitePool};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt, AsyncSeekExt};
use tokio_util::io::ReaderStream;

use super::types::{ListItem, ListResult, ObjectMetadata};
use super::{blob_path, sanitize_bucket, sanitize_key};

fn escape_like_pattern(s: &str) -> String {
    s.replace("\\", "\\\\")
     .replace("%", "\\%")
     .replace("_", "\\_")
}

#[derive(Clone)]
pub struct StorageEngine {
    pool: Pool<Sqlite>,
    data_dir: String,
}

struct TempFileGuard {
    path: PathBuf,
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        if self.path.exists() {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

impl StorageEngine {
    pub async fn new(meta_path: &str, data_dir: &str) -> Result<Self> {
        let conn_str = if meta_path.starts_with("file:") {
            meta_path.to_string()
        } else {
            let meta_path = meta_path.strip_prefix("./").unwrap_or(meta_path);
            let meta_path_buf = PathBuf::from(meta_path);
            let meta_path_buf = if meta_path_buf.is_absolute() {
                meta_path_buf
            } else {
                std::env::current_dir()?.join(meta_path_buf)
            };
            if let Some(parent) = meta_path_buf.parent() {
                fs::create_dir_all(parent).await?;
            }
            // Ensure the database file exists before connecting (Windows compat)
            if !meta_path_buf.exists() {
                fs::File::create(&meta_path_buf).await?;
            }
            meta_path_buf.to_string_lossy().to_string()
        };
        let pool = SqlitePool::connect(&conn_str).await?;

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
                PRIMARY KEY (bucket, key)
            )"
        )
        .execute(&pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_prefix ON objects(bucket, key)")
            .execute(&pool)
            .await?;

        sqlx::query("PRAGMA foreign_keys = ON").execute(&pool).await?;
        sqlx::query("PRAGMA journal_mode = WAL").execute(&pool).await?;

        fs::create_dir_all(data_dir).await?;
        fs::create_dir_all(format!("{}/.tmp", data_dir)).await?;

        Ok(Self {
            pool,
            data_dir: data_dir.to_string(),
        })
    }

    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
        custom_meta: Option<&str>,
        mut body: impl tokio::io::AsyncRead + Unpin,
    ) -> Result<ObjectMetadata> {
        let bucket = sanitize_bucket(bucket)?;
        let safe_key = sanitize_key(key)?;
        tracing::debug!(%bucket, key = %safe_key, content_type, "storage::put_object start");

        let tmp_path = format!("{}/.tmp/{}.tmp", self.data_dir, uuid::Uuid::new_v4());
        let final_path = blob_path(&self.data_dir, &bucket, &safe_key);

        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let mut file = fs::File::create(&tmp_path).await?;
        let _guard = TempFileGuard { path: PathBuf::from(&tmp_path) };
        let mut hasher = md5::Context::new();
        let mut size: u64 = 0;
        let mut buf = [0u8; 65536];

        loop {
            let n = body.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            hasher.consume(&buf[..n]);
            file.write_all(&buf[..n]).await?;
            size += n as u64;
        }

        file.flush().await?;
        drop(file);

        let digest = hasher.compute();
        let etag = format!("{:x}", digest);

        if final_path.exists() {
            fs::remove_file(&final_path).await?;
        }
        fs::rename(&tmp_path, &final_path).await?;

        let now = chrono::Utc::now();
        let unix_now = now.timestamp();

        sqlx::query(
            "INSERT INTO objects (bucket, key, size, mime_type, etag, created_at, updated_at, custom_meta)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(bucket, key) DO UPDATE SET
                 size = excluded.size,
                 mime_type = excluded.mime_type,
                 etag = excluded.etag,
                 updated_at = excluded.updated_at,
                 custom_meta = excluded.custom_meta"
        )
        .bind(&bucket)
        .bind(&safe_key)
        .bind(size as i64)
        .bind(content_type)
        .bind(&etag)
        .bind(unix_now)
        .bind(unix_now)
        .bind(custom_meta)
        .execute(&self.pool)
        .await?;

        tracing::info!(%bucket, key = %safe_key, size, etag = %etag, "storage::put_object stored");

        Ok(ObjectMetadata {
            bucket: bucket.to_string(),
            key: safe_key,
            size: size as i64,
            mime_type: content_type.map(|s| s.to_string()),
            etag: Some(etag),
            created_at: now,
            updated_at: now,
            custom_meta: custom_meta.map(|s| s.to_string()),
        })
    }

    pub async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        range: Option<(u64, u64)>,
    ) -> Result<(ReaderStream<tokio::io::Take<fs::File>>, u64, u64, Option<String>)> {
        let bucket = sanitize_bucket(bucket)?;
        let safe_key = sanitize_key(key)?;
        let meta: ObjectMetadata = sqlx::query_as(
            "SELECT bucket, key, size, mime_type, etag, created_at, updated_at, custom_meta
             FROM objects WHERE bucket = ? AND key = ?"
        )
        .bind(&bucket)
        .bind(&safe_key)
        .fetch_optional(&self.pool)
        .await?
        .with_context(|| format!("object not found: {}/{}", bucket, safe_key))?;

        let path = blob_path(&self.data_dir, &bucket, &safe_key);
        let file = fs::File::open(&path).await?;

        let total_size = meta.size as u64;
        let (start, _end, content_length) = match range {
            Some((_s, _e)) if total_size == 0 => {
                anyhow::bail!("range not satisfiable: empty object");
            }
            Some((s, e)) => {
                if s >= total_size {
                    anyhow::bail!("range not satisfiable: start >= size");
                }
                let end = e.min(total_size - 1);
                (s, end, end - s + 1)
            }
            None => {
                if total_size == 0 {
                    (0, 0, 0)
                } else {
                    (0, total_size - 1, total_size)
                }
            }
        };

        let mut file = file;
        file.seek(std::io::SeekFrom::Start(start)).await?;
        let limited = file.take(content_length);
        let stream = ReaderStream::new(limited);

        tracing::info!(%bucket, key = %safe_key, content_length, total_size, ?range, mime_type = ?meta.mime_type, "storage::get_object streaming");

        Ok((stream, content_length, total_size, meta.mime_type))
    }

    pub async fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectMetadata> {
        let bucket = sanitize_bucket(bucket)?;
        let safe_key = sanitize_key(key)?;
        let meta: ObjectMetadata = sqlx::query_as(
            "SELECT bucket, key, size, mime_type, etag, created_at, updated_at, custom_meta
             FROM objects WHERE bucket = ? AND key = ?"
        )
        .bind(&bucket)
        .bind(&safe_key)
        .fetch_optional(&self.pool)
        .await?
        .with_context(|| format!("object not found: {}/{}", bucket, safe_key))?;

        tracing::info!(%bucket, key = %safe_key, size = meta.size, "storage::head_object found");
        Ok(meta)
    }

    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        let bucket = sanitize_bucket(bucket)?;
        let safe_key = sanitize_key(key)?;
        let result = sqlx::query("DELETE FROM objects WHERE bucket = ? AND key = ?")
            .bind(&bucket)
            .bind(&safe_key)
            .execute(&self.pool)
            .await?;

        if result.rows_affected() > 0 {
            let path = blob_path(&self.data_dir, &bucket, &safe_key);
            match fs::remove_file(&path).await {
                Ok(()) => {
                    tracing::info!(%bucket, key = %safe_key, "storage::delete_object removed");
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    tracing::warn!(%bucket, key = %safe_key, "storage::delete_object blob already missing");
                }
                Err(e) => return Err(e.into()),
            }
        } else {
            tracing::warn!(%bucket, key = %safe_key, "storage::delete_object not found in meta");
        }
        Ok(())
    }

    pub async fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        limit: Option<u64>,
        start_after: Option<&str>,
    ) -> Result<ListResult> {
        let bucket = sanitize_bucket(bucket)?;
        let limit = limit.unwrap_or(100).min(1000);
        let prefix = prefix.unwrap_or("");
        let start_after = start_after.unwrap_or("");

        let prefix_pattern = format!("{}%", escape_like_pattern(prefix));

        let rows: Vec<ObjectMetadata> = sqlx::query_as(
            "SELECT bucket, key, size, mime_type, etag, created_at, updated_at, custom_meta
             FROM objects
             WHERE bucket = ? AND key > ? AND key LIKE ? ESCAPE '\\'
             ORDER BY key
             LIMIT ?"
        )
        .bind(&bucket)
        .bind(start_after)
        .bind(prefix_pattern)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;

        let item_count = rows.len();
        let items = rows
            .into_iter()
            .map(|r| ListItem {
                key: r.key,
                size: r.size,
                mime_type: r.mime_type,
                etag: r.etag,
                last_modified: r.updated_at,
            })
            .collect();

        tracing::debug!(%bucket, %prefix, item_count, "storage::list_objects returned");

        Ok(ListResult {
            items,
            prefix: Some(prefix.to_string()),
            delimiter: delimiter.map(|s| s.to_string()),
        })
    }

    pub async fn object_exists(&self, bucket: &str, key: &str) -> Result<bool> {
        let bucket = sanitize_bucket(bucket)?;
        let safe_key = sanitize_key(key)?;
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM objects WHERE bucket = ? AND key = ?"
        )
        .bind(&bucket)
        .bind(&safe_key)
        .fetch_one(&self.pool)
        .await?;
        Ok(count > 0)
    }

    pub async fn object_count(&self) -> Result<i64> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM objects")
            .fetch_one(&self.pool)
            .await?;
        Ok(count)
    }

    pub async fn total_bytes(&self) -> Result<i64> {
        let total: i64 = sqlx::query_scalar("SELECT COALESCE(SUM(size), 0) FROM objects")
            .fetch_one(&self.pool)
            .await?;
        Ok(total)
    }
}
