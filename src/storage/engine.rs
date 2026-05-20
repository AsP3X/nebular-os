use std::collections::BTreeSet;
use std::path::PathBuf;

use sqlx::{Pool, Sqlite, SqlitePool};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio_util::io::ReaderStream;
use xxhash_rust::xxh3::Xxh3;

use super::error::{internal, map_io_error, StorageError};
use super::types::{ListItem, ListResult, ObjectMetadata};
use super::{blob_path, sanitize_bucket, sanitize_key};

const DEFAULT_UPLOAD_BUFFER: usize = 256 * 1024;
const LIST_SCAN_CAP: i64 = 4096;

fn escape_like_pattern(s: &str) -> String {
    s.replace("\\", "\\\\")
        .replace("%", "\\%")
        .replace("_", "\\_")
}

#[derive(Clone)]
pub struct StorageEngine {
    pool: Pool<Sqlite>,
    data_dir: String,
    upload_buffer_size: usize,
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
    pub async fn new(meta_path: &str, data_dir: &str) -> Result<Self, StorageError> {
        Self::with_options(meta_path, data_dir, DEFAULT_UPLOAD_BUFFER).await
    }

    pub async fn with_options(
        meta_path: &str,
        data_dir: &str,
        upload_buffer_size: usize,
    ) -> Result<Self, StorageError> {
        let conn_str = if meta_path.starts_with("file:") {
            meta_path.to_string()
        } else {
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
                fs::create_dir_all(parent)
                    .await
                    .map_err(internal)?;
            }
            if !meta_path_buf.exists() {
                fs::File::create(&meta_path_buf)
                    .await
                    .map_err(internal)?;
            }
            meta_path_buf.to_string_lossy().to_string()
        };
        let pool = SqlitePool::connect(&conn_str)
            .await
            .map_err(internal)?;

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
            )",
        )
        .execute(&pool)
        .await
        .map_err(internal)?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_prefix ON objects(bucket, key)")
            .execute(&pool)
            .await
            .map_err(internal)?;

        sqlx::query("PRAGMA foreign_keys = ON")
            .execute(&pool)
            .await
            .map_err(internal)?;
        sqlx::query("PRAGMA journal_mode = WAL")
            .execute(&pool)
            .await
            .map_err(internal)?;

        fs::create_dir_all(data_dir)
            .await
            .map_err(internal)?;
        fs::create_dir_all(format!("{}/.tmp", data_dir))
            .await
            .map_err(internal)?;

        Ok(Self {
            pool,
            data_dir: data_dir.to_string(),
            upload_buffer_size: upload_buffer_size.max(4096),
        })
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
        tracing::debug!(%bucket, key = %safe_key, content_type, "storage::put_object start");

        let tmp_path = format!("{}/.tmp/{}.tmp", self.data_dir, uuid::Uuid::new_v4());
        let final_path = blob_path(&self.data_dir, &bucket, &safe_key);

        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(internal)?;
        }

        let mut file = fs::File::create(&tmp_path)
            .await
            .map_err(internal)?;
        let _guard = TempFileGuard {
            path: PathBuf::from(&tmp_path),
        };
        let mut hasher = Xxh3::new();
        let mut size: u64 = 0;
        let mut buf = vec![0u8; self.upload_buffer_size];

        loop {
            let n = body.read(&mut buf).await.map_err(map_io_error)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
            file.write_all(&buf[..n])
                .await
                .map_err(internal)?;
            size += n as u64;
        }

        file.flush().await.map_err(internal)?;
        drop(file);

        let etag = format!("{:016x}", hasher.digest());

        if final_path.exists() {
            fs::remove_file(&final_path)
                .await
                .map_err(internal)?;
        }
        fs::rename(&tmp_path, &final_path)
            .await
            .map_err(internal)?;

        let now = chrono::Utc::now();
        let unix_now = now.timestamp();

        if let Err(e) = sqlx::query(
            "INSERT INTO objects (bucket, key, size, mime_type, etag, created_at, updated_at, custom_meta)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(bucket, key) DO UPDATE SET
                 size = excluded.size,
                 mime_type = excluded.mime_type,
                 etag = excluded.etag,
                 updated_at = excluded.updated_at,
                 custom_meta = excluded.custom_meta",
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
        .await
        {
            let _ = fs::remove_file(&final_path).await;
            return Err(StorageError::Internal(e.into()));
        }

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
    ) -> Result<(ReaderStream<tokio::io::Take<fs::File>>, u64, u64, Option<String>), StorageError>
    {
        let bucket = sanitize_bucket(bucket).map_err(|_| StorageError::InvalidBucket)?;
        let safe_key = sanitize_key(key).map_err(|_| StorageError::InvalidKey)?;
        let meta: ObjectMetadata = sqlx::query_as(
            "SELECT bucket, key, size, mime_type, etag, created_at, updated_at, custom_meta
             FROM objects WHERE bucket = ? AND key = ?",
        )
        .bind(&bucket)
        .bind(&safe_key)
        .fetch_optional(&self.pool)
        .await
        .map_err(internal)?
        .ok_or(StorageError::NotFound)?;

        let path = blob_path(&self.data_dir, &bucket, &safe_key);
        let file = fs::File::open(&path)
            .await
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    StorageError::NotFound
                } else {
                    StorageError::Internal(e.into())
                }
            })?;

        let total_size = meta.size as u64;
        let (start, _end, content_length) = match range {
            Some((_s, _e)) if total_size == 0 => return Err(StorageError::RangeNotSatisfiable),
            Some((s, e)) => {
                if s >= total_size {
                    return Err(StorageError::RangeNotSatisfiable);
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
        file.seek(std::io::SeekFrom::Start(start))
            .await
            .map_err(internal)?;
        let limited = file.take(content_length);
        let stream = ReaderStream::new(limited);

        tracing::info!(%bucket, key = %safe_key, content_length, total_size, ?range, mime_type = ?meta.mime_type, "storage::get_object streaming");

        Ok((stream, content_length, total_size, meta.mime_type))
    }

    pub async fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectMetadata, StorageError> {
        let bucket = sanitize_bucket(bucket).map_err(|_| StorageError::InvalidBucket)?;
        let safe_key = sanitize_key(key).map_err(|_| StorageError::InvalidKey)?;
        let meta: ObjectMetadata = sqlx::query_as(
            "SELECT bucket, key, size, mime_type, etag, created_at, updated_at, custom_meta
             FROM objects WHERE bucket = ? AND key = ?",
        )
        .bind(&bucket)
        .bind(&safe_key)
        .fetch_optional(&self.pool)
        .await
        .map_err(internal)?
        .ok_or(StorageError::NotFound)?;

        tracing::info!(%bucket, key = %safe_key, size = meta.size, "storage::head_object found");
        Ok(meta)
    }

    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), StorageError> {
        let bucket = sanitize_bucket(bucket).map_err(|_| StorageError::InvalidBucket)?;
        let safe_key = sanitize_key(key).map_err(|_| StorageError::InvalidKey)?;

        let exists: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM objects WHERE bucket = ? AND key = ?",
        )
        .bind(&bucket)
        .bind(&safe_key)
        .fetch_one(&self.pool)
        .await
        .map_err(internal)?;

        if exists == 0 {
            tracing::warn!(%bucket, key = %safe_key, "storage::delete_object not found in meta");
            return Ok(());
        }

        let path = blob_path(&self.data_dir, &bucket, &safe_key);
        match fs::remove_file(&path).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::warn!(%bucket, key = %safe_key, "storage::delete_object blob already missing");
            }
            Err(e) => return Err(StorageError::Internal(e.into())),
        }

        sqlx::query("DELETE FROM objects WHERE bucket = ? AND key = ?")
            .bind(&bucket)
            .bind(&safe_key)
            .execute(&self.pool)
            .await
            .map_err(internal)?;

        tracing::info!(%bucket, key = %safe_key, "storage::delete_object removed");
        Ok(())
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
            LIST_SCAN_CAP
        } else {
            (limit as i64).saturating_add(1)
        };

        let rows: Vec<ObjectMetadata> = sqlx::query_as(
            "SELECT bucket, key, size, mime_type, etag, created_at, updated_at, custom_meta
             FROM objects
             WHERE bucket = ? AND key > ? AND key LIKE ? ESCAPE '\\'
             ORDER BY key
             LIMIT ?",
        )
        .bind(&bucket)
        .bind(start_after)
        .bind(prefix_pattern)
        .bind(scan_limit)
        .fetch_all(&self.pool)
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
            });
        }

        if !is_truncated {
            if scanned_len as i64 >= LIST_SCAN_CAP {
                is_truncated = true;
            } else if let Some(ref last) = last_scanned {
                let count: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM objects
                     WHERE bucket = ? AND key > ? AND key LIKE ? ESCAPE '\\'",
                )
                .bind(&bucket)
                .bind(last)
                .bind(format!("{}%", escape_like_pattern(prefix)))
                .fetch_one(&self.pool)
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
            next_start_after: if is_truncated {
                last_scanned
            } else {
                None
            },
        })
    }

    pub async fn object_exists(&self, bucket: &str, key: &str) -> Result<bool, StorageError> {
        let bucket = sanitize_bucket(bucket).map_err(|_| StorageError::InvalidBucket)?;
        let safe_key = sanitize_key(key).map_err(|_| StorageError::InvalidKey)?;
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM objects WHERE bucket = ? AND key = ?",
        )
        .bind(&bucket)
        .bind(&safe_key)
        .fetch_one(&self.pool)
        .await
        .map_err(internal)?;
        Ok(count > 0)
    }

    pub async fn object_count(&self) -> Result<i64, StorageError> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM objects")
            .fetch_one(&self.pool)
            .await
            .map_err(internal)?;
        Ok(count)
    }

    pub async fn total_bytes(&self) -> Result<i64, StorageError> {
        let total: i64 = sqlx::query_scalar("SELECT COALESCE(SUM(size), 0) FROM objects")
            .fetch_one(&self.pool)
            .await
            .map_err(internal)?;
        Ok(total)
    }
}
