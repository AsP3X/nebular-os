use std::path::PathBuf;

use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use xxhash_rust::xxh3::Xxh3;

use super::compression::compress_blob;
use super::engine::{StorageEngine, TempFileGuard};
use super::error::{internal, map_io_error, StorageError};
use super::{blob_path, sanitize_bucket, sanitize_key};

#[derive(Debug, serde::Serialize)]
pub struct InitMultipartResult {
    pub upload_id: String,
    pub part_size: usize,
}

#[derive(Debug, serde::Serialize)]
pub struct PartUploadResult {
    pub etag: String,
}

struct MultipartSession {
    content_type: Option<String>,
}

impl StorageEngine {
    fn multipart_dir(&self, upload_id: &str) -> PathBuf {
        PathBuf::from(self.data_dir())
            .join(".multipart")
            .join(upload_id)
    }

    /// Starts a multipart session for the target object key.
    pub async fn init_multipart(
        &self,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
    ) -> Result<InitMultipartResult, StorageError> {
        let bucket = sanitize_bucket(bucket).map_err(|_| StorageError::InvalidBucket)?;
        let safe_key = sanitize_key(key).map_err(|_| StorageError::InvalidKey)?;
        let upload_id = uuid::Uuid::new_v4().to_string();
        let now = chrono::Utc::now().timestamp();

        sqlx::query(
            "INSERT INTO multipart_uploads (upload_id, bucket, key, content_type, created_at)
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(&upload_id)
        .bind(&bucket)
        .bind(&safe_key)
        .bind(content_type)
        .bind(now)
        .execute(self.write_pool())
        .await
        .map_err(internal)?;

        fs::create_dir_all(self.multipart_dir(&upload_id))
            .await
            .map_err(internal)?;

        Ok(InitMultipartResult {
            upload_id,
            part_size: self.multipart_part_size(),
        })
    }

    /// Stores one numbered part for an active multipart upload.
    pub async fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i32,
        mut body: impl tokio::io::AsyncRead + Unpin,
    ) -> Result<PartUploadResult, StorageError> {
        if part_number < 1 {
            return Err(StorageError::InvalidKey);
        }
        let bucket = sanitize_bucket(bucket).map_err(|_| StorageError::InvalidBucket)?;
        let safe_key = sanitize_key(key).map_err(|_| StorageError::InvalidKey)?;
        self.ensure_multipart_session(upload_id, &bucket, &safe_key)
            .await?;

        let part_path = self
            .multipart_dir(upload_id)
            .join(format!("{:05}", part_number));
        let mut file = fs::File::create(&part_path).await.map_err(internal)?;
        let mut hasher = Xxh3::new();
        let mut size: u64 = 0;
        let mut buf = vec![0u8; self.upload_buffer_size().min(self.multipart_part_size())];

        loop {
            let n = body.read(&mut buf).await.map_err(map_io_error)?;
            if n == 0 {
                break;
            }
            if size + n as u64 > self.multipart_part_size() as u64 {
                return Err(StorageError::PayloadTooLarge);
            }
            hasher.update(&buf[..n]);
            file.write_all(&buf[..n]).await.map_err(internal)?;
            size += n as u64;
        }
        file.flush().await.map_err(internal)?;
        let etag = format!("{:016x}", hasher.digest());

        sqlx::query(
            "INSERT INTO multipart_parts (upload_id, part_number, size, etag)
             VALUES (?, ?, ?, ?)
             ON CONFLICT(upload_id, part_number) DO UPDATE SET
                size = excluded.size,
                etag = excluded.etag",
        )
        .bind(upload_id)
        .bind(part_number)
        .bind(size as i64)
        .bind(&etag)
        .execute(self.write_pool())
        .await
        .map_err(internal)?;

        Ok(PartUploadResult { etag })
    }

    /// Concatenates uploaded parts into the final object blob and metadata row.
    pub async fn complete_multipart(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        custom_meta: Option<&str>,
    ) -> Result<super::types::ObjectMetadata, StorageError> {
        let bucket = sanitize_bucket(bucket).map_err(|_| StorageError::InvalidBucket)?;
        let safe_key = sanitize_key(key).map_err(|_| StorageError::InvalidKey)?;
        let session = self.ensure_multipart_session(upload_id, &bucket, &safe_key).await?;

        let parts: Vec<(i32,)> = sqlx::query_as(
            "SELECT part_number FROM multipart_parts WHERE upload_id = ? ORDER BY part_number",
        )
        .bind(upload_id)
        .fetch_all(self.write_pool())
        .await
        .map_err(internal)?;

        if parts.is_empty() {
            return Err(StorageError::InvalidKey);
        }

        let tmp_path = format!("{}/.tmp/{}.tmp", self.data_dir(), uuid::Uuid::new_v4());
        let final_path = blob_path(self.data_dir(), &bucket, &safe_key);
        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent).await.map_err(internal)?;
        }

        let mut out = fs::File::create(&tmp_path).await.map_err(internal)?;
        let _guard = TempFileGuard {
            path: PathBuf::from(&tmp_path),
        };
        let mut hasher = Xxh3::new();
        let mut uncompressed = Vec::new();

        // Human: Concatenate logical part bytes in memory, then compress once into the final object blob.
        // Agent: READS numbered part files; XXH3 over concatenated bytes; WRITES compress_blob to final path.
        for (part_number,) in parts {
            let part_path = self
                .multipart_dir(upload_id)
                .join(format!("{:05}", part_number));
            let mut part = fs::File::open(&part_path).await.map_err(internal)?;
            let mut buf = vec![0u8; self.upload_buffer_size()];
            loop {
                let n = part.read(&mut buf).await.map_err(internal)?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
                uncompressed.extend_from_slice(&buf[..n]);
            }
        }

        let total_size = uncompressed.len() as u64;
        let blob = compress_blob(&uncompressed)?;
        out.write_all(&blob).await.map_err(internal)?;
        out.flush().await.map_err(internal)?;
        drop(out);

        let etag = format!("{:016x}", hasher.digest());
        if final_path.exists() {
            fs::remove_file(&final_path).await.map_err(internal)?;
        }
        fs::rename(&tmp_path, &final_path)
            .await
            .map_err(internal)?;

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
        .bind(&bucket)
        .bind(&safe_key)
        .bind(total_size as i64)
        .bind(&session.content_type)
        .bind(&etag)
        .bind(unix_now)
        .bind(unix_now)
        .bind(custom_meta)
        .execute(self.write_pool())
        .await
        {
            let _ = fs::remove_file(&final_path).await;
            return Err(StorageError::Internal(e.into()));
        }

        self.cleanup_multipart(upload_id).await?;
        Ok(super::types::ObjectMetadata {
            bucket: bucket.to_string(),
            key: safe_key,
            size: total_size as i64,
            mime_type: session.content_type,
            etag: Some(etag),
            created_at: now,
            updated_at: now,
            custom_meta: custom_meta.map(|s| s.to_string()),
            deleted_at: None,
        })
    }

    /// Aborts a multipart session and deletes staged part files.
    pub async fn abort_multipart(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<(), StorageError> {
        let bucket = sanitize_bucket(bucket).map_err(|_| StorageError::InvalidBucket)?;
        let safe_key = sanitize_key(key).map_err(|_| StorageError::InvalidKey)?;
        self.ensure_multipart_session(upload_id, &bucket, &safe_key)
            .await?;
        self.cleanup_multipart(upload_id).await
    }

    async fn cleanup_multipart(&self, upload_id: &str) -> Result<(), StorageError> {
        sqlx::query("DELETE FROM multipart_parts WHERE upload_id = ?")
            .bind(upload_id)
            .execute(self.write_pool())
            .await
            .map_err(internal)?;
        sqlx::query("DELETE FROM multipart_uploads WHERE upload_id = ?")
            .bind(upload_id)
            .execute(self.write_pool())
            .await
            .map_err(internal)?;
        let _ = fs::remove_dir_all(self.multipart_dir(upload_id)).await;
        Ok(())
    }

    /// Resolves the object key for an active multipart session.
    pub async fn multipart_key_for_upload(
        &self,
        upload_id: &str,
    ) -> Result<String, StorageError> {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT key FROM multipart_uploads WHERE upload_id = ?",
        )
        .bind(upload_id)
        .fetch_optional(self.read_pool())
        .await
        .map_err(internal)?;
        row.map(|(k,)| k).ok_or(StorageError::NotFound)
    }

    async fn ensure_multipart_session(
        &self,
        upload_id: &str,
        bucket: &str,
        key: &str,
    ) -> Result<MultipartSession, StorageError> {
        let row: Option<(String, String, Option<String>)> = sqlx::query_as(
            "SELECT bucket, key, content_type FROM multipart_uploads WHERE upload_id = ?",
        )
        .bind(upload_id)
        .fetch_optional(self.read_pool())
        .await
        .map_err(internal)?;

        let Some((b, k, content_type)) = row else {
            return Err(StorageError::NotFound);
        };
        if b != *bucket || k != *key {
            return Err(StorageError::NotFound);
        }
        Ok(MultipartSession { content_type })
    }
}
