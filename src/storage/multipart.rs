use std::path::PathBuf;

use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use xxhash_rust::xxh3::Xxh3;

use super::object_meta::blob_rel_path;
use super::streaming::{finalize_temp_to_blob, hash_temp_file};
use super::engine::{StorageEngine, TempFileGuard};
use super::error::{internal, map_io_error, StorageError};
use super::{blob_path, sanitize_bucket, sanitize_key};

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct InitMultipartResult {
    pub upload_id: String,
    pub part_size: usize,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
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

    pub async fn init_multipart(
        &self,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
    ) -> Result<InitMultipartResult, StorageError> {
        let bucket = sanitize_bucket(bucket).map_err(|_| StorageError::InvalidBucket)?;
        let safe_key = sanitize_key(key).map_err(|_| StorageError::InvalidKey)?;
        let upload_id = uuid::Uuid::new_v4().to_string();

        self.object_meta()
            .init_multipart(
                &upload_id,
                &bucket,
                &safe_key,
                content_type,
                self.multipart_upload_ttl_secs(),
            )
            .await?;

        fs::create_dir_all(self.multipart_dir(&upload_id))
            .await
            .map_err(internal)?;

        Ok(InitMultipartResult {
            upload_id,
            part_size: self.multipart_part_size(),
        })
    }

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
        let part_blob = blob_rel_path(upload_id, &format!("{:05}", part_number));

        self.object_meta()
            .upsert_multipart_part(upload_id, part_number, size as i64, &etag, Some(&part_blob))
            .await?;

        Ok(PartUploadResult { etag })
    }

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

        self.ensure_capacity_for_multipart_complete(&bucket, &safe_key, upload_id)
            .await?;

        let parts = self
            .object_meta()
            .list_multipart_part_numbers(upload_id)
            .await?;

        if parts.is_empty() {
            return Err(StorageError::InvalidKey);
        }

        let tmp_path = format!("{}/.tmp/{}.tmp", self.data_dir(), uuid::Uuid::new_v4());
        let final_path = blob_path(self.data_dir(), &bucket, &safe_key);
        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent).await.map_err(internal)?;
        }

        let _guard = TempFileGuard {
            path: PathBuf::from(&tmp_path),
        };
        let mut out = fs::File::create(&tmp_path).await.map_err(internal)?;

        for part_number in parts {
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
                out.write_all(&buf[..n]).await.map_err(internal)?;
            }
        }
        out.flush().await.map_err(internal)?;
        drop(out);

        let (total_size, etag) = hash_temp_file(
            PathBuf::from(&tmp_path).as_path(),
            self.upload_buffer_size(),
        )?;

        let existing = if final_path.exists() {
            Some(final_path.clone())
        } else {
            None
        };
        finalize_temp_to_blob(
            PathBuf::from(&tmp_path).as_path(),
            &final_path,
            total_size,
            self.blob_finalize_options(existing),
        )
        .await?;

        let meta = match self
            .object_meta()
            .upsert_object(
                self.data_dir(),
                &bucket,
                &safe_key,
                total_size as i64,
                session.content_type.as_deref(),
                &etag,
                custom_meta,
                None,
                None,
            )
            .await
        {
            Ok(m) => m,
            Err(e) => {
                let _ = fs::remove_file(&final_path).await;
                return Err(e);
            }
        };

        self.cleanup_multipart(upload_id).await?;
        Ok(meta)
    }

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
        self.object_meta().cleanup_multipart(upload_id).await?;
        let _ = fs::remove_dir_all(self.multipart_dir(upload_id)).await;
        Ok(())
    }

    pub async fn multipart_key_for_upload(
        &self,
        upload_id: &str,
    ) -> Result<String, StorageError> {
        self.object_meta()
            .multipart_object_key(upload_id)
            .await?
            .ok_or(StorageError::NotFound)
    }

    async fn ensure_multipart_session(
        &self,
        upload_id: &str,
        bucket: &str,
        key: &str,
    ) -> Result<MultipartSession, StorageError> {
        match self
            .object_meta()
            .fetch_multipart_session(upload_id, bucket, key)
            .await?
        {
            Some(content_type) => Ok(MultipartSession { content_type }),
            None => Err(StorageError::NotFound),
        }
    }

    pub async fn purge_stale_multipart_uploads(&self) -> Result<u64, StorageError> {
        if self.multipart_upload_ttl_secs() <= 0 {
            return Ok(0);
        }
        let cutoff = chrono::Utc::now().timestamp() - self.multipart_upload_ttl_secs();
        let upload_ids = self
            .object_meta()
            .list_stale_multipart_upload_ids(cutoff)
            .await?;

        let mut purged = 0u64;
        for upload_id in upload_ids {
            self.cleanup_multipart(&upload_id).await?;
            purged += 1;
        }
        if purged > 0 {
            tracing::info!(purged, "storage::purge_stale_multipart_uploads completed");
        }
        Ok(purged)
    }
}
