use std::path::PathBuf;

use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use xxhash_rust::xxh3::Xxh3;

use super::blob_rel_path;
use super::engine::{StorageEngine, TempFileGuard};
use super::error::{internal, map_io_error, StorageError};
use super::write_path::WriteConditions;
use super::{sanitize_bucket, sanitize_key};

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct InitMultipartResult {
    pub upload_id: String,
    pub part_size: usize,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct PartUploadResult {
    pub etag: String,
}

/// One entry of a client's complete-multipart part list (S3 `CompleteMultipartUpload` semantics).
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct CompletedPart {
    pub part_number: i32,
    /// ETag returned by the part upload; when given it must still match the stored part.
    #[serde(default)]
    pub etag: Option<String>,
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
        super::check_new_object(&bucket, &safe_key)?;
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

        // Human: Receive into a scratch file and rename only after the whole body arrived, so a failed
        // retry of a part can never truncate the copy that already succeeded.
        let part_path = self
            .multipart_dir(upload_id)
            .join(format!("{:05}", part_number));
        let partial = self
            .multipart_dir(upload_id)
            .join(format!("{:05}.{}.partial", part_number, uuid::Uuid::new_v4()));
        let _partial_guard = TempFileGuard {
            path: partial.clone(),
        };
        let mut file = fs::File::create(&partial).await.map_err(internal)?;
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
        drop(file);
        fs::rename(&partial, &part_path).await.map_err(internal)?;
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
        self.complete_multipart_with_parts(bucket, key, upload_id, custom_meta, None)
            .await
    }

    /// Human: Assemble an upload into the object. With a client part list, exactly those parts are used (in
    /// ascending order, ETags must match); without one, stored parts must run 1..=N with no gaps. Every part's
    /// bytes are checked against the size and ETag recorded when it was uploaded.
    /// Agent: WRITES assembled temp file; COMMITS via commit_upload_file (atomic swap); CLEANS session after.
    pub async fn complete_multipart_with_parts(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        custom_meta: Option<&str>,
        parts: Option<&[CompletedPart]>,
    ) -> Result<super::types::ObjectMetadata, StorageError> {
        self.complete_multipart_conditional(
            bucket,
            key,
            upload_id,
            custom_meta,
            parts,
            WriteConditions::default(),
        )
        .await
    }

    /// `complete_multipart_with_parts` with `conditions` (preconditions and hook) evaluated at the commit.
    pub async fn complete_multipart_conditional(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        custom_meta: Option<&str>,
        parts: Option<&[CompletedPart]>,
        conditions: WriteConditions<'_>,
    ) -> Result<super::types::ObjectMetadata, StorageError> {
        let bucket = sanitize_bucket(bucket).map_err(|_| StorageError::InvalidBucket)?;
        let safe_key = sanitize_key(key).map_err(|_| StorageError::InvalidKey)?;
        let session = self.ensure_multipart_session(upload_id, &bucket, &safe_key).await?;

        self.ensure_capacity_for_multipart_complete(&bucket, &safe_key, upload_id)
            .await?;

        let recorded = self.object_meta().list_multipart_parts(upload_id).await?;
        let selected = select_parts(&recorded, parts)?;

        let tmp_path = PathBuf::from(format!(
            "{}/.tmp/{}.tmp",
            self.data_dir(),
            uuid::Uuid::new_v4()
        ));
        let _guard = TempFileGuard {
            path: tmp_path.clone(),
        };
        let mut out = fs::File::create(&tmp_path).await.map_err(internal)?;
        let mut whole = Xxh3::new();
        let mut total_size = 0u64;
        let mut buf = vec![0u8; self.upload_buffer_size()];

        for (part_number, size, etag) in selected {
            let part_path = self
                .multipart_dir(upload_id)
                .join(format!("{:05}", part_number));
            let mut part = fs::File::open(&part_path).await.map_err(internal)?;
            let mut part_hash = Xxh3::new();
            let mut part_len = 0u64;
            loop {
                let n = part.read(&mut buf).await.map_err(internal)?;
                if n == 0 {
                    break;
                }
                part_hash.update(&buf[..n]);
                whole.update(&buf[..n]);
                out.write_all(&buf[..n]).await.map_err(internal)?;
                part_len += n as u64;
            }
            if part_len != size as u64 || format!("{:016x}", part_hash.digest()) != etag {
                return Err(StorageError::InvalidRequest(format!(
                    "part {part_number} does not match its recorded upload; upload it again"
                )));
            }
            total_size += part_len;
        }
        out.flush().await.map_err(internal)?;
        drop(out);
        let etag = format!("{:016x}", whole.digest());

        let meta = self
            .commit_upload_file(
                &bucket,
                &safe_key,
                &tmp_path,
                total_size,
                &etag,
                session.content_type.as_deref(),
                custom_meta,
                conditions,
            )
            .await?;

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

/// Human: Decide which recorded parts form the object, rejecting gaps, unknown parts and stale ETags.
/// Agent: RETURNS (part_number, size, etag) in assembly order; ERR InvalidRequest names the offending part.
fn select_parts(
    recorded: &[(i32, i64, String)],
    requested: Option<&[CompletedPart]>,
) -> Result<Vec<(i32, i64, String)>, StorageError> {
    let invalid = |msg: String| Err(StorageError::InvalidRequest(msg));
    match requested {
        None => {
            if recorded.is_empty() {
                return invalid("no parts were uploaded".into());
            }
            for (expected, (number, _, _)) in (1..).zip(recorded) {
                if *number != expected {
                    return invalid(format!(
                        "part {expected} is missing; upload it or send the part list"
                    ));
                }
            }
            Ok(recorded.to_vec())
        }
        Some([]) => invalid("the part list is empty".into()),
        Some(list) => {
            let mut selected = Vec::with_capacity(list.len());
            let mut previous = 0;
            for part in list {
                if part.part_number <= previous {
                    return invalid("part numbers must be listed in ascending order".into());
                }
                previous = part.part_number;
                let Some(found) = recorded.iter().find(|(n, _, _)| *n == part.part_number) else {
                    return invalid(format!("part {} was not uploaded", part.part_number));
                };
                if let Some(etag) = &part.etag
                    && etag.trim_matches('"') != found.2
                {
                    return invalid(format!(
                        "part {} ETag does not match the uploaded part",
                        part.part_number
                    ));
                }
                selected.push(found.clone());
            }
            Ok(selected)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(parts: &[i32]) -> Vec<(i32, i64, String)> {
        parts.iter().map(|&n| (n, 10, format!("etag{n}"))).collect()
    }

    fn part(n: i32, etag: Option<&str>) -> CompletedPart {
        CompletedPart {
            part_number: n,
            etag: etag.map(str::to_string),
        }
    }

    #[test]
    fn implicit_list_requires_contiguous_parts() {
        assert_eq!(select_parts(&rec(&[1, 2, 3]), None).unwrap().len(), 3);
        assert!(select_parts(&rec(&[1, 3]), None).is_err());
        assert!(select_parts(&rec(&[2, 3]), None).is_err());
        assert!(select_parts(&[], None).is_err());
    }

    #[test]
    fn explicit_list_selects_and_verifies_parts() {
        let recorded = rec(&[1, 2, 5]);
        let picked = select_parts(&recorded, Some(&[part(1, None), part(5, Some("\"etag5\""))])).unwrap();
        assert_eq!(picked.iter().map(|p| p.0).collect::<Vec<_>>(), vec![1, 5]);
        assert!(select_parts(&recorded, Some(&[part(1, None), part(3, None)])).is_err());
        assert!(select_parts(&recorded, Some(&[part(2, None), part(1, None)])).is_err());
        assert!(select_parts(&recorded, Some(&[part(2, Some("other"))])).is_err());
        assert!(select_parts(&recorded, Some(&[])).is_err());
    }
}
