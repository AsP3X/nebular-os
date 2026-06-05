use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;

use sqlx::{Pool, Sqlite};
use tokio::fs;
use tokio::sync::Semaphore;

use super::blob_ops::link_or_copy_blob;
use super::blocks::BlockStore;
use super::compression::{self, DEFAULT_ZSTD_LEVEL, DEFAULT_ZSTD_LEVEL_UPLOAD};
use super::dict_store::DictStore;
use super::error::{internal, StorageError};
use super::metadata_backend::MetadataBackendKind;
use super::metadata_mode::MetadataMode;
use super::object_meta::{ObjectMetaConnect, ObjectMetaStore};
use super::range::parse_content_range;
use super::streaming::{
    finalize_temp_to_blob, open_object_body_stream, stream_body_to_temp, BlobFinalizeOptions,
    GuardedObjectBodyStream,
};
use super::blob_finalize::ReadContext;
use super::precondition::{check_write_preconditions, etag_matches};
use super::types::{
    DeletedObjectRef, DeletePrefixFailure, DeletePrefixOutcome, ListCountResult, ListItem,
    ListResult, ObjectMetadata,
};
use super::{blob_path, sanitize_bucket, sanitize_key};

pub(crate) const DEFAULT_UPLOAD_BUFFER: usize = 256 * 1024;
const DEFAULT_LIST_SCAN_CAP: i64 = 4096;
const DEFAULT_BULK_DELETE_CONCURRENCY: usize = 32;
const DEFAULT_BULK_DELETE_BATCH_LIMIT: u64 = 1000;

fn escape_like_pattern(s: &str) -> String {
    s.replace("\\", "\\\\")
        .replace("%", "\\%")
        .replace("_", "\\_")
}

/// Outcome of GET after conditional header checks against stored metadata.
/// Per-check results for `GET /health/ready`.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ReadinessChecks {
    pub metadata_backend: String,
    pub metadata_write: bool,
    pub metadata_read: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub postgres_ok: Option<bool>,
    pub sqlite_write: bool,
    pub sqlite_read: bool,
    pub data_dir_writable: bool,
}

impl ReadinessChecks {
    pub fn ready(&self) -> bool {
        self.metadata_write
            && self.metadata_read
            && self.data_dir_writable
            && self.sqlite_write
            && self.sqlite_read
            && self.postgres_ok.unwrap_or(true)
    }
}

pub enum GetObjectOutcome {
    NotModified(ObjectMetadata),
    Content {
        stream: GuardedObjectBodyStream,
        content_length: u64,
        total_size: u64,
        meta: Box<ObjectMetadata>,
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
    pub zstd_level_upload: i32,
    pub zstd_dict_enabled: bool,
    pub zstd_dict_max_bytes: usize,
    pub zstd_dict_train_batch: usize,
    pub dedup_enabled: bool,
    pub dedup_block_size: usize,
    pub dedup_min_size: u64,
    pub metadata_backend: MetadataBackendKind,
    pub metadata_mode: MetadataMode,
    pub metadata_database_url: Option<String>,
    pub max_logical_bytes: i64,
    pub bulk_delete_concurrency: usize,
    pub bulk_delete_batch_limit: u64,
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
            zstd_level_upload: DEFAULT_ZSTD_LEVEL_UPLOAD,
            zstd_dict_enabled: false,
            zstd_dict_max_bytes: 112_640,
            zstd_dict_train_batch: 32,
            dedup_enabled: false,
            dedup_block_size: 256 * 1024,
            dedup_min_size: 1024 * 1024,
            metadata_backend: MetadataBackendKind::Sqlite,
            metadata_mode: MetadataMode::Full,
            metadata_database_url: None,
            max_logical_bytes: 0,
            bulk_delete_concurrency: DEFAULT_BULK_DELETE_CONCURRENCY,
            bulk_delete_batch_limit: DEFAULT_BULK_DELETE_BATCH_LIMIT,
        }
    }
}

#[derive(Clone)]
pub struct StorageEngine {
    object_meta: ObjectMetaStore,
    system_write: Pool<Sqlite>,
    system_read: Pool<Sqlite>,
    metadata_backend: MetadataBackendKind,
    metadata_mode: MetadataMode,
    max_logical_bytes: i64,
    data_dir: String,
    upload_buffer_size: usize,
    list_scan_cap: i64,
    multipart_part_size: usize,
    soft_delete_ttl_secs: i64,
    soft_delete_drop_blob: bool,
    multipart_upload_ttl_secs: i64,
    recompress_batch_size: usize,
    zstd_level: i32,
    zstd_level_upload: i32,
    zstd_dict_enabled: bool,
    zstd_dict_max_bytes: usize,
    zstd_dict_train_batch: usize,
    dedup_enabled: bool,
    dedup_block_size: usize,
    dedup_min_size: u64,
    dict_store: DictStore,
    block_store: BlockStore,
    bulk_delete_concurrency: usize,
    bulk_delete_batch_limit: u64,
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
        let object_meta = ObjectMetaStore::connect(ObjectMetaConnect {
            backend: opts.metadata_backend,
            sqlite_path: meta_path.to_string(),
            postgres_url: opts.metadata_database_url.clone(),
            read_pool_size: opts.read_pool_size,
        })
        .await?;

        let (system_write, system_read) = match opts.metadata_backend {
            MetadataBackendKind::Sqlite => {
                let w = object_meta
                    .sqlite_write_pool()
                    .expect("sqlite object meta pool")
                    .clone();
                let r = object_meta
                    .sqlite_read_pool()
                    .expect("sqlite object meta pool")
                    .clone();
                (w, r)
            }
            MetadataBackendKind::Postgres => {
                super::object_meta::connect_system_sqlite(meta_path, opts.read_pool_size).await?
            }
        };

        fs::create_dir_all(data_dir).await.map_err(internal)?;
        fs::create_dir_all(format!("{}/.tmp", data_dir))
            .await
            .map_err(internal)?;
        fs::create_dir_all(format!("{}/.multipart", data_dir))
            .await
            .map_err(internal)?;
        fs::create_dir_all(format!("{}/.dict", data_dir))
            .await
            .map_err(internal)?;
        fs::create_dir_all(format!("{}/.blocks", data_dir))
            .await
            .map_err(internal)?;

        BlockStore::init_schema(&system_write).await?;

        Ok(Self {
            object_meta,
            system_write,
            system_read,
            metadata_backend: opts.metadata_backend,
            metadata_mode: opts.metadata_mode,
            max_logical_bytes: opts.max_logical_bytes.max(0),
            data_dir: data_dir.to_string(),
            upload_buffer_size: opts.upload_buffer_size.max(4096),
            list_scan_cap: opts.list_scan_cap.max(100),
            multipart_part_size: opts.multipart_part_size.max(1024 * 1024),
            soft_delete_ttl_secs: opts.soft_delete_ttl_secs.max(0),
            soft_delete_drop_blob: opts.soft_delete_drop_blob,
            multipart_upload_ttl_secs: opts.multipart_upload_ttl_secs.max(0),
            recompress_batch_size: opts.recompress_batch_size.max(1),
            zstd_level: compression::clamp_zstd_level(opts.zstd_level),
            zstd_level_upload: compression::clamp_zstd_level(opts.zstd_level_upload),
            zstd_dict_enabled: opts.zstd_dict_enabled,
            zstd_dict_max_bytes: opts.zstd_dict_max_bytes.max(1024),
            zstd_dict_train_batch: opts.zstd_dict_train_batch.max(2),
            dedup_enabled: opts.dedup_enabled,
            dedup_block_size: opts.dedup_block_size.max(4096),
            dedup_min_size: opts.dedup_min_size,
            dict_store: DictStore::new(data_dir),
            block_store: BlockStore::new(data_dir),
            bulk_delete_concurrency: opts.bulk_delete_concurrency.clamp(1, 256),
            bulk_delete_batch_limit: opts.bulk_delete_batch_limit.clamp(1, 10_000),
        })
    }

    pub fn write_pool(&self) -> &Pool<Sqlite> {
        &self.system_write
    }

    pub fn read_pool(&self) -> &Pool<Sqlite> {
        &self.system_read
    }

    pub fn metadata_backend(&self) -> MetadataBackendKind {
        self.metadata_backend
    }

    pub fn metadata_mode(&self) -> MetadataMode {
        self.metadata_mode
    }

    pub fn max_logical_bytes(&self) -> i64 {
        self.max_logical_bytes
    }

    pub fn object_meta(&self) -> &ObjectMetaStore {
        &self.object_meta
    }

    /// Rejects writes when active logical bytes plus incoming would exceed NOS_MAX_LOGICAL_BYTES.
    pub async fn ensure_capacity_for_write(
        &self,
        bucket: &str,
        key: &str,
        incoming_bytes: u64,
    ) -> Result<(), StorageError> {
        if self.max_logical_bytes <= 0 {
            return Ok(());
        }
        let current = self.total_bytes().await?;
        let existing = self
            .try_fetch_active_metadata(bucket, key)
            .await?
            .map(|m| m.size)
            .unwrap_or(0);
        let projected = current - existing + incoming_bytes as i64;
        if projected > self.max_logical_bytes {
            return Err(StorageError::InsufficientStorage);
        }
        Ok(())
    }

    pub async fn ensure_capacity_for_multipart_complete(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<(), StorageError> {
        if self.max_logical_bytes <= 0 {
            return Ok(());
        }
        let total_parts = self.object_meta.sum_multipart_parts_size(upload_id).await?;
        self.ensure_capacity_for_write(bucket, key, total_parts as u64)
            .await
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

    pub fn bulk_delete_concurrency(&self) -> usize {
        self.bulk_delete_concurrency
    }

    pub fn bulk_delete_batch_limit(&self) -> u64 {
        self.bulk_delete_batch_limit
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

    pub fn zstd_level_upload(&self) -> i32 {
        self.zstd_level_upload
    }

    pub fn zstd_dict_enabled(&self) -> bool {
        self.zstd_dict_enabled
    }

    pub fn zstd_dict_max_bytes(&self) -> usize {
        self.zstd_dict_max_bytes
    }

    pub fn zstd_dict_train_batch(&self) -> usize {
        self.zstd_dict_train_batch
    }

    pub fn dedup_enabled(&self) -> bool {
        self.dedup_enabled
    }

    pub fn dedup_block_size(&self) -> usize {
        self.dedup_block_size
    }

    pub fn dedup_min_size(&self) -> u64 {
        self.dedup_min_size
    }

    pub fn dict_store(&self) -> &DictStore {
        &self.dict_store
    }

    pub fn block_store(&self) -> &BlockStore {
        &self.block_store
    }

    pub fn system_write_pool(&self) -> &Pool<Sqlite> {
        &self.system_write
    }

    pub(crate) fn blob_finalize_options(&self, existing: Option<PathBuf>) -> BlobFinalizeOptions {
        let dict = if self.zstd_dict_enabled {
            self.dict_store.global_dict()
        } else {
            None
        };
        BlobFinalizeOptions {
            level: self.zstd_level_upload,
            dict_id: 0,
            dict,
            dedup_enabled: self.dedup_enabled,
            dedup_block_size: self.dedup_block_size,
            dedup_min_size: self.dedup_min_size,
            data_dir: self.data_dir.clone(),
            system_pool: self.system_write.clone(),
            existing_blob: existing,
        }
    }

    pub fn read_context(&self) -> ReadContext {
        ReadContext {
            data_dir: self.data_dir.clone(),
            dict: if self.zstd_dict_enabled {
                self.dict_store.global_dict()
            } else {
                None
            },
        }
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
        self.object_meta
            .try_fetch_active_metadata(&bucket, &safe_key)
            .await
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
        self.ensure_capacity_for_write(&dst_bucket, &dst_key, src_meta.size as u64)
            .await?;
        let entries = BlockStore::manifest_entries(&src_path)?;
        if entries.is_empty() {
            link_or_copy_blob(&src_path, &dst_path).await?;
        } else {
            if dst_path.exists() {
                BlockStore::release_blob(&self.system_write, &self.data_dir, &dst_path).await?;
            }
            link_or_copy_blob(&src_path, &dst_path).await?;
            BlockStore::inc_refs(&self.system_write, &entries).await?;
        }

        self.object_meta
            .copy_object_metadata(&src_meta, &dst_bucket, &dst_key)
            .await
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

        let existing = if final_path.exists() {
            Some(final_path.clone())
        } else {
            None
        };
        finalize_temp_to_blob(
            PathBuf::from(&tmp_path).as_path(),
            &final_path,
            size,
            self.blob_finalize_options(existing),
        )
        .await?;

        if let Err(e) = self
            .ensure_capacity_for_write(bucket, safe_key, size)
            .await
        {
            let _ = fs::remove_file(&final_path).await;
            return Err(e);
        }

        if self.metadata_mode.is_blob_only() {
            let now = chrono::Utc::now();
            return Ok((
                ObjectMetadata {
                    bucket: bucket.to_string(),
                    key: safe_key.to_string(),
                    size: size as i64,
                    mime_type: content_type.map(str::to_string),
                    etag: Some(etag.clone()),
                    created_at: now,
                    updated_at: now,
                    custom_meta: custom_meta.map(str::to_string),
                    deleted_at: None,
                    storage_class: None,
                    origin_node: None,
                },
                etag,
            ));
        }

        let meta = match self
            .object_meta
            .upsert_object(
                &self.data_dir,
                bucket,
                safe_key,
                size as i64,
                content_type,
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
        let bucket = sanitize_bucket(bucket).map_err(|_| StorageError::InvalidBucket)?;
        let safe_key = sanitize_key(key).map_err(|_| StorageError::InvalidKey)?;
        let meta = if self.metadata_mode.is_blob_only() {
            self.fetch_blob_only_metadata(&bucket, &safe_key).await?
        } else {
            self.fetch_active_metadata(&bucket, &safe_key).await?
        };

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
            &self.read_context(),
        )
        .await?;

        Ok(GetObjectOutcome::Content {
            stream,
            content_length,
            total_size,
            meta: Box::new(meta),
        })
    }

    pub async fn head_object(
        &self,
        bucket: &str,
        key: &str,
        if_none_match: Option<&str>,
        if_modified_since: Option<i64>,
    ) -> Result<Option<ObjectMetadata>, StorageError> {
        let bucket = sanitize_bucket(bucket).map_err(|_| StorageError::InvalidBucket)?;
        let safe_key = sanitize_key(key).map_err(|_| StorageError::InvalidKey)?;
        let meta = if self.metadata_mode.is_blob_only() {
            self.fetch_blob_only_metadata(&bucket, &safe_key).await?
        } else {
            self.fetch_active_metadata(&bucket, &safe_key).await?
        };
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

        if if_match.is_some() && !self.metadata_mode.is_blob_only() {
            self.ensure_write_preconditions(&bucket, &safe_key, if_match, None)
                .await?;
        }

        if self.metadata_mode.is_blob_only() {
            let path = blob_path(&self.data_dir, &bucket, &safe_key);
            if !path.exists() {
                return Ok(());
            }
            return self.drop_object_blob(&bucket, &safe_key).await;
        }

        if self.object_meta.active_row_count(&bucket, &safe_key).await? == 0 {
            return Ok(());
        }

        let path = blob_path(&self.data_dir, &bucket, &safe_key);

        if self.soft_delete_ttl_secs <= 0 {
            if path.exists() {
                BlockStore::release_blob(&self.system_write, &self.data_dir, &path).await?;
            }
            let _ = fs::remove_file(&path).await;
            self.object_meta.hard_delete_object(&bucket, &safe_key).await?;
            return Ok(());
        }

        if self.soft_delete_drop_blob {
            if path.exists() {
                BlockStore::release_blob(&self.system_write, &self.data_dir, &path).await?;
            }
            let _ = fs::remove_file(&path).await;
        }

        self.object_meta.soft_delete_object(&bucket, &safe_key).await?;
        Ok(())
    }

    async fn drop_object_blob(&self, bucket: &str, key: &str) -> Result<(), StorageError> {
        let path = blob_path(&self.data_dir, bucket, key);
        if path.exists() {
            BlockStore::release_blob(&self.system_write, &self.data_dir, &path).await?;
        }
        let _ = fs::remove_file(&path).await;
        Ok(())
    }

    /// Deletes explicit object keys using parallel blob drops and a batch metadata transaction.
    pub async fn delete_objects_batch(
        &self,
        bucket: &str,
        keys: &[String],
    ) -> Result<DeletePrefixOutcome, StorageError> {
        let bucket = sanitize_bucket(bucket).map_err(|_| StorageError::InvalidBucket)?;
        if keys.is_empty() {
            return Ok(DeletePrefixOutcome {
                deleted: 0,
                failed: Vec::new(),
                truncated: false,
                next_start_after: None,
                deleted_objects: Vec::new(),
            });
        }

        let mut pending = Vec::with_capacity(keys.len());
        let mut failed = Vec::new();
        for key in keys {
            match sanitize_key(key) {
                Ok(safe_key) => {
                    if self.metadata_mode.is_blob_only() {
                        pending.push(DeletedObjectRef {
                            key: safe_key,
                            storage_class: None,
                        });
                    } else if self.object_meta.active_row_count(&bucket, &safe_key).await? > 0 {
                        let meta = self.object_meta.fetch_active_metadata(&bucket, &safe_key).await;
                        match meta {
                            Ok(m) => pending.push(DeletedObjectRef {
                                key: safe_key,
                                storage_class: m.storage_class,
                            }),
                            Err(e) => failed.push(DeletePrefixFailure {
                                key: safe_key,
                                error: e.to_string(),
                            }),
                        }
                    }
                }
                Err(_) => failed.push(DeletePrefixFailure {
                    key: key.clone(),
                    error: "invalid key".to_string(),
                }),
            }
        }

        let mut outcome = self.delete_objects_internal(&bucket, pending).await?;
        outcome.failed.extend(failed);
        Ok(outcome)
    }

    /// Deletes up to `limit` active objects whose keys start with `prefix`, using parallel blob
    /// drops and a single metadata transaction per batch.
    pub async fn delete_objects_by_prefix(
        &self,
        bucket: &str,
        prefix: &str,
        limit: Option<u64>,
        start_after: Option<&str>,
    ) -> Result<DeletePrefixOutcome, StorageError> {
        let bucket = sanitize_bucket(bucket).map_err(|_| StorageError::InvalidBucket)?;
        if prefix.is_empty() {
            return Err(StorageError::InvalidKey);
        }
        let safe_prefix = sanitize_key(prefix).map_err(|_| StorageError::InvalidKey)?;
        let limit = limit
            .unwrap_or(self.bulk_delete_batch_limit)
            .min(self.bulk_delete_batch_limit)
            .max(1) as usize;
        let start_after = start_after.unwrap_or("");

        if self.metadata_mode.is_blob_only() {
            return self
                .delete_blob_only_prefix(&bucket, &safe_prefix, limit, start_after)
                .await;
        }

        let prefix_pattern = format!("{}%", escape_like_pattern(&safe_prefix));

        let rows = self
            .object_meta
            .list_active_rows(
                &bucket,
                start_after,
                &prefix_pattern,
                (limit as i64).saturating_add(1),
            )
            .await?;

        let truncated = rows.len() > limit;
        let page: Vec<_> = rows.into_iter().take(limit).collect();
        let next_start_after = if truncated {
            page.last().map(|r| r.key.clone())
        } else {
            None
        };

        if page.is_empty() {
            return Ok(DeletePrefixOutcome {
                deleted: 0,
                failed: Vec::new(),
                truncated: false,
                next_start_after: None,
                deleted_objects: Vec::new(),
            });
        }

        let pending: Vec<DeletedObjectRef> = page
            .iter()
            .map(|r| DeletedObjectRef {
                key: r.key.clone(),
                storage_class: r.storage_class.clone(),
            })
            .collect();

        let mut outcome = self.delete_objects_internal(&bucket, pending).await?;
        outcome.truncated = truncated;
        outcome.next_start_after = next_start_after;
        Ok(outcome)
    }

    async fn delete_objects_internal(
        &self,
        bucket: &str,
        pending: Vec<DeletedObjectRef>,
    ) -> Result<DeletePrefixOutcome, StorageError> {
        if pending.is_empty() {
            return Ok(DeletePrefixOutcome {
                deleted: 0,
                failed: Vec::new(),
                truncated: false,
                next_start_after: None,
                deleted_objects: Vec::new(),
            });
        }

        let keys: Vec<String> = pending.iter().map(|r| r.key.clone()).collect();
        let needs_blob_drop =
            self.metadata_mode.is_blob_only() || self.soft_delete_ttl_secs <= 0 || self.soft_delete_drop_blob;

        let mut blob_ok_keys: Vec<String> = Vec::with_capacity(keys.len());
        let mut failed: Vec<DeletePrefixFailure> = Vec::new();

        if needs_blob_drop {
            let semaphore = Arc::new(Semaphore::new(self.bulk_delete_concurrency));
            let mut join_set = tokio::task::JoinSet::new();
            for key in keys {
                let permit = semaphore
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| internal(anyhow::anyhow!("bulk delete worker pool closed")))?;
                let engine = self.clone();
                let bucket = bucket.to_string();
                join_set.spawn(async move {
                    let result = engine.drop_object_blob(&bucket, &key).await;
                    drop(permit);
                    (key, result)
                });
            }
            while let Some(joined) = join_set.join_next().await {
                match joined {
                    Ok((key, Ok(()))) => blob_ok_keys.push(key),
                    Ok((key, Err(e))) => failed.push(DeletePrefixFailure {
                        key,
                        error: e.to_string(),
                    }),
                    Err(e) => failed.push(DeletePrefixFailure {
                        key: String::new(),
                        error: format!("bulk delete task failed: {e}"),
                    }),
                }
            }
        } else {
            blob_ok_keys = keys;
        }

        let deleted_objects: Vec<DeletedObjectRef> = pending
            .into_iter()
            .filter(|r| blob_ok_keys.iter().any(|k| k == &r.key))
            .collect();

        let (deleted, deleted_objects) = if blob_ok_keys.is_empty() || self.metadata_mode.is_blob_only() {
            (blob_ok_keys.len() as u64, deleted_objects)
        } else if self.soft_delete_ttl_secs <= 0 {
            let metadata_result = self
                .object_meta
                .hard_delete_objects(bucket, &blob_ok_keys)
                .await;
            match metadata_result {
                Ok(n) => (n, deleted_objects),
                Err(e) => {
                    let err = e.to_string();
                    for key in blob_ok_keys {
                        failed.push(DeletePrefixFailure {
                            key,
                            error: err.clone(),
                        });
                    }
                    (0, Vec::new())
                }
            }
        } else {
            let metadata_result = self
                .object_meta
                .soft_delete_objects(bucket, &blob_ok_keys)
                .await;
            match metadata_result {
                Ok(n) => (n, deleted_objects),
                Err(e) => {
                    let err = e.to_string();
                    for key in blob_ok_keys {
                        failed.push(DeletePrefixFailure {
                            key,
                            error: err.clone(),
                        });
                    }
                    (0, Vec::new())
                }
            }
        };

        failed.retain(|f| !f.key.is_empty());

        Ok(DeletePrefixOutcome {
            deleted,
            failed,
            truncated: false,
            next_start_after: None,
            deleted_objects,
        })
    }

    async fn delete_blob_only_prefix(
        &self,
        bucket: &str,
        prefix: &str,
        limit: usize,
        start_after: &str,
    ) -> Result<DeletePrefixOutcome, StorageError> {
        let keys = self
            .list_blob_only_keys(bucket, Some(prefix), limit.saturating_add(1))
            .await?;
        let truncated = keys.len() > limit;
        let page: Vec<_> = keys
            .into_iter()
            .filter(|k| k.as_str() > start_after)
            .take(limit)
            .collect();
        let next_start_after = if truncated {
            page.last().cloned()
        } else {
            None
        };
        let pending: Vec<DeletedObjectRef> = page
            .into_iter()
            .map(|key| DeletedObjectRef {
                key,
                storage_class: None,
            })
            .collect();
        let mut outcome = self.delete_objects_internal(bucket, pending).await?;
        outcome.truncated = truncated;
        outcome.next_start_after = next_start_after;
        Ok(outcome)
    }

    /// Human: Probes SQLite pools and blob directory writability for orchestrator readiness checks.
    /// Agent: SELECT 1 on write+read pools; WRITE+DELETE probe file under NOS_DATA_DIR/.nos-ready-probe.
    pub async fn probe_readiness(&self) -> ReadinessChecks {
        let (metadata_write, metadata_read) = self.object_meta.probe().await;
        let sqlite_write = sqlx::query("SELECT 1")
            .fetch_one(&self.system_write)
            .await
            .is_ok();
        let sqlite_read = sqlx::query("SELECT 1")
            .fetch_one(&self.system_read)
            .await
            .is_ok();
        let data_dir_writable = Self::probe_data_dir_writable(&self.data_dir).await;
        let postgres_ok = if self.metadata_backend == MetadataBackendKind::Postgres {
            Some(metadata_write && metadata_read)
        } else {
            None
        };
        ReadinessChecks {
            metadata_backend: self.metadata_backend.as_str().to_string(),
            metadata_write,
            metadata_read,
            postgres_ok,
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
        self.object_meta.fetch_active_metadata(bucket, key).await
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

    /// Returns the count of active objects under `prefix` without listing every key.
    pub async fn count_objects_by_prefix(
        &self,
        bucket: &str,
        prefix: Option<&str>,
    ) -> Result<ListCountResult, StorageError> {
        let bucket = sanitize_bucket(bucket).map_err(|_| StorageError::InvalidBucket)?;
        let prefix = prefix.unwrap_or("");
        if self.metadata_mode.is_blob_only() {
            let keys = self
                .list_blob_only_keys(&bucket, Some(prefix), i64::MAX as usize)
                .await?;
            return Ok(ListCountResult {
                count: keys.len() as u64,
                prefix: Some(prefix.to_string()),
            });
        }
        let prefix_pattern = format!("{}%", escape_like_pattern(prefix));
        let count = self
            .object_meta
            .count_active_with_prefix(&bucket, &prefix_pattern)
            .await?;
        Ok(ListCountResult {
            count: count.max(0) as u64,
            prefix: Some(prefix.to_string()),
        })
    }

    async fn fetch_blob_only_metadata(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<ObjectMetadata, StorageError> {
        let path = blob_path(&self.data_dir, bucket, key);
        if !path.exists() {
            return Err(StorageError::NotFound);
        }
        let fs_meta = fs::metadata(&path).await.map_err(internal)?;
        let file_len = fs_meta.len();
        let path_for_size = path.clone();
        let logical = tokio::task::spawn_blocking(move || {
            match std::fs::File::open(&path_for_size) {
                Ok(f) => super::compression::read_blob_header_size(f).unwrap_or(file_len),
                Err(_) => file_len,
            }
        })
        .await
        .map_err(internal)?;
        let now = chrono::Utc::now();
        let updated_at = fs_meta
            .modified()
            .ok()
            .map(chrono::DateTime::<chrono::Utc>::from)
            .unwrap_or(now);
        Ok(ObjectMetadata {
            bucket: bucket.to_string(),
            key: key.to_string(),
            size: logical as i64,
            mime_type: None,
            etag: None,
            created_at: updated_at,
            updated_at,
            custom_meta: None,
            deleted_at: None,
            storage_class: None,
            origin_node: None,
        })
    }

    async fn list_blob_only_keys(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        limit: usize,
    ) -> Result<Vec<String>, StorageError> {
        let bucket_dir = std::path::PathBuf::from(self.data_dir()).join(bucket);
        if !bucket_dir.exists() {
            return Ok(Vec::new());
        }
        let prefix = prefix.unwrap_or("");
        let mut keys = Vec::new();
        let mut stack = vec![bucket_dir.clone()];
        while let Some(dir) = stack.pop() {
            if keys.len() >= limit {
                break;
            }
            let mut rd = fs::read_dir(&dir).await.map_err(internal)?;
            while let Some(ent) = rd.next_entry().await.map_err(internal)? {
                if keys.len() >= limit {
                    break;
                }
                let ft = ent.file_type().await.map_err(internal)?;
                if ft.is_dir() {
                    stack.push(ent.path());
                    continue;
                }
                if !ft.is_file() {
                    continue;
                }
                let rel = ent
                    .path()
                    .strip_prefix(&bucket_dir)
                    .map_err(internal)?
                    .to_string_lossy()
                    .replace('\\', "/");
                let key = rel
                    .split_once('/')
                    .map(|(_, k)| k.to_string())
                    .unwrap_or(rel);
                if key.is_empty() {
                    continue;
                }
                if !prefix.is_empty() && !key.starts_with(prefix) {
                    continue;
                }
                keys.push(key);
            }
        }
        keys.sort();
        Ok(keys)
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

        let rows = self
            .object_meta
            .list_active_rows(&bucket, start_after, &prefix_pattern, scan_limit)
            .await?;

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
                let count = self
                    .object_meta
                    .count_keys_after(&bucket, last, &prefix_pattern)
                    .await?;
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
        self.object_meta.object_exists(&bucket, &safe_key).await
    }

    pub async fn object_count(&self) -> Result<i64, StorageError> {
        self.object_meta.object_count().await
    }

    pub async fn total_bytes(&self) -> Result<i64, StorageError> {
        self.object_meta.total_bytes().await
    }

    pub async fn set_object_placement(
        &self,
        bucket: &str,
        key: &str,
        storage_class: &str,
        origin_node: &str,
    ) -> Result<(), StorageError> {
        let bucket = sanitize_bucket(bucket).map_err(|_| StorageError::InvalidBucket)?;
        let safe_key = sanitize_key(key).map_err(|_| StorageError::InvalidKey)?;
        self.object_meta
            .set_object_placement(&bucket, &safe_key, storage_class, origin_node)
            .await
    }

    pub async fn active_storage_class(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<String>, StorageError> {
        let bucket = sanitize_bucket(bucket).map_err(|_| StorageError::InvalidBucket)?;
        let safe_key = sanitize_key(key).map_err(|_| StorageError::InvalidKey)?;
        self.object_meta
            .active_storage_class(&bucket, &safe_key)
            .await
    }

    pub async fn objects_by_storage_class(
        &self,
    ) -> Result<Vec<(String, i64)>, StorageError> {
        self.object_meta.objects_by_storage_class().await
    }
}
