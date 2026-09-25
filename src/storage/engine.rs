use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;

use sqlx::{Pool, Sqlite};
use tokio::fs;
use tokio::sync::Semaphore;

use super::blob_ops::{link_or_copy_blob, sync_file};
use super::block_cache::BlockDecodeCache;
use super::blocks::BlockStore;
use super::compressibility::DEFAULT_MIN_COMPRESSIBLE_SIZE;
use super::compression::{self, DEFAULT_BLOCK_SIZE, DEFAULT_ZSTD_LEVEL, DEFAULT_ZSTD_LEVEL_UPLOAD};
use super::dict_store::{DictStore, IdentifiedDict};
use super::error::{internal, StorageError};
use super::key_locks::KeyLocks;
use super::metadata_backend::MetadataBackendKind;
use super::metadata_mode::MetadataMode;
use super::object_meta::{ObjectMetaConnect, ObjectMetaStore};
use super::range::{evaluate_range, RangeRequest};
use super::streaming::{
    open_object_body_stream, stream_body_to_temp, BlobFinalizeOptions, GuardedObjectBodyStream,
};
use super::write_path::{CommitHook, Committed, MetaCommit, WriteConditions};
use super::blob_finalize::ReadContext;
use super::precondition::check_write_preconditions;
use super::types::{
    DeletedObjectRef, DeletePrefixFailure, DeletePrefixOutcome, ListCountResult, ListItem,
    ListResult, ObjectMetadata,
};
use super::{
    blob_path_variants, check_new_object, first_existing_blob_path, object_key_from_blob_relpath,
    sanitize_bucket, sanitize_key,
};

/// Keys a bulk delete locks and removes at a time.
const BULK_DELETE_CHUNK: usize = 64;

pub(crate) const DEFAULT_UPLOAD_BUFFER: usize = 256 * 1024;
const DEFAULT_LIST_SCAN_CAP: i64 = 4096;
const DEFAULT_BULK_DELETE_CONCURRENCY: usize = 32;
const DEFAULT_BULK_DELETE_BATCH_LIMIT: u64 = 1000;

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
        /// Inclusive byte span served when a Range was honored (206); `None` means the full body (200).
        range: Option<(u64, u64)>,
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
    pub compress_min_size: usize,
    pub compress_block_size: usize,
    pub compress_exclude_extensions: Vec<String>,
    pub block_cache_entries: usize,
    /// Byte budget for decoded blocks held by the cache (NOS_BLOCK_CACHE_MAX_BYTES).
    pub block_cache_max_bytes: usize,
    pub verify_batch_size: usize,
    pub scrub_sample_denom: u64,
    pub scrub_mode_light: bool,
    pub verify_on_read: bool,
    pub read_buffer_size: usize,
    /// fsync blob data and its directory before a write becomes visible (NOS_FSYNC_WRITES).
    pub fsync_writes: bool,
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
            compress_min_size: DEFAULT_MIN_COMPRESSIBLE_SIZE,
            compress_block_size: DEFAULT_BLOCK_SIZE,
            compress_exclude_extensions: Vec::new(),
            block_cache_entries: 256,
            block_cache_max_bytes: super::block_cache::DEFAULT_BLOCK_CACHE_MAX_BYTES,
            verify_batch_size: 100,
            scrub_sample_denom: 1,
            scrub_mode_light: false,
            verify_on_read: false,
            read_buffer_size: 256 * 1024,
            fsync_writes: true,
        }
    }
}

/// maintenance_state key holding the last key the periodic scrub visited ("" = start over).
const SCRUB_CURSOR: &str = "scrub_cursor";
/// maintenance_state key holding how many full passes the periodic scrub has completed (its sample slice).
const SCRUB_EPOCH: &str = "scrub_epoch";

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
    compress_min_size: usize,
    compress_block_size: usize,
    compress_exclude_extensions: Arc<Vec<String>>,
    block_decode_cache: Option<BlockDecodeCache>,
    verify_batch_size: usize,
    scrub_sample_denom: u64,
    scrub_mode_light: bool,
    verify_on_read: bool,
    read_buffer_pool: super::buffer_pool::BufferPool,
    read_buffer_size: usize,
    key_locks: KeyLocks,
    capacity_lock: Arc<tokio::sync::Mutex<()>>,
    /// Bytes admitted under NOS_MAX_LOGICAL_BYTES by writes that haven't committed yet (`CapacityReservation`).
    capacity_in_flight: Arc<std::sync::atomic::AtomicI64>,
    fsync_writes: bool,
}

/// Human: Room a write was admitted for under NOS_MAX_LOGICAL_BYTES, counted against the cap until the write has
/// committed (its size is then in the metadata totals) or failed; released on drop.
pub(crate) struct CapacityReservation {
    in_flight: Arc<std::sync::atomic::AtomicI64>,
    bytes: i64,
}

impl Drop for CapacityReservation {
    fn drop(&mut self) {
        self.in_flight
            .fetch_sub(self.bytes, std::sync::atomic::Ordering::SeqCst);
    }
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
        let tmp_dir = PathBuf::from(format!("{data_dir}/.tmp"));
        let portable = tokio::task::spawn_blocking(move || super::blob_paths::configure_filenames_for(&tmp_dir))
            .await
            .map_err(internal)?
            .map_err(internal)?;
        if portable {
            tracing::info!("blob filenames: portable (filesystem ignores case, or Windows)");
        }
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

        let engine = Self {
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
            compress_min_size: opts.compress_min_size.max(1),
            compress_block_size: opts.compress_block_size.max(4096),
            compress_exclude_extensions: Arc::new(opts.compress_exclude_extensions),
            block_decode_cache: BlockDecodeCache::with_byte_budget(
                opts.block_cache_entries,
                opts.block_cache_max_bytes,
            ),
            verify_batch_size: opts.verify_batch_size.max(1),
            scrub_sample_denom: opts.scrub_sample_denom.max(1),
            scrub_mode_light: opts.scrub_mode_light,
            verify_on_read: opts.verify_on_read,
            read_buffer_pool: super::buffer_pool::BufferPool::new(
                opts.read_buffer_size.max(4096),
                32,
            ),
            read_buffer_size: opts.read_buffer_size.max(4096),
            key_locks: KeyLocks::default(),
            capacity_lock: Arc::new(tokio::sync::Mutex::new(())),
            capacity_in_flight: Arc::default(),
            fsync_writes: opts.fsync_writes,
        };
        engine.recover_interrupted_swaps().await?;
        Ok(engine)
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

    /// Per-object write locks shared by every clone of this engine.
    pub fn key_locks(&self) -> &KeyLocks {
        &self.key_locks
    }

    /// Human: Admit a write of `incoming_bytes` replacing `existing_bytes` under NOS_MAX_LOGICAL_BYTES. Writes still
    /// in flight count too, so concurrent writes can't overshoot the cap together; the lock covers only this check
    /// (it used to cover each write's file and metadata I/O, which ran every write on a capped node one at a time).
    /// Agent: RETURNS None without a cap; Err(InsufficientStorage) over it; HOLD the reservation until committed.
    pub(crate) async fn reserve_capacity(
        &self,
        existing_bytes: i64,
        incoming_bytes: u64,
    ) -> Result<Option<CapacityReservation>, StorageError> {
        if self.max_logical_bytes <= 0 {
            return Ok(None);
        }
        let _check = self.capacity_lock.lock().await;
        // Human: In-flight bytes first, stored total second. A write releases its reservation only after its
        // metadata has committed, so it is then counted at least once. Read the other way round, a write committing
        // and releasing between the two reads was counted in neither, and concurrent writes overshot the cap.
        // Agent: ORDER MATTERS — load(in_flight) happens-before total_bytes(); commit happens-before release.
        let in_flight = self
            .capacity_in_flight
            .load(std::sync::atomic::Ordering::SeqCst);
        let current = self.total_bytes().await?;
        let incoming = i64::try_from(incoming_bytes).unwrap_or(i64::MAX);
        let projected = current
            .saturating_add(in_flight)
            .saturating_sub(existing_bytes)
            .saturating_add(incoming);
        if projected > self.max_logical_bytes {
            return Err(StorageError::InsufficientStorage);
        }
        self.capacity_in_flight
            .fetch_add(incoming, std::sync::atomic::Ordering::SeqCst);
        Ok(Some(CapacityReservation {
            in_flight: self.capacity_in_flight.clone(),
            bytes: incoming,
        }))
    }

    pub fn fsync_writes(&self) -> bool {
        self.fsync_writes
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

    pub fn compress_min_size(&self) -> usize {
        self.compress_min_size
    }

    pub fn compress_block_size(&self) -> usize {
        self.compress_block_size
    }

    /// Unified block size for compression and dedup (compress_block_size after config resolution).
    pub fn block_size(&self) -> usize {
        self.compress_block_size
    }

    pub fn verify_batch_size(&self) -> usize {
        self.verify_batch_size
    }

    pub fn block_decode_cache(&self) -> Option<&BlockDecodeCache> {
        self.block_decode_cache.as_ref()
    }

    pub fn compress_exclude_extensions(&self) -> &[String] {
        &self.compress_exclude_extensions
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

    /// The dictionary new blobs are compressed with, and its id (none unless NOS_ZSTD_DICT_ENABLED).
    pub(crate) fn write_dictionary(&self) -> Option<IdentifiedDict> {
        self.zstd_dict_enabled.then(|| self.dict_store.current()).flatten()
    }

    pub(crate) fn blob_finalize_options(
        &self,
        existing: Option<PathBuf>,
        object_key: &str,
        content_type: Option<&str>,
    ) -> BlobFinalizeOptions {
        let (dict_id, dict) = match self.write_dictionary() {
            Some((id, dict)) => (id, Some(dict)),
            None => (0, None),
        };
        BlobFinalizeOptions {
            level: self.zstd_level_upload,
            dict_id,
            dict,
            dedup_enabled: self.dedup_enabled,
            dedup_block_size: self.dedup_block_size,
            dedup_min_size: self.dedup_min_size,
            compress_min_size: self.compress_min_size,
            compress_block_size: self.compress_block_size,
            extra_excluded_extensions: self.compress_exclude_extensions.clone(),
            object_key: Some(object_key.to_string()),
            content_type: content_type.map(str::to_string),
            data_dir: self.data_dir.clone(),
            system_pool: self.system_write.clone(),
            existing_blob: existing,
        }
    }

    pub fn read_context(&self) -> ReadContext {
        ReadContext {
            data_dir: self.data_dir.clone(),
            // Human: Always, whatever NOS_ZSTD_DICT_ENABLED says now — blobs written while it was on need the
            // dictionary to decode, and turning the setting off used to make them unreadable.
            dict: self.dict_store.global_dict(),
            block_cache: self.block_decode_cache.clone(),
            read_buffer_size: self.read_buffer_size,
            verify_on_read: self.verify_on_read,
            buffer_pool: self.read_buffer_pool.clone(),
            expected_etag: None,
        }
    }

    pub fn scrub_sample_denom(&self) -> u64 {
        self.scrub_sample_denom
    }

    pub fn scrub_mode_light(&self) -> bool {
        self.scrub_mode_light
    }

    pub async fn get_maintenance_state(&self, key: &str) -> Result<Option<String>, StorageError> {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT value FROM maintenance_state WHERE key = ?",
        )
        .bind(key)
        .fetch_optional(&self.system_read)
        .await
        .map_err(internal)?;
        Ok(row.map(|(v,)| v))
    }

    pub async fn set_maintenance_state(&self, key: &str, value: &str) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO maintenance_state (key, value) VALUES (?, ?)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        )
        .bind(key)
        .bind(value)
        .execute(&self.system_write)
        .await
        .map_err(internal)?;
        Ok(())
    }

    pub async fn scrub_with_defaults(&self, limit: usize) -> Result<super::maintenance::VerifyBlobsReport, StorageError> {
        let opts = self.next_scrub_options(limit).await?;
        let report = self.scrub_objects(opts.clone()).await?;
        self.save_scrub_progress(&opts, &report).await?;
        Ok(report)
    }

    /// Human: Options for the next periodic scrub batch: the configured mode and sampling, resuming after the
    /// key the previous batch stopped at, in the current pass's sample slice.
    pub async fn next_scrub_options(&self, limit: usize) -> Result<super::scrub::ScrubOptions, StorageError> {
        let start_after = self
            .get_maintenance_state(SCRUB_CURSOR)
            .await?
            .filter(|c| !c.is_empty());
        let sample_epoch = self
            .get_maintenance_state(SCRUB_EPOCH)
            .await?
            .and_then(|e| e.parse().ok())
            .unwrap_or(0);
        let mode = if self.scrub_mode_light {
            super::scrub::ScrubMode::Light
        } else {
            super::scrub::ScrubMode::Deep
        };
        Ok(super::scrub::ScrubOptions {
            limit,
            sample_denom: self.scrub_sample_denom,
            sample_epoch,
            mode,
            start_after,
        })
    }

    /// Human: Remember where the periodic scrub continues. After the last key it starts over — it used to keep
    /// the final key, so every later batch was empty and objects were never re-verified — and moves to the next
    /// sample slice, so sampled scrubs cover every key within `sample_denom` passes.
    pub async fn save_scrub_progress(
        &self,
        opts: &super::scrub::ScrubOptions,
        report: &super::maintenance::VerifyBlobsReport,
    ) -> Result<(), StorageError> {
        match report.next_start_after.as_deref() {
            Some(key) if report.is_truncated => self.set_maintenance_state(SCRUB_CURSOR, key).await,
            _ => {
                let next_epoch = opts.sample_epoch.wrapping_add(1).to_string();
                self.set_maintenance_state(SCRUB_EPOCH, &next_epoch).await?;
                self.set_maintenance_state(SCRUB_CURSOR, "").await
            }
        }
    }

    async fn existing_blob_path(&self, bucket: &str, key: &str) -> Result<PathBuf, StorageError> {
        let variants = blob_path_variants(&self.data_dir, bucket, key);
        first_existing_blob_path(&variants)
            .await
            .map_err(internal)?
            .ok_or(StorageError::NotFound)
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
        body: impl tokio::io::AsyncRead + Unpin,
    ) -> Result<ObjectMetadata, StorageError> {
        self.put_object_conditional(
            bucket,
            key,
            content_type,
            custom_meta,
            body,
            WriteConditions::default(),
        )
        .await
    }

    /// Human: PUT with If-Match / If-None-Match evaluated atomically with the write (under the key lock).
    pub async fn put_object_conditional(
        &self,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
        custom_meta: Option<&str>,
        mut body: impl tokio::io::AsyncRead + Unpin,
        conditions: WriteConditions<'_>,
    ) -> Result<ObjectMetadata, StorageError> {
        let bucket = sanitize_bucket(bucket).map_err(|_| StorageError::InvalidBucket)?;
        let safe_key = sanitize_key(key).map_err(|_| StorageError::InvalidKey)?;
        check_new_object(&bucket, &safe_key)?;
        let (meta, _) = self
            .write_object_stream(&bucket, &safe_key, content_type, custom_meta, &mut body, conditions)
            .await?;
        Ok(meta)
    }

    /// Server-side copy: the destination gets its own hard link (or copy) of the source blob.
    pub async fn copy_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
        if_match: Option<&str>,
        if_none_match: Option<&str>,
    ) -> Result<ObjectMetadata, StorageError> {
        let conditions = WriteConditions {
            if_match,
            if_none_match,
            hook: None,
        };
        self.copy_object_conditional(src_bucket, src_key, dst_bucket, dst_key, conditions)
            .await
    }

    /// `copy_object` with `conditions` (preconditions and hook) evaluated against the destination.
    pub async fn copy_object_conditional(
        &self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
        conditions: WriteConditions<'_>,
    ) -> Result<ObjectMetadata, StorageError> {
        let src_bucket = sanitize_bucket(src_bucket).map_err(|_| StorageError::InvalidBucket)?;
        let src_key = sanitize_key(src_key).map_err(|_| StorageError::InvalidKey)?;
        let dst_bucket = sanitize_bucket(dst_bucket).map_err(|_| StorageError::InvalidBucket)?;
        let dst_key = sanitize_key(dst_key).map_err(|_| StorageError::InvalidKey)?;
        check_new_object(&dst_bucket, &dst_key)?;

        // Human: Lock source and destination so the source's bytes and metadata can't change mid-copy.
        let _locks = self
            .key_locks
            .lock_many([
                (src_bucket.as_str(), src_key.as_str()),
                (dst_bucket.as_str(), dst_key.as_str()),
            ])
            .await;

        let src_meta = self.fetch_active_metadata(&src_bucket, &src_key).await?;
        let src_path = self.existing_blob_path(&src_bucket, &src_key).await?;

        // Human: Stage a hard link (same inode, no byte copy) and commit it like an upload.
        // Agent: CALLS link_or_copy_blob(src, staging); inc_refs for the new owner; commit_staged_locked swaps it in.
        let staging = PathBuf::from(format!(
            "{}/.tmp/{}.stage",
            self.data_dir,
            uuid::Uuid::new_v4()
        ));
        let _staging_guard = TempFileGuard {
            path: staging.clone(),
        };
        link_or_copy_blob(&src_path, &staging).await?;
        if self.fsync_writes {
            sync_file(&staging).await?;
        }
        let refs = BlockStore::manifest_entries(&staging)?;
        if !refs.is_empty() {
            BlockStore::inc_refs(&self.system_write, &refs).await?;
        }

        self.commit_staged_locked(
            &dst_bucket,
            &dst_key,
            &staging,
            &refs,
            MetaCommit::CopyOf(&src_meta),
            conditions,
        )
        .await
    }

    async fn write_object_stream(
        &self,
        bucket: &str,
        safe_key: &str,
        content_type: Option<&str>,
        custom_meta: Option<&str>,
        body: &mut (impl tokio::io::AsyncRead + Unpin),
        conditions: WriteConditions<'_>,
    ) -> Result<(ObjectMetadata, String), StorageError> {
        let tmp_path = PathBuf::from(format!(
            "{}/.tmp/{}.tmp",
            self.data_dir,
            uuid::Uuid::new_v4()
        ));
        let _tmp_guard = TempFileGuard {
            path: tmp_path.clone(),
        };

        // Human: Stream the body to a temp file (hashing on the fly), then encode and commit it atomically.
        // Agent: CALLS stream_body_to_temp; commit_upload_file (stage -> key lock -> checks -> rename -> metadata).
        let (size, etag) =
            stream_body_to_temp(body, tmp_path.as_path(), self.upload_buffer_size).await?;
        let meta = self
            .commit_upload_file(
                bucket,
                safe_key,
                &tmp_path,
                size,
                &etag,
                content_type,
                custom_meta,
                conditions,
            )
            .await?;
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
        // Human: Read the metadata and open the blob as one step relative to writers of this key; the stream
        // keeps its own handle, so the lock is released before any byte is sent.
        let _reading = self.key_locks.read(&bucket, &safe_key).await;
        let meta = if self.metadata_mode.is_blob_only() {
            self.fetch_blob_only_metadata(&bucket, &safe_key).await?
        } else {
            self.fetch_active_metadata(&bucket, &safe_key).await?
        };

        if self.is_not_modified(&meta, if_none_match, if_modified_since) {
            return Ok(GetObjectOutcome::NotModified(meta));
        }

        let total_size = meta.size as u64;
        let range = match range_header.map(|h| evaluate_range(h, total_size)) {
            None | Some(RangeRequest::Ignore) => None,
            Some(RangeRequest::Satisfiable { start, end }) => Some((start, end)),
            Some(RangeRequest::Unsatisfiable) => {
                return Err(StorageError::RangeNotSatisfiable { size: total_size });
            }
        };

        let path = self
            .existing_blob_path(&meta.bucket, &meta.key)
            .await?;
        let (start, content_length) = match range {
            Some((start, end)) => (start, end - start + 1),
            None => (0, total_size),
        };

        // Human: Stream object bytes from disk, decompressing via spill file or channel when the blob is zstd-wrapped.
        // Agent: CALLS open_object_body_stream(path, logical_size, range_start, content_length, data_dir); no full-blob RAM buffer.
        let mut ctx = self.read_context();
        ctx.expected_etag = meta.etag.clone();
        let stream = open_object_body_stream(
            path.as_path(),
            total_size,
            start,
            content_length,
            &ctx,
        )
        .await?;

        Ok(GetObjectOutcome::Content {
            stream,
            content_length,
            total_size,
            range,
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
        let _reading = self.key_locks.read(&bucket, &safe_key).await;
        let meta = if self.metadata_mode.is_blob_only() {
            self.fetch_blob_only_metadata(&bucket, &safe_key).await?
        } else {
            let meta = self.fetch_active_metadata(&bucket, &safe_key).await?;
            // Human: Answer like GET would — a row whose blob is gone is not a readable object.
            self.existing_blob_path(&meta.bucket, &meta.key).await?;
            meta
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
        super::precondition::is_not_modified(meta, if_none_match, if_modified_since)
    }

    pub async fn delete_object(
        &self,
        bucket: &str,
        key: &str,
        if_match: Option<&str>,
    ) -> Result<(), StorageError> {
        let conditions = WriteConditions {
            if_match,
            ..WriteConditions::default()
        };
        self.delete_object_conditional(bucket, key, conditions).await
    }

    /// Human: Delete with If-Match and a hook evaluated under the key's write lock. Deleting a missing object
    /// succeeds; the hook sees it all the same (a delete replicates whether or not this node had the object).
    /// Agent: KEY LOCK → If-Match check → hook.before → delete_object_locked → hook.after(Deleted { storage_class }).
    pub async fn delete_object_conditional(
        &self,
        bucket: &str,
        key: &str,
        conditions: WriteConditions<'_>,
    ) -> Result<(), StorageError> {
        let bucket = sanitize_bucket(bucket).map_err(|_| StorageError::InvalidBucket)?;
        let safe_key = sanitize_key(key).map_err(|_| StorageError::InvalidKey)?;
        // Human: Same lock as writers, so If-Match and the delete see one version and never race a PUT.
        let _key_guard = self.key_locks.lock(&bucket, &safe_key).await;

        if conditions.if_match.is_some() && !self.metadata_mode.is_blob_only() {
            self.ensure_write_preconditions(&bucket, &safe_key, conditions.if_match, None)
                .await?;
        }
        let storage_class = if self.metadata_mode.is_blob_only() {
            None
        } else {
            self.object_meta
                .try_fetch_active_metadata(&bucket, &safe_key)
                .await?
                .map(|meta| meta.storage_class)
        };
        if let Some(hook) = conditions.hook {
            hook.before(&bucket, &safe_key).await?;
        }
        self.delete_object_locked(&bucket, &safe_key, storage_class.is_some())
            .await?;
        if let Some(hook) = conditions.hook {
            let storage_class = storage_class.as_ref().and_then(|class| class.as_deref());
            hook.after(&bucket, &safe_key, Committed::Deleted { storage_class })
                .await?;
        }
        Ok(())
    }

    /// Agent: CALLER HOLDS the key lock; `has_row` = an active metadata row exists (ignored in blob-only mode).
    async fn delete_object_locked(&self, bucket: &str, key: &str, has_row: bool) -> Result<(), StorageError> {
        if self.metadata_mode.is_blob_only() {
            let variants = blob_path_variants(&self.data_dir, bucket, key);
            if first_existing_blob_path(&variants)
                .await
                .map_err(internal)?
                .is_none()
            {
                return Ok(());
            }
            return self.drop_object_blob(bucket, key).await;
        }
        if !has_row {
            return Ok(());
        }

        // Human: Metadata first, then the bytes: a crash in between leaves an unreferenced file (reclaimed by
        // orphan GC), never a listed object whose bytes are gone.
        if self.soft_delete_ttl_secs <= 0 {
            self.object_meta.hard_delete_object(bucket, key).await?;
            self.drop_object_blob_best_effort(bucket, key).await;
            return Ok(());
        }

        self.object_meta.soft_delete_object(bucket, key).await?;
        if self.soft_delete_drop_blob {
            self.drop_object_blob_best_effort(bucket, key).await;
        }
        Ok(())
    }

    /// Drop the blob of an object whose metadata is already gone; a failure only leaves an orphan behind.
    async fn drop_object_blob_best_effort(&self, bucket: &str, key: &str) {
        if let Err(e) = self.drop_object_blob(bucket, key).await {
            tracing::warn!(%bucket, %key, error = %e, "dropping a deleted object's blob failed; orphan GC reclaims it");
        }
    }

    async fn drop_object_blob(&self, bucket: &str, key: &str) -> Result<(), StorageError> {
        for path in super::existing_blob_paths(&blob_path_variants(&self.data_dir, bucket, key)) {
            BlockStore::release_blob(&self.system_write, &self.data_dir, &path).await?;
            let _ = fs::remove_file(&path).await;
        }
        Ok(())
    }

    /// Deletes explicit object keys using parallel blob drops and a batch metadata transaction.
    pub async fn delete_objects_batch(
        &self,
        bucket: &str,
        keys: &[String],
    ) -> Result<DeletePrefixOutcome, StorageError> {
        self.delete_objects_batch_hooked(bucket, keys, None).await
    }

    /// `delete_objects_batch`, calling `hook.after` for each deleted key under its write lock.
    pub async fn delete_objects_batch_hooked(
        &self,
        bucket: &str,
        keys: &[String],
        hook: Option<&dyn CommitHook>,
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

        let mut outcome = self.delete_objects_internal(&bucket, pending, hook).await?;
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
        self.delete_objects_by_prefix_hooked(bucket, prefix, limit, start_after, None)
            .await
    }

    /// `delete_objects_by_prefix`, calling `hook.after` for each deleted key under its write lock.
    pub async fn delete_objects_by_prefix_hooked(
        &self,
        bucket: &str,
        prefix: &str,
        limit: Option<u64>,
        start_after: Option<&str>,
        hook: Option<&dyn CommitHook>,
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
                .delete_blob_only_prefix(&bucket, &safe_prefix, limit, start_after, hook)
                .await;
        }

        let rows = self
            .object_meta
            .list_active_rows(
                &bucket,
                start_after,
                &safe_prefix,
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

        let mut outcome = self.delete_objects_internal(&bucket, pending, hook).await?;
        outcome.truncated = truncated;
        outcome.next_start_after = next_start_after;
        Ok(outcome)
    }

    async fn delete_objects_internal(
        &self,
        bucket: &str,
        pending: Vec<DeletedObjectRef>,
        hook: Option<&dyn CommitHook>,
    ) -> Result<DeletePrefixOutcome, StorageError> {
        let mut outcome = DeletePrefixOutcome {
            deleted: 0,
            failed: Vec::new(),
            truncated: false,
            next_start_after: None,
            deleted_objects: Vec::new(),
        };
        let mut hook_error = None;
        // Human: A chunk at a time. Each key's write lock is held across its metadata update and blob drop, so a
        // concurrent PUT can't land between them — but only for one chunk's keys: holding every lock of a large
        // delete (thousands of lock stripes) stalled reads and writes of unrelated keys until it finished.
        // Agent: BULK_DELETE_CHUNK keys per lock_many; a failed chunk is reported in `failed`, the rest continue.
        for chunk in pending.chunks(BULK_DELETE_CHUNK) {
            let keys: Vec<String> = chunk.iter().map(|r| r.key.clone()).collect();
            let _key_guards = self
                .key_locks
                .lock_many(keys.iter().map(|k| (bucket, k.as_str())))
                .await;
            let removed: Vec<DeletedObjectRef> = if self.metadata_mode.is_blob_only() {
                let (dropped, failed) = self.drop_blobs_concurrently(bucket, keys).await?;
                outcome.failed.extend(failed);
                outcome.deleted += dropped.len() as u64;
                chunk.iter().filter(|r| dropped.contains(&r.key)).cloned().collect()
            } else {
                // Human: Metadata first, then the bytes: if the metadata update fails nothing is deleted, and a
                // blob that can't be dropped afterwards is only an orphan for GC — never a listed object without bytes.
                let metadata_result = if self.soft_delete_ttl_secs <= 0 {
                    self.object_meta.hard_delete_objects(bucket, &keys).await
                } else {
                    self.object_meta.soft_delete_objects(bucket, &keys).await
                };
                match metadata_result {
                    Ok(n) => outcome.deleted += n,
                    Err(e) => {
                        let error = e.to_string();
                        outcome.failed.extend(keys.into_iter().map(|key| DeletePrefixFailure {
                            key,
                            error: error.clone(),
                        }));
                        continue;
                    }
                }
                if self.soft_delete_ttl_secs <= 0 || self.soft_delete_drop_blob {
                    let (_, blob_failures) = self.drop_blobs_concurrently(bucket, keys).await?;
                    for failure in blob_failures {
                        tracing::warn!(
                            %bucket, key = %failure.key, error = %failure.error,
                            "dropping a deleted object's blob failed; orphan GC reclaims it"
                        );
                    }
                }
                chunk.to_vec()
            };
            if let Err(e) = report_bulk_deletes(hook, bucket, &removed).await {
                hook_error.get_or_insert(e);
            }
            outcome.deleted_objects.extend(removed);
        }
        match hook_error {
            Some(e) => Err(e),
            None => Ok(outcome),
        }
    }

    /// Drop the blobs of `keys` with bounded concurrency; RETURNS the keys dropped and the failures.
    async fn drop_blobs_concurrently(
        &self,
        bucket: &str,
        keys: Vec<String>,
    ) -> Result<(Vec<String>, Vec<DeletePrefixFailure>), StorageError> {
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
        let mut dropped = Vec::new();
        let mut failed = Vec::new();
        while let Some(joined) = join_set.join_next().await {
            match joined {
                Ok((key, Ok(()))) => dropped.push(key),
                Ok((key, Err(e))) => failed.push(DeletePrefixFailure {
                    key,
                    error: e.to_string(),
                }),
                Err(e) => tracing::error!(%bucket, error = %e, "bulk delete task failed"),
            }
        }
        Ok((dropped, failed))
    }

    async fn delete_blob_only_prefix(
        &self,
        bucket: &str,
        prefix: &str,
        limit: usize,
        start_after: &str,
        hook: Option<&dyn CommitHook>,
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
        let mut outcome = self.delete_objects_internal(bucket, pending, hook).await?;
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
        let count = self
            .object_meta
            .count_active_with_prefix(&bucket, prefix)
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
        let path = self.existing_blob_path(bucket, key).await?;
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
                let Some(key) = object_key_from_blob_relpath(&rel) else {
                    continue;
                };
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
        let scan_limit = if delimiter.is_some() {
            self.list_scan_cap
        } else {
            (limit as i64).saturating_add(1)
        };

        let rows = self
            .object_meta
            .list_active_rows(&bucket, start_after, prefix, scan_limit)
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
            // Human: The store returns only keys under `prefix`; skip anything else rather than slice blindly.
            let Some(remainder) = key.strip_prefix(prefix) else {
                continue;
            };
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
                    .count_keys_after(&bucket, last, prefix)
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

/// Human: Tell `hook` about each key a bulk delete removed (their write locks are still held). A failure for one
/// key doesn't stop the others: their rows are already gone, so no retry of the delete would reach them again.
/// Agent: CALLS hook.after for every key; LOGS each failure; RETURNS the first error.
async fn report_bulk_deletes(
    hook: Option<&dyn CommitHook>,
    bucket: &str,
    deleted: &[DeletedObjectRef],
) -> Result<(), StorageError> {
    let Some(hook) = hook else {
        return Ok(());
    };
    let mut first_error = None;
    for object in deleted {
        let storage_class = object.storage_class.as_deref();
        if let Err(e) = hook
            .after(bucket, &object.key, Committed::Deleted { storage_class })
            .await
        {
            tracing::error!(%bucket, key = %object.key, error = %e, "recording a deleted object failed");
            first_error.get_or_insert(e);
        }
    }
    first_error.map_or(Ok(()), Err)
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use futures_util::future::BoxFuture;

    use super::*;
    use crate::storage::key_locks::KeyLocks;

    /// Takes its time over every deleted key, like a replication hook waiting on a busy database.
    struct SlowRecorder;

    impl CommitHook for SlowRecorder {
        fn before<'a>(&'a self, _: &'a str, _: &'a str) -> BoxFuture<'a, Result<(), StorageError>> {
            Box::pin(async { Ok(()) })
        }

        fn after<'a>(&'a self, _: &'a str, _: &'a str, _: Committed<'a>) -> BoxFuture<'a, Result<(), StorageError>> {
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(5)).await;
                Ok(())
            })
        }
    }

    /// Records every key it is told about and fails for one of them.
    struct FlakyRecorder {
        seen: std::sync::Mutex<Vec<String>>,
        fail_on: &'static str,
    }

    impl CommitHook for FlakyRecorder {
        fn before<'a>(&'a self, _: &'a str, _: &'a str) -> BoxFuture<'a, Result<(), StorageError>> {
            Box::pin(async { Ok(()) })
        }

        fn after<'a>(&'a self, _: &'a str, key: &'a str, _: Committed<'a>) -> BoxFuture<'a, Result<(), StorageError>> {
            Box::pin(async move {
                self.seen.lock().unwrap().push(key.to_string());
                if key == self.fail_on {
                    return Err(StorageError::Internal(anyhow::anyhow!("database is locked")));
                }
                Ok(())
            })
        }
    }

    async fn test_engine() -> (StorageEngine, tempfile::TempDir) {
        let tmp = tempfile::TempDir::new().unwrap();
        let data_dir = tmp.path().join("blobs");
        std::fs::create_dir_all(&data_dir).unwrap();
        let meta = format!("file:{}?mode=memory&cache=shared", uuid::Uuid::new_v4());
        let opts = EngineOptions {
            fsync_writes: false,
            ..EngineOptions::default()
        };
        let engine = StorageEngine::with_full_options(&meta, &data_dir.to_string_lossy(), opts)
            .await
            .unwrap();
        (engine, tmp)
    }

    #[tokio::test]
    async fn a_failed_record_doesnt_stop_a_bulk_delete_recording_the_rest() {
        let (engine, _tmp) = test_engine().await;
        let keys: Vec<String> = (0..10).map(|i| format!("k-{i}")).collect();
        for key in &keys {
            engine
                .put_object("bulk", key, None, None, std::io::Cursor::new(b"x".to_vec()))
                .await
                .unwrap();
        }
        let recorder = FlakyRecorder {
            seen: std::sync::Mutex::new(Vec::new()),
            fail_on: "k-3",
        };
        let result = engine.delete_objects_batch_hooked("bulk", &keys, Some(&recorder)).await;
        assert!(result.is_err(), "the failure is reported");
        // Human: The rows are gone either way; keys after the failure used to go unrecorded (never replicated).
        assert_eq!(*recorder.seen.lock().unwrap(), keys);
        assert_eq!(engine.object_count().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn bulk_deletes_lock_one_chunk_of_keys_at_a_time() {
        let (engine, _tmp) = test_engine().await;
        let keys: Vec<String> = (0..640).map(|i| format!("k-{i}")).collect();
        for key in &keys {
            engine
                .put_object("bulk", key, None, None, std::io::Cursor::new(b"x".to_vec()))
                .await
                .unwrap();
        }
        // Human: An unrelated object sharing a lock stripe with the delete's last key.
        let stripe = KeyLocks::stripe("bulk", keys.last().unwrap());
        let neighbour = (0..)
            .map(|i| format!("neighbour-{i}"))
            .find(|k| KeyLocks::stripe("bulk", k) == stripe)
            .unwrap();
        engine
            .put_object("bulk", &neighbour, None, None, std::io::Cursor::new(b"n".to_vec()))
            .await
            .unwrap();

        let deleting = {
            let engine = engine.clone();
            let keys = keys.clone();
            tokio::spawn(async move { engine.delete_objects_batch_hooked("bulk", &keys, Some(&SlowRecorder)).await })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        let started = Instant::now();
        engine.head_object("bulk", &neighbour, None, None).await.unwrap();
        // Human: Holding all 640 keys' locks kept this read waiting for the whole delete (~3 s of hook time).
        assert!(started.elapsed() < Duration::from_secs(1), "read waited {:?}", started.elapsed());
        let outcome = deleting.await.unwrap().unwrap();
        assert_eq!(outcome.deleted, 640);
        assert_eq!(outcome.deleted_objects.len(), 640);
    }
}
