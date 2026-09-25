use std::path::{Path, PathBuf};
use std::sync::Arc;

use sqlx::Pool;
use sqlx::Sqlite;

use super::blob_ops::{sync_dir, sync_file};
use super::block_cache::BlockDecodeCache;
use super::blocks::BlockStore;
use super::compressibility::CompressionContext;
use super::compression::{
    encode_file_for_storage, detect_blob_format, FileEncoding, is_dedup_manifest, is_compressed_blob,
    read_indexed_dict_id, read_stored_dict_id, read_stored_zstd_level, BlobFormat,
    EncodeOptions, BLOB_MAGIC, BLOB_MAGIC_V2, DEDUP_MAGIC, HEADER_LEN, HEADER_LEN_V2,
    NOSI_MAGIC, NOSB_MAGIC,
};
use super::error::{internal, map_io_error, StorageError};

pub struct BlobFinalizeOptions {
    pub level: i32,
    pub dict_id: u16,
    pub dict: Option<Arc<Vec<u8>>>,
    pub dedup_enabled: bool,
    pub dedup_block_size: usize,
    pub dedup_min_size: u64,
    pub compress_min_size: usize,
    pub compress_block_size: usize,
    pub extra_excluded_extensions: Arc<Vec<String>>,
    pub object_key: Option<String>,
    pub content_type: Option<String>,
    pub data_dir: String,
    pub system_pool: Pool<Sqlite>,
    pub existing_blob: Option<PathBuf>,
}

/// Where an upload's bytes live once staged for commit.
#[derive(Debug)]
pub enum StagedBlob {
    /// Raw payload: the upload temp file itself becomes the blob.
    Raw,
    /// Indexed blob written to the staging path; its dedup refs are already counted.
    Encoded { refs: Vec<(u64, u32)> },
}

/// Human: Encode an uploaded temp file into `staging` without touching the object's blob path.
/// Agent: spawn_blocking(encode_file_for_storage); inc_refs for Encoded; caller commits or releases the refs.
pub async fn stage_temp_blob(
    tmp_path: &Path,
    staging: &Path,
    logical_size: u64,
    opts: &BlobFinalizeOptions,
) -> Result<StagedBlob, StorageError> {
    let use_dedup = opts.dedup_enabled && logical_size >= opts.dedup_min_size;
    let tmp = tmp_path.to_path_buf();
    let out = staging.to_path_buf();
    let data_dir = opts.data_dir.clone();
    let pool = opts.system_pool.clone();
    let level = opts.level;
    let dict_id = opts.dict_id;
    let dict = opts.dict.clone();
    let block_size = if use_dedup {
        opts.dedup_block_size
    } else {
        opts.compress_block_size
    };
    let min_size = if use_dedup {
        opts.dedup_min_size as usize
    } else {
        opts.compress_min_size
    };
    let object_key = opts.object_key.clone();
    let content_type = opts.content_type.clone();
    let extra_ext = opts.extra_excluded_extensions.clone();

    let encoding = tokio::task::spawn_blocking(move || {
        let ctx = CompressionContext::new(
            object_key.as_deref(),
            content_type.as_deref(),
            logical_size,
            min_size,
            &extra_ext,
        );
        let store = BlockStore::new(&data_dir);
        let encode_opts = EncodeOptions {
            dict_id,
            dict: dict.as_deref().map(|v| v.as_slice()),
            dedup_store: if use_dedup { Some(&store) } else { None },
        };
        encode_file_for_storage(&tmp, &out, logical_size, level, block_size, ctx, encode_opts)
    })
    .await
    .map_err(internal)??;

    match encoding {
        FileEncoding::Raw => Ok(StagedBlob::Raw),
        FileEncoding::Indexed(refs) => {
            if !refs.is_empty() {
                BlockStore::inc_refs(&pool, &refs).await?;
            }
            Ok(StagedBlob::Encoded { refs })
        }
    }
}

/// Human: Encode a temp upload and move it to `final_path` — staged, fsynced, then renamed over the old blob.
/// Agent: Library helper without locking or metadata; StorageEngine commits uploads via commit_staged_locked.
#[deprecated(note = "StorageEngine::put_object commits uploads atomically with metadata")]
pub async fn finalize_temp_to_blob(
    tmp_path: &Path,
    final_path: &Path,
    logical_size: u64,
    opts: BlobFinalizeOptions,
) -> Result<(), StorageError> {
    let staging = final_path.with_extension(format!("stage-{}", uuid::Uuid::new_v4()));
    let staged = stage_temp_blob(tmp_path, &staging, logical_size, &opts).await?;
    let source = match &staged {
        StagedBlob::Raw => tmp_path,
        StagedBlob::Encoded { .. } => staging.as_path(),
    };
    let old_refs = match &opts.existing_blob {
        Some(existing) if existing.exists() => BlockStore::manifest_entries(existing)?,
        _ => Vec::new(),
    };
    if let Some(parent) = final_path.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(internal)?;
    }
    sync_file(source).await?;
    if let Err(e) = tokio::fs::rename(source, final_path).await {
        let _ = tokio::fs::remove_file(&staging).await;
        return Err(map_io_error(e));
    }
    if let Some(parent) = final_path.parent() {
        sync_dir(parent).await?;
    }
    if !old_refs.is_empty() {
        BlockStore::dec_refs(&opts.system_pool, &opts.data_dir, &old_refs).await?;
    }
    Ok(())
}

pub struct ReadContext {
    pub data_dir: String,
    pub dict: Option<Arc<Vec<u8>>>,
    pub block_cache: Option<BlockDecodeCache>,
    pub read_buffer_size: usize,
    pub verify_on_read: bool,
    pub buffer_pool: super::buffer_pool::BufferPool,
    pub expected_etag: Option<String>,
}

impl ReadContext {
    /// Human: A read context built from the data directory alone (dictionary and dedup blocks live there),
    /// for code without an engine handle — the replication worker — that must ship logical object bytes.
    pub fn for_data_dir(data_dir: &str) -> Self {
        // Human: Frames find their dictionary by the ID in their header once the directory is registered.
        super::dict_store::register_data_dir(data_dir);
        Self {
            data_dir: data_dir.to_string(),
            dict: None,
            block_cache: None,
            read_buffer_size: 256 * 1024,
            verify_on_read: false,
            buffer_pool: super::buffer_pool::BufferPool::new(256 * 1024, 4),
            expected_etag: None,
        }
    }

    pub fn dict_bytes(&self) -> Option<&[u8]> {
        self.dict.as_deref().map(|v| v.as_slice())
    }
}

pub fn blob_needs_dict_for_read(header: &[u8]) -> Option<u16> {
    match detect_blob_format(header) {
        BlobFormat::Nos2 => read_stored_dict_id(header),
        BlobFormat::Nosi => read_indexed_dict_id(header),
        _ => None,
    }
}

pub fn is_zstd_or_dedup_blob(header: &[u8]) -> bool {
    is_compressed_blob(header) || is_dedup_manifest(header)
}

pub fn zstd_header_len(header: &[u8]) -> usize {
    match detect_blob_format(header) {
        BlobFormat::Nosz => HEADER_LEN,
        BlobFormat::Nos2 => HEADER_LEN_V2,
        _ => 0,
    }
}

pub fn blob_format_from_header(header: &[u8]) -> BlobFormat {
    detect_blob_format(header)
}

pub fn magic_matches_compressed(header: &[u8]) -> bool {
    header.starts_with(NOSI_MAGIC)
        || header.starts_with(NOSB_MAGIC)
        || header.starts_with(BLOB_MAGIC)
        || header.starts_with(BLOB_MAGIC_V2)
        || header.starts_with(DEDUP_MAGIC)
}

pub fn stored_level_from_header(header: &[u8]) -> Option<u8> {
    read_stored_zstd_level(header)
}
