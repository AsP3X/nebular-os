use std::path::{Path, PathBuf};
use std::sync::Arc;

use sqlx::Pool;
use sqlx::Sqlite;

use super::blocks::BlockStore;
use super::compressibility::CompressionContext;
use super::compression::{
    compress_file_to_storage, detect_blob_format, is_dedup_manifest, is_compressed_blob,
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

/// Human: After temp upload, NOSI block-compress-or-store (with optional dedup refs) to final blob path.
pub async fn finalize_temp_to_blob(
    tmp_path: &Path,
    final_path: &Path,
    logical_size: u64,
    opts: BlobFinalizeOptions,
) -> Result<(), StorageError> {
    if let Some(existing) = &opts.existing_blob
        && existing.exists()
    {
        BlockStore::release_blob(&opts.system_pool, &opts.data_dir, existing).await?;
    }

    if let Some(parent) = final_path.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(internal)?;
    }
    if final_path.exists() {
        tokio::fs::remove_file(final_path)
            .await
            .map_err(map_io_error)?;
    }

    let use_dedup = opts.dedup_enabled && logical_size >= opts.dedup_min_size;
    let tmp = tmp_path.to_path_buf();
    let fin = final_path.to_path_buf();
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

    let entries = tokio::task::spawn_blocking(move || {
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
        compress_file_to_storage(&tmp, &fin, logical_size, level, block_size, ctx, encode_opts)
    })
    .await
    .map_err(internal)??;

    if !entries.is_empty() {
        BlockStore::inc_refs(&pool, &entries).await?;
    }
    Ok(())
}

pub struct ReadContext {
    pub data_dir: String,
    pub dict: Option<Arc<Vec<u8>>>,
}

impl ReadContext {
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
