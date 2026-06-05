use std::path::{Path, PathBuf};
use std::sync::Arc;

use sqlx::Pool;
use sqlx::Sqlite;

use super::blocks::BlockStore;
use super::compression::{
    compress_file_to_storage, detect_blob_format, is_dedup_manifest, is_compressed_blob,
    read_stored_dict_id, read_stored_zstd_level, BlobFormat, RuntimeCompressParams, BLOB_MAGIC,
    BLOB_MAGIC_V2, DEDUP_MAGIC, HEADER_LEN, HEADER_LEN_V2,
};
use super::error::{internal, map_io_error, StorageError};

pub struct BlobFinalizeOptions {
    pub level: i32,
    pub dict_id: u16,
    pub dict: Option<Arc<Vec<u8>>>,
    pub dedup_enabled: bool,
    pub dedup_block_size: usize,
    pub dedup_min_size: u64,
    pub data_dir: String,
    pub system_pool: Pool<Sqlite>,
    pub existing_blob: Option<PathBuf>,
}

impl BlobFinalizeOptions {
    pub fn compress_params(&self) -> RuntimeCompressParams<'_> {
        RuntimeCompressParams::with_dict(
            self.level,
            self.dict_id,
            self.dict.as_deref().map(|v| v.as_slice()),
        )
    }
}

/// Human: After temp upload, compress-or-store (or dedup) to final blob path without loading the whole object into RAM.
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

    if use_dedup {
        let block_size = opts.dedup_block_size;
        let entries = tokio::task::spawn_blocking(move || {
            let store = BlockStore::new(&data_dir);
            store.write_dedup_from_file(&tmp, &fin, logical_size, block_size)
        })
        .await
        .map_err(internal)??;
        BlockStore::inc_refs(&pool, &entries).await?;
        return Ok(());
    }

    let level = opts.level;
    let dict_id = opts.dict_id;
    let dict = opts.dict.clone();
    let params = RuntimeCompressParams::with_dict(
        level,
        dict_id,
        dict.as_deref().map(|v| v.as_slice()),
    );
    let params = (
        params.level,
        params.dict_id,
        dict,
    );
    tokio::task::spawn_blocking(move || {
        let compress = RuntimeCompressParams::with_dict(
            params.0,
            params.1,
            params.2.as_deref().map(|v| v.as_slice()),
        );
        compress_file_to_storage(&tmp, &fin, logical_size, &compress)
    })
        .await
        .map_err(internal)??;
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
    if detect_blob_format(header) == BlobFormat::Nos2 {
        read_stored_dict_id(header)
    } else {
        None
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
    header.starts_with(BLOB_MAGIC) || header.starts_with(BLOB_MAGIC_V2) || header.starts_with(DEDUP_MAGIC)
}

pub fn stored_level_from_header(header: &[u8]) -> Option<u8> {
    read_stored_zstd_level(header)
}
