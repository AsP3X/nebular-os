use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

use xxhash_rust::xxh3::xxh3_64;
use zstd::zstd_safe::CParameter;

use super::super::blocks::BlockStore;
use super::super::compressibility::{
    prefix_looks_incompressible, should_attempt_compression, CompressionContext,
};
use super::super::error::{internal, StorageError};
use super::format::{
    write_blob_header_v1, write_block_header_v1, IndexEntry, BLOCK_COMPRESSED, BLOCK_DEDUP_REF,
    BLOCK_STORED, DEFAULT_BLOCK_SIZE, NOSI_FLAG_DEDUP,
};

/// Human: Default zstd level when env does not override (22 = smallest on disk, highest CPU).
pub const DEFAULT_ZSTD_LEVEL: i32 = 22;

const LDM_SIZE_THRESHOLD: u64 = 128 * 1024;

/// Human: Optional zstd dictionary and dedup store for NOSI indexed writes.
#[derive(Clone, Copy)]
pub struct EncodeOptions<'a> {
    pub dict_id: u16,
    pub dict: Option<&'a [u8]>,
    pub dedup_store: Option<&'a BlockStore>,
}

impl<'a> Default for EncodeOptions<'a> {
    fn default() -> Self {
        Self {
            dict_id: 0,
            dict: None,
            dedup_store: None,
        }
    }
}

pub fn clamp_zstd_level(level: i32) -> i32 {
    level.clamp(1, 22)
}

fn adaptive_window_log(logical_size: u64) -> u32 {
    let bits = 64 - logical_size.max(1).leading_zeros();
    bits.clamp(10, 27)
}

fn tune_block_compressor(
    compressor: &mut zstd::bulk::Compressor<'_>,
    chunk_len: u64,
    level: i32,
) -> Result<(), StorageError> {
    let level = clamp_zstd_level(level);
    if chunk_len >= 4096 {
        compressor
            .set_parameter(CParameter::WindowLog(adaptive_window_log(chunk_len)))
            .map_err(internal)?;
    }
    if chunk_len >= LDM_SIZE_THRESHOLD && level >= 10 {
        compressor
            .set_parameter(CParameter::EnableLongDistanceMatching(true))
            .map_err(internal)?;
    }
    Ok(())
}

fn compress_block(
    chunk: &[u8],
    level: i32,
    dict: Option<&[u8]>,
) -> Result<(u8, Vec<u8>), StorageError> {
    let level = clamp_zstd_level(level);
    let compressed = if let Some(d) = dict.filter(|d| !d.is_empty()) {
        let mut compressor = zstd::bulk::Compressor::with_dictionary(level, d).map_err(internal)?;
        tune_block_compressor(&mut compressor, chunk.len() as u64, level)?;
        compressor.compress(chunk).map_err(internal)?
    } else {
        let mut compressor = zstd::bulk::Compressor::new(level).map_err(internal)?;
        tune_block_compressor(&mut compressor, chunk.len() as u64, level)?;
        compressor.compress(chunk).map_err(internal)?
    };
    if compressed.len() < chunk.len() {
        Ok((BLOCK_COMPRESSED, compressed))
    } else {
        Ok((BLOCK_STORED, chunk.to_vec()))
    }
}

fn store_dedup_chunk(
    store: &BlockStore,
    chunk: &[u8],
    zstd_level: i32,
) -> Result<(u8, Vec<u8>, u64), StorageError> {
    let hash = store.write_logical_block(chunk, zstd_level)?;
    let mut payload = Vec::with_capacity(12);
    payload.extend_from_slice(&hash.to_le_bytes());
    payload.extend_from_slice(&(chunk.len() as u32).to_le_bytes());
    Ok((BLOCK_DEDUP_REF, payload, hash))
}

fn encode_logical_block(
    chunk: &[u8],
    level: i32,
    dict: Option<&[u8]>,
    dedup_store: Option<&BlockStore>,
) -> Result<(u8, Vec<u8>, u64), StorageError> {
    let checksum = xxh3_64(chunk);
    if let Some(store) = dedup_store {
        return store_dedup_chunk(store, chunk, level);
    }
    let (block_type, payload) = compress_block(chunk, level, dict)?;
    Ok((block_type, payload, checksum))
}

fn append_block_v1(
    staging: &mut Vec<u8>,
    block_type: u8,
    payload: &[u8],
    logical_checksum: u64,
) -> Result<(), StorageError> {
    write_block_header_v1(staging, block_type, payload.len() as u32, logical_checksum)?;
    staging.extend_from_slice(payload);
    Ok(())
}

fn encode_blocks_from_reader<R: Read>(
    mut source: R,
    logical_size: u64,
    block_size: usize,
    level: i32,
    opts: EncodeOptions<'_>,
) -> Result<(Vec<IndexEntry>, Vec<u8>, u16), StorageError> {
    let mut index = Vec::new();
    let mut staging = Vec::new();
    let mut logical_end = 0u64;
    let mut chunk_buf = vec![0u8; block_size.max(1)];
    let flags = if opts.dedup_store.is_some() {
        NOSI_FLAG_DEDUP
    } else {
        0
    };

    loop {
        let mut filled = 0usize;
        while filled < block_size {
            let n = source
                .read(&mut chunk_buf[filled..])
                .map_err(|e| internal(anyhow::anyhow!(e)))?;
            if n == 0 {
                break;
            }
            filled += n;
        }
        if filled == 0 {
            break;
        }

        let chunk = &chunk_buf[..filled];
        let (block_type, payload, checksum) =
            encode_logical_block(chunk, level, opts.dict, opts.dedup_store)?;
        let compressed_offset = staging.len() as u64;
        append_block_v1(&mut staging, block_type, &payload, checksum)?;
        logical_end += filled as u64;
        index.push(IndexEntry {
            compressed_offset,
            logical_end,
        });

        if logical_end >= logical_size {
            break;
        }
    }

    if logical_end != logical_size {
        return Err(internal(anyhow::anyhow!(
            "block encoder size mismatch: encoded={logical_end} expected={logical_size}"
        )));
    }
    Ok((index, staging, flags))
}

pub fn encode_blob_for_storage(
    uncompressed: &[u8],
    level: i32,
    block_size: usize,
    ctx: CompressionContext<'_>,
    opts: EncodeOptions<'_>,
) -> Result<Vec<u8>, StorageError> {
    if !should_attempt_compression(ctx) {
        return Ok(uncompressed.to_vec());
    }
    if opts.dedup_store.is_none()
        && prefix_looks_incompressible(&uncompressed[..uncompressed.len().min(16)])
    {
        return Ok(uncompressed.to_vec());
    }

    let logical_size = uncompressed.len() as u64;
    let (index, staging, flags) =
        encode_blocks_from_reader(uncompressed, logical_size, block_size, level, opts)?;
    let level_byte = clamp_zstd_level(level) as u8;
    let header_len =
        super::format::FIXED_HEADER_LEN_V1_LEVEL + index.len() * super::format::INDEX_ENTRY_LEN;
    let total_len = header_len + staging.len();
    if opts.dedup_store.is_none() && total_len >= uncompressed.len() {
        return Ok(uncompressed.to_vec());
    }

    let mut out = Vec::with_capacity(total_len);
    write_blob_header_v1(
        &mut out,
        logical_size,
        block_size as u32,
        &index,
        opts.dict_id,
        flags,
        level_byte,
    )?;
    out.extend_from_slice(&staging);
    Ok(out)
}

pub fn compress_file_to_storage(
    tmp_path: &Path,
    final_path: &Path,
    logical_size: u64,
    level: i32,
    block_size: usize,
    ctx: CompressionContext<'_>,
    opts: EncodeOptions<'_>,
) -> Result<Vec<(u64, u32)>, StorageError> {
    if !should_attempt_compression(ctx) {
        std::fs::copy(tmp_path, final_path).map_err(|e| internal(anyhow::anyhow!(e)))?;
        return Ok(vec![]);
    }

    if opts.dedup_store.is_none() {
        let mut prefix = [0u8; 16];
        {
            let mut raw = File::open(tmp_path).map_err(|e| internal(anyhow::anyhow!(e)))?;
            let read = raw.read(&mut prefix).map_err(|e| internal(anyhow::anyhow!(e)))?;
            if prefix_looks_incompressible(&prefix[..read]) {
                std::fs::copy(tmp_path, final_path).map_err(|e| internal(anyhow::anyhow!(e)))?;
                return Ok(vec![]);
            }
        }
    }

    let raw_len = std::fs::metadata(tmp_path)
        .map_err(|e| internal(anyhow::anyhow!(e)))?
        .len();
    let part_path = final_path.with_extension("blkpart");
    let block_size = block_size.max(4096);

    let source = File::open(tmp_path).map_err(|e| internal(anyhow::anyhow!(e)))?;
    let (index, staging, flags) =
        encode_blocks_from_reader(source, logical_size, block_size, level, opts)?;
    let level_byte = clamp_zstd_level(level) as u8;
    let header_len =
        super::format::FIXED_HEADER_LEN_V1_LEVEL + index.len() * super::format::INDEX_ENTRY_LEN;
    let total_len = header_len + staging.len();

    if opts.dedup_store.is_none() && total_len as u64 >= raw_len {
        std::fs::copy(tmp_path, final_path).map_err(|e| internal(anyhow::anyhow!(e)))?;
        return Ok(vec![]);
    }

    {
        let mut out = File::create(&part_path).map_err(|e| internal(anyhow::anyhow!(e)))?;
        write_blob_header_v1(
            &mut out,
            logical_size,
            block_size as u32,
            &index,
            opts.dict_id,
            flags,
            level_byte,
        )?;
        out.write_all(&staging)
            .map_err(|e| internal(anyhow::anyhow!(e)))?;
    }

    std::fs::rename(&part_path, final_path).map_err(|e| internal(anyhow::anyhow!(e)))?;

    let blob = std::fs::read(final_path).map_err(|e| internal(anyhow::anyhow!(e)))?;
    super::format::collect_dedup_refs(&blob)
}

pub fn compress_blob(
    uncompressed: &[u8],
    level: i32,
    block_size: usize,
    ctx: CompressionContext<'_>,
    opts: EncodeOptions<'_>,
) -> Result<Vec<u8>, StorageError> {
    encode_blob_for_storage(uncompressed, level, block_size, ctx, opts)
}

pub fn default_block_size() -> usize {
    DEFAULT_BLOCK_SIZE
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::compressibility::DEFAULT_MIN_COMPRESSIBLE_SIZE;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn text_ctx(size: u64) -> CompressionContext<'static> {
        CompressionContext::new(
            Some("data/log.txt"),
            Some("text/plain"),
            size,
            DEFAULT_MIN_COMPRESSIBLE_SIZE,
            &[],
        )
    }

    #[test]
    fn file_encoder_roundtrip_via_decode_path() {
        use super::super::decode::decompress_blob;
        use super::super::format::{is_indexed_blob, BlobFormat, detect_blob_format};

        let mut tmp = NamedTempFile::new().unwrap();
        let payload = b"block compress me ".repeat(800);
        tmp.write_all(&payload).unwrap();
        let final_path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
        compress_file_to_storage(
            tmp.path(),
            &final_path,
            payload.len() as u64,
            DEFAULT_ZSTD_LEVEL,
            64 * 1024,
            text_ctx(payload.len() as u64),
            EncodeOptions::default(),
        )
        .unwrap();
        let on_disk = std::fs::read(&final_path).unwrap();
        assert!(is_indexed_blob(&on_disk));
        assert_eq!(detect_blob_format(&on_disk), BlobFormat::Nosi);
        let restored = decompress_blob(&on_disk, payload.len() as u64, None, None).unwrap();
        assert_eq!(restored, payload);
    }
}
