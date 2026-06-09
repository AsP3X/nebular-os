use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

use zstd::zstd_safe::CParameter;

use super::super::compressibility::{
    prefix_looks_incompressible, should_attempt_compression, CompressionContext,
};
use super::super::error::{internal, StorageError};
use super::format::{
    write_blob_header, IndexEntry, BLOCK_COMPRESSED, BLOCK_STORED, DEFAULT_BLOCK_SIZE,
};

/// Human: Default zstd level when env does not override (22 = smallest on disk, highest CPU).
/// Agent: DEFAULT_ZSTD_LEVEL=22; overridden by NOS_ZSTD_LEVEL in config/engine.
pub const DEFAULT_ZSTD_LEVEL: i32 = 22;

const LDM_SIZE_THRESHOLD: u64 = 128 * 1024;

/// Human: Clamp user-provided zstd level into the range the zstd crate supports.
/// Agent: CLAMP 1..=22; used for NOS_ZSTD_LEVEL parsing.
pub fn clamp_zstd_level(level: i32) -> i32 {
    level.clamp(1, 22)
}

fn adaptive_window_log(logical_size: u64) -> u32 {
    let bits = 64 - logical_size.max(1).leading_zeros();
    bits.clamp(10, 27)
}

// Human: Apply per-block zstd tuning based on chunk size and configured level.
// Agent: SETS window_log and optional LDM on bulk compressor before compress().
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

// Human: Try zstd on one block and keep the smaller of compressed vs raw bytes.
// Agent: RETURNS (BLOCK_COMPRESSED|BLOCK_STORED, payload bytes).
fn compress_block(
    chunk: &[u8],
    level: i32,
) -> Result<(u8, Vec<u8>), StorageError> {
    let mut compressor =
        zstd::bulk::Compressor::new(clamp_zstd_level(level)).map_err(internal)?;
    tune_block_compressor(&mut compressor, chunk.len() as u64, level)?;
    let compressed = compressor.compress(chunk).map_err(internal)?;
    if compressed.len() < chunk.len() {
        Ok((BLOCK_COMPRESSED, compressed))
    } else {
        Ok((BLOCK_STORED, chunk.to_vec()))
    }
}

// Human: Serialize one block header plus payload into the staging buffer.
// Agent: WRITES 8-byte block header then payload; updates staging Vec.
fn append_block(staging: &mut Vec<u8>, block_type: u8, payload: &[u8]) -> Result<(), StorageError> {
    super::format::write_block_header(staging, block_type, payload.len() as u32)?;
    staging.extend_from_slice(payload);
    Ok(())
}

// Human: Split bytes into block_size chunks and build index + block staging area.
// Agent: READS source; PER-BLOCK compress_block; BUILDS IndexEntry list and payload blob.
fn encode_blocks_from_reader<R: Read>(
    mut source: R,
    logical_size: u64,
    block_size: usize,
    level: i32,
) -> Result<(Vec<IndexEntry>, Vec<u8>), StorageError> {
    let mut index = Vec::new();
    let mut staging = Vec::new();
    let mut logical_end = 0u64;
    let mut chunk_buf = vec![0u8; block_size.max(1)];

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
        let (block_type, payload) = compress_block(chunk, level)?;
        let compressed_offset = staging.len() as u64;
        append_block(&mut staging, block_type, &payload)?;
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
    Ok((index, staging))
}

// Human: Build a complete NOSB blob in memory for maintenance recompression.
// Agent: CALLS encode_blocks_from_reader; WRITES header+index+blocks; MAY return raw bytes.
pub fn encode_blob_for_storage(
    uncompressed: &[u8],
    level: i32,
    block_size: usize,
    ctx: CompressionContext<'_>,
) -> Result<Vec<u8>, StorageError> {
    if !should_attempt_compression(ctx) {
        return Ok(uncompressed.to_vec());
    }
    if prefix_looks_incompressible(&uncompressed[..uncompressed.len().min(16)]) {
        return Ok(uncompressed.to_vec());
    }

    let logical_size = uncompressed.len() as u64;
    let (index, staging) =
        encode_blocks_from_reader(uncompressed, logical_size, block_size, level)?;
    let header_len = super::format::FIXED_HEADER_LEN + index.len() * super::format::INDEX_ENTRY_LEN;
    let total_len = header_len + staging.len();
    if total_len >= uncompressed.len() {
        return Ok(uncompressed.to_vec());
    }

    let mut out = Vec::with_capacity(total_len);
    write_blob_header(
        &mut out,
        logical_size,
        block_size as u32,
        &index,
    )?;
    out.extend_from_slice(&staging);
    Ok(out)
}

// Human: Write a block-compressed blob from a temp file without buffering the whole object.
// Agent: STREAM encode_blocks_from_reader; IF smaller than raw THEN write NOSB ELSE copy raw.
pub fn compress_file_to_storage(
    tmp_path: &Path,
    final_path: &Path,
    logical_size: u64,
    level: i32,
    block_size: usize,
    ctx: CompressionContext<'_>,
) -> Result<(), StorageError> {
    if !should_attempt_compression(ctx) {
        std::fs::copy(tmp_path, final_path).map_err(|e| internal(anyhow::anyhow!(e)))?;
        return Ok(());
    }

    let mut prefix = [0u8; 16];
    {
        let mut raw = File::open(tmp_path).map_err(|e| internal(anyhow::anyhow!(e)))?;
        let read = raw.read(&mut prefix).map_err(|e| internal(anyhow::anyhow!(e)))?;
        if prefix_looks_incompressible(&prefix[..read]) {
            std::fs::copy(tmp_path, final_path).map_err(|e| internal(anyhow::anyhow!(e)))?;
            return Ok(());
        }
    }

    let raw_len = std::fs::metadata(tmp_path)
        .map_err(|e| internal(anyhow::anyhow!(e)))?
        .len();
    let part_path = final_path.with_extension("blkpart");
    let block_size = block_size.max(4096);

    let source = File::open(tmp_path).map_err(|e| internal(anyhow::anyhow!(e)))?;
    let (index, staging) =
        encode_blocks_from_reader(source, logical_size, block_size, level)?;
    let header_len = super::format::FIXED_HEADER_LEN + index.len() * super::format::INDEX_ENTRY_LEN;
    let total_len = header_len + staging.len();

    if total_len as u64 >= raw_len {
        std::fs::copy(tmp_path, final_path).map_err(|e| internal(anyhow::anyhow!(e)))?;
        return Ok(());
    }

    {
        let mut out = File::create(&part_path).map_err(|e| internal(anyhow::anyhow!(e)))?;
        write_blob_header(
            &mut out,
            logical_size,
            block_size as u32,
            &index,
        )?;
        out.write_all(&staging)
            .map_err(|e| internal(anyhow::anyhow!(e)))?;
    }

    std::fs::rename(&part_path, final_path).map_err(|e| internal(anyhow::anyhow!(e)))?;
    Ok(())
}

// Human: In-memory helper used by unit tests to build a NOSB blob from bytes.
// Agent: WRAPS encode_blob_for_storage; REQUIRES compressible ctx; RETURNS NOSB or raw.
pub fn compress_blob(
    uncompressed: &[u8],
    level: i32,
    block_size: usize,
    ctx: CompressionContext<'_>,
) -> Result<Vec<u8>, StorageError> {
    encode_blob_for_storage(uncompressed, level, block_size, ctx)
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
        )
    }

    #[test]
    fn file_encoder_roundtrip_via_decode_path() {
        use super::super::decode::decompress_blob;
        use super::super::format::is_compressed_blob;

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
        )
        .unwrap();
        let on_disk = std::fs::read(&final_path).unwrap();
        assert!(is_compressed_blob(&on_disk));
        let restored = decompress_blob(&on_disk, payload.len() as u64, None).unwrap();
        assert_eq!(restored, payload);
    }
}
