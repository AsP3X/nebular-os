use std::fs::File;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

use xxhash_rust::xxh3::xxh3_64;
use zstd::zstd_safe::CParameter;

use super::super::blocks::BlockStore;
use super::super::compressibility::{
    prefix_looks_incompressible, should_attempt_compression, CompressionContext,
};
use super::super::error::{internal, StorageError};
use super::format::{
    detect_blob_format, write_blob_header_v1, write_block_header_v1, BlobFormat, IndexEntry,
    BLOCK_COMPRESSED, BLOCK_DEDUP_REF, BLOCK_HEADER_LEN_V1, BLOCK_STORED, DEFAULT_BLOCK_SIZE,
    FIXED_HEADER_LEN_V1, FIXED_HEADER_LEN_V1_LEVEL, INDEX_ENTRY_LEN, NOSI_FLAG_DEDUP,
};

/// Human: Default zstd level when env does not override (22 = smallest on disk, highest CPU).
pub const DEFAULT_ZSTD_LEVEL: i32 = 22;

const LDM_SIZE_THRESHOLD: u64 = 128 * 1024;

/// Human: Optional zstd dictionary and dedup store for NOSI indexed writes.
#[derive(Clone, Copy, Default)]
pub struct EncodeOptions<'a> {
    pub dict_id: u16,
    pub dict: Option<&'a [u8]>,
    pub dedup_store: Option<&'a BlockStore>,
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


fn encode_logical_block(
    chunk: &[u8],
    level: i32,
    dict: Option<&[u8]>,
    dedup_store: Option<&BlockStore>,
) -> Result<(u8, Vec<u8>, u64), StorageError> {
    let checksum = xxh3_64(chunk);
    // Human: Share the chunk through the block store unless another block already holds its hash; then it
    // stays in this object, like any undeduplicated block.
    if let Some(store) = dedup_store
        && let Some(hash) = store.store_block(chunk, level)?
    {
        let mut payload = Vec::with_capacity(12);
        payload.extend_from_slice(&hash.to_le_bytes());
        payload.extend_from_slice(&(chunk.len() as u32).to_le_bytes());
        return Ok((BLOCK_DEDUP_REF, payload, hash));
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

/// Human: Encode blocks straight into `out` behind a reserved header, then write the header last — memory
/// stays at one block whatever the object size (the old path buffered every compressed block).
/// Agent: header length follows from the block count, which `logical_size` fixes; RETURNS (file_len, dedup refs).
fn encode_blocks_to_file(
    mut source: impl Read,
    out: &mut File,
    logical_size: u64,
    block_size: usize,
    level: i32,
    opts: EncodeOptions<'_>,
) -> Result<(u64, Vec<(u64, u32)>), StorageError> {
    let io = |e: std::io::Error| internal(anyhow::anyhow!(e));
    let block_count = logical_size.div_ceil(block_size as u64) as usize;
    let header_len = (FIXED_HEADER_LEN_V1_LEVEL + block_count * INDEX_ENTRY_LEN) as u64;
    let flags = if opts.dedup_store.is_some() {
        NOSI_FLAG_DEDUP
    } else {
        0
    };

    out.seek(SeekFrom::Start(header_len)).map_err(io)?;
    let mut writer = BufWriter::new(&mut *out);
    let mut index = Vec::with_capacity(block_count);
    let mut refs = Vec::new();
    let mut chunk_buf = vec![0u8; block_size];
    let mut block_header = Vec::with_capacity(BLOCK_HEADER_LEN_V1);
    let mut logical_end = 0u64;
    let mut data_len = 0u64;
    while logical_end < logical_size {
        let mut filled = 0usize;
        while filled < block_size {
            let n = source.read(&mut chunk_buf[filled..]).map_err(io)?;
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
        if block_type == BLOCK_DEDUP_REF {
            refs.push((checksum, filled as u32));
        }
        index.push(IndexEntry {
            compressed_offset: data_len,
            logical_end: logical_end + filled as u64,
        });
        block_header.clear();
        write_block_header_v1(&mut block_header, block_type, payload.len() as u32, checksum)?;
        writer.write_all(&block_header).map_err(io)?;
        writer.write_all(&payload).map_err(io)?;
        data_len += (block_header.len() + payload.len()) as u64;
        logical_end += filled as u64;
    }
    writer.flush().map_err(io)?;
    drop(writer);

    if logical_end != logical_size || index.len() != block_count {
        return Err(internal(anyhow::anyhow!(
            "block encoder size mismatch: encoded={logical_end} expected={logical_size}"
        )));
    }
    out.seek(SeekFrom::Start(0)).map_err(io)?;
    write_blob_header_v1(
        out,
        logical_size,
        block_size as u32,
        &index,
        opts.dict_id,
        flags,
        clamp_zstd_level(level) as u8,
    )?;
    Ok((header_len + data_len, refs))
}

pub fn encode_blob_for_storage(
    uncompressed: &[u8],
    level: i32,
    block_size: usize,
    ctx: CompressionContext<'_>,
    opts: EncodeOptions<'_>,
) -> Result<Vec<u8>, StorageError> {
    let must_wrap = raw_would_be_misread(uncompressed);
    if !must_wrap && !should_attempt_compression(ctx) {
        return Ok(uncompressed.to_vec());
    }
    if !must_wrap
        && opts.dedup_store.is_none()
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
    if !must_wrap && opts.dedup_store.is_none() && total_len >= uncompressed.len() {
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

/// Human: Reads pick the blob format from its first bytes, so raw bytes that begin with a Nebular
/// magic (NOSI/NOSB/NOSZ/NOS2/NOSD) would be misread; such payloads are always wrapped in NOSI.
/// Agent: SAME test as the read path — detect_blob_format on the leading bytes (thresholds <= 16).
pub fn raw_would_be_misread(head: &[u8]) -> bool {
    detect_blob_format(&head[..head.len().min(FIXED_HEADER_LEN_V1)]) != BlobFormat::Raw
}

/// How an uploaded file ends up on disk.
#[derive(Debug)]
pub enum FileEncoding {
    /// Keep the source bytes as-is; nothing was written to the output path.
    Raw,
    /// An indexed (NOSI) blob was written to the output path, referencing these dedup blocks.
    Indexed(Vec<(u64, u32)>),
}

/// Human: Encodes `src` into an indexed blob at `out`, or reports that the raw bytes should be stored unchanged.
/// Agent: WRITES out via `<out>.blkpart` + rename; NEVER returns Raw for bytes raw_would_be_misread flags.
pub fn encode_file_for_storage(
    src: &Path,
    out: &Path,
    logical_size: u64,
    level: i32,
    block_size: usize,
    ctx: CompressionContext<'_>,
    opts: EncodeOptions<'_>,
) -> Result<FileEncoding, StorageError> {
    let mut head = Vec::with_capacity(FIXED_HEADER_LEN_V1);
    File::open(src)
        .and_then(|f| f.take(FIXED_HEADER_LEN_V1 as u64).read_to_end(&mut head))
        .map_err(|e| internal(anyhow::anyhow!(e)))?;
    let must_wrap = raw_would_be_misread(&head);

    if !must_wrap && !should_attempt_compression(ctx) {
        return Ok(FileEncoding::Raw);
    }
    if !must_wrap
        && opts.dedup_store.is_none()
        && prefix_looks_incompressible(&head[..head.len().min(16)])
    {
        return Ok(FileEncoding::Raw);
    }

    let block_size = block_size.max(4096);
    let part_path = out.with_extension("blkpart");
    let encoded = File::open(src)
        .and_then(|source| Ok((source, File::create(&part_path)?)))
        .map_err(|e| internal(anyhow::anyhow!(e)))
        .and_then(|(source, mut part)| {
            encode_blocks_to_file(source, &mut part, logical_size, block_size, level, opts)
        });
    let (total_len, refs) = match encoded {
        Ok(done) => done,
        Err(e) => {
            let _ = std::fs::remove_file(&part_path);
            return Err(e);
        }
    };
    if !must_wrap && opts.dedup_store.is_none() && total_len >= logical_size {
        let _ = std::fs::remove_file(&part_path);
        return Ok(FileEncoding::Raw);
    }
    if let Err(e) = std::fs::rename(&part_path, out) {
        let _ = std::fs::remove_file(&part_path);
        return Err(internal(anyhow::anyhow!(e)));
    }
    Ok(FileEncoding::Indexed(refs))
}

/// Human: Encode `tmp_path` straight to `final_path` (raw payloads are copied there unchanged).
pub fn compress_file_to_storage(
    tmp_path: &Path,
    final_path: &Path,
    logical_size: u64,
    level: i32,
    block_size: usize,
    ctx: CompressionContext<'_>,
    opts: EncodeOptions<'_>,
) -> Result<Vec<(u64, u32)>, StorageError> {
    match encode_file_for_storage(tmp_path, final_path, logical_size, level, block_size, ctx, opts)? {
        FileEncoding::Raw => {
            std::fs::copy(tmp_path, final_path).map_err(|e| internal(anyhow::anyhow!(e)))?;
            Ok(Vec::new())
        }
        FileEncoding::Indexed(refs) => Ok(refs),
    }
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
    fn streaming_file_encoder_matches_in_memory_encoder() {
        let payload: Vec<u8> = (0..300_000u32).flat_map(|i| (i % 251).to_le_bytes()).collect();
        let mut tmp = NamedTempFile::new().unwrap();
        tmp.write_all(&payload).unwrap();
        let out = tempfile::NamedTempFile::new().unwrap().into_temp_path();
        let encoded = encode_file_for_storage(
            tmp.path(),
            &out,
            payload.len() as u64,
            3,
            64 * 1024,
            text_ctx(payload.len() as u64),
            EncodeOptions::default(),
        )
        .unwrap();
        assert!(matches!(encoded, FileEncoding::Indexed(ref refs) if refs.is_empty()));
        let in_memory = encode_blob_for_storage(
            &payload,
            3,
            64 * 1024,
            text_ctx(payload.len() as u64),
            EncodeOptions::default(),
        )
        .unwrap();
        assert_eq!(std::fs::read(&out).unwrap(), in_memory, "same bytes, one block of memory");
    }

    #[test]
    fn streaming_encoder_reports_dedup_refs() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path().to_str().unwrap());
        let payload = b"dedup me please ".repeat(40_000);
        let mut tmp = NamedTempFile::new().unwrap();
        tmp.write_all(&payload).unwrap();
        let out = dir.path().join("blob.bin");
        let opts = EncodeOptions {
            dedup_store: Some(&store),
            ..EncodeOptions::default()
        };
        let FileEncoding::Indexed(refs) = encode_file_for_storage(
            tmp.path(),
            &out,
            payload.len() as u64,
            3,
            64 * 1024,
            text_ctx(payload.len() as u64),
            opts,
        )
        .unwrap() else {
            panic!("dedup encodes are always indexed");
        };
        let on_disk = std::fs::read(&out).unwrap();
        assert_eq!(refs, super::super::format::collect_dedup_refs(&on_disk).unwrap());
        assert_eq!(refs.len(), payload.len().div_ceil(64 * 1024));
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
