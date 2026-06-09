use std::fs::File;
use std::io::{copy, Read, Write};
use std::path::Path;

use zstd::zstd_safe::CParameter;

use super::compressibility::{
    prefix_looks_incompressible, should_attempt_compression, CompressionContext,
};
use super::error::{internal, StorageError};

// Human: Every stored blob is prefixed with a magic tag and logical size so reads can tell compressed from legacy raw files.
// Agent: BLOB_MAGIC="NOSZ"; HEADER_LEN=12 (magic + uncompressed_size u64 LE); legacy blobs without magic are served raw.
pub const BLOB_MAGIC: &[u8; 4] = b"NOSZ";
pub const HEADER_LEN: usize = 12;

/// Human: Default zstd level when env does not override (22 = smallest on disk, highest CPU).
/// Agent: DEFAULT_ZSTD_LEVEL=22; overridden by NOS_ZSTD_LEVEL in config/engine.
pub const DEFAULT_ZSTD_LEVEL: i32 = 22;

/// Human: Enable long-distance matching once payloads are large enough to benefit.
/// Agent: LDM_THRESHOLD=128KiB; paired with level>=10 in tune_encoder_for_size.
const LDM_SIZE_THRESHOLD: u64 = 128 * 1024;

/// Human: Clamp user-provided zstd level into the range the zstd crate supports.
/// Agent: CLAMP 1..=22; used for NOS_ZSTD_LEVEL parsing.
pub fn clamp_zstd_level(level: i32) -> i32 {
    level.clamp(1, 22)
}

/// Returns true when `data` begins with the Nebular compressed-blob header.
pub fn is_compressed_blob(data: &[u8]) -> bool {
    data.len() >= HEADER_LEN && data.starts_with(BLOB_MAGIC)
}

/// Human: Read the logical size field from a compressed blob header on disk.
/// Agent: READS first 12 bytes; REQUIRES NOSZ magic; RETURNS u64 LE size from bytes 4..12.
pub fn read_blob_header_size(mut file: File) -> Result<u64, StorageError> {
    let mut header = [0u8; HEADER_LEN];
    file.read_exact(&mut header)
        .map_err(|e| internal(anyhow::anyhow!(e)))?;
    if !header.starts_with(BLOB_MAGIC) {
        return Err(internal(anyhow::anyhow!("not a compressed blob")));
    }
    Ok(u64::from_le_bytes(header[4..HEADER_LEN].try_into().unwrap()))
}

// Human: Pick a zstd window large enough for the payload but within library limits.
// Agent: window_log=ilog2(size).clamp(10,27); 2^log is ZSTD search distance.
fn adaptive_window_log(logical_size: u64) -> u32 {
    let bits = 64 - logical_size.max(1).leading_zeros();
    bits.clamp(10, 27)
}

// Human: Apply size-aware zstd knobs that improve ratio without changing the on-disk format.
// Agent: SETS pledged size, content-size flag, window_log, optional LDM via set_parameter.
fn tune_encoder_for_size<E>(
    encoder: &mut E,
    logical_size: u64,
    level: i32,
    pledged_src_size: bool,
) -> Result<(), StorageError>
where
    E: EncoderTuning,
{
    let level = clamp_zstd_level(level);
    if pledged_src_size && logical_size > 0 {
        encoder
            .set_pledged_src_size(Some(logical_size))
            .map_err(internal)?;
        encoder.include_contentsize(true).map_err(internal)?;
    }
    if logical_size >= 4096 {
        encoder
            .set_parameter(CParameter::WindowLog(adaptive_window_log(logical_size)))
            .map_err(internal)?;
    }
    if logical_size >= LDM_SIZE_THRESHOLD && level >= 10 {
        encoder
            .set_parameter(CParameter::EnableLongDistanceMatching(true))
            .map_err(internal)?;
    }
    Ok(())
}

trait EncoderTuning {
    fn set_pledged_src_size(&mut self, size: Option<u64>) -> std::io::Result<()>;
    fn include_contentsize(&mut self, include: bool) -> std::io::Result<()>;
    fn set_parameter(&mut self, parameter: CParameter) -> std::io::Result<()>;
}

impl EncoderTuning for zstd::stream::write::Encoder<'_, File> {
    fn set_pledged_src_size(&mut self, size: Option<u64>) -> std::io::Result<()> {
        self.set_pledged_src_size(size)
    }
    fn include_contentsize(&mut self, include: bool) -> std::io::Result<()> {
        self.include_contentsize(include)
    }
    fn set_parameter(&mut self, parameter: CParameter) -> std::io::Result<()> {
        self.set_parameter(parameter)
    }
}

impl EncoderTuning for zstd::bulk::Compressor<'_> {
    fn set_pledged_src_size(&mut self, _size: Option<u64>) -> std::io::Result<()> {
        Ok(())
    }
    fn include_contentsize(&mut self, include: bool) -> std::io::Result<()> {
        self.include_contentsize(include)
    }
    fn set_parameter(&mut self, parameter: CParameter) -> std::io::Result<()> {
        self.set_parameter(parameter)
    }
}

// Human: Compress arbitrary bytes with zstd and wrap them in the Nebular blob header.
// Agent: WRITES magic+uncompressed_size LE + zstd payload; INPUT logical bytes; OUTPUT on-disk blob bytes.
pub fn compress_blob(
    uncompressed: &[u8],
    level: i32,
    ctx: CompressionContext<'_>,
) -> Result<Vec<u8>, StorageError> {
    if !should_attempt_compression(ctx) {
        return Ok(uncompressed.to_vec());
    }

    let mut compressor =
        zstd::bulk::Compressor::new(clamp_zstd_level(level)).map_err(internal)?;
    tune_encoder_for_size(
        &mut compressor,
        uncompressed.len() as u64,
        level,
        false,
    )?;
    let compressed = compressor.compress(uncompressed).map_err(internal)?;

    let mut out = Vec::with_capacity(HEADER_LEN + compressed.len());
    out.extend_from_slice(BLOB_MAGIC);
    out.extend_from_slice(&(uncompressed.len() as u64).to_le_bytes());
    out.extend_from_slice(&compressed);
    Ok(out)
}

// Human: Pick zstd-wrapped storage when smaller than raw; otherwise keep bytes unwrapped for incompressible payloads.
// Agent: CALLS compress_blob; IF compressed.len < raw.len THEN NOSZ ELSE raw Vec (no header).
pub fn encode_blob_for_storage(
    uncompressed: &[u8],
    level: i32,
    ctx: CompressionContext<'_>,
) -> Result<Vec<u8>, StorageError> {
    if !should_attempt_compression(ctx) {
        return Ok(uncompressed.to_vec());
    }
    let compressed = compress_blob(uncompressed, level, ctx)?;
    if compressed.len() < uncompressed.len() && is_compressed_blob(&compressed) {
        Ok(compressed)
    } else {
        Ok(uncompressed.to_vec())
    }
}

// Human: Turn on-disk bytes back into the original object payload, or pass through legacy raw blobs unchanged.
// Agent: IF magic NOSZ THEN zstd decode and verify len==expected_size ELSE return data as-is (pre-compression objects).
pub fn decompress_blob(blob: &[u8], expected_size: u64) -> Result<Vec<u8>, StorageError> {
    if !is_compressed_blob(blob) {
        return Ok(blob.to_vec());
    }

    let stored_size = u64::from_le_bytes(
        blob[4..HEADER_LEN]
            .try_into()
            .map_err(|_| internal(anyhow::anyhow!("blob header truncated")))?,
    );
    if stored_size != expected_size {
        return Err(internal(anyhow::anyhow!(
            "blob header size mismatch: header={stored_size} metadata={expected_size}"
        )));
    }

    let decompressed = zstd::decode_all(&blob[HEADER_LEN..]).map_err(internal)?;
    if decompressed.len() as u64 != expected_size {
        return Err(internal(anyhow::anyhow!(
            "decompressed size mismatch: got {} expected {expected_size}",
            decompressed.len()
        )));
    }
    Ok(decompressed)
}

// Human: Write a compressed blob from a temp file without holding the full payload in memory.
// Agent: STREAM zstd from tmp_path; IF smaller than raw THEN rename to final_path ELSE copy raw file.
pub fn compress_file_to_storage(
    tmp_path: &Path,
    final_path: &Path,
    logical_size: u64,
    level: i32,
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
    let part_path = final_path.with_extension("zstpart");
    {
        let mut raw = File::open(tmp_path).map_err(|e| internal(anyhow::anyhow!(e)))?;
        let out = File::create(&part_path).map_err(|e| internal(anyhow::anyhow!(e)))?;
        let mut out = out;
        out.write_all(BLOB_MAGIC)
            .map_err(|e| internal(anyhow::anyhow!(e)))?;
        out.write_all(&logical_size.to_le_bytes())
            .map_err(|e| internal(anyhow::anyhow!(e)))?;
        let mut encoder =
            zstd::stream::write::Encoder::new(out, clamp_zstd_level(level)).map_err(internal)?;
        tune_encoder_for_size(&mut encoder, logical_size, level, true)?;
        copy(&mut raw, &mut encoder).map_err(|e| internal(anyhow::anyhow!(e)))?;
        encoder.finish().map_err(|e| internal(anyhow::anyhow!(e)))?;
    }
    let compressed_len = std::fs::metadata(&part_path)
        .map_err(|e| internal(anyhow::anyhow!(e)))?
        .len();
    if compressed_len < raw_len {
        std::fs::rename(&part_path, final_path).map_err(|e| internal(anyhow::anyhow!(e)))?;
    } else {
        std::fs::copy(tmp_path, final_path).map_err(|e| internal(anyhow::anyhow!(e)))?;
        let _ = std::fs::remove_file(&part_path);
    }
    Ok(())
}

// Human: Materialize logical bytes to a spill file for ranged reads on compressed objects (disk, not RAM).
// Agent: IF NOSZ THEN stream decode to spill_path ELSE copy raw blob; VERIFY output len==logical_size.
pub fn decompress_file_to_temp(
    blob_path: &Path,
    logical_size: u64,
    spill_path: &Path,
) -> Result<(), StorageError> {
    let mut header = [0u8; HEADER_LEN];
    let mut infile = File::open(blob_path).map_err(|e| internal(anyhow::anyhow!(e)))?;
    let read_header = infile.read(&mut header).map_err(|e| internal(anyhow::anyhow!(e)))?;
    if read_header < HEADER_LEN || !header.starts_with(BLOB_MAGIC) {
        std::fs::copy(blob_path, spill_path).map_err(|e| internal(anyhow::anyhow!(e)))?;
        return Ok(());
    }
    let stored = u64::from_le_bytes(header[4..HEADER_LEN].try_into().unwrap());
    if stored != logical_size {
        return Err(internal(anyhow::anyhow!(
            "blob header size mismatch: header={stored} metadata={logical_size}"
        )));
    }
    let mut out = File::create(spill_path).map_err(|e| internal(anyhow::anyhow!(e)))?;
    let mut decoder = zstd::stream::read::Decoder::new(infile).map_err(internal)?;
    copy(&mut decoder, &mut out).map_err(|e| internal(anyhow::anyhow!(e)))?;
    let written = std::fs::metadata(spill_path)
        .map_err(|e| internal(anyhow::anyhow!(e)))?
        .len();
    if written != logical_size {
        return Err(internal(anyhow::anyhow!(
            "decompressed spill size mismatch: got {written} expected {logical_size}"
        )));
    }
    Ok(())
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
    fn roundtrip_compresses_and_restores() {
        let original = b"hello world ".repeat(500);
        let blob = compress_blob(
            &original,
            DEFAULT_ZSTD_LEVEL,
            text_ctx(original.len() as u64),
        )
        .unwrap();
        assert!(is_compressed_blob(&blob));
        assert!(blob.len() < original.len());
        let restored = decompress_blob(&blob, original.len() as u64).unwrap();
        assert_eq!(restored, original);
    }

    #[test]
    fn legacy_raw_blob_passes_through() {
        let raw = b"legacy uncompressed payload";
        let restored = decompress_blob(raw, raw.len() as u64).unwrap();
        assert_eq!(restored, raw);
        assert!(!is_compressed_blob(raw));
    }

    #[test]
    fn incompressible_payload_stays_raw() {
        let payload = b"x".to_vec();
        let stored = encode_blob_for_storage(
            &payload,
            DEFAULT_ZSTD_LEVEL,
            text_ctx(payload.len() as u64),
        )
        .unwrap();
        assert!(!is_compressed_blob(&stored));
        assert_eq!(stored, payload);
    }

    #[test]
    fn excluded_media_extension_stays_raw() {
        let payload = b"fake mp3 payload ".repeat(400);
        let ctx = CompressionContext::new(
            Some("music/song.mp3"),
            Some("audio/mpeg"),
            payload.len() as u64,
            DEFAULT_MIN_COMPRESSIBLE_SIZE,
        );
        let stored = encode_blob_for_storage(&payload, DEFAULT_ZSTD_LEVEL, ctx).unwrap();
        assert!(!is_compressed_blob(&stored));
        assert_eq!(stored, payload);
    }

    #[test]
    fn compress_file_to_storage_roundtrip() {
        let mut tmp = NamedTempFile::new().unwrap();
        let payload = b"compress me ".repeat(400);
        tmp.write_all(&payload).unwrap();
        let final_path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
        compress_file_to_storage(
            tmp.path(),
            &final_path,
            payload.len() as u64,
            DEFAULT_ZSTD_LEVEL,
            text_ctx(payload.len() as u64),
        )
        .unwrap();
        let on_disk = std::fs::read(&final_path).unwrap();
        assert!(is_compressed_blob(&on_disk));
        let restored = decompress_blob(&on_disk, payload.len() as u64).unwrap();
        assert_eq!(restored, payload);
    }

    #[test]
    fn tuned_encoder_compresses_large_repetitive_payload() {
        let payload = b"structured log line ".repeat(8_192);
        let tuned = compress_blob(
            &payload,
            DEFAULT_ZSTD_LEVEL,
            text_ctx(payload.len() as u64),
        )
        .unwrap();
        assert!(is_compressed_blob(&tuned));
        assert!(tuned.len() < payload.len());
    }
}
