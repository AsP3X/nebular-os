use std::fs::File;
use std::io::{copy, Read, Write};
use std::path::Path;

use super::error::{internal, StorageError};

// Human: v1 compressed blob header — magic + logical size + zstd payload (legacy, still readable).
// Agent: BLOB_MAGIC="NOSZ"; HEADER_LEN=12; legacy blobs without magic served raw.
pub const BLOB_MAGIC: &[u8; 4] = b"NOSZ";
pub const HEADER_LEN: usize = 12;

// Human: v2 header adds dict_id + stored zstd level for tiered recompression.
// Agent: BLOB_MAGIC_V2="NOS2"; HEADER_LEN_V2=16; new writes use NOS2.
pub const BLOB_MAGIC_V2: &[u8; 4] = b"NOS2";
pub const HEADER_LEN_V2: usize = 16;

// Human: Dedup manifest — magic + logical size + block table pointing at `.blocks/`.
// Agent: DEDUP_MAGIC="NOSD"; entries are (hash u64, size u32) pairs.
pub const DEDUP_MAGIC: &[u8; 4] = b"NOSD";
pub const DEDUP_HEADER_LEN: usize = 16;
pub const DEDUP_ENTRY_LEN: usize = 12;

/// Human: Default zstd level when env does not override (22 = smallest on disk, highest CPU).
/// Agent: DEFAULT_ZSTD_LEVEL=22; overridden by NOS_ZSTD_LEVEL in config/engine.
pub const DEFAULT_ZSTD_LEVEL: i32 = 22;

/// Human: Default fast upload level when tiered compression is enabled.
pub const DEFAULT_ZSTD_LEVEL_UPLOAD: i32 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlobFormat {
    Raw,
    Nosz,
    Nos2,
    Nosd,
}

#[derive(Debug, Clone, Copy)]
pub struct RuntimeCompressParams<'a> {
    pub level: i32,
    pub dict_id: u16,
    pub dict: Option<&'a [u8]>,
}

impl<'a> RuntimeCompressParams<'a> {
    pub fn new(level: i32) -> Self {
        Self {
            level,
            dict_id: 0,
            dict: None,
        }
    }

    pub fn with_dict(level: i32, dict_id: u16, dict: Option<&'a [u8]>) -> Self {
        Self {
            level,
            dict_id,
            dict,
        }
    }
}

/// Human: Clamp user-provided zstd level into the range the zstd crate supports.
/// Agent: CLAMP 1..=22; used for NOS_ZSTD_LEVEL parsing.
pub fn clamp_zstd_level(level: i32) -> i32 {
    level.clamp(1, 22)
}

pub fn detect_blob_format(data: &[u8]) -> BlobFormat {
    if data.len() >= HEADER_LEN && data.starts_with(BLOB_MAGIC) {
        return BlobFormat::Nosz;
    }
    if data.len() >= HEADER_LEN_V2 && data.starts_with(BLOB_MAGIC_V2) {
        return BlobFormat::Nos2;
    }
    if data.len() >= DEDUP_HEADER_LEN && data.starts_with(DEDUP_MAGIC) {
        return BlobFormat::Nosd;
    }
    BlobFormat::Raw
}

/// Returns true when `data` begins with a Nebular zstd-wrapped blob header (NOSZ or NOS2).
pub fn is_compressed_blob(data: &[u8]) -> bool {
    matches!(
        detect_blob_format(data),
        BlobFormat::Nosz | BlobFormat::Nos2
    )
}

pub fn is_dedup_manifest(data: &[u8]) -> bool {
    detect_blob_format(data) == BlobFormat::Nosd
}

pub fn zstd_payload_offset(data: &[u8]) -> Option<usize> {
    match detect_blob_format(data) {
        BlobFormat::Nosz => Some(HEADER_LEN),
        BlobFormat::Nos2 => Some(HEADER_LEN_V2),
        _ => None,
    }
}

pub fn read_stored_zstd_level(data: &[u8]) -> Option<u8> {
    if detect_blob_format(data) == BlobFormat::Nos2 && data.len() >= HEADER_LEN_V2 {
        return Some(data[14]);
    }
    None
}

pub fn read_stored_dict_id(data: &[u8]) -> Option<u16> {
    if detect_blob_format(data) == BlobFormat::Nos2 && data.len() >= HEADER_LEN_V2 {
        return Some(u16::from_le_bytes([data[12], data[13]]));
    }
    None
}

/// Human: Read the logical size field from a compressed blob header on disk.
/// Agent: READS first 12 bytes; REQUIRES NOSZ/NOS2 magic; RETURNS u64 LE size from bytes 4..12.
pub fn read_blob_header_size(mut file: File) -> Result<u64, StorageError> {
    let mut header = [0u8; HEADER_LEN_V2];
    file.read_exact(&mut header[..HEADER_LEN])
        .map_err(|e| internal(anyhow::anyhow!(e)))?;
    if header.starts_with(BLOB_MAGIC) || header.starts_with(BLOB_MAGIC_V2) {
        return Ok(u64::from_le_bytes(header[4..HEADER_LEN].try_into().unwrap()));
    }
    Err(internal(anyhow::anyhow!("not a compressed blob")))
}

fn compress_payload(uncompressed: &[u8], params: &RuntimeCompressParams<'_>) -> Result<Vec<u8>, StorageError> {
    let level = clamp_zstd_level(params.level);
    if let Some(dict) = params.dict.filter(|d| !d.is_empty()) {
        let mut compressor =
            zstd::bulk::Compressor::with_dictionary(level, dict).map_err(internal)?;
        return compressor.compress(uncompressed).map_err(internal);
    }
    zstd::encode_all(uncompressed, level).map_err(internal)
}

fn write_nos2_header(out: &mut Vec<u8>, logical_size: u64, dict_id: u16, level: i32) {
    out.extend_from_slice(BLOB_MAGIC_V2);
    out.extend_from_slice(&logical_size.to_le_bytes());
    out.extend_from_slice(&dict_id.to_le_bytes());
    out.push(clamp_zstd_level(level) as u8);
    out.push(0);
}

// Human: Compress arbitrary bytes with zstd and wrap them in the Nebular v2 blob header.
pub fn compress_blob(uncompressed: &[u8], params: &RuntimeCompressParams<'_>) -> Result<Vec<u8>, StorageError> {
    let mut out = Vec::with_capacity(HEADER_LEN_V2 + uncompressed.len() / 2 + 64);
    write_nos2_header(&mut out, uncompressed.len() as u64, params.dict_id, params.level);
    let compressed = compress_payload(uncompressed, params)?;
    out.extend_from_slice(&compressed);
    Ok(out)
}

// Human: Pick zstd-wrapped storage when smaller than raw; otherwise keep bytes unwrapped for incompressible payloads.
pub fn encode_blob_for_storage(
    uncompressed: &[u8],
    params: &RuntimeCompressParams<'_>,
) -> Result<Vec<u8>, StorageError> {
    let compressed = compress_blob(uncompressed, params)?;
    if compressed.len() < uncompressed.len() {
        Ok(compressed)
    } else {
        Ok(uncompressed.to_vec())
    }
}

// Human: Turn on-disk bytes back into the original object payload, or pass through legacy raw blobs unchanged.
pub fn decompress_blob(
    blob: &[u8],
    expected_size: u64,
    dict: Option<&[u8]>,
) -> Result<Vec<u8>, StorageError> {
    let format = detect_blob_format(blob);
    if format == BlobFormat::Raw {
        return Ok(blob.to_vec());
    }
    if format == BlobFormat::Nosd {
        return Err(internal(anyhow::anyhow!(
            "decompress_blob called on dedup manifest"
        )));
    }

    let header_len = match format {
        BlobFormat::Nosz => HEADER_LEN,
        BlobFormat::Nos2 => HEADER_LEN_V2,
        _ => unreachable!(),
    };

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

    let payload = &blob[header_len..];
    let decompressed = if let Some(d) = dict.filter(|d| !d.is_empty()) {
        let mut dec = zstd::bulk::Decompressor::with_dictionary(d).map_err(internal)?;
        dec.decompress(payload, expected_size as usize)
            .map_err(internal)?
    } else {
        zstd::decode_all(payload).map_err(internal)?
    };

    if decompressed.len() as u64 != expected_size {
        return Err(internal(anyhow::anyhow!(
            "decompressed size mismatch: got {} expected {expected_size}",
            decompressed.len()
        )));
    }
    Ok(decompressed)
}

pub fn parse_dedup_manifest(data: &[u8], expected_logical: u64) -> Result<Vec<(u64, u32)>, StorageError> {
    if detect_blob_format(data) != BlobFormat::Nosd {
        return Err(internal(anyhow::anyhow!("not a dedup manifest")));
    }
    let logical = u64::from_le_bytes(data[4..12].try_into().unwrap());
    if logical != expected_logical {
        return Err(internal(anyhow::anyhow!(
            "dedup manifest logical size mismatch"
        )));
    }
    let count = u32::from_le_bytes(data[12..16].try_into().unwrap()) as usize;
    let expected_len = DEDUP_HEADER_LEN + count * DEDUP_ENTRY_LEN;
    if data.len() < expected_len {
        return Err(internal(anyhow::anyhow!("dedup manifest truncated")));
    }
    let mut entries = Vec::with_capacity(count);
    let mut off = DEDUP_HEADER_LEN;
    for _ in 0..count {
        let hash = u64::from_le_bytes(data[off..off + 8].try_into().unwrap());
        let size = u32::from_le_bytes(data[off + 8..off + 12].try_into().unwrap());
        entries.push((hash, size));
        off += DEDUP_ENTRY_LEN;
    }
    Ok(entries)
}

// Human: Write a compressed blob from a temp file without holding the full payload in memory.
pub fn compress_file_to_storage(
    tmp_path: &Path,
    final_path: &Path,
    logical_size: u64,
    params: &RuntimeCompressParams<'_>,
) -> Result<(), StorageError> {
    let raw_len = std::fs::metadata(tmp_path)
        .map_err(|e| internal(anyhow::anyhow!(e)))?
        .len();
    let part_path = final_path.with_extension("zstpart");
    {
        let mut raw = File::open(tmp_path).map_err(|e| internal(anyhow::anyhow!(e)))?;
        let out = File::create(&part_path).map_err(|e| internal(anyhow::anyhow!(e)))?;
        let mut out = out;
        out.write_all(BLOB_MAGIC_V2)
            .map_err(|e| internal(anyhow::anyhow!(e)))?;
        out.write_all(&logical_size.to_le_bytes())
            .map_err(|e| internal(anyhow::anyhow!(e)))?;
        out.write_all(&params.dict_id.to_le_bytes())
            .map_err(|e| internal(anyhow::anyhow!(e)))?;
        out.write_all(&[clamp_zstd_level(params.level) as u8, 0])
            .map_err(|e| internal(anyhow::anyhow!(e)))?;

        if let Some(dict) = params.dict.filter(|d| !d.is_empty()) {
            let mut encoder = zstd::stream::write::Encoder::with_dictionary(
                out,
                clamp_zstd_level(params.level),
                dict,
            )
            .map_err(internal)?;
            copy(&mut raw, &mut encoder).map_err(|e| internal(anyhow::anyhow!(e)))?;
            encoder.finish().map_err(|e| internal(anyhow::anyhow!(e)))?;
        } else {
            let mut encoder =
                zstd::stream::write::Encoder::new(out, clamp_zstd_level(params.level))
                    .map_err(internal)?;
            copy(&mut raw, &mut encoder).map_err(|e| internal(anyhow::anyhow!(e)))?;
            encoder.finish().map_err(|e| internal(anyhow::anyhow!(e)))?;
        }
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
pub fn decompress_file_to_temp(
    blob_path: &Path,
    logical_size: u64,
    spill_path: &Path,
    dict: Option<&[u8]>,
) -> Result<(), StorageError> {
    let data = std::fs::read(blob_path).map_err(|e| internal(anyhow::anyhow!(e)))?;
    let format = detect_blob_format(&data);
    if format == BlobFormat::Raw {
        std::fs::copy(blob_path, spill_path).map_err(|e| internal(anyhow::anyhow!(e)))?;
        return Ok(());
    }
    if format == BlobFormat::Nosd {
        return Err(internal(anyhow::anyhow!(
            "decompress_file_to_temp called on dedup manifest"
        )));
    }
    let restored = decompress_blob(&data, logical_size, dict)?;
    std::fs::write(spill_path, &restored).map_err(|e| internal(anyhow::anyhow!(e)))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn roundtrip_compresses_and_restores() {
        let original = b"hello world ".repeat(500);
        let params = RuntimeCompressParams::new(DEFAULT_ZSTD_LEVEL);
        let blob = compress_blob(&original, &params).unwrap();
        assert!(is_compressed_blob(&blob));
        assert!(blob.starts_with(BLOB_MAGIC_V2));
        assert!(blob.len() < original.len());
        let restored = decompress_blob(&blob, original.len() as u64, None).unwrap();
        assert_eq!(restored, original);
    }

    #[test]
    fn legacy_nosz_still_readable() {
        let original = b"legacy nosz ".repeat(100);
        let mut blob = Vec::new();
        blob.extend_from_slice(BLOB_MAGIC);
        blob.extend_from_slice(&(original.len() as u64).to_le_bytes());
        blob.extend_from_slice(&zstd::encode_all(&original[..], DEFAULT_ZSTD_LEVEL).unwrap());
        assert_eq!(detect_blob_format(&blob), BlobFormat::Nosz);
        let restored = decompress_blob(&blob, original.len() as u64, None).unwrap();
        assert_eq!(restored, original);
    }

    #[test]
    fn legacy_raw_blob_passes_through() {
        let raw = b"legacy uncompressed payload";
        let restored = decompress_blob(raw, raw.len() as u64, None).unwrap();
        assert_eq!(restored, raw);
        assert!(!is_compressed_blob(raw));
    }

    #[test]
    fn incompressible_payload_stays_raw() {
        let payload = b"x".to_vec();
        let params = RuntimeCompressParams::new(DEFAULT_ZSTD_LEVEL);
        let stored = encode_blob_for_storage(&payload, &params).unwrap();
        assert!(!is_compressed_blob(&stored));
        assert_eq!(stored, payload);
    }

    #[test]
    fn compress_file_to_storage_roundtrip() {
        let mut tmp = NamedTempFile::new().unwrap();
        let payload = b"compress me ".repeat(400);
        tmp.write_all(&payload).unwrap();
        let final_path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
        let params = RuntimeCompressParams::new(DEFAULT_ZSTD_LEVEL);
        compress_file_to_storage(tmp.path(), &final_path, payload.len() as u64, &params).unwrap();
        let on_disk = std::fs::read(&final_path).unwrap();
        assert!(is_compressed_blob(&on_disk));
        let restored = decompress_blob(&on_disk, payload.len() as u64, None).unwrap();
        assert_eq!(restored, payload);
    }

    #[test]
    fn nos2_stores_level() {
        let original = b"level marker ".repeat(50);
        let params = RuntimeCompressParams::new(7);
        let blob = compress_blob(&original, &params).unwrap();
        assert_eq!(read_stored_zstd_level(&blob), Some(7));
    }
}
