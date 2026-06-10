use xxhash_rust::xxh3::xxh3_64;

use super::compression::{
    detect_blob_format, parse_layout_bytes, verify_indexed_blob, BlobFormat,
};
use super::error::{internal, StorageError};
use super::streaming::hash_file_xxh3_hex;

/// Human: Background integrity pass intensity — light checks headers/sizes; deep decodes checksums.
/// Agent: Serialized in scrub reports; deep matches legacy verify_blob_integrity behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ScrubMode {
    Light,
    Deep,
}

impl ScrubMode {
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "light" => Some(Self::Light),
            "deep" => Some(Self::Deep),
            _ => None,
        }
    }
}

/// Human: Tunables for one scrub batch (sampling, mode, batch size).
/// Agent: sample_denom=1 checks every candidate; N>1 keeps keys where hash(bucket/key)%N==0.
#[derive(Debug, Clone)]
pub struct ScrubOptions {
    pub limit: usize,
    pub sample_denom: u64,
    pub mode: ScrubMode,
    pub start_after: Option<String>,
}

impl Default for ScrubOptions {
    fn default() -> Self {
        Self {
            limit: 100,
            sample_denom: 1,
            mode: ScrubMode::Deep,
            start_after: None,
        }
    }
}

/// Human: Decide whether this object key is in the current hash sample window.
/// Agent: xxh3 over "bucket/key" modulo sample_denom; denom 0 treated as 1 (always sample).
pub fn scrub_sample_selected(bucket: &str, key: &str, sample_denom: u64) -> bool {
    let denom = sample_denom.max(1);
    if denom == 1 {
        return true;
    }
    let id = format!("{bucket}/{key}");
    xxh3_64(id.as_bytes()) % denom == 0
}

/// Human: Light indexed-blob check — layout header and index bounds without decoding blocks.
/// Agent: parse_layout_bytes + per-entry file offset sanity; no zstd/dedup IO.
pub fn verify_indexed_blob_light(blob: &[u8], expected_size: u64) -> Result<(), StorageError> {
    let layout = parse_layout_bytes(blob)?;
    if layout.logical_size != expected_size {
        return Err(internal(anyhow::anyhow!("indexed header size mismatch")));
    }
    let file_len = blob.len() as u64;
    for (idx, _entry) in layout.index.iter().enumerate() {
        let offset = layout.file_offset_for_block(idx);
        let block_len = layout.logical_len(idx);
        let end = offset.saturating_add(block_len);
        if end > file_len {
            return Err(internal(anyhow::anyhow!("indexed block extends past file end")));
        }
    }
    Ok(())
}

/// Human: Verify one on-disk blob against metadata for light or deep scrub modes.
/// Agent: Raw => size (+ optional etag hash in deep); indexed => light layout or full checksum walk.
pub async fn verify_blob_for_scrub(
    blob: &[u8],
    format: BlobFormat,
    size: i64,
    mode: ScrubMode,
    path: &std::path::Path,
    etag: Option<&str>,
    dict_bytes: Option<&[u8]>,
    data_dir: Option<&str>,
    decode_for_maintenance: impl FnOnce(
        &[u8],
        BlobFormat,
        i64,
        Option<&[u8]>,
    ) -> Result<Vec<u8>, StorageError>,
) -> bool {
    match mode {
        ScrubMode::Light => match format {
            BlobFormat::Raw => blob.len() as i64 == size,
            BlobFormat::Nosb | BlobFormat::Nosi => {
                verify_indexed_blob_light(blob, size as u64).is_ok()
            }
            BlobFormat::Nosd | BlobFormat::Nosz | BlobFormat::Nos2 => {
                detect_blob_format(blob) != BlobFormat::Raw && blob.len() > 8
            }
        },
        ScrubMode::Deep => match format {
            BlobFormat::Raw => {
                if blob.len() as i64 != size {
                    return false;
                }
                if let Some(expected) = etag.filter(|e| !e.is_empty()) {
                    match hash_file_xxh3_hex(path, 256 * 1024) {
                        Ok(actual) => actual == *expected,
                        Err(_) => false,
                    }
                } else {
                    true
                }
            }
            BlobFormat::Nosd => decode_for_maintenance(blob, format, size, dict_bytes).is_ok(),
            BlobFormat::Nosb | BlobFormat::Nosi => {
                verify_indexed_blob(blob, size as u64, dict_bytes, data_dir).is_ok()
            }
            BlobFormat::Nosz | BlobFormat::Nos2 => {
                super::compression::decompress_blob(blob, size as u64, dict_bytes, data_dir).is_ok()
            }
        },
    }
}
