use xxhash_rust::xxh3::xxh3_64;

use super::compression::{
    detect_blob_format, parse_layout_bytes, verify_indexed_blob, BlobFormat, BlobLayout,
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
/// Agent: sample_denom=1 checks every candidate; N>1 checks the keys in slice `sample_epoch` of N.
#[derive(Debug, Clone)]
pub struct ScrubOptions {
    pub limit: usize,
    pub sample_denom: u64,
    /// Which 1/sample_denom slice of the keys to check; the periodic scrub moves on after every full pass.
    pub sample_epoch: u64,
    pub mode: ScrubMode,
    pub start_after: Option<String>,
}

impl Default for ScrubOptions {
    fn default() -> Self {
        Self {
            limit: 100,
            sample_denom: 1,
            sample_epoch: 0,
            mode: ScrubMode::Deep,
            start_after: None,
        }
    }
}

/// Human: Decide whether this object key is in the first hash sample slice (see `scrub_sample_selected_in`).
pub fn scrub_sample_selected(bucket: &str, key: &str, sample_denom: u64) -> bool {
    scrub_sample_selected_in(bucket, key, sample_denom, 0)
}

/// Human: Whether this object key is in sample slice `epoch`. Each epoch selects a different 1/`sample_denom`
/// of the keys, and any `sample_denom` consecutive epochs select every key exactly once — the sample used to be
/// fixed, so the periodic scrub re-checked the same keys forever and never reached the rest.
/// Agent: (xxh3("bucket/key") + epoch) mod sample_denom == 0; denom 0 treated as 1 (always sample).
pub fn scrub_sample_selected_in(bucket: &str, key: &str, sample_denom: u64, epoch: u64) -> bool {
    let denom = sample_denom.max(1);
    if denom == 1 {
        return true;
    }
    let id = format!("{bucket}/{key}");
    (u128::from(xxh3_64(id.as_bytes())) + u128::from(epoch)).is_multiple_of(u128::from(denom))
}

/// Human: Light indexed-blob check — layout header and index bounds without decoding blocks.
/// Agent: parse_layout_bytes + per-entry file offset sanity; no zstd/dedup IO.
pub fn verify_indexed_blob_light(blob: &[u8], expected_size: u64) -> Result<(), StorageError> {
    let layout = parse_layout_bytes(blob)?;
    if indexed_extents_fit(&layout, expected_size, blob.len() as u64) {
        Ok(())
    } else {
        Err(internal(anyhow::anyhow!("indexed layout does not fit the file")))
    }
}

/// Human: Light structural check of an indexed blob: the header's size matches metadata and every block's
/// on-disk extent (from its offset to the next one) holds at least a block header and stays inside the file.
/// Agent: Compares STORED extents — logical lengths of compressed blocks legitimately exceed them.
pub fn indexed_extents_fit(layout: &BlobLayout, expected_size: u64, file_len: u64) -> bool {
    if layout.logical_size != expected_size {
        return false;
    }
    let header = layout.block_header_len() as u64;
    (0..layout.block_count()).all(|idx| {
        let start = layout.file_offset_for_block(idx);
        let end = if idx + 1 < layout.block_count() {
            layout.file_offset_for_block(idx + 1)
        } else {
            file_len
        };
        start.saturating_add(header) <= end && end <= file_len
    })
}

/// Human: Verify one on-disk blob against metadata for light or deep scrub modes.
/// Agent: Raw => size (+ optional etag hash in deep); indexed => light layout or full checksum walk.
#[allow(clippy::too_many_arguments)]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consecutive_sample_slices_cover_every_key_once() {
        let keys: Vec<String> = (0..500).map(|i| format!("photos/{i}.jpg")).collect();
        for denom in [2u64, 7, 1024] {
            for first_epoch in [0u64, 5, u64::MAX - 3] {
                for key in &keys {
                    let hits = (0..denom)
                        .filter(|offset| {
                            scrub_sample_selected_in("b", key, denom, first_epoch.wrapping_add(*offset))
                        })
                        .count();
                    // Human: u64 wrap-around breaks the cycle only at the very end of the epoch range.
                    if first_epoch.checked_add(denom).is_some() {
                        assert_eq!(hits, 1, "{key} denom {denom} epochs from {first_epoch}");
                    }
                }
            }
        }
        assert!(scrub_sample_selected_in("b", "k", 1, 12345));
        assert_eq!(scrub_sample_selected("b", "k", 8), scrub_sample_selected_in("b", "k", 8, 0));
    }
}
