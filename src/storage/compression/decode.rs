use std::fs::File;
use std::path::Path;

use bytes::Bytes;
use xxhash_rust::xxh3::xxh3_64;

use super::super::block_cache::BlockDecodeCache;
use super::super::blocks::BlockStore;
use super::super::error::{internal, StorageError};
use super::format::{
    parse_block_header_at, parse_layout_bytes, read_blob_layout, BlobLayout, BLOCK_COMPRESSED,
    BLOCK_DEDUP_REF, BLOCK_STORED,
};

fn decode_compressed_payload(payload: &[u8], dict: Option<&[u8]>) -> Result<Vec<u8>, StorageError> {
    if let Some(d) = dict.filter(|d| !d.is_empty()) {
        let mut dec = zstd::bulk::Decompressor::with_dictionary(d).map_err(internal)?;
        let cap = payload.len().saturating_mul(4).max(4096);
        dec.decompress(payload, cap).map_err(internal)
    } else {
        zstd::decode_all(payload).map_err(internal)
    }
}

fn verify_checksum(decoded: &[u8], expected: Option<u64>) -> Result<(), StorageError> {
    if let Some(expected) = expected {
        let actual = xxh3_64(decoded);
        if actual != expected {
            return Err(internal(anyhow::anyhow!("block checksum mismatch")));
        }
    }
    Ok(())
}

fn load_dedup_block(
    data_dir: &str,
    hash: u64,
    expected_len: u64,
    expected_checksum: Option<u64>,
) -> Result<Vec<u8>, StorageError> {
    let store = BlockStore::new(data_dir);
    let data = store.read_logical_block(hash, expected_len as usize)?;
    verify_checksum(&data, expected_checksum)?;
    Ok(data)
}

fn decode_block_payload(
    layout: &BlobLayout,
    blob: &[u8],
    file_offset: u64,
    expected_len: u64,
    dict: Option<&[u8]>,
    data_dir: Option<&str>,
) -> Result<Vec<u8>, StorageError> {
    let parsed = parse_block_header_at(blob, file_offset as usize, layout)?;
    let payload_start = file_offset as usize + parsed.header_len;
    let payload_end = payload_start
        .checked_add(parsed.payload_len as usize)
        .ok_or_else(|| internal(anyhow::anyhow!("block payload overflow")))?;
    if blob.len() < payload_end {
        return Err(internal(anyhow::anyhow!("block payload truncated")));
    }
    let payload = &blob[payload_start..payload_end];

    let decoded = match parsed.block_type {
        BLOCK_COMPRESSED => decode_compressed_payload(payload, dict)?,
        BLOCK_STORED => payload.to_vec(),
        BLOCK_DEDUP_REF => {
            if payload.len() != 12 {
                return Err(internal(anyhow::anyhow!("invalid dedup ref")));
            }
            let hash = u64::from_le_bytes(payload[0..8].try_into().unwrap());
            let size = u32::from_le_bytes(payload[8..12].try_into().unwrap());
            if size as u64 != expected_len {
                return Err(internal(anyhow::anyhow!("dedup ref size mismatch")));
            }
            let dir = data_dir.ok_or_else(|| {
                internal(anyhow::anyhow!("dedup block read requires data_dir"))
            })?;
            load_dedup_block(dir, hash, expected_len, parsed.logical_checksum)?
        }
        other => {
            return Err(internal(anyhow::anyhow!("unknown block type: {other}")));
        }
    };

    if decoded.len() as u64 != expected_len {
        return Err(internal(anyhow::anyhow!(
            "block size mismatch: got {} expected {expected_len}",
            decoded.len()
        )));
    }
    verify_checksum(&decoded, parsed.logical_checksum)?;
    Ok(decoded)
}

fn read_block_payload_bytes(
    blob: &[u8],
    layout: &BlobLayout,
    block_idx: usize,
    file_offset: u64,
    expected_len: u64,
    dict: Option<&[u8]>,
    data_dir: Option<&str>,
    cache: Option<&BlockDecodeCache>,
    cache_key: Option<&str>,
) -> Result<Vec<u8>, StorageError> {
    if let (Some(cache), Some(key)) = (cache, cache_key) {
        if let Some(hit) = cache.get(key, block_idx) {
            if hit.len() as u64 == expected_len {
                return Ok((*hit).clone());
            }
        }
    }
    let decoded = decode_block_payload(layout, blob, file_offset, expected_len, dict, data_dir)?;
    if let (Some(cache), Some(key)) = (cache, cache_key) {
        cache.insert(key, block_idx, decoded.clone());
    }
    Ok(decoded)
}

fn decompress_indexed_blob(
    blob: &[u8],
    expected_size: u64,
    dict: Option<&[u8]>,
    data_dir: Option<&str>,
) -> Result<Vec<u8>, StorageError> {
    let layout = parse_layout_bytes(blob)?;
    if layout.logical_size != expected_size {
        return Err(internal(anyhow::anyhow!(
            "blob header size mismatch: header={} metadata={expected_size}",
            layout.logical_size
        )));
    }

    let mut out = Vec::with_capacity(expected_size as usize);
    for (idx, _entry) in layout.index.iter().enumerate() {
        let block_len = layout.logical_len(idx);
        let offset = layout.file_offset_for_block(idx);
        let block = read_block_payload_bytes(
            blob,
            &layout,
            idx,
            offset,
            block_len,
            dict,
            data_dir,
            None,
            None,
        )?;
        out.extend_from_slice(&block);
    }
    Ok(out)
}

/// Verify per-block checksums on an indexed blob without materializing the full object.
pub fn verify_indexed_blob(
    blob: &[u8],
    expected_size: u64,
    dict: Option<&[u8]>,
    data_dir: Option<&str>,
) -> Result<(), StorageError> {
    let layout = parse_layout_bytes(blob)?;
    if layout.logical_size != expected_size {
        return Err(internal(anyhow::anyhow!(
            "blob header size mismatch: header={} metadata={expected_size}",
            layout.logical_size
        )));
    }
    for (idx, _entry) in layout.index.iter().enumerate() {
        let block_len = layout.logical_len(idx);
        let offset = layout.file_offset_for_block(idx);
        read_block_payload_bytes(
            blob,
            &layout,
            idx,
            offset,
            block_len,
            dict,
            data_dir,
            None,
            None,
        )?;
    }
    Ok(())
}

pub fn decompress_blob(
    blob: &[u8],
    expected_size: u64,
    dict: Option<&[u8]>,
    data_dir: Option<&str>,
) -> Result<Vec<u8>, StorageError> {
    match super::format::detect_blob_format(blob) {
        super::format::BlobFormat::Raw => Ok(blob.to_vec()),
        super::format::BlobFormat::Nosd => Err(internal(anyhow::anyhow!(
            "decompress_blob called on dedup manifest"
        ))),
        super::format::BlobFormat::Nosb | super::format::BlobFormat::Nosi => {
            decompress_indexed_blob(blob, expected_size, dict, data_dir)
        }
        super::format::BlobFormat::Nosz | super::format::BlobFormat::Nos2 => {
            super::legacy::decompress_zstd_blob(blob, expected_size, dict)
        }
    }
}

pub fn decompress_file_to_temp(
    blob_path: &Path,
    logical_size: u64,
    spill_path: &Path,
    dict: Option<&[u8]>,
    data_dir: Option<&str>,
) -> Result<(), StorageError> {
    let data = std::fs::read(blob_path).map_err(|e| internal(anyhow::anyhow!(e)))?;
    let format = super::format::detect_blob_format(&data);
    if format == super::format::BlobFormat::Raw {
        std::fs::copy(blob_path, spill_path).map_err(|e| internal(anyhow::anyhow!(e)))?;
        return Ok(());
    }
    if format == super::format::BlobFormat::Nosd {
        return Err(internal(anyhow::anyhow!(
            "decompress_file_to_temp called on dedup manifest"
        )));
    }
    let restored = decompress_blob(&data, logical_size, dict, data_dir)?;
    std::fs::write(spill_path, &restored).map_err(|e| internal(anyhow::anyhow!(e)))?;
    Ok(())
}

pub struct IndexedReadContext {
    pub dict: Option<Vec<u8>>,
    pub data_dir: String,
    pub block_cache: Option<BlockDecodeCache>,
}

pub fn pump_block_blob_full(
    blob_path: std::path::PathBuf,
    logical_size: u64,
    ctx: IndexedReadContext,
    tx: tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
) {
    let layout = match read_blob_layout(
        File::open(&blob_path)
            .unwrap_or_else(|_| std::fs::File::open(&blob_path).expect("reopen blob")),
    ) {
        Ok(l) => l,
        Err(e) => {
            let _ = tx.blocking_send(Err(std::io::Error::other(e.to_string())));
            return;
        }
    };

    if layout.logical_size != logical_size {
        let _ = tx.blocking_send(Err(std::io::Error::other("blob header size mismatch")));
        return;
    }

    let blob = match std::fs::read(&blob_path) {
        Ok(b) => b,
        Err(e) => {
            let _ = tx.blocking_send(Err(e));
            return;
        }
    };

    let cache_key = blob_path.to_string_lossy().into_owned();
    let data_dir = if ctx.data_dir.is_empty() {
        None
    } else {
        Some(ctx.data_dir.as_str())
    };
    for (idx, _entry) in layout.index.iter().enumerate() {
        let block_len = layout.logical_len(idx);
        let offset = layout.file_offset_for_block(idx);
        let block = match read_block_payload_bytes(
            &blob,
            &layout,
            idx,
            offset,
            block_len,
            ctx.dict.as_deref(),
            data_dir,
            ctx.block_cache.as_ref(),
            Some(cache_key.as_str()),
        ) {
            Ok(b) => b,
            Err(e) => {
                let _ = tx.blocking_send(Err(std::io::Error::other(e.to_string())));
                return;
            }
        };
        if tx.blocking_send(Ok(Bytes::from(block))).is_err() {
            return;
        }
    }
}

pub fn pump_block_blob_range(
    blob_path: std::path::PathBuf,
    logical_size: u64,
    range_start: u64,
    length: u64,
    ctx: IndexedReadContext,
    tx: tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
) {
    let blob = match std::fs::read(&blob_path) {
        Ok(b) => b,
        Err(e) => {
            let _ = tx.blocking_send(Err(e));
            return;
        }
    };

    let layout = match parse_layout_bytes(&blob) {
        Ok(l) => l,
        Err(e) => {
            let _ = tx.blocking_send(Err(std::io::Error::other(e.to_string())));
            return;
        }
    };

    if layout.logical_size != logical_size {
        let _ = tx.blocking_send(Err(std::io::Error::other("blob header size mismatch")));
        return;
    }

    let mut remaining = length;
    let mut logical_pos = range_start;
    let mut block_idx = layout.block_for_offset(range_start);

    let cache_key = blob_path.to_string_lossy().into_owned();
    let data_dir = if ctx.data_dir.is_empty() {
        None
    } else {
        Some(ctx.data_dir.as_str())
    };
    while remaining > 0 && block_idx < layout.block_count() {
        let block_start = layout.logical_start(block_idx);
        let block_len = layout.logical_len(block_idx);
        let offset = layout.file_offset_for_block(block_idx);
        let block = match read_block_payload_bytes(
            &blob,
            &layout,
            block_idx,
            offset,
            block_len,
            ctx.dict.as_deref(),
            data_dir,
            ctx.block_cache.as_ref(),
            Some(cache_key.as_str()),
        ) {
            Ok(b) => b,
            Err(e) => {
                let _ = tx.blocking_send(Err(std::io::Error::other(e.to_string())));
                return;
            }
        };

        let skip = logical_pos.saturating_sub(block_start) as usize;
        if skip >= block.len() {
            block_idx += 1;
            logical_pos = layout.logical_start(block_idx);
            continue;
        }
        let take = (remaining as usize).min(block.len() - skip);
        if tx
            .blocking_send(Ok(Bytes::copy_from_slice(&block[skip..skip + take])))
            .is_err()
        {
            return;
        }
        remaining -= take as u64;
        logical_pos += take as u64;
        if logical_pos >= layout.index[block_idx].logical_end {
            block_idx += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;
    use crate::storage::compressibility::DEFAULT_MIN_COMPRESSIBLE_SIZE;
    use crate::storage::compression::encode::{compress_blob, EncodeOptions};
    use crate::storage::compressibility::CompressionContext;

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
    fn range_pump_returns_slice_without_full_file_decode() {
        let payload = b"abcdefghijklmnopqrstuvwxyz".repeat(8_000);
        let blob = compress_blob(
            &payload,
            3,
            4096,
            text_ctx(payload.len() as u64),
            EncodeOptions::default(),
        )
        .unwrap();
        assert!(super::super::format::is_indexed_blob(&blob));
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.write_all(&blob).unwrap();

        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let path = tmp.path().to_path_buf();
        let size = payload.len() as u64;
        pump_block_blob_range(
            path,
            size,
            10_000,
            50,
            IndexedReadContext {
                dict: None,
                data_dir: String::new(),
                block_cache: None,
            },
            tx,
        );

        let mut collected = Vec::new();
        while let Some(chunk) = rx.blocking_recv() {
            collected.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(collected.len(), 50);
        assert_eq!(&collected[..], &payload[10_000..10_050]);
    }

    #[test]
    fn checksum_mismatch_fails_decode() {
        let payload = b"checksum test payload".repeat(200);
        let mut blob = compress_blob(
            &payload,
            3,
            4096,
            text_ctx(payload.len() as u64),
            EncodeOptions::default(),
        )
        .unwrap();
        if let Some(byte) = blob.last_mut() {
            *byte ^= 0xFF;
        }
        assert!(decompress_blob(&blob, payload.len() as u64, None, None).is_err());
    }
}
