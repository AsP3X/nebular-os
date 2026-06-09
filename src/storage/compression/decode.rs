use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use bytes::Bytes;

use super::super::error::{internal, StorageError};
use super::format::{
    parse_layout_bytes, read_blob_layout, BLOCK_COMPRESSED, BLOCK_HEADER_LEN,
    BLOCK_STORED,
};

// Human: Decode a block payload from a byte slice at the given file offset.
// Agent: READS 8-byte header at offset; DECODES zstd or copies stored bytes; VERIFY len.
fn read_block_payload_bytes(
    blob: &[u8],
    file_offset: u64,
    expected_len: u64,
) -> Result<Vec<u8>, StorageError> {
    let start = file_offset as usize;
    if blob.len() < start + BLOCK_HEADER_LEN {
        return Err(internal(anyhow::anyhow!("block header truncated")));
    }
    let header = &blob[start..start + BLOCK_HEADER_LEN];
    let block_type = header[0];
    let payload_len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
    let payload_start = start + BLOCK_HEADER_LEN;
    let payload_end = payload_start
        .checked_add(payload_len)
        .ok_or_else(|| internal(anyhow::anyhow!("block payload overflow")))?;
    if blob.len() < payload_end {
        return Err(internal(anyhow::anyhow!("block payload truncated")));
    }
    let payload = &blob[payload_start..payload_end];

    let decoded = match block_type {
        BLOCK_COMPRESSED => zstd::decode_all(payload).map_err(internal)?,
        BLOCK_STORED => payload.to_vec(),
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
    Ok(decoded)
}

// Human: Read and decode a single block payload from an open blob file.
// Agent: SEEKS file_offset; READS header+payload; DELEGATES to byte decoder.
fn read_block_payload(
    file: &mut File,
    file_offset: u64,
    expected_len: u64,
) -> Result<Vec<u8>, StorageError> {
    file.seek(SeekFrom::Start(file_offset))
        .map_err(|e| internal(anyhow::anyhow!(e)))?;
    let mut header = [0u8; BLOCK_HEADER_LEN];
    file.read_exact(&mut header)
        .map_err(|e| internal(anyhow::anyhow!(e)))?;
    let payload_len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
    let mut payload = vec![0u8; payload_len];
    file.read_exact(&mut payload)
        .map_err(|e| internal(anyhow::anyhow!(e)))?;
    let block_type = header[0];
    let decoded = match block_type {
        BLOCK_COMPRESSED => zstd::decode_all(payload.as_slice()).map_err(internal)?,
        BLOCK_STORED => payload,
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
    Ok(decoded)
}

fn decompress_nosb_blob(blob: &[u8], expected_size: u64) -> Result<Vec<u8>, StorageError> {
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
        let block = read_block_payload_bytes(blob, offset, block_len)?;
        out.extend_from_slice(&block);
    }
    Ok(out)
}

// Human: Turn on-disk bytes back into the original object payload across all supported formats.
// Agent: ROUTES NOSB block decode vs legacy NOSZ/NOS2 zstd; raw pass-through; NOSD errors.
pub fn decompress_blob(
    blob: &[u8],
    expected_size: u64,
    dict: Option<&[u8]>,
) -> Result<Vec<u8>, StorageError> {
    match super::format::detect_blob_format(blob) {
        super::format::BlobFormat::Raw => Ok(blob.to_vec()),
        super::format::BlobFormat::Nosd => Err(internal(anyhow::anyhow!(
            "decompress_blob called on dedup manifest"
        ))),
        super::format::BlobFormat::Nosb => decompress_nosb_blob(blob, expected_size),
        super::format::BlobFormat::Nosz | super::format::BlobFormat::Nos2 => {
            super::legacy::decompress_zstd_blob(blob, expected_size, dict)
        }
    }
}

// Human: Materialize logical bytes from a blob file to a spill path (fallback for legacy zstd range reads).
// Agent: READS layout; DECODES all blocks sequentially; WRITES spill file; VERIFY len.
pub fn decompress_file_to_temp(
    blob_path: &Path,
    logical_size: u64,
    spill_path: &Path,
    dict: Option<&[u8]>,
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
    let restored = decompress_blob(&data, logical_size, dict)?;
    std::fs::write(spill_path, &restored).map_err(|e| internal(anyhow::anyhow!(e)))?;
    Ok(())
}

// Human: Stream the full logical object by decoding blocks sequentially into a channel.
// Agent: SPAWN_BLOCKING friendly; READS each block; SENDS Bytes chunks to tx until EOF.
pub fn pump_block_blob_full(
    blob_path: std::path::PathBuf,
    logical_size: u64,
    tx: tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
) {
    let mut file = match File::open(&blob_path) {
        Ok(f) => f,
        Err(e) => {
            let _ = tx.blocking_send(Err(e));
            return;
        }
    };

    let layout = match read_blob_layout(
        file.try_clone()
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

    for (idx, _entry) in layout.index.iter().enumerate() {
        let block_len = layout.logical_len(idx);
        let offset = layout.file_offset_for_block(idx);
        let block = match read_block_payload(&mut file, offset, block_len) {
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

// Human: Stream only the bytes in [range_start, range_start+length) without full-object decode.
// Agent: FINDS start block via layout.block_for_offset; DECODES affected blocks; SKIPS/slices inside block.
pub fn pump_block_blob_range(
    blob_path: std::path::PathBuf,
    logical_size: u64,
    range_start: u64,
    length: u64,
    tx: tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
) {
    let mut file = match File::open(&blob_path) {
        Ok(f) => f,
        Err(e) => {
            let _ = tx.blocking_send(Err(e));
            return;
        }
    };

    let layout = match read_blob_layout(
        file.try_clone()
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

    let mut remaining = length;
    let mut logical_pos = range_start;
    let mut block_idx = layout.block_for_offset(range_start);

    while remaining > 0 && block_idx < layout.block_count() {
        let block_start = layout.logical_start(block_idx);
        let block_len = layout.logical_len(block_idx);
        let offset = layout.file_offset_for_block(block_idx);
        let block = match read_block_payload(&mut file, offset, block_len) {
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
    use crate::storage::compression::encode::compress_blob;
    use crate::storage::compressibility::CompressionContext;

    fn text_ctx(size: u64) -> CompressionContext<'static> {
        CompressionContext::new(
            Some("data/log.txt"),
            Some("text/plain"),
            size,
            DEFAULT_MIN_COMPRESSIBLE_SIZE,
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
        )
        .unwrap();
        assert!(super::super::format::is_nosb_blob(&blob));
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.write_all(&blob).unwrap();

        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let path = tmp.path().to_path_buf();
        let size = payload.len() as u64;
        pump_block_blob_range(path, size, 10_000, 50, tx);

        let mut collected = Vec::new();
        while let Some(chunk) = rx.blocking_recv() {
            collected.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(collected.len(), 50);
        assert_eq!(&collected[..], &payload[10_000..10_050]);
    }
}
