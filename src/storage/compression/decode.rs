use std::sync::Arc;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use bytes::Bytes;
use xxhash_rust::xxh3::xxh3_64;

use super::super::block_cache::BlockDecodeCache;
use super::super::blocks::BlockStore;
use super::super::error::{internal, StorageError};
use super::format::{
    parse_block_header_at, parse_layout_bytes, read_blob_layout, BlobLayout, ParsedBlockHeader,
    BLOCK_COMPRESSED, BLOCK_DEDUP_REF, BLOCK_STORED,
};

/// Decoded-block cache plus the blob path its entries are scoped to.
type BlockCacheRef<'a> = Option<(&'a BlockDecodeCache, &'a str)>;

/// Largest up-front reservation for one decoded block; larger blocks grow only as output arrives.
const MAX_BLOCK_PREALLOC: u64 = 8 * 1024 * 1024;

/// Human: Decode one zstd frame of at most `expected_len` bytes, with the dictionary its header names (`dict`
/// is only a fallback: see `dictionary_for_frame`).
pub(crate) fn decode_compressed_payload(
    payload: &[u8],
    dict: Option<&[u8]>,
    expected_len: u64,
) -> Result<Vec<u8>, StorageError> {
    // Human: Stream-decode and stop one byte past the logical length, so memory follows real output rather
    // than a size claimed by the blob index (text routinely beats 4:1, so no ratio-based cap either).
    // Agent: caller rejects len != expected_len; an empty dict means "no dictionary" to zstd.
    let dict = crate::storage::dict_store::dictionary_for_frame(payload, dict)?;
    let decoder = zstd::stream::read::Decoder::with_dictionary(
        payload,
        dict.as_deref().map_or(&[][..], |d| d.as_slice()),
    )
    .map_err(internal)?;
    let mut out = Vec::with_capacity(expected_len.min(MAX_BLOCK_PREALLOC) as usize);
    decoder
        .take(expected_len.saturating_add(1))
        .read_to_end(&mut out)
        .map_err(internal)?;
    Ok(out)
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

fn block_payload<'a>(
    blob: &'a [u8],
    parsed: &ParsedBlockHeader,
    file_offset: u64,
) -> Result<&'a [u8], StorageError> {
    let payload_start = file_offset as usize + parsed.header_len;
    let payload_end = payload_start
        .checked_add(parsed.payload_len as usize)
        .ok_or_else(|| internal(anyhow::anyhow!("block payload overflow")))?;
    if blob.len() < payload_end {
        return Err(internal(anyhow::anyhow!("block payload truncated")));
    }
    Ok(&blob[payload_start..payload_end])
}

fn decode_block_payload(
    payload: &[u8],
    parsed: &ParsedBlockHeader,
    expected_len: u64,
    dict: Option<&[u8]>,
    data_dir: Option<&str>,
) -> Result<Vec<u8>, StorageError> {
    let decoded = match parsed.block_type {
        BLOCK_COMPRESSED => decode_compressed_payload(payload, dict, expected_len)?,
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

/// Human: Decode one block through the block cache. Entries are tagged with the block's content identity —
/// its stored logical checksum (NOSI v1) or a hash of its stored bytes (NOSB) — so a blob rewritten in
/// place never hits decodes of its previous content.
#[allow(clippy::too_many_arguments)]
fn decode_with_cache(
    payload: &[u8],
    parsed: &ParsedBlockHeader,
    layout: &BlobLayout,
    block_idx: usize,
    dict: Option<&[u8]>,
    data_dir: Option<&str>,
    cache: BlockCacheRef<'_>,
    populate: bool,
) -> Result<Vec<u8>, StorageError> {
    let expected_len = layout.logical_len(block_idx);
    let cache = cache.map(|(cache, path)| {
        let tag = parsed.logical_checksum.unwrap_or_else(|| xxh3_64(payload));
        (cache, path, tag)
    });
    if let Some((cache, path, tag)) = cache
        && let Some(hit) = cache.get(path, block_idx, tag)
        && hit.len() as u64 == expected_len
    {
        return Ok((*hit).clone());
    }
    let decoded = decode_block_payload(payload, parsed, expected_len, dict, data_dir)?;
    if populate && let Some((cache, path, tag)) = cache {
        cache.insert(path, block_idx, tag, decoded.clone());
    }
    Ok(decoded)
}

fn read_block_payload_bytes(
    blob: &[u8],
    layout: &BlobLayout,
    block_idx: usize,
    dict: Option<&[u8]>,
    data_dir: Option<&str>,
    cache: BlockCacheRef<'_>,
) -> Result<Vec<u8>, StorageError> {
    let file_offset = layout.file_offset_for_block(block_idx);
    let parsed = parse_block_header_at(blob, file_offset as usize, layout)?;
    let payload = block_payload(blob, &parsed, file_offset)?;
    decode_with_cache(payload, &parsed, layout, block_idx, dict, data_dir, cache, true)
}

/// Human: Read and decode one block from an open blob file — only that block's header and payload are read,
/// so serving a range (or streaming a whole object) never holds more than one block of the blob in memory.
/// Agent: block extent = [offset(i), offset(i+1) or file_len); cached checksummed blocks skip the payload read;
/// `populate` false = consult the cache but don't fill it (full sequential reads would flush useful entries).
#[allow(clippy::too_many_arguments)]
fn read_block_from_file(
    file: &mut File,
    file_len: u64,
    layout: &BlobLayout,
    block_idx: usize,
    dict: Option<&[u8]>,
    data_dir: Option<&str>,
    cache: BlockCacheRef<'_>,
    populate: bool,
) -> Result<Vec<u8>, StorageError> {
    let io = |e: std::io::Error| internal(anyhow::anyhow!(e));
    let start = layout.file_offset_for_block(block_idx);
    let end = if block_idx + 1 < layout.block_count() {
        layout.file_offset_for_block(block_idx + 1)
    } else {
        file_len
    };
    let header_len = layout.block_header_len() as u64;
    if end > file_len || end < start.saturating_add(header_len) {
        return Err(internal(anyhow::anyhow!("block {block_idx} extends past the blob")));
    }
    let mut header = vec![0u8; header_len as usize];
    file.seek(SeekFrom::Start(start)).map_err(io)?;
    file.read_exact(&mut header).map_err(io)?;
    let parsed = parse_block_header_at(&header, 0, layout)?;
    if let (Some((cache, path)), Some(checksum)) = (cache, parsed.logical_checksum)
        && let Some(hit) = cache.get(path, block_idx, checksum)
        && hit.len() as u64 == layout.logical_len(block_idx)
    {
        return Ok((*hit).clone());
    }
    if header_len + parsed.payload_len as u64 > end - start {
        return Err(internal(anyhow::anyhow!("block payload truncated")));
    }
    let mut payload = vec![0u8; parsed.payload_len as usize];
    file.read_exact(&mut payload).map_err(io)?;
    decode_with_cache(&payload, &parsed, layout, block_idx, dict, data_dir, cache, populate)
}

/// Read just the header + block index of an open indexed blob.
fn read_indexed_layout(
    mut file: File,
    logical_size: u64,
) -> Result<(File, u64, BlobLayout), StorageError> {
    let io = |e: std::io::Error| internal(anyhow::anyhow!(e));
    file.seek(SeekFrom::Start(0)).map_err(io)?;
    let file_len = file.metadata().map_err(io)?.len();
    let layout = read_blob_layout(&mut file)?;
    if layout.logical_size != logical_size {
        return Err(internal(anyhow::anyhow!("blob header size mismatch")));
    }
    Ok((file, file_len, layout))
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
        let block = read_block_payload_bytes(blob, &layout, idx, dict, data_dir, None)?;
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
        read_block_payload_bytes(blob, &layout, idx, dict, data_dir, None)?;
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
    pub dict: Option<Arc<Vec<u8>>>,
    pub data_dir: String,
    pub block_cache: Option<BlockDecodeCache>,
}

/// Human: Reads a block-compressed blob's logical bytes in order, at most one block per `next_chunk` call, so
/// the caller can run each call on the blocking pool and wait for its consumer in between (without parking a
/// blocking thread for the whole download).
/// Agent: Holds the open file and parsed index; full reads consult the block cache without filling it.
pub struct IndexedBlobReader {
    file: File,
    file_len: u64,
    layout: BlobLayout,
    ctx: IndexedReadContext,
    cache_key: String,
    populate_cache: bool,
    block_idx: usize,
    pos: u64,
    end: u64,
}

impl IndexedBlobReader {
    /// Open `blob_path` to read `length` logical bytes from `range_start` of an object of `logical_size`.
    pub fn open(
        blob_path: &Path,
        logical_size: u64,
        range_start: u64,
        length: u64,
        ctx: IndexedReadContext,
    ) -> Result<Self, StorageError> {
        let file = File::open(blob_path).map_err(|e| internal(anyhow::anyhow!(e)))?;
        Self::from_file(file, blob_path, logical_size, range_start, length, ctx)
    }

    /// Like `open`, reading from a handle the caller already holds — e.g. the one it sniffed the format from,
    /// so the read stays on that file even if an overwrite replaces the path meanwhile.
    pub fn from_file(
        file: File,
        blob_path: &Path,
        logical_size: u64,
        range_start: u64,
        length: u64,
        ctx: IndexedReadContext,
    ) -> Result<Self, StorageError> {
        let (file, file_len, layout) = read_indexed_layout(file, logical_size)?;
        let end = range_start
            .checked_add(length)
            .filter(|end| *end <= logical_size)
            .ok_or_else(|| {
                internal(anyhow::anyhow!(
                    "range {range_start}+{length} is outside an object of {logical_size} bytes"
                ))
            })?;
        Ok(Self {
            block_idx: layout.block_for_offset(range_start),
            file,
            file_len,
            layout,
            ctx,
            cache_key: blob_path.to_string_lossy().into_owned(),
            populate_cache: !(range_start == 0 && length == logical_size),
            pos: range_start,
            end,
        })
    }

    /// The next piece of the range (at most one block), or `None` once the range is complete.
    pub fn next_chunk(&mut self) -> Result<Option<Bytes>, StorageError> {
        if self.pos >= self.end {
            return Ok(None);
        }
        let data_dir = (!self.ctx.data_dir.is_empty()).then_some(self.ctx.data_dir.as_str());
        let cache = self
            .ctx
            .block_cache
            .as_ref()
            .map(|c| (c, self.cache_key.as_str()));
        while self.block_idx < self.layout.block_count() {
            let idx = self.block_idx;
            self.block_idx += 1;
            let block = read_block_from_file(
                &mut self.file,
                self.file_len,
                &self.layout,
                idx,
                self.ctx.dict.as_deref().map(|d| d.as_slice()),
                data_dir,
                cache,
                self.populate_cache,
            )?;
            let skip = self.pos.saturating_sub(self.layout.logical_start(idx)) as usize;
            if skip >= block.len() {
                continue;
            }
            let take = (self.end - self.pos).min((block.len() - skip) as u64) as usize;
            self.pos += take as u64;
            return Ok(Some(if skip == 0 && take == block.len() {
                Bytes::from(block)
            } else {
                Bytes::copy_from_slice(&block[skip..skip + take])
            }));
        }
        Err(internal(anyhow::anyhow!(
            "blob index ends before logical offset {}",
            self.pos
        )))
    }
}

#[deprecated(note = "parks a blocking thread until the receiver drains; step an IndexedBlobReader instead")]
pub fn pump_block_blob_full(
    blob_path: std::path::PathBuf,
    logical_size: u64,
    ctx: IndexedReadContext,
    tx: tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
) {
    pump_blocking(
        IndexedBlobReader::open(&blob_path, logical_size, 0, logical_size, ctx),
        tx,
    );
}

#[deprecated(note = "parks a blocking thread until the receiver drains; step an IndexedBlobReader instead")]
pub fn pump_block_blob_range(
    blob_path: std::path::PathBuf,
    logical_size: u64,
    range_start: u64,
    length: u64,
    ctx: IndexedReadContext,
    tx: tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
) {
    pump_blocking(
        IndexedBlobReader::open(&blob_path, logical_size, range_start, length, ctx),
        tx,
    );
}

fn pump_blocking(
    reader: Result<IndexedBlobReader, StorageError>,
    tx: tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
) {
    let fail = |e: StorageError| std::io::Error::other(e.to_string());
    let mut reader = match reader {
        Ok(reader) => reader,
        Err(e) => {
            let _ = tx.blocking_send(Err(fail(e)));
            return;
        }
    };
    loop {
        let sent = match reader.next_chunk() {
            Ok(Some(chunk)) => tx.blocking_send(Ok(chunk)),
            Ok(None) => return,
            Err(e) => {
                let _ = tx.blocking_send(Err(fail(e)));
                return;
            }
        };
        if sent.is_err() {
            return;
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

    fn read_all(mut reader: IndexedBlobReader) -> Vec<u8> {
        let mut out = Vec::new();
        while let Some(chunk) = reader.next_chunk().unwrap() {
            assert!(!chunk.is_empty());
            out.extend_from_slice(&chunk);
        }
        out
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

        let size = payload.len() as u64;
        let ctx = IndexedReadContext {
            dict: None,
            data_dir: String::new(),
            block_cache: None,
        };
        let collected = read_all(IndexedBlobReader::open(tmp.path(), size, 10_000, 50, ctx).unwrap());
        assert_eq!(collected.len(), 50);
        assert_eq!(&collected[..], &payload[10_000..10_050]);
    }

    #[test]
    fn compressed_block_decode_stops_past_logical_length() {
        let data = vec![b'a'; 1 << 20];
        let frame = zstd::bulk::compress(&data, 3).unwrap();
        assert_eq!(decode_compressed_payload(&frame, None, data.len() as u64).unwrap(), data);
        // Human: A block claiming 1 KiB whose frame expands to 1 MiB is cut at 1 KiB + 1 (caller rejects it).
        assert_eq!(decode_compressed_payload(&frame, None, 1024).unwrap().len(), 1025);
    }

    #[test]
    fn pumps_read_multi_block_files_block_by_block() {
        let payload: Vec<u8> = (0..400_000u32).map(|i| (i % 97) as u8).collect();
        let blob = compress_blob(
            &payload,
            3,
            64 * 1024,
            text_ctx(payload.len() as u64),
            EncodeOptions::default(),
        )
        .unwrap();
        assert!(parse_layout_bytes(&blob).unwrap().block_count() > 5);
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.write_all(&blob).unwrap();
        let ctx = || IndexedReadContext {
            dict: None,
            data_dir: String::new(),
            block_cache: BlockDecodeCache::new(8),
        };
        let size = payload.len() as u64;
        let full = IndexedBlobReader::open(tmp.path(), size, 0, size, ctx()).unwrap();
        assert_eq!(read_all(full), payload);

        // Human: A range spanning several block boundaries.
        let (start, len) = (60_000u64, 200_000u64);
        let range = IndexedBlobReader::open(tmp.path(), size, start, len, ctx()).unwrap();
        assert_eq!(read_all(range), &payload[start as usize..(start + len) as usize]);
        assert!(IndexedBlobReader::open(tmp.path(), size, size - 10, 11, ctx()).is_err());
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
