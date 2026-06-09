use std::fs::File;
use std::io::Read;

use super::super::error::{internal, StorageError};

// Human: v1 compressed blob header — magic + logical size + zstd payload (legacy, still readable).
// Agent: BLOB_MAGIC="NOSZ"; HEADER_LEN=12; legacy blobs without magic served raw.
pub const BLOB_MAGIC: &[u8; 4] = b"NOSZ";
pub const HEADER_LEN: usize = 12;

// Human: v2 header adds dict_id + stored zstd level for tiered recompression (legacy writes).
// Agent: BLOB_MAGIC_V2="NOS2"; HEADER_LEN_V2=16.
pub const BLOB_MAGIC_V2: &[u8; 4] = b"NOS2";
pub const HEADER_LEN_V2: usize = 16;

// Human: Dedup manifest — magic + logical size + block table pointing at `.blocks/`.
// Agent: DEDUP_MAGIC="NOSD"; entries are (hash u64, size u32) pairs.
pub const DEDUP_MAGIC: &[u8; 4] = b"NOSD";
pub const DEDUP_HEADER_LEN: usize = 16;
pub const DEDUP_ENTRY_LEN: usize = 12;

// Human: Block-compressed blobs begin with NOSB and carry a per-block seek index.
// Agent: NOSB_MAGIC="NOSB"; FIXED_HEADER_LEN=20; index follows header; block payloads after index.
pub const NOSB_MAGIC: &[u8; 4] = b"NOSB";
pub const FIXED_HEADER_LEN: usize = 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlobFormat {
    Raw,
    Nosz,
    Nos2,
    Nosd,
    Nosb,
}

pub fn detect_blob_format(data: &[u8]) -> BlobFormat {
    if data.len() >= HEADER_LEN && data.starts_with(NOSB_MAGIC) {
        return BlobFormat::Nosb;
    }
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

pub fn is_nosb_blob(data: &[u8]) -> bool {
    detect_blob_format(data) == BlobFormat::Nosb
}

pub fn is_zstd_blob(data: &[u8]) -> bool {
    matches!(
        detect_blob_format(data),
        BlobFormat::Nosz | BlobFormat::Nos2
    )
}

pub fn is_dedup_manifest(data: &[u8]) -> bool {
    detect_blob_format(data) == BlobFormat::Nosd
}
pub const INDEX_ENTRY_LEN: usize = 16;
pub const BLOCK_HEADER_LEN: usize = 8;

pub const BLOCK_COMPRESSED: u8 = 0;
pub const BLOCK_STORED: u8 = 1;

/// Human: Reject absurd index sizes from corrupt or hostile blob headers before allocation.
/// Agent: MAX_BLOCK_COUNT caps block_count in parse_layout_bytes/read_blob_layout.
const MAX_BLOCK_COUNT: usize = 16_777_216;

/// Human: Target uncompressed chunk size before per-block zstd (default 1 MiB).
/// Agent: DEFAULT_BLOCK_SIZE=1<<20; overridden by NOS_COMPRESS_BLOCK_SIZE.
pub const DEFAULT_BLOCK_SIZE: usize = 1 << 20;

/// Human: One index row mapping a logical byte range to a compressed blob offset.
/// Agent: compressed_offset relative to data_offset; logical_end is exclusive cumulative size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexEntry {
    pub compressed_offset: u64,
    pub logical_end: u64,
}

/// Human: Parsed on-disk layout for a NOSB object used by readers and range logic.
/// Agent: READS header+index; data_offset=FIXED_HEADER+index.len()*16; validates logical_size.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobLayout {
    pub logical_size: u64,
    pub block_size: u32,
    pub index: Vec<IndexEntry>,
    pub data_offset: u64,
}

impl BlobLayout {
    pub fn header_len(&self) -> usize {
        FIXED_HEADER_LEN + self.index.len() * INDEX_ENTRY_LEN
    }

    pub fn block_count(&self) -> usize {
        self.index.len()
    }

    pub fn logical_start(&self, block_idx: usize) -> u64 {
        if block_idx == 0 {
            0
        } else {
            self.index[block_idx - 1].logical_end
        }
    }

    pub fn logical_len(&self, block_idx: usize) -> u64 {
        self.index[block_idx].logical_end.saturating_sub(self.logical_start(block_idx))
    }

    /// Human: Locate the block index that contains `offset`.
    /// Agent: BINARY SEARCH on index.logical_end; clamps to last block when offset is at EOF edge.
    pub fn block_for_offset(&self, offset: u64) -> usize {
        if self.index.is_empty() {
            return 0;
        }
        let mut lo = 0usize;
        let mut hi = self.index.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if offset < self.index[mid].logical_end {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        lo.min(self.index.len().saturating_sub(1))
    }

    pub fn file_offset_for_block(&self, block_idx: usize) -> u64 {
        self.data_offset + self.index[block_idx].compressed_offset
    }
}

/// Returns true when `data` begins with a Nebular compressed blob header (NOSB, NOSZ, or NOS2).
pub fn is_compressed_blob(data: &[u8]) -> bool {
    matches!(
        detect_blob_format(data),
        BlobFormat::Nosb | BlobFormat::Nosz | BlobFormat::Nos2
    )
}

/// Human: Parse a NOSB header and index from bytes already read from disk.
/// Agent: REQUIRES magic NOSB; READS block_count; BUILDS BlobLayout; ERRORS on truncation.
pub fn parse_layout_bytes(data: &[u8]) -> Result<BlobLayout, StorageError> {
    if data.len() < FIXED_HEADER_LEN || !data.starts_with(NOSB_MAGIC) {
        return Err(internal(anyhow::anyhow!("not a block-compressed blob")));
    }
    let logical_size = u64::from_le_bytes(data[4..12].try_into().unwrap());
    let block_size = u32::from_le_bytes(data[12..16].try_into().unwrap());
    let block_count = u32::from_le_bytes(data[16..20].try_into().unwrap()) as usize;
    if block_count > MAX_BLOCK_COUNT {
        return Err(internal(anyhow::anyhow!("block count exceeds limit")));
    }
    let index_bytes = block_count
        .checked_mul(INDEX_ENTRY_LEN)
        .ok_or_else(|| internal(anyhow::anyhow!("block index overflow")))?;
    let needed = FIXED_HEADER_LEN
        .checked_add(index_bytes)
        .ok_or_else(|| internal(anyhow::anyhow!("header length overflow")))?;
    if data.len() < needed {
        return Err(internal(anyhow::anyhow!("block index truncated")));
    }
    let mut index = Vec::with_capacity(block_count);
    let mut pos = FIXED_HEADER_LEN;
    for _ in 0..block_count {
        let compressed_offset = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        let logical_end = u64::from_le_bytes(data[pos + 8..pos + 16].try_into().unwrap());
        index.push(IndexEntry {
            compressed_offset,
            logical_end,
        });
        pos += INDEX_ENTRY_LEN;
    }
    validate_index(&index, logical_size)?;
    if let Some(last) = index.last() {
        if last.logical_end != logical_size {
            return Err(internal(anyhow::anyhow!(
                "index logical_end mismatch: index={} header={logical_size}",
                last.logical_end
            )));
        }
    } else if logical_size != 0 {
        return Err(internal(anyhow::anyhow!("empty index for non-empty object")));
    }
    Ok(BlobLayout {
        logical_size,
        block_size,
        index,
        data_offset: needed as u64,
    })
}

/// Human: Read logical size and layout metadata from the start of a blob file.
/// Agent: READS FIXED_HEADER+index from file; REQUIRES NOSB; RETURNS BlobLayout.
pub fn read_blob_layout(mut file: File) -> Result<BlobLayout, StorageError> {
    let mut fixed = [0u8; FIXED_HEADER_LEN];
    file.read_exact(&mut fixed)
        .map_err(|e| internal(anyhow::anyhow!(e)))?;
    if !fixed.starts_with(NOSB_MAGIC) {
        return Err(internal(anyhow::anyhow!("not a block-compressed blob")));
    }
    let block_count = u32::from_le_bytes(fixed[16..20].try_into().unwrap()) as usize;
    if block_count > MAX_BLOCK_COUNT {
        return Err(internal(anyhow::anyhow!("block count exceeds limit")));
    }
    let index_bytes = block_count
        .checked_mul(INDEX_ENTRY_LEN)
        .ok_or_else(|| internal(anyhow::anyhow!("block index overflow")))?;
    let mut rest = vec![0u8; index_bytes];
    if index_bytes > 0 {
        file.read_exact(&mut rest)
            .map_err(|e| internal(anyhow::anyhow!(e)))?;
    }
    let mut data = Vec::with_capacity(FIXED_HEADER_LEN + index_bytes);
    data.extend_from_slice(&fixed);
    data.extend_from_slice(&rest);
    parse_layout_bytes(&data)
}

// Human: Ensure index offsets are strictly increasing so block seeks cannot go backwards.
// Agent: REQUIRES logical_end and compressed_offset rise per entry; last logical_end checked later.
fn validate_index(index: &[IndexEntry], logical_size: u64) -> Result<(), StorageError> {
    let mut prev_logical = 0u64;
    let mut prev_compressed = 0u64;
    for entry in index {
        if entry.logical_end <= prev_logical || entry.logical_end > logical_size {
            return Err(internal(anyhow::anyhow!("invalid index logical_end")));
        }
        if entry.compressed_offset < prev_compressed {
            return Err(internal(anyhow::anyhow!("invalid index compressed_offset")));
        }
        prev_logical = entry.logical_end;
        prev_compressed = entry.compressed_offset;
    }
    Ok(())
}

/// Human: Read only the logical size field from a NOSB blob header.
/// Agent: READS bytes 4..12 after magic check; used for quick header validation.
pub fn read_blob_logical_size(mut file: File) -> Result<u64, StorageError> {
    let mut header = [0u8; FIXED_HEADER_LEN];
    file.read_exact(&mut header)
        .map_err(|e| internal(anyhow::anyhow!(e)))?;
    if !header.starts_with(NOSB_MAGIC) {
        return Err(internal(anyhow::anyhow!("not a block-compressed blob")));
    }
    Ok(u64::from_le_bytes(header[4..12].try_into().unwrap()))
}

/// Human: Read the logical size field from any supported compressed blob header on disk.
/// Agent: READS first 12 bytes; NOSB/NOSZ/NOS2; ERRORS on raw or dedup manifest.
pub fn read_blob_header_size(mut file: File) -> Result<u64, StorageError> {
    let mut header = [0u8; HEADER_LEN];
    file.read_exact(&mut header)
        .map_err(|e| internal(anyhow::anyhow!(e)))?;
    match detect_blob_format(&header) {
        BlobFormat::Nosb | BlobFormat::Nosz | BlobFormat::Nos2 => {
            Ok(u64::from_le_bytes(header[4..12].try_into().unwrap()))
        }
        _ => Err(internal(anyhow::anyhow!("not a compressed blob"))),
    }
}

/// Human: Serialize header and index ahead of block payloads when writing a NOSB blob.
/// Agent: WRITES magic, logical_size, block_size, block_count, index entries LE.
pub fn write_blob_header<W: std::io::Write>(
    writer: &mut W,
    logical_size: u64,
    block_size: u32,
    index: &[IndexEntry],
) -> Result<(), StorageError> {
    writer
        .write_all(NOSB_MAGIC)
        .map_err(|e| internal(anyhow::anyhow!(e)))?;
    writer
        .write_all(&logical_size.to_le_bytes())
        .map_err(|e| internal(anyhow::anyhow!(e)))?;
    writer
        .write_all(&block_size.to_le_bytes())
        .map_err(|e| internal(anyhow::anyhow!(e)))?;
    writer
        .write_all(&(index.len() as u32).to_le_bytes())
        .map_err(|e| internal(anyhow::anyhow!(e)))?;
    for entry in index {
        writer
            .write_all(&entry.compressed_offset.to_le_bytes())
            .map_err(|e| internal(anyhow::anyhow!(e)))?;
        writer
            .write_all(&entry.logical_end.to_le_bytes())
            .map_err(|e| internal(anyhow::anyhow!(e)))?;
    }
    Ok(())
}

/// Human: Encode one block's on-disk prefix before its payload bytes.
/// Agent: WRITES type(1)+reserved(3)+payload_len(4); type 0=zstd 1=stored.
pub fn write_block_header(
    writer: &mut Vec<u8>,
    block_type: u8,
    payload_len: u32,
) -> Result<(), StorageError> {
    let mut header = [0u8; BLOCK_HEADER_LEN];
    header[0] = block_type;
    header[4..8].copy_from_slice(&payload_len.to_le_bytes());
    writer.extend_from_slice(&header);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_non_monotonic_index() {
        let index = vec![
            IndexEntry {
                compressed_offset: 0,
                logical_end: 100,
            },
            IndexEntry {
                compressed_offset: 10,
                logical_end: 50,
            },
        ];
        let mut buf = Vec::new();
        write_blob_header(&mut buf, 50, 128, &index).unwrap();
        assert!(parse_layout_bytes(&buf).is_err());
    }

    #[test]
    fn layout_roundtrip_header_bytes() {
        let index = vec![
            IndexEntry {
                compressed_offset: 0,
                logical_end: 100,
            },
            IndexEntry {
                compressed_offset: 80,
                logical_end: 250,
            },
        ];
        let mut buf = Vec::new();
        write_blob_header(&mut buf, 250, 128, &index).unwrap();
        let layout = parse_layout_bytes(&buf).unwrap();
        assert_eq!(layout.logical_size, 250);
        assert_eq!(layout.block_size, 128);
        assert_eq!(layout.index, index);
        assert_eq!(layout.data_offset, FIXED_HEADER_LEN as u64 + 32);
        assert_eq!(layout.block_for_offset(0), 0);
        assert_eq!(layout.block_for_offset(99), 0);
        assert_eq!(layout.block_for_offset(100), 1);
        assert_eq!(layout.logical_len(0), 100);
        assert_eq!(layout.logical_len(1), 150);
    }
}
