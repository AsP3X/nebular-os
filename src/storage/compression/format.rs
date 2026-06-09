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

// Human: Dedup manifest — magic + logical size + block table pointing at `.blocks/` (legacy).
// Agent: DEDUP_MAGIC="NOSD"; entries are (hash u64, size u32) pairs.
pub const DEDUP_MAGIC: &[u8; 4] = b"NOSD";
pub const DEDUP_HEADER_LEN: usize = 16;
pub const DEDUP_ENTRY_LEN: usize = 12;

// Human: Block-compressed blobs v0 — NOSB magic, 20-byte header, 8-byte block headers.
pub const NOSB_MAGIC: &[u8; 4] = b"NOSB";
pub const FIXED_HEADER_LEN: usize = 20;
pub const BLOCK_HEADER_LEN: usize = 8;

// Human: Indexed blobs v1 — NOSI magic, dict_id, per-block checksums, optional dedup refs.
// Agent: NOSI_MAGIC="NOSI"; FIXED_HEADER_LEN_V1=24; BLOCK_HEADER_LEN_V1=16.
pub const NOSI_MAGIC: &[u8; 4] = b"NOSI";
pub const FIXED_HEADER_LEN_V1: usize = 24;
pub const BLOCK_HEADER_LEN_V1: usize = 16;

pub const BLOCK_COMPRESSED: u8 = 0;
pub const BLOCK_STORED: u8 = 1;
pub const BLOCK_DEDUP_REF: u8 = 2;

pub const NOSI_FLAG_DEDUP: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlobFormat {
    Raw,
    Nosz,
    Nos2,
    Nosd,
    Nosb,
    Nosi,
}

pub fn detect_blob_format(data: &[u8]) -> BlobFormat {
    if data.len() >= HEADER_LEN && data.starts_with(NOSI_MAGIC) {
        return BlobFormat::Nosi;
    }
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

pub fn is_indexed_blob(data: &[u8]) -> bool {
    matches!(
        detect_blob_format(data),
        BlobFormat::Nosb | BlobFormat::Nosi
    )
}

pub fn is_nosb_blob(data: &[u8]) -> bool {
    is_indexed_blob(data)
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

/// Human: Reject absurd index sizes from corrupt or hostile blob headers before allocation.
const MAX_BLOCK_COUNT: usize = 16_777_216;

/// Human: Target uncompressed chunk size before per-block zstd (default 1 MiB).
pub const DEFAULT_BLOCK_SIZE: usize = 1 << 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexEntry {
    pub compressed_offset: u64,
    pub logical_end: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexedFormat {
    V0,
    V1,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobLayout {
    pub format: IndexedFormat,
    pub logical_size: u64,
    pub block_size: u32,
    pub dict_id: u16,
    pub flags: u16,
    pub index: Vec<IndexEntry>,
    pub data_offset: u64,
}

impl BlobLayout {
    pub fn fixed_header_len(&self) -> usize {
        match self.format {
            IndexedFormat::V0 => FIXED_HEADER_LEN,
            IndexedFormat::V1 => FIXED_HEADER_LEN_V1,
        }
    }

    pub fn block_header_len(&self) -> usize {
        match self.format {
            IndexedFormat::V0 => BLOCK_HEADER_LEN,
            IndexedFormat::V1 => BLOCK_HEADER_LEN_V1,
        }
    }

    pub fn header_len(&self) -> usize {
        self.fixed_header_len() + self.index.len() * INDEX_ENTRY_LEN
    }

    pub fn block_count(&self) -> usize {
        self.index.len()
    }

    pub fn uses_dedup_blocks(&self) -> bool {
        self.format == IndexedFormat::V1 && (self.flags & NOSI_FLAG_DEDUP) != 0
    }

    pub fn logical_start(&self, block_idx: usize) -> u64 {
        if block_idx == 0 {
            0
        } else {
            self.index[block_idx - 1].logical_end
        }
    }

    pub fn logical_len(&self, block_idx: usize) -> u64 {
        self.index[block_idx]
            .logical_end
            .saturating_sub(self.logical_start(block_idx))
    }

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

#[derive(Debug, Clone, Copy)]
pub struct ParsedBlockHeader {
    pub block_type: u8,
    pub payload_len: u32,
    pub logical_checksum: Option<u64>,
    pub header_len: usize,
}

pub fn read_indexed_dict_id(data: &[u8]) -> Option<u16> {
    if detect_blob_format(data) == BlobFormat::Nosi && data.len() >= FIXED_HEADER_LEN_V1 {
        return Some(u16::from_le_bytes([data[20], data[21]]));
    }
    None
}

/// Returns true when `data` begins with a Nebular compressed blob header (NOSB/NOSI, NOSZ, or NOS2).
pub fn is_compressed_blob(data: &[u8]) -> bool {
    matches!(
        detect_blob_format(data),
        BlobFormat::Nosi | BlobFormat::Nosb | BlobFormat::Nosz | BlobFormat::Nos2
    )
}

fn parse_index(data: &[u8], fixed_len: usize, logical_size: u64) -> Result<Vec<IndexEntry>, StorageError> {
    if data.len() < fixed_len + 4 {
        return Err(internal(anyhow::anyhow!("block index truncated")));
    }
    let block_count = u32::from_le_bytes(data[16..20].try_into().unwrap()) as usize;
    if block_count > MAX_BLOCK_COUNT {
        return Err(internal(anyhow::anyhow!("block count exceeds limit")));
    }
    let index_bytes = block_count
        .checked_mul(INDEX_ENTRY_LEN)
        .ok_or_else(|| internal(anyhow::anyhow!("block index overflow")))?;
    let needed = fixed_len
        .checked_add(index_bytes)
        .ok_or_else(|| internal(anyhow::anyhow!("header length overflow")))?;
    if data.len() < needed {
        return Err(internal(anyhow::anyhow!("block index truncated")));
    }
    let mut index = Vec::with_capacity(block_count);
    let mut pos = fixed_len;
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
    Ok(index)
}

pub fn parse_layout_bytes(data: &[u8]) -> Result<BlobLayout, StorageError> {
    let format = detect_blob_format(data);
    match format {
        BlobFormat::Nosi => {
            if data.len() < FIXED_HEADER_LEN_V1 {
                return Err(internal(anyhow::anyhow!("not an indexed blob")));
            }
            let logical_size = u64::from_le_bytes(data[4..12].try_into().unwrap());
            let block_size = u32::from_le_bytes(data[12..16].try_into().unwrap());
            let dict_id = u16::from_le_bytes([data[20], data[21]]);
            let flags = u16::from_le_bytes([data[22], data[23]]);
            let index = parse_index(data, FIXED_HEADER_LEN_V1, logical_size)?;
            let needed = FIXED_HEADER_LEN_V1 + index.len() * INDEX_ENTRY_LEN;
            Ok(BlobLayout {
                format: IndexedFormat::V1,
                logical_size,
                block_size,
                dict_id,
                flags,
                index,
                data_offset: needed as u64,
            })
        }
        BlobFormat::Nosb => {
            if data.len() < FIXED_HEADER_LEN {
                return Err(internal(anyhow::anyhow!("not a block-compressed blob")));
            }
            let logical_size = u64::from_le_bytes(data[4..12].try_into().unwrap());
            let block_size = u32::from_le_bytes(data[12..16].try_into().unwrap());
            let index = parse_index(data, FIXED_HEADER_LEN, logical_size)?;
            let needed = FIXED_HEADER_LEN + index.len() * INDEX_ENTRY_LEN;
            Ok(BlobLayout {
                format: IndexedFormat::V0,
                logical_size,
                block_size,
                dict_id: 0,
                flags: 0,
                index,
                data_offset: needed as u64,
            })
        }
        _ => Err(internal(anyhow::anyhow!("not an indexed blob"))),
    }
}

pub fn read_blob_layout(mut file: File) -> Result<BlobLayout, StorageError> {
    let mut peek = [0u8; FIXED_HEADER_LEN_V1];
    file.read_exact(&mut peek)
        .map_err(|e| internal(anyhow::anyhow!(e)))?;
    let format = detect_blob_format(&peek);
    let fixed_len = match format {
        BlobFormat::Nosi => FIXED_HEADER_LEN_V1,
        BlobFormat::Nosb => FIXED_HEADER_LEN,
        _ => return Err(internal(anyhow::anyhow!("not an indexed blob"))),
    };
    let block_count = u32::from_le_bytes(peek[16..20].try_into().unwrap()) as usize;
    if block_count > MAX_BLOCK_COUNT {
        return Err(internal(anyhow::anyhow!("block count exceeds limit")));
    }
    let index_bytes = block_count * INDEX_ENTRY_LEN;
    let mut rest = vec![0u8; index_bytes];
    if index_bytes > 0 {
        file.read_exact(&mut rest)
            .map_err(|e| internal(anyhow::anyhow!(e)))?;
    }
    let mut data = Vec::with_capacity(fixed_len + index_bytes);
    data.extend_from_slice(&peek[..fixed_len]);
    data.extend_from_slice(&rest);
    parse_layout_bytes(&data)
}

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

pub fn read_blob_logical_size(mut file: File) -> Result<u64, StorageError> {
    let mut header = [0u8; HEADER_LEN];
    file.read_exact(&mut header)
        .map_err(|e| internal(anyhow::anyhow!(e)))?;
    match detect_blob_format(&header) {
        BlobFormat::Nosi | BlobFormat::Nosb | BlobFormat::Nosz | BlobFormat::Nos2 => {
            Ok(u64::from_le_bytes(header[4..12].try_into().unwrap()))
        }
        _ => Err(internal(anyhow::anyhow!("not a compressed blob"))),
    }
}

pub fn read_blob_header_size(file: File) -> Result<u64, StorageError> {
    read_blob_logical_size(file)
}

#[cfg(test)]
pub fn write_blob_header_v0<W: std::io::Write>(
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

pub fn write_blob_header_v1<W: std::io::Write>(
    writer: &mut W,
    logical_size: u64,
    block_size: u32,
    index: &[IndexEntry],
    dict_id: u16,
    flags: u16,
) -> Result<(), StorageError> {
    writer
        .write_all(NOSI_MAGIC)
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
    writer
        .write_all(&dict_id.to_le_bytes())
        .map_err(|e| internal(anyhow::anyhow!(e)))?;
    writer
        .write_all(&flags.to_le_bytes())
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

#[cfg(test)]
pub fn write_block_header_v0(writer: &mut Vec<u8>, block_type: u8, payload_len: u32) -> Result<(), StorageError> {
    let mut header = [0u8; BLOCK_HEADER_LEN];
    header[0] = block_type;
    header[4..8].copy_from_slice(&payload_len.to_le_bytes());
    writer.extend_from_slice(&header);
    Ok(())
}

pub fn write_block_header_v1(
    writer: &mut Vec<u8>,
    block_type: u8,
    payload_len: u32,
    logical_checksum: u64,
) -> Result<(), StorageError> {
    let mut header = [0u8; BLOCK_HEADER_LEN_V1];
    header[0] = block_type;
    header[4..8].copy_from_slice(&payload_len.to_le_bytes());
    header[8..16].copy_from_slice(&logical_checksum.to_le_bytes());
    writer.extend_from_slice(&header);
    Ok(())
}

pub fn parse_block_header_at(blob: &[u8], offset: usize, layout: &BlobLayout) -> Result<ParsedBlockHeader, StorageError> {
    let header_len = layout.block_header_len();
    if blob.len() < offset + header_len {
        return Err(internal(anyhow::anyhow!("block header truncated")));
    }
    let header = &blob[offset..offset + header_len];
    let block_type = header[0];
    let payload_len = u32::from_le_bytes(header[4..8].try_into().unwrap());
    let logical_checksum = match layout.format {
        IndexedFormat::V0 => None,
        IndexedFormat::V1 => Some(u64::from_le_bytes(header[8..16].try_into().unwrap())),
    };
    Ok(ParsedBlockHeader {
        block_type,
        payload_len,
        logical_checksum,
        header_len,
    })
}

/// Human: Collect dedup block refs from an indexed blob (NOSI with dedup flag or legacy NOSD).
pub fn collect_dedup_refs(data: &[u8]) -> Result<Vec<(u64, u32)>, StorageError> {
    let format = detect_blob_format(data);
    if format == BlobFormat::Nosd {
        let logical = u64::from_le_bytes(data[4..12].try_into().unwrap());
        return super::legacy::parse_dedup_manifest(data, logical);
    }
    if format != BlobFormat::Nosi {
        return Ok(vec![]);
    }
    let layout = parse_layout_bytes(data)?;
    if !layout.uses_dedup_blocks() {
        return Ok(vec![]);
    }
    let mut refs = Vec::new();
    for idx in 0..layout.block_count() {
        let file_offset = layout.file_offset_for_block(idx) as usize;
        let parsed = parse_block_header_at(data, file_offset, &layout)?;
        if parsed.block_type != BLOCK_DEDUP_REF {
            continue;
        }
        let payload_start = file_offset + parsed.header_len;
        let payload_end = payload_start + parsed.payload_len as usize;
        if data.len() < payload_end {
            return Err(internal(anyhow::anyhow!("dedup ref truncated")));
        }
        let payload = &data[payload_start..payload_end];
        if payload.len() != 12 {
            return Err(internal(anyhow::anyhow!("invalid dedup ref payload")));
        }
        let hash = u64::from_le_bytes(payload[0..8].try_into().unwrap());
        let size = u32::from_le_bytes(payload[8..12].try_into().unwrap());
        refs.push((hash, size));
    }
    Ok(refs)
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
        write_blob_header_v0(&mut buf, 50, 128, &index).unwrap();
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
        write_blob_header_v0(&mut buf, 250, 128, &index).unwrap();
        let layout = parse_layout_bytes(&buf).unwrap();
        assert_eq!(layout.logical_size, 250);
        assert_eq!(layout.block_size, 128);
        assert_eq!(layout.index, index);
        assert_eq!(layout.data_offset, FIXED_HEADER_LEN as u64 + 32);
    }

    #[test]
    fn nosi_header_roundtrip() {
        let index = vec![IndexEntry {
            compressed_offset: 0,
            logical_end: 64,
        }];
        let mut buf = Vec::new();
        write_blob_header_v1(&mut buf, 64, 4096, &index, 3, NOSI_FLAG_DEDUP).unwrap();
        let layout = parse_layout_bytes(&buf).unwrap();
        assert_eq!(layout.format, IndexedFormat::V1);
        assert_eq!(layout.dict_id, 3);
        assert!(layout.uses_dedup_blocks());
    }
}
