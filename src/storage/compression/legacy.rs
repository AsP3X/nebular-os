use super::super::error::{internal, StorageError};
use super::format::{
    detect_blob_format, BlobFormat, DEDUP_ENTRY_LEN, DEDUP_HEADER_LEN, HEADER_LEN, HEADER_LEN_V2,
};

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

pub fn decompress_zstd_blob(
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
        _ => {
            return Err(internal(anyhow::anyhow!("not a legacy zstd blob")));
        }
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

pub fn parse_dedup_manifest(
    data: &[u8],
    expected_logical: u64,
) -> Result<Vec<(u64, u32)>, StorageError> {
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
