use super::error::{internal, StorageError};

// Human: Every stored blob is prefixed with a magic tag and logical size so reads can tell compressed from legacy raw files.
// Agent: BLOB_MAGIC="NOSZ"; HEADER_LEN=12 (magic + uncompressed_size u64 LE); legacy blobs without magic are served raw.
pub const BLOB_MAGIC: &[u8; 4] = b"NOSZ";
pub const HEADER_LEN: usize = 12;

// Human: zstd level 22 is the maximum supported level and gives the smallest on-disk footprint at the cost of CPU on write.
// Agent: ZSTD_LEVEL=22; used on every final blob write; metadata size and etag stay logical/uncompressed.
const ZSTD_LEVEL: i32 = 22;

/// Returns true when `data` begins with the Nebular compressed-blob header.
pub fn is_compressed_blob(data: &[u8]) -> bool {
    data.len() >= HEADER_LEN && data.starts_with(BLOB_MAGIC)
}

// Human: Compress arbitrary bytes with zstd max level and wrap them in the Nebular blob header.
// Agent: WRITES magic+uncompressed_size LE + zstd payload; INPUT logical bytes; OUTPUT on-disk blob bytes.
pub fn compress_blob(uncompressed: &[u8]) -> Result<Vec<u8>, StorageError> {
    let mut out = Vec::with_capacity(HEADER_LEN + uncompressed.len() / 2 + 64);
    out.extend_from_slice(BLOB_MAGIC);
    out.extend_from_slice(&(uncompressed.len() as u64).to_le_bytes());

    let compressed = zstd::encode_all(uncompressed, ZSTD_LEVEL).map_err(internal)?;
    out.extend_from_slice(&compressed);
    Ok(out)
}

// Human: Pick zstd-wrapped storage when smaller than raw; otherwise keep bytes unwrapped for incompressible payloads.
// Agent: CALLS compress_blob; IF compressed.len < raw.len THEN NOSZ ELSE raw Vec (no header).
pub fn encode_blob_for_storage(uncompressed: &[u8]) -> Result<Vec<u8>, StorageError> {
    let compressed = compress_blob(uncompressed)?;
    if compressed.len() < uncompressed.len() {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_compresses_and_restores() {
        let original = b"hello world ".repeat(500);
        let blob = compress_blob(&original).unwrap();
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
        let compressed = compress_blob(&payload).unwrap();
        assert!(compressed.len() > payload.len());
        let stored = encode_blob_for_storage(&payload).unwrap();
        assert!(!is_compressed_blob(&stored));
        assert_eq!(stored, payload);
    }
}
