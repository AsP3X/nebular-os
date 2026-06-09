mod decode;
mod encode;
mod format;

pub use decode::{
    decompress_blob, decompress_file_to_temp, pump_block_blob_full, pump_block_blob_range,
};
pub use encode::{
    clamp_zstd_level, compress_blob, compress_file_to_storage, default_block_size,
    encode_blob_for_storage, DEFAULT_ZSTD_LEVEL,
};
pub use format::{
    is_compressed_blob, parse_layout_bytes, read_blob_layout, read_blob_logical_size,
    BlobLayout, BLOB_MAGIC, DEFAULT_BLOCK_SIZE, FIXED_HEADER_LEN,
};

// Human: Legacy alias kept for streaming code that peeks at the fixed header length.
// Agent: HEADER_LEN=FIXED_HEADER_LEN (20); was 12 under NOSZ whole-object format.
pub const HEADER_LEN: usize = format::FIXED_HEADER_LEN;

/// Human: Read logical size from a NOSB blob file header.
/// Agent: CALLS read_blob_logical_size; ERRORS when magic is not NOSB.
pub fn read_blob_header_size(file: std::fs::File) -> Result<u64, super::error::StorageError> {
    read_blob_logical_size(file)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::compressibility::DEFAULT_MIN_COMPRESSIBLE_SIZE;
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
    fn roundtrip_compresses_and_restores() {
        let original = b"hello world ".repeat(500);
        let blob = compress_blob(
            &original,
            DEFAULT_ZSTD_LEVEL,
            64 * 1024,
            text_ctx(original.len() as u64),
        )
        .unwrap();
        assert!(is_compressed_blob(&blob));
        assert!(blob.len() < original.len());
        let restored = decompress_blob(&blob, original.len() as u64).unwrap();
        assert_eq!(restored, original);
    }

    #[test]
    fn raw_blob_passes_through_decoder() {
        let raw = b"legacy uncompressed payload";
        let restored = decompress_blob(raw, raw.len() as u64).unwrap();
        assert_eq!(restored, raw);
        assert!(!is_compressed_blob(raw));
    }

    #[test]
    fn incompressible_payload_stays_raw() {
        let payload = b"x".to_vec();
        let stored = encode_blob_for_storage(
            &payload,
            DEFAULT_ZSTD_LEVEL,
            DEFAULT_BLOCK_SIZE,
            text_ctx(payload.len() as u64),
        )
        .unwrap();
        assert!(!is_compressed_blob(&stored));
        assert_eq!(stored, payload);
    }

    #[test]
    fn excluded_media_extension_stays_raw() {
        let payload = b"fake mp3 payload ".repeat(400);
        let ctx = CompressionContext::new(
            Some("music/song.mp3"),
            Some("audio/mpeg"),
            payload.len() as u64,
            DEFAULT_MIN_COMPRESSIBLE_SIZE,
        );
        let stored = encode_blob_for_storage(&payload, DEFAULT_ZSTD_LEVEL, DEFAULT_BLOCK_SIZE, ctx)
            .unwrap();
        assert!(!is_compressed_blob(&stored));
        assert_eq!(stored, payload);
    }

    #[test]
    fn multi_block_layout_has_multiple_index_entries() {
        let payload = vec![b'a'; 200_000];
        let blob = compress_blob(
            &payload,
            3,
            64 * 1024,
            text_ctx(payload.len() as u64),
        )
        .unwrap();
        let layout = parse_layout_bytes(&blob).unwrap();
        assert!(layout.block_count() >= 3);
    }
}
