mod decode;
mod encode;
mod format;
mod legacy;

pub use decode::{
    decompress_blob, decompress_file_to_temp, pump_block_blob_full, pump_block_blob_range,
    verify_indexed_blob, IndexedReadContext,
};
pub use encode::{
    clamp_zstd_level, compress_blob, compress_file_to_storage, default_block_size,
    encode_blob_for_storage, EncodeOptions, DEFAULT_ZSTD_LEVEL,
};
pub use format::{
    collect_dedup_refs, detect_blob_format, is_compressed_blob, is_dedup_manifest,
    is_indexed_blob, is_nosb_blob, is_zstd_blob, parse_layout_bytes, read_blob_header_size,
    read_blob_layout, read_blob_logical_size,     read_blob_stored_zstd_level, read_indexed_dict_id, read_indexed_zstd_level, BlobFormat,
    BlobLayout, IndexedFormat, BLOB_MAGIC, BLOB_MAGIC_V2, DEDUP_MAGIC, DEDUP_ENTRY_LEN,
    DEDUP_HEADER_LEN, BLOCK_DEDUP_REF, FIXED_HEADER_LEN, FIXED_HEADER_LEN_V1,
    FIXED_HEADER_LEN_V1_LEVEL, HEADER_LEN, HEADER_LEN_V2, NOSB_MAGIC, NOSI_MAGIC,
    NOSI_FLAG_HAS_LEVEL, DEFAULT_BLOCK_SIZE,
};
pub use legacy::{
    parse_dedup_manifest, read_stored_dict_id, read_stored_zstd_level, zstd_payload_offset,
};

pub const DEFAULT_ZSTD_LEVEL_UPLOAD: i32 = 3;

#[derive(Debug, Clone, Copy)]
pub struct RuntimeCompressParams<'a> {
    pub level: i32,
    pub dict_id: u16,
    pub dict: Option<&'a [u8]>,
}

impl<'a> RuntimeCompressParams<'a> {
    pub fn new(level: i32) -> Self {
        Self {
            level,
            dict_id: 0,
            dict: None,
        }
    }

    pub fn with_dict(level: i32, dict_id: u16, dict: Option<&'a [u8]>) -> Self {
        Self {
            level,
            dict_id,
            dict,
        }
    }
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
            &[],
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
            EncodeOptions::default(),
        )
        .unwrap();
        assert!(is_indexed_blob(&blob));
        assert_eq!(detect_blob_format(&blob), BlobFormat::Nosi);
        assert!(blob.len() < original.len());
        let restored = decompress_blob(&blob, original.len() as u64, None, None).unwrap();
        assert_eq!(restored, original);
    }

    #[test]
    fn legacy_nosz_still_readable() {
        let original = b"legacy nosz ".repeat(100);
        let mut blob = Vec::new();
        blob.extend_from_slice(BLOB_MAGIC);
        blob.extend_from_slice(&(original.len() as u64).to_le_bytes());
        blob.extend_from_slice(&zstd::encode_all(&original[..], DEFAULT_ZSTD_LEVEL).unwrap());
        assert_eq!(detect_blob_format(&blob), BlobFormat::Nosz);
        let restored = decompress_blob(&blob, original.len() as u64, None, None).unwrap();
        assert_eq!(restored, original);
    }

    #[test]
    fn raw_blob_passes_through_decoder() {
        let raw = b"legacy uncompressed payload";
        let restored = decompress_blob(raw, raw.len() as u64, None, None).unwrap();
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
            EncodeOptions::default(),
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
            &[],
        );
        let stored = encode_blob_for_storage(
            &payload,
            DEFAULT_ZSTD_LEVEL,
            DEFAULT_BLOCK_SIZE,
            ctx,
            EncodeOptions::default(),
        )
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
            EncodeOptions::default(),
        )
        .unwrap();
        let layout = parse_layout_bytes(&blob).unwrap();
        assert!(layout.block_count() >= 3);
    }
}
