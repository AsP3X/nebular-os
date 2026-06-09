//! Heuristics for deciding when on-disk blob compression is worth attempting.

/// Human: Skip compression below this size because header overhead usually wins.
/// Agent: DEFAULT_MIN_COMPRESSIBLE_SIZE=4096; overridable via NOS_COMPRESS_MIN_SIZE.
pub const DEFAULT_MIN_COMPRESSIBLE_SIZE: usize = 4096;

/// Human: Inputs used to decide whether we should spend CPU on zstd for a blob.
/// Agent: READS object_key, content_type, logical_size, min_size; no I/O.
#[derive(Debug, Clone, Copy)]
pub struct CompressionContext<'a> {
    pub object_key: Option<&'a str>,
    pub content_type: Option<&'a str>,
    pub logical_size: u64,
    pub min_size: usize,
}

impl<'a> CompressionContext<'a> {
    pub fn new(
        object_key: Option<&'a str>,
        content_type: Option<&'a str>,
        logical_size: u64,
        min_size: usize,
    ) -> Self {
        Self {
            object_key,
            content_type,
            logical_size,
            min_size: min_size.max(1),
        }
    }
}

// Human: File suffixes that are almost always already compressed or entropy-coded.
// Agent: LOWERCASE suffix match on object key; skips zstd attempt when suffix hits.
const EXCLUDED_EXTENSIONS: &[&str] = &[
    "gz", "bz2", "rar", "zip", "7z", "xz", "zst", "lz4", "br", "tgz",
    "jpg", "jpeg", "png", "gif", "webp", "avif", "heic", "heif", "jxl",
    "mp4", "mkv", "mov", "avi", "wmv", "flv", "webm", "m4v", "mpeg", "mpg",
    "mp3", "aac", "ogg", "flac", "wma", "m4a", "opus",
    "pdf", "docx", "xlsx", "pptx",
    "deb", "rpm", "jar", "war", "apk",
    "woff", "woff2",
];

// Human: MIME families that should not be re-compressed at rest.
// Agent: WILDCARD patterns like video/*; exact matches for archives and fonts.
const EXCLUDED_CONTENT_TYPES: &[&str] = &[
    "video/*",
    "audio/*",
    "image/*",
    "application/zip",
    "application/gzip",
    "application/x-gzip",
    "application/x-zip-compressed",
    "application/x-rar-compressed",
    "application/x-7z-compressed",
    "application/x-bzip",
    "application/x-bzip2",
    "application/x-xz",
    "application/zstd",
    "application/x-zstd",
    "application/x-tar",
    "application/tar",
    "application/pdf",
    "application/wasm",
    "font/*",
];

/// Human: Decide whether metadata alone says compression is worthwhile.
/// Agent: FALSE when size<min, excluded extension, or excluded content-type.
pub fn should_attempt_compression(ctx: CompressionContext<'_>) -> bool {
    if ctx.logical_size < ctx.min_size as u64 {
        return false;
    }
    if let Some(key) = ctx.object_key {
        if extension_is_excluded(key) {
            return false;
        }
    }
    if let Some(ct) = ctx.content_type {
        if content_type_is_excluded(ct) {
            return false;
        }
    }
    true
}

/// Human: Peek at the first bytes of a temp file to catch mislabeled binary payloads.
/// Agent: READS up to 16 prefix bytes; TRUE for known archive/media magic sequences.
pub fn prefix_looks_incompressible(head: &[u8]) -> bool {
    if head.starts_with(&[0x1F, 0x8B]) {
        return true;
    }
    if head.starts_with(&[0x50, 0x4B, 0x03, 0x04]) || head.starts_with(&[0x50, 0x4B, 0x05, 0x06]) {
        return true;
    }
    if head.starts_with(&[0x28, 0xB5, 0x2F, 0xFD]) {
        return true;
    }
    if head.starts_with(&[0x89, 0x50, 0x4E, 0x47]) {
        return true;
    }
    if head.starts_with(&[0xFF, 0xD8, 0xFF]) {
        return true;
    }
    if head.starts_with(b"GIF8") {
        return true;
    }
    if head.starts_with(b"ID3") {
        return true;
    }
    if head.len() >= 12 && &head[4..8] == b"ftyp" {
        return true;
    }
    if head.len() >= 12 && head.starts_with(b"RIFF") && &head[8..12] == b"WEBP" {
        return true;
    }
    false
}

fn extension_is_excluded(key: &str) -> bool {
    let Some(ext) = object_extension(key) else {
        return false;
    };
    let ext = ext.to_ascii_lowercase();
    EXCLUDED_EXTENSIONS.iter().any(|candidate| *candidate == ext)
}

fn content_type_is_excluded(content_type: &str) -> bool {
    let ct = content_type.trim().to_ascii_lowercase();
    if ct.is_empty() {
        return false;
    }
    EXCLUDED_CONTENT_TYPES
        .iter()
        .any(|pattern| content_type_matches_pattern(&ct, pattern))
}

fn content_type_matches_pattern(content_type: &str, pattern: &str) -> bool {
    let pattern = pattern.trim().to_ascii_lowercase();
    if let Some(prefix) = pattern.strip_suffix("/*") {
        content_type.starts_with(prefix)
            && (content_type.len() == prefix.len()
                || content_type.as_bytes().get(prefix.len()) == Some(&b'/'))
    } else {
        content_type == pattern
    }
}

fn object_extension(key: &str) -> Option<&str> {
    let segment = key.rsplit('/').next()?;
    let (base, ext) = segment.rsplit_once('.')?;
    if base.is_empty() || ext.is_empty() {
        None
    } else {
        Some(ext)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tiny_objects_are_skipped() {
        let ctx = CompressionContext::new(Some("notes.txt"), Some("text/plain"), 100, 4096);
        assert!(!should_attempt_compression(ctx));
    }

    #[test]
    fn text_payloads_remain_eligible() {
        let ctx = CompressionContext::new(Some("notes.txt"), Some("text/plain"), 8192, 4096);
        assert!(should_attempt_compression(ctx));
    }

    #[test]
    fn media_extensions_are_excluded() {
        let ctx = CompressionContext::new(Some("album/track.mp3"), None, 8192, 4096);
        assert!(!should_attempt_compression(ctx));
    }

    #[test]
    fn media_content_types_are_excluded() {
        let ctx = CompressionContext::new(Some("clip.bin"), Some("video/mp4"), 8192, 4096);
        assert!(!should_attempt_compression(ctx));
    }

    #[test]
    fn magic_prefix_detects_png_and_gzip() {
        assert!(prefix_looks_incompressible(&[0x89, 0x50, 0x4E, 0x47, 0x0D]));
        assert!(prefix_looks_incompressible(&[0x1F, 0x8B, 0x08]));
        assert!(!prefix_looks_incompressible(b"plain text"));
    }
}
