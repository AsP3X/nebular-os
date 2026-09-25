use std::path::{Path, PathBuf};
use std::sync::OnceLock;

/// Whether keys become portable filenames (see `encode_portable`); set once from the data directory's filesystem.
static PORTABLE_FILENAMES: OnceLock<bool> = OnceLock::new();

/// Human: Decide how keys become filenames for this process: portable names on filesystems that ignore case
/// (macOS by default, Windows) and always on Windows, plain names elsewhere. The first engine decides.
/// Agent: Plain names are unchanged from earlier releases; reads look for both forms (`blob_path_variants`).
pub fn configure_filenames_for(data_dir: &Path) -> std::io::Result<bool> {
    if let Some(portable) = PORTABLE_FILENAMES.get() {
        return Ok(*portable);
    }
    let portable = cfg!(windows) || filesystem_ignores_case(data_dir)?;
    Ok(*PORTABLE_FILENAMES.get_or_init(|| portable))
}

fn portable_filenames() -> bool {
    *PORTABLE_FILENAMES.get().unwrap_or(&cfg!(windows))
}

/// True when `dir`'s filesystem treats names differing only in case as the same file.
fn filesystem_ignores_case(dir: &Path) -> std::io::Result<bool> {
    let id = uuid::Uuid::new_v4().simple().to_string();
    let upper = dir.join(format!("NOS-CASE-PROBE-{id}"));
    let lower = dir.join(format!("nos-case-probe-{id}"));
    std::fs::write(&upper, b"")?;
    let folds = lower.exists();
    let _ = std::fs::remove_file(&upper);
    Ok(folds)
}

/// Filename of an object's blob: plain (`/` and `%` escaped) or, where the filesystem needs it, portable.
pub fn encode_blob_filename(key: &str) -> String {
    if portable_filenames() {
        encode_portable(key)
    } else {
        encode_plain(key)
    }
}

/// Human: The filename scheme of every release so far: `/` and `%` percent-encoded, everything else kept.
fn encode_plain(key: &str) -> String {
    let mut out = String::with_capacity(key.len());
    for ch in key.chars() {
        match ch {
            '/' => out.push_str("%2F"),
            '%' => out.push_str("%25"),
            c => out.push(c),
        }
    }
    out
}

/// Human: A filename two different keys can't share on case-insensitive or Windows filesystems: lowercase
/// ASCII and harmless punctuation are kept; uppercase letters, non-ASCII bytes (case- and normalization-folded
/// by those filesystems), control and Windows-invalid characters (`<>:"/\|?*`), a trailing dot or space and
/// the first letter of a reserved device name (`con`, `nul`, `com1`, …) are percent-encoded.
fn encode_portable(key: &str) -> String {
    let bytes = key.as_bytes();
    let reserved = is_windows_device_name(key);
    let mut out = String::with_capacity(bytes.len());
    for (i, &b) in bytes.iter().enumerate() {
        let trailing = i + 1 == bytes.len() && matches!(b, b'.' | b' ');
        let keep = !(trailing || (i == 0 && reserved))
            && (b.is_ascii_lowercase()
                || b.is_ascii_digit()
                || b" -_.()[]{}+,;=@!#$&'~^`".contains(&b));
        if keep {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn is_windows_device_name(key: &str) -> bool {
    let stem = key.split('.').next().unwrap_or_default().to_ascii_uppercase();
    matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || ((stem.starts_with("COM") || stem.starts_with("LPT"))
            && stem.len() == 4
            && stem.as_bytes()[3].is_ascii_digit())
}

/// Human: Decode a blob filename of either scheme: `%XX` sequences become their bytes, anything else is kept.
pub fn decode_blob_filename(encoded: &str) -> String {
    let bytes = encoded.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && let Some(hex) = encoded.get(i + 1..i + 3)
            && let Ok(byte) = u8::from_str_radix(hex, 16)
        {
            out.push(byte);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

pub fn hash_prefix(key: &str) -> String {
    let hash = xxhash_rust::xxh3::xxh3_64(key.as_bytes());
    format!("{:02x}", hash & 0xFF)
}

/// On-disk path for new writes: `{base}/{bucket}/{shard}/{encoded_key}`.
pub fn blob_path(base: &str, bucket: &str, key: &str) -> PathBuf {
    let prefix = hash_prefix(key);
    PathBuf::from(base)
        .join(bucket)
        .join(prefix)
        .join(encode_blob_filename(key))
}

/// Legacy nested layout kept for read/delete fallback on existing deployments.
pub fn blob_path_legacy(base: &str, bucket: &str, key: &str) -> PathBuf {
    let prefix = hash_prefix(key);
    PathBuf::from(base).join(bucket).join(prefix).join(key)
}

/// Relative path stored in metadata (`bucket/shard/filename`).
pub fn blob_rel_path(bucket: &str, key: &str) -> String {
    let prefix = hash_prefix(key);
    format!("{bucket}/{prefix}/{}", encode_blob_filename(key))
}

/// Human: Candidate on-disk paths for a key: the current filename first, then the other filename scheme (a data
/// directory written on a filesystem that ignores case, or on one that doesn't, stays readable after moving to
/// the other kind), then the legacy nested layout. Check existence with `existing_blob_paths` /
/// `first_existing_blob_path`: on a filesystem that ignores case a fallback can name another key's file.
pub fn blob_path_variants(base: &str, bucket: &str, key: &str) -> Vec<PathBuf> {
    let shard_dir = PathBuf::from(base).join(bucket).join(hash_prefix(key));
    let mut variants = vec![shard_dir.join(encode_blob_filename(key))];
    for name in [encode_plain(key), encode_portable(key)] {
        let path = shard_dir.join(name);
        if !variants.contains(&path) {
            variants.push(path);
        }
    }
    if key.contains('/') {
        let legacy = blob_path_legacy(base, bucket, key);
        if !variants.contains(&legacy) {
            variants.push(legacy);
        }
    }
    variants
}

/// Recover the logical object key from a path relative to the bucket directory (`{shard}/...`).
pub fn object_key_from_blob_relpath(rel: &str) -> Option<String> {
    let (_, tail) = rel.split_once('/')?;
    if tail.is_empty() {
        return None;
    }
    let key = if tail.contains('/') {
        tail.to_string()
    } else {
        decode_blob_filename(tail)
    };
    Some(key)
}

/// Human: Whether the names of a fallback path below the shard directory are on disk exactly as written. On a
/// filesystem that ignores case, "Photo.jpg" also opens "photo.jpg" — another key's blob — so each name must match
/// a directory entry byte for byte there. Elsewhere any existing path is exact.
/// Agent: READS each directory from `shard_dir` down to the file (one for flat names); TRUE without I/O when
/// filenames are plain (case-sensitive filesystem).
fn names_match_exactly(shard_dir: &Path, path: &Path) -> bool {
    if !portable_filenames() {
        return true;
    }
    let Ok(rel) = path.strip_prefix(shard_dir) else {
        return false;
    };
    let mut dir = shard_dir.to_path_buf();
    for component in rel.components() {
        let name = component.as_os_str();
        let listed = std::fs::read_dir(&dir)
            .is_ok_and(|mut entries| entries.any(|entry| entry.is_ok_and(|e| e.file_name() == name)));
        if !listed {
            return false;
        }
        dir.push(name);
    }
    true
}

/// Human: The paths among `variants` (see `blob_path_variants`, current name first) that hold this key's blob.
/// A fallback counts only when its names match the disk exactly (`names_match_exactly`): deleting or rewriting a
/// key used to remove a case-alike neighbour's blob on macOS and Windows.
/// Agent: SYNC fs calls; RETURNS existing paths in variant order.
pub fn existing_blob_paths(variants: &[PathBuf]) -> Vec<PathBuf> {
    let Some(shard_dir) = variants.first().and_then(|primary| primary.parent()) else {
        return Vec::new();
    };
    variants
        .iter()
        .enumerate()
        .filter(|(i, path)| path.exists() && (*i == 0 || names_match_exactly(shard_dir, path)))
        .map(|(_, path)| path.clone())
        .collect()
}

/// Human: The first path among `variants` holding this key's blob (see `existing_blob_paths`).
/// Agent: The current name is checked without blocking; fallbacks go through the blocking pool.
pub async fn first_existing_blob_path(
    variants: &[PathBuf],
) -> Result<Option<PathBuf>, std::io::Error> {
    let Some(primary) = variants.first() else {
        return Ok(None);
    };
    if tokio::fs::metadata(primary).await.is_ok() {
        return Ok(Some(primary.clone()));
    }
    let variants = variants.to_vec();
    tokio::task::spawn_blocking(move || existing_blob_paths(&variants).into_iter().next())
        .await
        .map_err(std::io::Error::other)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn variants_cover_both_filename_schemes_on_any_filesystem() {
        let variants = blob_path_variants("/data", "music", "Report.PDF");
        assert!(variants.iter().any(|p| p.ends_with("Report.PDF")), "{variants:?}");
        assert!(variants.iter().any(|p| p.ends_with("%52eport.%50%44%46")), "{variants:?}");
        let lower = blob_path_variants("/data", "music", "report.pdf");
        assert_eq!(lower.len(), 1, "one name serves both schemes: {lower:?}");
    }

    #[test]
    fn portable_names_differ_where_filesystems_fold() {
        assert_ne!(encode_portable("Photo.JPG"), encode_portable("photo.jpg"));
        assert_ne!(encode_portable("caf\u{e9}"), encode_portable("cafe\u{301}"));
        assert_eq!(encode_portable("users/abc-123/file.jpg"), "users%2Fabc-123%2Ffile.jpg");
        assert_eq!(encode_portable("2024-01-01T10:00:00Z.log"), "2024-01-01%5410%3A00%3A00%5A.log");
        assert_eq!(encode_portable("con.txt"), "%63on.txt");
        assert_eq!(encode_portable("com1"), "%63om1");
        assert_eq!(encode_portable("console.log"), "console.log");
        assert_eq!(encode_portable("ends with dot."), "ends with dot%2E");
        for key in ["Photo.JPG", "caf\u{e9}/na\u{ef}ve \u{1f600}", "a<b>c:d\"e|f?g*h\\i", "con.txt", "x. ", "100%"] {
            assert_eq!(decode_blob_filename(&encode_portable(key)), key);
            assert_eq!(decode_blob_filename(&encode_plain(key)), key);
        }
    }

    #[test]
    fn encode_decode_roundtrip() {
        for key in [
            "foo.bin",
            "users/uuid/files/img",
            "users/uuid/files/img/grid-thumbnail.jpg",
            "weird%name",
            "a/b/c",
        ] {
            assert_eq!(decode_blob_filename(&encode_blob_filename(key)), key);
            assert_eq!(decode_blob_filename(&encode_plain(key)), key);
        }
    }

    #[test]
    fn blob_path_is_flat_under_shard() {
        // Human: Lowercase keys get the same filename in both schemes.
        let path = blob_path("/data", "media", "users/uuid/file.jpg");
        assert_eq!(
            path,
            PathBuf::from("/data/media")
                .join(hash_prefix("users/uuid/file.jpg"))
                .join("users%2Fuuid%2Ffile.jpg")
        );
    }

    #[test]
    fn object_key_from_encoded_and_legacy_relpaths() {
        let encoded_rel = format!("{}/{}", hash_prefix("users/a/b"), encode_blob_filename("users/a/b"));
        assert_eq!(
            object_key_from_blob_relpath(&encoded_rel),
            Some("users/a/b".to_string())
        );

        let legacy_rel = format!("{}/users/a/b", hash_prefix("users/a/b"));
        assert_eq!(
            object_key_from_blob_relpath(&legacy_rel),
            Some("users/a/b".to_string())
        );
    }
}
