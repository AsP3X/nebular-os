use std::path::PathBuf;

/// Percent-encode `/` and `%` so each object key maps to a single filename under its hash shard.
pub fn encode_blob_filename(key: &str) -> String {
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

/// Decode a blob filename produced by [`encode_blob_filename`].
pub fn decode_blob_filename(encoded: &str) -> String {
    let mut out = String::with_capacity(encoded.len());
    let mut chars = encoded.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        let h1 = chars.next();
        let h2 = chars.next();
        if let (Some(a), Some(b)) = (h1, h2) {
            let hex: String = [a, b].iter().collect();
            if let Ok(v) = u8::from_str_radix(&hex, 16) {
                out.push(char::from(v));
                continue;
            }
            out.push('%');
            out.push(a);
            out.push(b);
            continue;
        }
        out.push('%');
        if let Some(x) = h1 {
            out.push(x);
        }
    }
    out
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

/// Candidate on-disk paths for a key (encoded first, then legacy when they differ).
pub fn blob_path_variants(base: &str, bucket: &str, key: &str) -> Vec<PathBuf> {
    let encoded = blob_path(base, bucket, key);
    if key.contains('/') {
        let legacy = blob_path_legacy(base, bucket, key);
        if legacy != encoded {
            return vec![encoded, legacy];
        }
    }
    vec![encoded]
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

pub async fn first_existing_blob_path(
    variants: &[PathBuf],
) -> Result<Option<PathBuf>, std::io::Error> {
    for path in variants {
        if tokio::fs::metadata(path).await.is_ok() {
            return Ok(Some(path.clone()));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

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
        }
    }

    #[test]
    fn blob_path_is_flat_under_shard() {
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
