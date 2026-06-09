pub mod blob_finalize;
pub mod blob_paths;
pub mod blob_ops;
pub mod block_cache;
pub mod blocks;
pub mod compressibility;
pub mod compression;
pub mod dict_store;
pub mod engine;
pub mod metadata_backend;
pub mod object_meta;
pub mod streaming;
pub mod error;
pub mod maintenance;
pub mod metadata_mode;
pub mod multipart;
pub mod precondition;
pub mod range;
pub mod reconcile;
pub mod types;

pub use engine::{GetObjectOutcome, StorageEngine};
pub use maintenance::{DictTrainReport, RecompressReport, VerifyBlobsReport};

pub fn sanitize_bucket(bucket: &str) -> anyhow::Result<String> {
    if bucket.is_empty() {
        anyhow::bail!("bucket cannot be empty");
    }
    let bucket = bucket.replace('\\', "/");
    if bucket.starts_with('/') || bucket.contains("..") {
        anyhow::bail!("invalid bucket name");
    }
    if bucket.len() >= 2 && bucket.as_bytes()[1] == b':' {
        anyhow::bail!("invalid bucket name");
    }
    Ok(bucket)
}

pub fn sanitize_key(key: &str) -> anyhow::Result<String> {
    if key.is_empty() {
        anyhow::bail!("key cannot be empty");
    }
    // Normalize backslashes to forward slashes first
    let key = key.replace('\\', "/");
    // Reject absolute paths
    if key.starts_with('/') {
        anyhow::bail!("invalid key: absolute paths are not allowed");
    }
    // Reject Windows drive-letter paths (e.g. C:/ or D:foo)
    if key.len() >= 2 && key.as_bytes()[1] == b':' {
        anyhow::bail!("invalid key: absolute paths are not allowed");
    }
    // Reject .. path segments (but allow .. inside a segment like foo..bar)
    if key.split('/').any(|segment| segment == "..") {
        anyhow::bail!("invalid key: directory traversal detected");
    }
    if key.contains('\n') {
        anyhow::bail!("invalid key: newlines are not allowed");
    }
    Ok(key)
}

pub use blob_paths::{
    blob_path, blob_path_legacy, blob_path_variants, blob_rel_path, decode_blob_filename,
    encode_blob_filename, first_existing_blob_path, hash_prefix, object_key_from_blob_relpath,
};
