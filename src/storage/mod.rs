pub mod blob_finalize;
pub mod blob_paths;
pub mod blob_ops;
pub mod block_cache;
pub mod buffer_pool;
pub mod blocks;
pub mod scrub;
pub mod compressibility;
pub mod compression;
pub mod dict_store;
pub mod engine;
pub mod key_locks;
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
pub mod write_path;

pub use engine::{GetObjectOutcome, StorageEngine};
pub use write_path::{CommitHook, Committed, WriteConditions};
pub use maintenance::{DictTrainReport, MigrateBlobsReport, RecompressReport, VerifyBlobsReport};

pub fn sanitize_bucket(bucket: &str) -> anyhow::Result<String> {
    if bucket.is_empty() {
        anyhow::bail!("bucket cannot be empty");
    }
    // Human: A bucket is one directory under NOS_DATA_DIR. `/` (e.g. from `%2F`) would nest it inside
    // another bucket's shards, and a leading `.` would land on system dirs (.tmp, .blocks, .multipart, .dict).
    if bucket.starts_with('.')
        || bucket.contains(['/', '\\'])
        || bucket.chars().any(char::is_control)
        || bucket.len() > 255
    {
        anyhow::bail!("invalid bucket name");
    }
    let bucket = bucket.to_string();
    if bucket.contains("..") {
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
    if key.contains('\n') || key.contains('\0') {
        anyhow::bail!("invalid key: newlines and NUL are not allowed");
    }
    Ok(key)
}

/// Longest on-disk blob filename (ext4/APFS/NTFS all stop at 255).
pub const MAX_BLOB_FILENAME_BYTES: usize = 255;

/// Human: New objects are stored as one filename (`/` -> `%2F`), which filesystems cap at 255 bytes, so a longer
/// key would fail deep in the write path with a 500. Checked on writes only: legacy nested-layout objects may
/// have longer keys and must stay readable and deletable.
pub fn check_new_key_len(key: &str) -> Result<(), error::StorageError> {
    if encode_blob_filename(key).len() > MAX_BLOB_FILENAME_BYTES {
        return Err(error::StorageError::InvalidRequest(format!(
            "key too long: stored keys may use at most {MAX_BLOB_FILENAME_BYTES} bytes (each '/' counts as 3)"
        )));
    }
    Ok(())
}

/// Bucket names whose whole namespace system routes claim (`/_nos/…`, `/_cluster/…`).
pub const RESERVED_BUCKETS: [&str; 2] = ["_cluster", "_nos"];

/// Human: Whether a system route answers the requests for this object, so it could never be read back: anything
/// under `_nos`/`_cluster`, `health/ready`, and a bucket's `_batch_delete` and multipart paths. Other objects in
/// buckets named like an endpoint (`health`, `metrics`) are reachable and stay writable; only listing such a bucket
/// is shadowed (`GET /health`, `GET /metrics`).
/// Agent: MIRRORS the routes in server.rs; a path matching a route answers 405 for other methods, never falls through.
pub fn shadowed_by_system_route(bucket: &str, key: &str) -> bool {
    if RESERVED_BUCKETS.contains(&bucket) || (bucket == "health" && key == "ready") {
        return true;
    }
    let segments: Vec<&str> = key.split('/').collect();
    matches!(
        segments.as_slice(),
        ["_batch_delete"]
            | ["_multipart"]
            | ["_multipart", _]
            | ["_multipart", _, "complete"]
            | ["_multipart", _, "parts", _]
    )
}

/// Checks for a write that creates or replaces an object: a path no system route shadows and a key short enough
/// to store (see `check_new_key_len`).
pub fn check_new_object(bucket: &str, key: &str) -> Result<(), error::StorageError> {
    if shadowed_by_system_route(bucket, key) {
        return Err(error::StorageError::InvalidRequest(format!(
            "'{bucket}/{key}' is answered by a system route; use another bucket or key"
        )));
    }
    check_new_key_len(key)
}

pub use blob_paths::{
    blob_path, blob_path_legacy, blob_path_variants, blob_rel_path, decode_blob_filename,
    encode_blob_filename, existing_blob_paths, first_existing_blob_path, hash_prefix,
    object_key_from_blob_relpath,
};
