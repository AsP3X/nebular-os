use std::io;
use thiserror::Error;

#[derive(Debug)]
pub struct PayloadTooLarge;

impl std::fmt::Display for PayloadTooLarge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("payload too large")
    }
}

impl std::error::Error for PayloadTooLarge {}

/// Marker carried in an `io::Error` when an upload body stops sending bytes (NOS_UPLOAD_IDLE_TIMEOUT_SECS).
#[derive(Debug)]
pub struct UploadIdleTimeout;

impl std::fmt::Display for UploadIdleTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("upload body idle timeout")
    }
}

impl std::error::Error for UploadIdleTimeout {}

/// Marker carried in an `io::Error` when an upload body doesn't match its `Content-MD5` or signed SHA-256.
#[derive(Debug)]
pub struct BadDigest;

impl std::fmt::Display for BadDigest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("request body does not match its Content-MD5 or signed payload hash")
    }
}

impl std::error::Error for BadDigest {}

pub fn is_payload_too_large(err: &io::Error) -> bool {
    err.get_ref()
        .and_then(|inner| inner.downcast_ref::<PayloadTooLarge>())
        .is_some()
}

pub fn internal<E: Into<anyhow::Error>>(err: E) -> StorageError {
    StorageError::Internal(err.into())
}

pub fn map_io_error(err: io::Error) -> StorageError {
    if is_payload_too_large(&err) {
        StorageError::PayloadTooLarge
    } else if err
        .get_ref()
        .is_some_and(|inner| inner.is::<UploadIdleTimeout>())
    {
        StorageError::RequestTimeout
    } else if err.get_ref().is_some_and(|inner| inner.is::<BadDigest>()) {
        StorageError::InvalidRequest(BadDigest.to_string())
    } else {
        StorageError::Internal(err.into())
    }
}

/// Storage-layer failures mapped to stable HTTP responses in route handlers.
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("not found")]
    NotFound,
    #[error("range not satisfiable")]
    RangeNotSatisfiable { size: u64 },
    #[error("payload too large")]
    PayloadTooLarge,
    #[error("request timeout")]
    RequestTimeout,
    #[error("insufficient storage")]
    InsufficientStorage,
    #[error("invalid bucket name")]
    InvalidBucket,
    #[error("invalid key")]
    InvalidKey,
    /// Client error with a specific, safe-to-return explanation (HTTP 400).
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("precondition failed")]
    PreconditionFailed,
    #[error("node is read-only replica")]
    ReadOnlyReplica,
    #[error("object not assigned to this node")]
    NotAssigned {
        assigned_node: String,
        storage_class: String,
    },
    #[error("storage error")]
    Internal(#[from] anyhow::Error),
}

impl StorageError {
    pub fn client_message(&self) -> &'static str {
        match self {
            StorageError::NotFound => "not found",
            StorageError::RangeNotSatisfiable { .. } => "range not satisfiable",
            StorageError::PayloadTooLarge => "payload too large",
            StorageError::RequestTimeout => "request timeout",
            StorageError::InsufficientStorage => "insufficient storage",
            StorageError::InvalidBucket
            | StorageError::InvalidKey
            | StorageError::InvalidRequest(_) => "invalid request",
            StorageError::PreconditionFailed => "precondition failed",
            StorageError::ReadOnlyReplica => "node is read-only replica",
            StorageError::NotAssigned { .. } => "object not assigned to this node",
            StorageError::Internal(_) => "storage error",
        }
    }
}
