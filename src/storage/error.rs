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
    RangeNotSatisfiable,
    #[error("payload too large")]
    PayloadTooLarge,
    #[error("invalid bucket name")]
    InvalidBucket,
    #[error("invalid key")]
    InvalidKey,
    #[error("precondition failed")]
    PreconditionFailed,
    #[error("storage error")]
    Internal(#[from] anyhow::Error),
}

impl StorageError {
    pub fn client_message(&self) -> &'static str {
        match self {
            StorageError::NotFound => "not found",
            StorageError::RangeNotSatisfiable => "range not satisfiable",
            StorageError::PayloadTooLarge => "payload too large",
            StorageError::InvalidBucket | StorageError::InvalidKey => "invalid request",
            StorageError::PreconditionFailed => "precondition failed",
            StorageError::Internal(_) => "storage error",
        }
    }
}
