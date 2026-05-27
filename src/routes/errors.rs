use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;
use std::io;

use crate::storage::error::StorageError;

pub use crate::storage::error::PayloadTooLarge;

pub const PAYLOAD_TOO_LARGE_MSG: &str = "payload too large";

/// Maps route and storage failures to HTTP status and `{ "error": "..." }` JSON.
pub fn map_storage_error(err: StorageError) -> (StatusCode, Json<serde_json::Value>) {
    let status = match &err {
        StorageError::NotFound => StatusCode::NOT_FOUND,
        StorageError::RangeNotSatisfiable => StatusCode::RANGE_NOT_SATISFIABLE,
        StorageError::PayloadTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
        StorageError::InvalidBucket | StorageError::InvalidKey => StatusCode::BAD_REQUEST,
        StorageError::PreconditionFailed => StatusCode::PRECONDITION_FAILED,
        StorageError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, Json(json!({ "error": err.client_message() })))
}

pub fn payload_too_large_response() -> Response {
    map_storage_error(StorageError::PayloadTooLarge).into_response()
}

pub fn is_payload_too_large(err: &io::Error) -> bool {
    crate::storage::error::is_payload_too_large(err)
}
