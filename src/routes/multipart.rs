use axum::{
    extract::{Path, Query, Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Map};
use std::sync::Arc;

use crate::routes::errors::map_storage_error;
use crate::storage::error::StorageError;
use crate::storage::multipart::CompletedPart;
use crate::routes::helpers::write_context_from_headers;
use crate::routes::body_digest::ExpectedDigests;
use crate::routes::object::upload_body_reader;
use crate::routes::AppState;

#[derive(Debug, Deserialize)]
pub struct BucketParams {
    bucket: String,
}

#[derive(Debug, Deserialize)]
pub struct InitQuery {
    key: String,
}

#[derive(Debug, Deserialize)]
pub struct UploadPartParams {
    bucket: String,
    upload_id: String,
    part_number: i32,
}

#[derive(Debug, Deserialize)]
pub struct UploadSessionParams {
    bucket: String,
    upload_id: String,
}

/// Largest accepted complete-multipart body (a 10,000-part list is well under this).
const MAX_COMPLETE_BODY: usize = 1024 * 1024;

pub async fn init_multipart(
    State(state): State<Arc<AppState>>,
    Path(params): Path<BucketParams>,
    Query(query): Query<InitQuery>,
    req: Request,
) -> Response {
    let headers = req.headers();
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok());
    let write_ctx = write_context_from_headers(headers, None);

    match state
        .backend()
        .init_multipart(
            &params.bucket,
            &query.key,
            content_type,
            Some(&write_ctx),
        )
        .await
    {
        Ok(result) => (StatusCode::OK, Json(result)).into_response(),
        Err(e) => map_storage_error(e).into_response(),
    }
}

/// Largest part number, as in S3; with NOS_MULTIPART_PART_SIZE per part this also bounds an upload's size.
const MAX_PART_NUMBER: i32 = 10_000;

pub async fn upload_part(
    State(state): State<Arc<AppState>>,
    Path(params): Path<UploadPartParams>,
    req: Request,
) -> Response {
    if !(1..=MAX_PART_NUMBER).contains(&params.part_number) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": format!("part number must be between 1 and {MAX_PART_NUMBER}") })),
        )
            .into_response();
    }
    let key = match state
        .backend()
        .multipart_key_for_upload(&params.upload_id)
        .await
    {
        Ok(k) => k,
        Err(e) => return map_storage_error(e).into_response(),
    };

    let write_ctx = write_context_from_headers(req.headers(), None);
    let digests = match ExpectedDigests::from_request(req.headers(), req.extensions()) {
        Ok(digests) => digests,
        Err(message) => {
            return (StatusCode::BAD_REQUEST, Json(json!({ "error": message }))).into_response();
        }
    };
    let max_part = state.backend().multipart_part_size();
    let body_reader = upload_body_reader(
        req.into_body(),
        max_part,
        state.config.upload_idle_timeout_secs,
        digests,
    );

    match state
        .backend()
        .upload_part(
            &params.bucket,
            &key,
            &params.upload_id,
            params.part_number,
            body_reader,
            Some(&write_ctx),
        )
        .await
    {
        Ok(result) => (StatusCode::OK, Json(result)).into_response(),
        Err(e) => map_storage_error(e).into_response(),
    }
}

pub async fn complete_multipart(
    State(state): State<Arc<AppState>>,
    Path(params): Path<UploadSessionParams>,
    req: Request,
) -> Response {
    let key = match state
        .backend()
        .multipart_key_for_upload(&params.upload_id)
        .await
    {
        Ok(k) => k,
        Err(e) => return map_storage_error(e).into_response(),
    };

    let mut custom_meta_map = Map::new();
    for (k, v) in req.headers().iter() {
        let name = k.as_str();
        if let Some(meta_key) = name.strip_prefix("x-nd-custom-meta-")
            && let Ok(val) = v.to_str() {
                custom_meta_map.insert(meta_key.to_string(), serde_json::Value::String(val.to_string()));
            }
    }
    let custom_meta = if custom_meta_map.is_empty() {
        None
    } else {
        Some(serde_json::to_string(&custom_meta_map).unwrap_or_default())
    };
    let write_ctx = write_context_from_headers(req.headers(), custom_meta.as_deref());

    // Human: Optional S3-style part list `{"parts":[{"part_number":1,"etag":"..."}]}`; without it the
    // stored parts must be contiguous from 1.
    let declared_len = req
        .headers()
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok());
    if declared_len.is_some_and(|len| len > MAX_COMPLETE_BODY) {
        return map_storage_error(StorageError::PayloadTooLarge).into_response();
    }
    let Ok(body) = axum::body::to_bytes(req.into_body(), MAX_COMPLETE_BODY).await else {
        return map_storage_error(StorageError::InvalidRequest(
            "could not read the part list".into(),
        ))
        .into_response();
    };
    // Human: Only a JSON object with a `parts` field is a part list; any other body (`{}`, `null`, XML, text) is
    // ignored as before — earlier releases ignored the body entirely, so clients send all sorts.
    let parts = match serde_json::from_slice::<serde_json::Value>(&body) {
        Ok(serde_json::Value::Object(mut fields)) => match fields.remove("parts") {
            None | Some(serde_json::Value::Null) => None,
            Some(list) => match serde_json::from_value::<Vec<CompletedPart>>(list) {
                Ok(list) => Some(list),
                Err(e) => {
                    return map_storage_error(StorageError::InvalidRequest(format!(
                        "invalid part list: {e}"
                    )))
                    .into_response();
                }
            },
        },
        _ => None,
    };

    match state
        .backend()
        .complete_multipart(
            &params.bucket,
            &key,
            &params.upload_id,
            custom_meta.as_deref(),
            Some(&write_ctx),
            parts.as_deref(),
        )
        .await
    {
        Ok(meta) => (StatusCode::CREATED, Json(json!({ "etag": meta.etag }))).into_response(),
        Err(e) => map_storage_error(e).into_response(),
    }
}

pub async fn abort_multipart(
    State(state): State<Arc<AppState>>,
    Path(params): Path<UploadSessionParams>,
) -> Response {
    let key = match state
        .backend()
        .multipart_key_for_upload(&params.upload_id)
        .await
    {
        Ok(k) => k,
        Err(e) => return map_storage_error(e).into_response(),
    };

    match state
        .backend()
        .abort_multipart(&params.bucket, &key, &params.upload_id)
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => map_storage_error(e).into_response(),
    }
}
