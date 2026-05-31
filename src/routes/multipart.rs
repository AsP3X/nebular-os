use axum::{
    extract::{Path, Query, Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{json, Map};
use std::io;
use std::sync::Arc;

use crate::routes::errors::map_storage_error;
use crate::routes::object::LimitReader;
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

pub async fn init_multipart(
    State(state): State<Arc<AppState>>,
    Path(params): Path<BucketParams>,
    Query(query): Query<InitQuery>,
    req: Request,
) -> Response {
    let content_type = req
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok());

    match state
        .backend
        .init_multipart(&params.bucket, &query.key, content_type)
        .await
    {
        Ok(result) => (StatusCode::OK, Json(result)).into_response(),
        Err(e) => map_storage_error(e).into_response(),
    }
}

pub async fn upload_part(
    State(state): State<Arc<AppState>>,
    Path(params): Path<UploadPartParams>,
    req: Request,
) -> Response {
    let key = match state
        .backend
        .multipart_key_for_upload(&params.upload_id)
        .await
    {
        Ok(k) => k,
        Err(e) => return map_storage_error(e).into_response(),
    };

    let max_part = state.backend.multipart_part_size();
    let body_stream = req.into_body().into_data_stream();
    let body_reader = tokio_util::io::StreamReader::new(
        body_stream.map(|result| {
            result.map_err(io::Error::other)
        }),
    );
    let body_reader = LimitReader {
        inner: body_reader,
        remaining: max_part,
    };

    match state
        .backend
        .upload_part(
            &params.bucket,
            &key,
            &params.upload_id,
            params.part_number,
            body_reader,
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
        .backend
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

    match state
        .backend
        .complete_multipart(
            &params.bucket,
            &key,
            &params.upload_id,
            custom_meta.as_deref(),
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
        .backend
        .multipart_key_for_upload(&params.upload_id)
        .await
    {
        Ok(k) => k,
        Err(e) => return map_storage_error(e).into_response(),
    };

    match state
        .backend
        .abort_multipart(&params.bucket, &key, &params.upload_id)
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => map_storage_error(e).into_response(),
    }
}
