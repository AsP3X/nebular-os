use axum::{
    body::Body,
    extract::{Path, Request, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{json, Map};
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

use crate::routes::errors::{map_storage_error, PayloadTooLarge};
use crate::routes::helpers::{
    apply_object_headers, parse_if_match, parse_if_modified_since, parse_if_none_match,
};
use crate::routes::AppState;
use crate::storage::engine::GetObjectOutcome;
use crate::storage::error::StorageError;

pub(crate) struct LimitReader<R> {
    pub inner: R,
    pub remaining: usize,
}

impl<R: AsyncRead + Unpin> AsyncRead for LimitReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let result = Pin::new(&mut self.inner).poll_read(cx, buf);
        let after = buf.filled().len();
        let read = after - before;
        if read > self.remaining {
            return Poll::Ready(Err(io::Error::other(PayloadTooLarge)));
        }
        self.remaining -= read;
        result
    }
}

#[derive(Debug, Deserialize)]
pub struct ObjectParams {
    bucket: String,
    key: String,
}

fn extract_custom_meta(headers: &HeaderMap) -> Option<String> {
    let mut custom_meta_map = Map::new();
    for (k, v) in headers.iter() {
        let name = k.as_str();
        if let Some(key) = name.strip_prefix("x-nd-custom-meta-")
            && let Ok(val) = v.to_str() {
                custom_meta_map.insert(key.to_string(), serde_json::Value::String(val.to_string()));
            }
    }
    if custom_meta_map.is_empty() {
        None
    } else {
        Some(serde_json::to_string(&custom_meta_map).unwrap_or_default())
    }
}

fn parse_copy_source(headers: &HeaderMap) -> Option<(String, String)> {
    // Human: Accept Nebular and S3 copy-source headers so compat clients can use CopyObject semantics.
    // Agent: READS x-nd-copy-source OR x-amz-copy-source; PARSES bucket/key from "bucket/key" value.
    let raw = headers
        .get("x-nd-copy-source")
        .or_else(|| headers.get("x-amz-copy-source"))?
        .to_str()
        .ok()?;
    let (bucket, key) = raw.split_once('/')?;
    if bucket.is_empty() || key.is_empty() {
        return None;
    }
    Some((bucket.to_string(), key.to_string()))
}

pub async fn put_object(
    State(state): State<Arc<AppState>>,
    Path(params): Path<ObjectParams>,
    req: Request,
) -> Response {
    tracing::info!(bucket = %params.bucket, key = %params.key, "put_object started");
    let headers = req.headers().clone();
    let custom_meta = extract_custom_meta(&headers);
    let if_match = parse_if_match(&headers);
    let if_none_match = parse_if_none_match(&headers);

    if let Err(e) = state
        .backend
        .ensure_write_preconditions(
            &params.bucket,
            &params.key,
            if_match.as_deref(),
            if_none_match.as_deref(),
        )
        .await
    {
        state.metrics.inc_errors();
        return map_storage_error(e).into_response();
    }

    if let Some((src_bucket, src_key)) = parse_copy_source(&headers) {
        match state
            .backend
            .copy_object(
                &src_bucket,
                &src_key,
                &params.bucket,
                &params.key,
                if_match.as_deref(),
                if_none_match.as_deref(),
            )
            .await
        {
            Ok(meta) => {
                state.metrics.add_uploaded(meta.size as u64);
                return (
                    StatusCode::CREATED,
                    Json(json!({ "etag": meta.etag })),
                )
                    .into_response();
            }
            Err(e) => return map_storage_error(e).into_response(),
        }
    }

    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let body_stream = req.into_body().into_data_stream();
    let body_reader = tokio_util::io::StreamReader::new(
        body_stream.map(|result| {
            result.map_err(io::Error::other)
        }),
    );
    let body_reader = LimitReader {
        inner: body_reader,
        remaining: state.max_body_size,
    };

    match state
        .backend
        .put_object(
            &params.bucket,
            &params.key,
            content_type.as_deref(),
            custom_meta.as_deref(),
            body_reader,
        )
        .await
    {
        Ok(meta) => {
            state.metrics.add_uploaded(meta.size as u64);
            let mut resp = (StatusCode::CREATED, Json(json!({ "etag": meta.etag }))).into_response();
            if let Some(etag) = meta.etag
                && let Ok(etag_header) = etag.parse() {
                    resp.headers_mut().insert(header::ETAG, etag_header);
                }
            resp
        }
        Err(e @ StorageError::PayloadTooLarge) => {
            state.metrics.inc_errors();
            map_storage_error(e).into_response()
        }
        Err(e) => {
            state.metrics.inc_errors();
            map_storage_error(e).into_response()
        }
    }
}

pub async fn get_object(
    State(state): State<Arc<AppState>>,
    Path(params): Path<ObjectParams>,
    req: Request,
) -> Response {
    let headers = req.headers();
    let range_header = headers.get(header::RANGE).and_then(|v| v.to_str().ok());
    let if_none_match = parse_if_none_match(headers);
    let if_modified_since = parse_if_modified_since(headers);

    match state
        .backend
        .get_object(
            &params.bucket,
            &params.key,
            range_header,
            if_none_match.as_deref(),
            if_modified_since,
        )
        .await
    {
        Ok(GetObjectOutcome::NotModified(meta)) => {
            let mut resp = Response::new(Body::empty());
            *resp.status_mut() = StatusCode::NOT_MODIFIED;
            apply_object_headers(resp.headers_mut(), &meta);
            resp
        }
        Ok(GetObjectOutcome::Content {
            stream,
            content_length,
            total_size,
            meta,
        }) => {
            state.metrics.add_downloaded(content_length);
            let body = Body::from_stream(stream);
            let mut resp = Response::new(body);
            apply_object_headers(resp.headers_mut(), &meta);
            if let Ok(ar) = "bytes".parse() {
                resp.headers_mut().insert(header::ACCEPT_RANGES, ar);
            }
            if let Some(range_hdr) = range_header {
                let start = super::helpers::parse_range(range_hdr, total_size)
                    .map(|(s, _)| s)
                    .unwrap_or(0);
                let end = start + content_length.saturating_sub(1);
                let value = format!("bytes {}-{}/{}", start, end, total_size);
                if let Ok(cr) = value.parse() {
                    resp.headers_mut().insert(header::CONTENT_RANGE, cr);
                }
                *resp.status_mut() = StatusCode::PARTIAL_CONTENT;
            }
            resp
        }
        Err(e) => {
            state.metrics.inc_errors();
            map_storage_error(e).into_response()
        }
    }
}

pub async fn head_object(
    State(state): State<Arc<AppState>>,
    Path(params): Path<ObjectParams>,
    req: Request,
) -> Response {
    let headers = req.headers();
    let if_none_match = parse_if_none_match(headers);
    let if_modified_since = parse_if_modified_since(headers);

    match state
        .backend
        .head_object(
            &params.bucket,
            &params.key,
            if_none_match.as_deref(),
            if_modified_since,
        )
        .await
    {
        Ok(None) => {
            let mut resp = Response::new(Body::empty());
            *resp.status_mut() = StatusCode::NOT_MODIFIED;
            resp
        }
        Ok(Some(meta)) => {
            let mut resp = Response::new(Body::empty());
            apply_object_headers(resp.headers_mut(), &meta);
            resp
        }
        Err(e) => {
            state.metrics.inc_errors();
            map_storage_error(e).into_response()
        }
    }
}

pub async fn delete_object(
    State(state): State<Arc<AppState>>,
    Path(params): Path<ObjectParams>,
    req: Request,
) -> Response {
    let if_match = parse_if_match(req.headers());
    match state
        .backend
        .delete_object(&params.bucket, &params.key, if_match.as_deref())
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => {
            state.metrics.inc_errors();
            map_storage_error(e).into_response()
        }
    }
}
