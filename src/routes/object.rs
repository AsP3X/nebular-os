use axum::{
    body::Body,
    extract::{Path, Request, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{json, Map};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

use crate::routes::AppState;

struct LimitReader<R> {
    inner: R,
    remaining: usize,
}

impl<R: AsyncRead + Unpin> AsyncRead for LimitReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let result = Pin::new(&mut self.inner).poll_read(cx, buf);
        let after = buf.filled().len();
        let read = after - before;
        if read > self.remaining {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "payload too large",
            )));
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

pub async fn put_object(
    State(state): State<Arc<AppState>>,
    Path(params): Path<ObjectParams>,
    req: Request,
) -> Response {
    tracing::info!(bucket = %params.bucket, key = %params.key, "put_object started");

    let content_type = req
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let mut custom_meta_map = Map::new();
    for (k, v) in req.headers().iter() {
        let name = k.as_str();
        if let Some(key) = name.strip_prefix("x-nd-custom-meta-") {
            if let Ok(val) = v.to_str() {
                custom_meta_map.insert(key.to_string(), serde_json::Value::String(val.to_string()));
            }
        }
    }
    let custom_meta = if custom_meta_map.is_empty() {
        None
    } else {
        Some(serde_json::to_string(&custom_meta_map).unwrap_or_default())
    };

    let body_stream = req.into_body().into_data_stream();
    let body_reader = tokio_util::io::StreamReader::new(
        body_stream.map(|result| {
            result.map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err))
        }),
    );
    let body_reader = LimitReader {
        inner: body_reader,
        remaining: state.max_body_size,
    };

    match state
        .storage
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
            tracing::info!(bucket = %meta.bucket, key = %meta.key, size = meta.size, etag = ?meta.etag, "put_object completed");
            let mut resp = (StatusCode::CREATED, Json(json!({ "etag": meta.etag }))).into_response();
            if let Some(etag) = meta.etag {
                if let Ok(etag_header) = etag.parse() {
                    resp.headers_mut().insert(header::ETAG, etag_header);
                }
            }
            resp
        }
        Err(e) => {
            tracing::error!(bucket = %params.bucket, key = %params.key, error = %e, "put_object failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            )
                .into_response()
        }
    }
}

pub async fn get_object(
    State(state): State<Arc<AppState>>,
    Path(params): Path<ObjectParams>,
    req: Request,
) -> Response {
    let range = req
        .headers()
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .and_then(parse_range);

    tracing::info!(bucket = %params.bucket, key = %params.key, ?range, "get_object started");

    match state.storage.get_object(&params.bucket, &params.key, range).await {
        Ok((stream, content_length, total_size, mime_type)) => {
            tracing::info!(bucket = %params.bucket, key = %params.key, content_length, total_size, mime_type = ?mime_type, "get_object completed");
            let body = Body::from_stream(stream);
            let mut resp = Response::new(body);
            if let Ok(cl) = content_length.to_string().parse() {
                resp.headers_mut().insert(header::CONTENT_LENGTH, cl);
            }
            if let Some(mt) = mime_type {
                if let Ok(ct) = mt.parse() {
                    resp.headers_mut().insert(header::CONTENT_TYPE, ct);
                }
            }
            if let Ok(ar) = "bytes".parse() {
                resp.headers_mut().insert(header::ACCEPT_RANGES, ar);
            }
            if let Some((start, _)) = range {
                let end = start + content_length.saturating_sub(1);
                let value = format!("bytes {}-{}/{}", start, end, total_size);
                if let Ok(cr) = value.parse() {
                    resp.headers_mut().insert(header::CONTENT_RANGE, cr);
                }
                *resp.status_mut() = StatusCode::PARTIAL_CONTENT;
            }
            resp
        }
        Err(e) if e.to_string().contains("not found") => {
            tracing::warn!(bucket = %params.bucket, key = %params.key, "get_object not found");
            (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "not found" })),
            )
                .into_response()
        }
        Err(e) if e.to_string().contains("range not satisfiable") => {
            tracing::warn!(bucket = %params.bucket, key = %params.key, ?range, "get_object range not satisfiable");
            (
                StatusCode::RANGE_NOT_SATISFIABLE,
                Json(json!({ "error": "range not satisfiable" })),
            )
                .into_response()
        }
        Err(e) => {
            tracing::error!(bucket = %params.bucket, key = %params.key, error = %e, "get_object failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            )
                .into_response()
        }
    }
}

pub async fn head_object(
    State(state): State<Arc<AppState>>,
    Path(params): Path<ObjectParams>,
) -> Response {
    tracing::info!(bucket = %params.bucket, key = %params.key, "head_object started");

    match state.storage.head_object(&params.bucket, &params.key).await {
        Ok(meta) => {
            tracing::info!(bucket = %meta.bucket, key = %meta.key, size = meta.size, mime_type = ?meta.mime_type, "head_object completed");
            let mut resp = Response::new(Body::empty());
            if let Ok(cl) = meta.size.to_string().parse() {
                resp.headers_mut().insert(header::CONTENT_LENGTH, cl);
            }
            if let Some(mt) = meta.mime_type {
                if let Ok(ct) = mt.parse() {
                    resp.headers_mut().insert(header::CONTENT_TYPE, ct);
                }
            }
            if let Some(etag) = meta.etag {
                if let Ok(etag_header) = etag.parse() {
                    resp.headers_mut().insert(header::ETAG, etag_header);
                }
            }
            resp
        }
        Err(e) if e.to_string().contains("not found") => {
            tracing::warn!(bucket = %params.bucket, key = %params.key, "head_object not found");
            (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "not found" })),
            )
                .into_response()
        }
        Err(e) => {
            tracing::error!(bucket = %params.bucket, key = %params.key, error = %e, "head_object failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            )
                .into_response()
        }
    }
}

pub async fn delete_object(
    State(state): State<Arc<AppState>>,
    Path(params): Path<ObjectParams>,
) -> Response {
    tracing::info!(bucket = %params.bucket, key = %params.key, "delete_object started");

    match state.storage.delete_object(&params.bucket, &params.key).await {
        Ok(()) => {
            tracing::info!(bucket = %params.bucket, key = %params.key, "delete_object completed");
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => {
            tracing::error!(bucket = %params.bucket, key = %params.key, error = %e, "delete_object failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            )
                .into_response()
        }
    }
}

fn parse_range(value: &str) -> Option<(u64, u64)> {
    let value = value.trim();
    if !value.starts_with("bytes=") {
        return None;
    }
    let range = &value[6..];
    let parts: Vec<&str> = range.split('-').collect();
    if parts.len() != 2 {
        return None;
    }
    let start = parts[0].parse().ok()?;
    let end = if parts[1].is_empty() {
        u64::MAX
    } else {
        parts[1].parse().ok()?
    };
    if start > end {
        return None;
    }
    Some((start, end))
}
