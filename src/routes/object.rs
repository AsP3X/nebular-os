use axum::{
    body::Body,
    extract::{Path, Request, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use serde::Deserialize;
use serde_json::{json, Map};
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, ReadBuf};

use crate::auth::{authorize_copy_source, AuthMethod, Claims};
use crate::routes::errors::{map_storage_error, PayloadTooLarge};
use crate::routes::helpers::{
    apply_object_headers, object_content_response, parse_if_match, parse_if_modified_since,
    parse_if_none_match, range_not_satisfiable_response, write_context_from_headers,
};
use crate::routes::AppState;
use crate::storage::engine::GetObjectOutcome;
use crate::storage::error::{StorageError, UploadIdleTimeout};
use crate::routes::body_digest::{DigestCheck, ExpectedDigests};
use crate::cluster::StorageBackend;
use crate::storage::precondition::{if_range_matches, is_not_modified};
use crate::storage::write_path::WriteConditions;

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

/// Human: Request body as a size-capped AsyncRead that fails once the client goes quiet for `idle_secs`
/// — an admitted upload holds its share of the upload budget, so a stalled sender must not keep it forever.
/// Agent: idle_secs 0 = no timeout; timeout surfaces as io::Error(UploadIdleTimeout) => StorageError::RequestTimeout (408).
pub(crate) fn upload_body_reader(
    body: Body,
    limit: usize,
    idle_secs: u64,
    digests: ExpectedDigests,
) -> LimitReader<impl AsyncRead + Unpin + Send> {
    let chunks = body.into_data_stream();
    let chunks: Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send>> = if idle_secs > 0 {
        let idle = Duration::from_secs(idle_secs);
        Box::pin(
            tokio_stream::StreamExt::timeout(chunks, idle).map(|item| match item {
                Ok(chunk) => chunk.map_err(io::Error::other),
                Err(_) => Err(io::Error::other(UploadIdleTimeout)),
            }),
        )
    } else {
        Box::pin(chunks.map(|chunk| chunk.map_err(io::Error::other)))
    };
    let chunks: Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send>> = if digests.is_empty() {
        chunks
    } else {
        Box::pin(DigestCheck::new(chunks, digests))
    };
    LimitReader {
        inner: tokio_util::io::StreamReader::new(chunks),
        remaining: limit,
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

/// Error for a copy-source header that is present but not `bucket/key`.
struct InvalidCopySource;

fn parse_copy_source(headers: &HeaderMap) -> Result<Option<(String, String)>, InvalidCopySource> {
    // Human: Accept Nebular and S3 copy-source headers so compat clients can use CopyObject semantics.
    // Agent: READS x-nd-copy-source OR x-amz-copy-source; Ok(None) = no header; Err = present but not "bucket/key".
    let Some(value) = headers
        .get("x-nd-copy-source")
        .or_else(|| headers.get("x-amz-copy-source"))
    else {
        return Ok(None);
    };
    let raw = value.to_str().map_err(|_| InvalidCopySource)?;
    match raw.split_once('/') {
        Some((bucket, key)) if !bucket.is_empty() && !key.is_empty() => {
            Ok(Some((bucket.to_string(), key.to_string())))
        }
        _ => Err(InvalidCopySource),
    }
}

pub async fn put_object(
    State(state): State<Arc<AppState>>,
    Path(params): Path<ObjectParams>,
    req: Request,
) -> Response {
    tracing::info!(bucket = %params.bucket, key = %params.key, "put_object started");
    let headers = req.headers().clone();

    // Human: Middleware authorized the destination only; a copy also reads its source, which needs its own check.
    // Agent: Present-but-unparseable header => 400 (never fall through to a plain PUT); unauthorized source => 403.
    let Ok(copy_source) = parse_copy_source(&headers) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "invalid copy source" })),
        )
            .into_response();
    };
    if let Some((src_bucket, _)) = copy_source.as_ref()
        && !authorize_copy_source(
            req.extensions().get::<AuthMethod>().copied(),
            req.extensions().get::<Claims>(),
            src_bucket,
            &state.config.bucket_policy,
        )
    {
        tracing::warn!(bucket = %params.bucket, key = %params.key, %src_bucket, "copy source denied");
        return (StatusCode::FORBIDDEN, Json(json!({ "error": "forbidden" }))).into_response();
    }

    let custom_meta = extract_custom_meta(&headers);
    let write_ctx = write_context_from_headers(&headers, custom_meta.as_deref());
    let if_match = parse_if_match(&headers);
    let if_none_match = parse_if_none_match(&headers);

    if let Err(e) = state
        .backend()
        .ensure_write_preconditions(
            &params.bucket,
            &params.key,
            if_match.as_deref(),
            if_none_match.as_deref(),
            Some(&write_ctx),
        )
        .await
    {
        state.metrics.inc_errors();
        return map_storage_error(e).into_response();
    }

    if let Some((src_bucket, src_key)) = copy_source {
        match state
            .backend()
            .copy_object(
                &src_bucket,
                &src_key,
                &params.bucket,
                &params.key,
                if_match.as_deref(),
                if_none_match.as_deref(),
                Some(&write_ctx),
            )
            .await
        {
        Ok(meta) => {
            state.metrics.add_uploaded(meta.size as u64);
            state.webhooks.dispatch_put(
                &params.bucket,
                &params.key,
                meta.size,
                meta.etag.as_deref(),
            );
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

    let digests = match ExpectedDigests::from_request(&headers, req.extensions()) {
        Ok(digests) => digests,
        Err(message) => {
            return (StatusCode::BAD_REQUEST, Json(json!({ "error": message }))).into_response();
        }
    };
    let body_reader = upload_body_reader(
        req.into_body(),
        state.max_body_size,
        state.config.upload_idle_timeout_secs,
        digests,
    );

    match state
        .backend()
        .put_object(
            &params.bucket,
            &params.key,
            content_type.as_deref(),
            custom_meta.as_deref(),
            body_reader,
            Some(&write_ctx),
            WriteConditions {
                if_match: if_match.as_deref(),
                if_none_match: if_none_match.as_deref(),
                hook: None,
            },
        )
        .await
    {
        Ok(meta) => {
            state.metrics.add_uploaded(meta.size as u64);
            state.webhooks.dispatch_put(
                &params.bucket,
                &params.key,
                meta.size,
                meta.etag.as_deref(),
            );
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

async fn read_object(
    backend: &StorageBackend,
    params: &ObjectParams,
    range: Option<&str>,
    if_none_match: Option<&str>,
    if_modified_since: Option<i64>,
) -> Result<GetObjectOutcome, StorageError> {
    backend
        .get_object(&params.bucket, &params.key, range, if_none_match, if_modified_since)
        .await
}

pub async fn get_object(
    State(state): State<Arc<AppState>>,
    Path(params): Path<ObjectParams>,
    req: Request,
) -> Response {
    let headers = req.headers();
    let range_header = headers.get(header::RANGE).and_then(|v| v.to_str().ok());
    let if_range = headers.get(header::IF_RANGE).and_then(|v| v.to_str().ok());
    let if_none_match = parse_if_none_match(headers);
    let if_modified_since = parse_if_modified_since(headers);
    let backend = state.backend();
    let fetch = |range| read_object(&backend, &params, range, if_none_match.as_deref(), if_modified_since);

    let mut outcome = fetch(range_header).await;
    // Human: RFC 9110 §13.1.5 — honour Range only while If-Range still names the version being served
    // (checked against what was read, so a concurrent overwrite can't slip through); else send it all.
    if let Some(if_range) = if_range {
        match &outcome {
            Ok(GetObjectOutcome::Content { range: Some(_), meta, .. }) if !if_range_matches(meta, if_range) => {
                outcome = fetch(None).await;
            }
            Err(StorageError::RangeNotSatisfiable { size }) => {
                let size = *size;
                outcome = match fetch(None).await {
                    Ok(GetObjectOutcome::Content { meta, .. }) if if_range_matches(&meta, if_range) => {
                        Err(StorageError::RangeNotSatisfiable { size })
                    }
                    other => other,
                };
            }
            _ => {}
        }
    }

    match outcome {
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
            range,
            meta,
        }) => {
            state.metrics.add_downloaded(content_length);
            object_content_response(stream, content_length, total_size, range, &meta)
        }
        Err(StorageError::RangeNotSatisfiable { size }) => {
            state.metrics.inc_errors();
            range_not_satisfiable_response(size)
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

    // Human: Evaluate the conditions here, on the metadata just read, so a 304 carries the same ETag and
    // Last-Modified a 200 would (RFC 9110 §15.4.5); it used to be sent bare.
    match state
        .backend()
        .head_object(&params.bucket, &params.key, None, None)
        .await
    {
        Ok(Some(meta)) => {
            let mut resp = Response::new(Body::empty());
            if is_not_modified(&meta, if_none_match.as_deref(), if_modified_since) {
                *resp.status_mut() = StatusCode::NOT_MODIFIED;
            }
            apply_object_headers(resp.headers_mut(), &meta);
            resp
        }
        Ok(None) => {
            let mut resp = Response::new(Body::empty());
            *resp.status_mut() = StatusCode::NOT_MODIFIED;
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
    let write_ctx = write_context_from_headers(req.headers(), None);
    match state
        .backend()
        .delete_object(
            &params.bucket,
            &params.key,
            if_match.as_deref(),
            Some(&write_ctx),
        )
        .await
    {
        Ok(()) => {
            state
                .webhooks
                .dispatch_delete(&params.bucket, &params.key);
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => {
            state.metrics.inc_errors();
            map_storage_error(e).into_response()
        }
    }
}
