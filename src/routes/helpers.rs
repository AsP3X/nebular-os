use axum::body::Body;
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use chrono::{DateTime, Utc};
use serde_json::{Map, Value};

use crate::cluster::assignment::WriteContext;
use crate::routes::errors::map_storage_error;
use crate::storage::error::StorageError;
use crate::storage::streaming::GuardedObjectBodyStream;
use crate::storage::types::ObjectMetadata;

/// Replays stored custom metadata as `x-nd-custom-meta-*` response headers.
pub fn apply_custom_meta_headers(headers: &mut HeaderMap, meta: &ObjectMetadata) {
    let Some(raw) = meta.custom_meta.as_ref() else {
        return;
    };
    let Ok(map) = serde_json::from_str::<Map<String, Value>>(raw) else {
        return;
    };
    for (k, v) in map {
        let Some(s) = v.as_str() else { continue };
        let name = format!("x-nd-custom-meta-{}", k);
        let Ok(header_name) = HeaderName::from_bytes(name.as_bytes()) else {
            continue;
        };
        if let Ok(value) = HeaderValue::from_str(s) {
            headers.insert(header_name, value);
        }
    }
}

/// Human: Single, well-formed types that browsers render passively when an object URL is opened directly
/// (images other than SVG, audio/video, PDF, plain text). Everything else — HTML/XML/SVG, unknown or missing
/// types, and type lists like "text/plain, text/html" that browsers may resolve to HTML — is sandboxed.
/// Agent: ALLOWLIST on purpose; PDF stays unsandboxed because browser PDF viewers refuse sandboxed documents.
fn is_passive_content(mime: &str) -> bool {
    if mime.contains(',') {
        return false;
    }
    let essence = mime.split(';').next().unwrap_or("").trim().to_ascii_lowercase();
    let Some((top, sub)) = essence.split_once('/') else {
        return false;
    };
    match top {
        "image" => !sub.is_empty() && !sub.contains("xml"),
        "video" | "audio" => !sub.is_empty(),
        "application" => sub == "pdf",
        "text" => sub == "plain",
        _ => false,
    }
}

pub fn apply_object_headers(headers: &mut HeaderMap, meta: &ObjectMetadata) {
    // Human: Objects are user uploads served to browsers (presigned links): never let a browser sniff a
    // different type, and render anything that isn't plainly passive sandboxed — no scripts, unique origin.
    headers.insert(header::X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    if !meta.mime_type.as_deref().is_some_and(is_passive_content) {
        headers.insert(header::CONTENT_SECURITY_POLICY, HeaderValue::from_static("sandbox"));
    }
    if let Ok(cl) = meta.size.to_string().parse::<HeaderValue>() {
        headers.insert(header::CONTENT_LENGTH, cl);
    }
    if let Some(mt) = &meta.mime_type
        && let Ok(ct) = HeaderValue::from_str(mt) {
            headers.insert(header::CONTENT_TYPE, ct);
        }
    if let Some(etag) = &meta.etag
        && let Ok(v) = HeaderValue::from_str(etag) {
            headers.insert(header::ETAG, v);
        }
    // Human: HTTP dates are IMF-fixdate ("Sun, 06 Nov 1994 08:49:37 GMT"), not RFC 2822 with "+0000".
    if let Ok(v) = HeaderValue::from_str(&meta.updated_at.format("%a, %d %b %Y %H:%M:%S GMT").to_string()) {
        headers.insert(header::LAST_MODIFIED, v);
    }
    if let Some(class) = &meta.storage_class
        && let Ok(v) = HeaderValue::from_str(class)
    {
        headers.insert(HeaderName::from_static("x-nd-storage-class"), v);
    }
    if let Some(node) = &meta.origin_node
        && let Ok(v) = HeaderValue::from_str(node)
    {
        headers.insert(HeaderName::from_static("x-nd-origin-node"), v);
    }
    apply_custom_meta_headers(headers, meta);
}

/// Parses `If-Modified-Since` into a unix timestamp for storage comparisons.
pub fn parse_if_modified_since(headers: &HeaderMap) -> Option<i64> {
    let raw = headers.get(header::IF_MODIFIED_SINCE)?.to_str().ok()?;
    DateTime::parse_from_rfc2822(raw)
        .ok()
        .map(|dt| dt.with_timezone(&Utc).timestamp())
        .or_else(|| {
            DateTime::parse_from_rfc3339(raw)
                .ok()
                .map(|dt| dt.with_timezone(&Utc).timestamp())
        })
}

/// Parses `If-None-Match` — `*` or a comma-separated entity-tag list, matched by `etag_matches`.
pub fn parse_if_none_match(headers: &HeaderMap) -> Option<String> {
    parse_etag_precondition(headers.get(header::IF_NONE_MATCH)?)
}

/// Parses `If-Match` — `*` or a comma-separated entity-tag list, matched by `etag_matches`.
pub fn parse_if_match(headers: &HeaderMap) -> Option<String> {
    parse_etag_precondition(headers.get(header::IF_MATCH)?)
}

fn parse_etag_precondition(value: &axum::http::HeaderValue) -> Option<String> {
    let raw = value.to_str().ok()?.trim();
    (!raw.is_empty()).then(|| raw.to_string())
}

/// Satisfiable span of a `Range` value; kept for library callers.
#[deprecated(note = "use storage::range::evaluate_range")]
pub fn parse_range(value: &str, total_size: u64) -> Option<(u64, u64)> {
    #[allow(deprecated)]
    crate::storage::range::parse_content_range(value, total_size)
}

/// Human: GET response for streamed object bytes — 206 + Content-Range for an honored range, else 200.
/// Agent: OVERRIDES Content-Length (apply_object_headers writes the full object size) with the streamed length.
pub fn object_content_response(
    stream: GuardedObjectBodyStream,
    content_length: u64,
    total_size: u64,
    range: Option<(u64, u64)>,
    meta: &ObjectMetadata,
) -> Response {
    let mut resp = Response::new(Body::from_stream(stream));
    let headers = resp.headers_mut();
    apply_object_headers(headers, meta);
    headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    headers.insert(header::CONTENT_LENGTH, HeaderValue::from(content_length));
    if let Some((start, end)) = range {
        if let Ok(v) = HeaderValue::from_str(&format!("bytes {start}-{end}/{total_size}")) {
            headers.insert(header::CONTENT_RANGE, v);
        }
        *resp.status_mut() = StatusCode::PARTIAL_CONTENT;
    }
    resp
}

/// 416 carrying `Content-Range: bytes */{size}` so clients learn the current length (RFC 9110 §15.5.17).
pub fn range_not_satisfiable_response(size: u64) -> Response {
    let mut resp =
        map_storage_error(StorageError::RangeNotSatisfiable { size }).into_response();
    if let Ok(v) = HeaderValue::from_str(&format!("bytes */{size}")) {
        resp.headers_mut().insert(header::CONTENT_RANGE, v);
    }
    resp
}

/// Human: Collect optional assignment hints from object upload headers.
/// Agent: READS x-nd-storage-class, Content-Type, Content-Length, x-nd-custom-meta-storage-class.
pub fn write_context_from_headers(
    headers: &HeaderMap,
    custom_meta_json: Option<&str>,
) -> WriteContext {
    let storage_class_header = headers
        .get("x-nd-storage-class")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let content_length = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse().ok());
    let custom_meta_storage_class = custom_meta_json.and_then(|raw| {
        let map: Map<String, Value> = serde_json::from_str(raw).ok()?;
        map.get("storage-class")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    });
    let authorization = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let replication_group_header = headers
        .get("x-nd-replication-group")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    WriteContext {
        storage_class_header,
        content_type,
        custom_meta_storage_class,
        content_length,
        authorization,
        replication_group_header,
        forwarded: headers.contains_key(crate::cluster::forward::FORWARDED_HEADER),
    }
}
