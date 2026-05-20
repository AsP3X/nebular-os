use axum::http::{header, HeaderMap, HeaderName, HeaderValue};
use chrono::{DateTime, Utc};
use serde_json::{Map, Value};

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

pub fn apply_object_headers(headers: &mut HeaderMap, meta: &ObjectMetadata) {
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
    if let Ok(v) = HeaderValue::from_str(&meta.updated_at.to_rfc2822()) {
        headers.insert(header::LAST_MODIFIED, v);
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

/// Parses `If-None-Match` (first etag token only).
pub fn parse_if_none_match(headers: &HeaderMap) -> Option<String> {
    let raw = headers.get(header::IF_NONE_MATCH)?.to_str().ok()?;
    let token = raw.split(',').next()?.trim();
    if token == "*" {
        return Some("*".to_string());
    }
    Some(token.trim_matches('"').to_string())
}

/// Parses RFC 7233 range values including suffix form `bytes=-N`.
pub fn parse_range(value: &str, total_size: u64) -> Option<(u64, u64)> {
    crate::storage::range::parse_content_range(value, total_size)
}
