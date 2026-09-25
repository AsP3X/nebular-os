//! Human: On local GET miss, optionally stream object bytes from a peer without persisting.
//! Agent: Used when NOS_REPLICATION_READ_REPAIR=true; GET /_cluster/objects on peers.

use axum::http::header;
use chrono::{TimeZone, Utc};
use futures_util::StreamExt;
use reqwest::StatusCode;

use crate::cluster::peer::PeerRegistry;
use crate::cluster::replication_recover::custom_meta_json_from_headers;
use crate::storage::engine::GetObjectOutcome;
use crate::storage::error::StorageError;
use crate::storage::streaming::{GuardedObjectBodyStream, ObjectBodyStream};
use crate::storage::types::ObjectMetadata;

/// Human: Try each peer until one returns 200/206/304 for the object key.
/// Agent: Does not write to local disk; streams HTTP body into GetObjectOutcome::Content.
#[allow(clippy::too_many_arguments)]
pub async fn fetch_from_peers(
    client: &reqwest::Client,
    peers: &PeerRegistry,
    self_id: &str,
    token: &str,
    bucket: &str,
    key: &str,
    range_header: Option<&str>,
    if_none_match: Option<&str>,
    if_modified_since: Option<i64>,
) -> Result<GetObjectOutcome, StorageError> {
    for (peer_id, peer) in &peers.peers {
        if peer_id == self_id {
            continue;
        }
        let Some(path) = crate::cluster::forward::object_path(bucket, key) else {
            break;
        };
        let url = format!("{}/_cluster/objects/{path}", peer.url.trim_end_matches('/'));
        let mut req = client
            .get(&url)
            .header(header::AUTHORIZATION, format!("Bearer {token}"));
        if let Some(r) = range_header {
            req = req.header(header::RANGE, r);
        }
        if let Some(v) = if_none_match {
            req = req.header(header::IF_NONE_MATCH, v);
        }
        if let Some(ts) = if_modified_since
            && let Some(dt) = Utc.timestamp_opt(ts, 0).single()
        {
            req = req.header(header::IF_MODIFIED_SINCE, dt.to_rfc2822());
        }

        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(peer_id = %peer_id, error = %e, "read repair peer request failed");
                continue;
            }
        };

        let status = resp.status();
        if status == StatusCode::NOT_FOUND {
            continue;
        }
        let content_range = resp
            .headers()
            .get(header::CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            .and_then(parse_content_range);
        if status == StatusCode::RANGE_NOT_SATISFIABLE
            && let Some((_, size)) = content_range
        {
            return Err(StorageError::RangeNotSatisfiable { size });
        }
        if !status.is_success() && status != StatusCode::NOT_MODIFIED {
            tracing::warn!(
                peer_id = %peer_id,
                status = %status,
                "read repair peer returned error status"
            );
            continue;
        }

        let etag = resp
            .headers()
            .get(header::ETAG)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let mime = resp
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let custom_meta = custom_meta_json_from_headers(resp.headers());
        let header_length: Option<u64> = resp
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse().ok());
        let total_size = content_range
            .map(|(_, total)| total)
            .or(header_length)
            .unwrap_or(0);

        let epoch = Utc.timestamp_opt(0, 0).single().unwrap();
        let meta = ObjectMetadata {
            bucket: bucket.to_string(),
            key: key.to_string(),
            size: total_size as i64,
            mime_type: mime,
            etag,
            created_at: epoch,
            updated_at: epoch,
            custom_meta,
            deleted_at: None,
            storage_class: resp
                .headers()
                .get(axum::http::HeaderName::from_static("x-nd-storage-class"))
                .and_then(|v| v.to_str().ok())
                .map(str::to_string),
            origin_node: resp
                .headers()
                .get(axum::http::HeaderName::from_static("x-nd-origin-node"))
                .and_then(|v| v.to_str().ok())
                .map(str::to_string),
        };

        if status == StatusCode::NOT_MODIFIED {
            return Ok(GetObjectOutcome::NotModified(meta));
        }

        // Human: The response we build states Content-Length, so it must match the bytes the peer streams:
        // the span for 206, the peer's own Content-Length for 200. Skip peers that give neither.
        let (range, content_length) = if status == StatusCode::PARTIAL_CONTENT {
            let Some((Some((start, end)), _)) = content_range.filter(|(span, _)| {
                span.is_some_and(|(start, end)| start <= end)
            }) else {
                tracing::warn!(peer_id = %peer_id, "read repair peer sent 206 without a usable Content-Range");
                continue;
            };
            (Some((start, end)), end - start + 1)
        } else {
            let Some(length) = header_length else {
                tracing::warn!(peer_id = %peer_id, "read repair peer response has no Content-Length");
                continue;
            };
            (None, length)
        };

        let http_stream = resp.bytes_stream().map(|chunk| {
            chunk.map_err(|e| std::io::Error::other(e.to_string()))
        });
        let stream =
            GuardedObjectBodyStream::from_http_stream(ObjectBodyStream::Http(Box::pin(http_stream)));

        return Ok(GetObjectOutcome::Content {
            stream,
            content_length,
            total_size,
            range,
            meta: Box::new(meta),
        });
    }

    Err(StorageError::NotFound)
}

/// `bytes start-end/total` => (Some((start, end)), total); `bytes */total` => (None, total).
fn parse_content_range(value: &str) -> Option<(Option<(u64, u64)>, u64)> {
    let (span, total) = value.strip_prefix("bytes ")?.trim().split_once('/')?;
    let total = total.trim().parse().ok()?;
    if span.trim() == "*" {
        return Some((None, total));
    }
    let (start, end) = span.split_once('-')?;
    Some((Some((start.trim().parse().ok()?, end.trim().parse().ok()?)), total))
}

#[cfg(test)]
mod tests {
    use super::parse_content_range;

    #[test]
    fn parses_satisfied_and_unsatisfied_forms() {
        assert_eq!(parse_content_range("bytes 0-4/26"), Some((Some((0, 4)), 26)));
        assert_eq!(parse_content_range("bytes */26"), Some((None, 26)));
        assert_eq!(parse_content_range("bytes 0-4/*"), None);
        assert_eq!(parse_content_range("items 0-4/26"), None);
        assert_eq!(parse_content_range("bytes a-4/26"), None);
    }
}
