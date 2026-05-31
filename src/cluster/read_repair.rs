//! Human: On local GET miss, optionally stream object bytes from a peer without persisting.
//! Agent: Used when NOS_REPLICATION_READ_REPAIR=true; GET /_cluster/objects on peers.

use axum::http::header;
use chrono::{TimeZone, Utc};
use futures_util::StreamExt;
use reqwest::StatusCode;

use crate::cluster::peer::PeerRegistry;
use crate::storage::engine::GetObjectOutcome;
use crate::storage::error::StorageError;
use crate::storage::streaming::{GuardedObjectBodyStream, ObjectBodyStream};
use crate::storage::types::ObjectMetadata;

/// Human: Try each peer until one returns 200/206/304 for the object key.
/// Agent: Does not write to local disk; streams HTTP body into GetObjectOutcome::Content.
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
        let url = format!(
            "{}/_cluster/objects/{}/{}",
            peer.url.trim_end_matches('/'),
            bucket,
            key
        );
        let mut req = client
            .get(&url)
            .header(header::AUTHORIZATION, format!("Bearer {token}"));
        if let Some(r) = range_header {
            req = req.header(header::RANGE, r);
        }
        if let Some(v) = if_none_match {
            req = req.header(header::IF_NONE_MATCH, v);
        }
        if let Some(ts) = if_modified_since {
            if let Some(dt) = Utc.timestamp_opt(ts, 0).single() {
                req = req.header(header::IF_MODIFIED_SINCE, dt.to_rfc2822());
            }
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
        let total_size = resp
            .headers()
            .get(header::CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            .and_then(parse_total_from_content_range)
            .or_else(|| {
                resp.headers()
                    .get(header::CONTENT_LENGTH)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse().ok())
            })
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
            custom_meta: None,
            deleted_at: None,
        };

        if status == StatusCode::NOT_MODIFIED {
            return Ok(GetObjectOutcome::NotModified(meta));
        }

        let content_length = resp
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse().ok())
            .unwrap_or(total_size);

        let http_stream = resp.bytes_stream().map(|chunk| {
            chunk.map_err(|e| std::io::Error::other(e.to_string()))
        });
        let stream =
            GuardedObjectBodyStream::from_http_stream(ObjectBodyStream::Http(Box::pin(http_stream)));

        return Ok(GetObjectOutcome::Content {
            stream,
            content_length,
            total_size,
            meta,
        });
    }

    Err(StorageError::NotFound)
}

fn parse_total_from_content_range(value: &str) -> Option<u64> {
    let part = value.strip_prefix("bytes ")?.split(' ').next()?;
    let (_, total) = part.rsplit_once('/')?;
    total.parse().ok()
}
