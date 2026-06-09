use axum::http::HeaderMap;
use reqwest::StatusCode;
use serde_json::{Map, Value};

use crate::cluster::peer::PeerRegistry;
use crate::storage::engine::StorageEngine;
use crate::storage::error::{internal, StorageError};

/// Human: Download a full object from the first healthy peer and write it locally.
/// Agent: GET /_cluster/objects without Range; put_object with headers from peer response.
pub async fn heal_object_from_peers(
    client: &reqwest::Client,
    peers: &PeerRegistry,
    self_id: &str,
    token: &str,
    engine: &StorageEngine,
    bucket: &str,
    key: &str,
) -> Result<bool, StorageError> {
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
        let resp = match client
            .get(&url)
            .bearer_auth(token)
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(peer_id = %peer_id, error = %e, "peer heal fetch failed");
                continue;
            }
        };
        if resp.status() == StatusCode::NOT_FOUND {
            continue;
        }
        if !resp.status().is_success() {
            tracing::warn!(
                peer_id = %peer_id,
                status = %resp.status(),
                "peer heal fetch returned error"
            );
            continue;
        }

        let content_type = resp
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let custom_meta = custom_meta_json_from_headers(resp.headers());
        let bytes = resp.bytes().await.map_err(internal)?;

        engine
            .put_object(
                bucket,
                key,
                content_type.as_deref(),
                custom_meta.as_deref(),
                std::io::Cursor::new(bytes.to_vec()),
            )
            .await?;

        tracing::info!(
            peer_id = %peer_id,
            %bucket,
            %key,
            "healed object from peer"
        );
        return Ok(true);
    }
    Ok(false)
}

pub fn custom_meta_json_from_headers(headers: &HeaderMap) -> Option<String> {
    let mut map = Map::new();
    for (name, value) in headers.iter() {
        let Some(name_str) = name.as_str().strip_prefix("x-nd-custom-meta-") else {
            continue;
        };
        if let Ok(v) = value.to_str() {
            map.insert(name_str.to_string(), Value::String(v.to_string()));
        }
    }
    if map.is_empty() {
        None
    } else {
        Some(serde_json::to_string(&map).unwrap_or_default())
    }
}
