use std::path::{Path, PathBuf};

use axum::http::HeaderMap;
use futures_util::StreamExt;
use reqwest::StatusCode;
use serde_json::{Map, Value};
use tokio::io::AsyncWriteExt;

use crate::cluster::peer::PeerRegistry;
use crate::cluster::replicated::hooks::Restore;
use crate::cluster::replicated::versions::{self, Version};
use crate::cluster::replicated::ReplicationLog;
use crate::storage::engine::{StorageEngine, TempFileGuard};
use crate::storage::error::{internal, StorageError};
use crate::storage::{CommitHook, WriteConditions};

/// Human: Which copy a heal may take, from what this node's metadata says the object should be.
#[derive(Debug, Clone, Copy)]
pub enum HealExpectation<'a> {
    /// The local row names this ETag but its bytes are damaged or missing: only a peer copy of exactly this
    /// version may replace them, and only while the row still names it.
    Version(&'a str),
    /// No local object: take the peer's copy, but never over an object written here meanwhile.
    Absent,
}

/// Human: Restore an object from a peer. A peer's copy is used only if its ETag is the expected version and its
/// bytes hash to that ETag, and it is written conditionally — heal used to take whatever the first peer had,
/// so a lagging peer's older copy (or a failed transfer) could replace the object. The copy is spooled to disk,
/// never held in memory. With a replication log, a key this node deleted is never restored.
/// Agent: RETURNS Ok(true) when healed; Ok(false) when no peer had a matching copy or the object changed here.
#[allow(clippy::too_many_arguments)]
pub async fn heal_object_from_peers(
    client: &reqwest::Client,
    peers: &PeerRegistry,
    self_id: &str,
    token: &str,
    engine: &StorageEngine,
    log: Option<&ReplicationLog>,
    bucket: &str,
    key: &str,
    expect: HealExpectation<'_>,
) -> Result<bool, StorageError> {
    for (peer_id, peer) in &peers.peers {
        if peer_id == self_id {
            continue;
        }
        let Some(path) = crate::cluster::forward::object_path(bucket, key) else {
            break;
        };
        let url = format!("{}/_cluster/objects/{path}", peer.url.trim_end_matches('/'));
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

        let peer_etag = resp
            .headers()
            .get(axum::http::header::ETAG)
            .and_then(|v| v.to_str().ok())
            .map(|v| v.trim_start_matches("W/").trim_matches('"').to_string());
        let Some(peer_etag) = peer_etag.filter(|e| !e.is_empty()) else {
            tracing::warn!(peer_id = %peer_id, %bucket, %key, "peer copy has no ETag; not healing from it");
            continue;
        };
        if let HealExpectation::Version(expected) = expect
            && peer_etag != expected
        {
            tracing::warn!(peer_id = %peer_id, %bucket, %key, %peer_etag, %expected, "peer holds another version; not healing from it");
            continue;
        }
        let content_type = resp
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let custom_meta = custom_meta_json_from_headers(resp.headers());
        let peer_version = resp
            .headers()
            .get(versions::HEADER)
            .and_then(|v| v.to_str().ok())
            .and_then(Version::from_header);

        let spool = TempFileGuard {
            path: PathBuf::from(format!(
                "{}/.tmp/{}.heal",
                engine.data_dir(),
                uuid::Uuid::new_v4()
            )),
        };
        match spool_body(resp, &spool.path).await {
            Ok(digest) if digest == peer_etag => {}
            Ok(_) => {
                tracing::warn!(peer_id = %peer_id, %bucket, %key, "peer copy does not match its ETag; not healing from it");
                continue;
            }
            Err(e) => {
                tracing::warn!(peer_id = %peer_id, %bucket, %key, error = %e, "peer heal transfer failed");
                continue;
            }
        }

        let restore = log.map(|log| Restore {
            log,
            version: peer_version,
        });
        let conditions = match expect {
            HealExpectation::Version(expected) => WriteConditions {
                if_match: Some(expected),
                ..WriteConditions::default()
            },
            HealExpectation::Absent => WriteConditions {
                if_none_match: Some("*"),
                hook: restore.as_ref().map(|hook| hook as &dyn CommitHook),
                ..WriteConditions::default()
            },
        };
        let body = tokio::fs::File::open(&spool.path).await.map_err(internal)?;
        match engine
            .put_object_conditional(
                bucket,
                key,
                content_type.as_deref(),
                custom_meta.as_deref(),
                tokio::io::BufReader::new(body),
                conditions,
            )
            .await
        {
            Ok(_) => {}
            // Human: The object was written or deleted here since it was found missing or damaged.
            Err(StorageError::PreconditionFailed) => return Ok(false),
            Err(e) => return Err(e),
        }

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

/// Write a response body to `path`; RETURNS the xxh3 of the bytes (the form object ETags take).
async fn spool_body(resp: reqwest::Response, path: &Path) -> Result<String, StorageError> {
    let mut file = tokio::fs::File::create(path).await.map_err(internal)?;
    let mut hasher = xxhash_rust::xxh3::Xxh3::new();
    let mut body = resp.bytes_stream();
    while let Some(chunk) = body.next().await {
        let chunk = chunk.map_err(internal)?;
        hasher.update(&chunk);
        file.write_all(&chunk).await.map_err(internal)?;
    }
    file.flush().await.map_err(internal)?;
    Ok(format!("{:016x}", hasher.digest()))
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
