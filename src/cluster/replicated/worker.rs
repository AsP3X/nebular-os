use std::sync::Arc;
use std::time::Duration;

use reqwest::multipart;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

use crate::cluster::config::ClusterConfig;
use crate::cluster::peer::PeerRegistry;
use crate::storage::error::{internal, StorageError};

use super::log::{ReplicationEvent, ReplicationLog, ReplicationOp};

/// Human: Background task drains replication_log and POSTs to peers asynchronously.
/// Agent: tokio::spawn loop; READS pending rows; reqwest multipart put / json delete; mark_sent/failed.
pub fn spawn_replication_worker(
    log: Arc<ReplicationLog>,
    peers: Arc<PeerRegistry>,
    cluster: Arc<ClusterConfig>,
    token: String,
) {
    if !cluster.mode_includes_replication() {
        return;
    }

    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let mut ticker = tokio::time::interval(Duration::from_secs(1));
        loop {
            ticker.tick().await;
            if let Err(e) = drain_once(&client, &log, &peers, &cluster, &token).await {
                tracing::error!(error = %e, "replication worker tick failed");
            }
        }
    });
}

/// Human: One worker iteration for tests and the background loop.
/// Agent: list_pending(32); push each; mark_sent when peer quota met or factor=1.
pub async fn drain_once(
    client: &reqwest::Client,
    log: &ReplicationLog,
    peers: &PeerRegistry,
    cluster: &ClusterConfig,
    token: &str,
) -> Result<(), StorageError> {
    let pending = log.list_pending(32).await?;
    let needed_peer_successes = cluster.replication_factor.saturating_sub(1);

    for event in pending {
        if needed_peer_successes == 0 {
            log.mark_sent(&event.event_id).await?;
            continue;
        }

        let mut successes = 0u32;
        let mut attempts = 0u32;
        for (peer_id, base_url) in &peers.peers {
            if peer_id == &cluster.node_id {
                continue;
            }
            attempts += 1;
            match push_event(client, log, base_url, token, &event).await {
                Ok(()) => {
                    successes += 1;
                    if successes >= needed_peer_successes {
                        break;
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        peer_id = %peer_id,
                        event_id = %event.event_id,
                        error = %e,
                        "replication push failed"
                    );
                }
            }
        }

        if successes >= needed_peer_successes {
            log.mark_sent(&event.event_id).await?;
        } else if attempts > 0 && successes == 0 {
            log.mark_failed(&event.event_id).await?;
        }
    }

    Ok(())
}

async fn push_event(
    client: &reqwest::Client,
    log: &ReplicationLog,
    base_url: &str,
    token: &str,
    event: &ReplicationEvent,
) -> Result<(), StorageError> {
    let url = format!(
        "{}/_cluster/replicate",
        base_url.trim_end_matches('/')
    );

    match event.op {
        ReplicationOp::Put => {
            let rel = event
                .payload_path
                .as_ref()
                .ok_or(StorageError::NotFound)?;
            let path = std::path::Path::new(log.data_dir()).join(rel);
            let mut file = File::open(&path).await.map_err(internal)?;
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes).await.map_err(internal)?;

            let event_json = serde_json::to_string(event).map_err(internal)?;
            let part_event = multipart::Part::text(event_json)
                .mime_str("application/json")
                .map_err(internal)?;
            let part_blob = multipart::Part::bytes(bytes)
                .mime_str("application/octet-stream")
                .map_err(internal)?;
            let form = multipart::Form::new()
                .part("event", part_event)
                .part("blob", part_blob);

            let resp = client
                .post(&url)
                .bearer_auth(token)
                .multipart(form)
                .send()
                .await
                .map_err(internal)?;

            if !resp.status().is_success() {
                return Err(internal(anyhow::anyhow!(
                    "peer returned {}",
                    resp.status()
                )));
            }
        }
        ReplicationOp::Delete => {
            let resp = client
                .post(&url)
                .bearer_auth(token)
                .json(event)
                .send()
                .await
                .map_err(internal)?;
            if !resp.status().is_success() {
                return Err(internal(anyhow::anyhow!(
                    "peer returned {}",
                    resp.status()
                )));
            }
        }
    }

    Ok(())
}
