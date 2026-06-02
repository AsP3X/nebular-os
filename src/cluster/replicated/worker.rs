use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

static WORKER_EPOCH: AtomicU64 = AtomicU64::new(0);

/// Human: Invalidate background replication loops after PUT /_cluster/config hot reload.
pub fn bump_worker_epoch() {
    WORKER_EPOCH.fetch_add(1, Ordering::SeqCst);
}

use reqwest::multipart;
use tokio::fs::File;

use crate::cluster::config::ClusterConfig;
use crate::cluster::peer::PeerRegistry;
use crate::observability::NosMetrics;
use crate::storage::error::{internal, StorageError};

use super::log::{ReplicationEvent, ReplicationLog, ReplicationOp};

/// Human: Background task drains replication_log and POSTs to peers asynchronously.
/// Agent: tokio::spawn loop; streams blob via multipart file part; exponential retry on failure.
pub fn spawn_replication_worker(
    log: Arc<ReplicationLog>,
    peers: Arc<PeerRegistry>,
    cluster: Arc<ClusterConfig>,
    token: String,
    metrics: Arc<NosMetrics>,
) {
    if !cluster.mode_includes_replication() {
        return;
    }

    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let mut ticker = tokio::time::interval(Duration::from_secs(1));
        let started_epoch = WORKER_EPOCH.load(Ordering::SeqCst);
        loop {
            if WORKER_EPOCH.load(Ordering::SeqCst) != started_epoch {
                tracing::info!("replication worker stopping after cluster config reload");
                break;
            }
            ticker.tick().await;
            if let Err(e) = drain_once(&client, &log, &peers, &cluster, &token, &metrics).await {
                tracing::error!(error = %e, "replication worker tick failed");
            }
        }
    });
}

/// Human: One worker iteration for tests and the background loop.
/// Agent: list_pending includes failed rows past next_retry_at; mark_sent when peer quota met.
pub async fn drain_once(
    client: &reqwest::Client,
    log: &ReplicationLog,
    peers: &PeerRegistry,
    cluster: &ClusterConfig,
    token: &str,
    metrics: &NosMetrics,
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
        for (peer_id, peer) in
            peers.peers_for_replication(&event.storage_class, &event.replication_group)
        {
            if peer_id == &cluster.node_id {
                continue;
            }
            attempts += 1;
            match push_event(client, log, &peer.url, token, &event).await {
                Ok(()) => {
                    successes += 1;
                    if successes >= needed_peer_successes {
                        break;
                    }
                }
                Err(e) => {
                    metrics.inc_replication_errors();
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
        } else if attempts == 0 {
            tracing::error!(
                event_id = %event.event_id,
                storage_class = %event.storage_class,
                replication_group = %event.replication_group,
                "no cluster peers match replication class and group"
            );
            log.mark_failed(&event.event_id).await?;
        } else if successes == 0 {
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
            let file = File::open(&path).await.map_err(internal)?;
            let event_json = serde_json::to_string(event).map_err(internal)?;
            let part_event = multipart::Part::text(event_json)
                .mime_str("application/json")
                .map_err(internal)?;
            let part_blob = multipart::Part::stream(file)
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
