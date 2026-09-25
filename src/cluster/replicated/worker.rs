use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::StreamExt;
use reqwest::multipart;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::cluster::config::ClusterConfig;
use crate::cluster::peer::PeerRegistry;
use crate::observability::NosMetrics;
use crate::storage::error::{internal, StorageError};
use crate::storage::streaming::{open_object_body_stream, ReadContext};

use super::log::{PendingDelivery, ReplicationEvent, ReplicationLog, ReplicationOp};

/// Events taken from the log per batch; the worker drains batches until none is due.
const BATCH: usize = 64;

/// Human: Background task that drains replication_log and pushes events to peers.
/// Agent: tokio::spawn loop; every second, delivers batches until no event is due; ENDS when `shutdown` fires
/// (the backend was replaced by a config reload), between batches.
pub fn spawn_replication_worker(
    log: Arc<ReplicationLog>,
    peers: Arc<PeerRegistry>,
    cluster: Arc<ClusterConfig>,
    token: String,
    metrics: Arc<NosMetrics>,
    shutdown: CancellationToken,
) {
    if !cluster.mode_includes_replication() {
        return;
    }

    tokio::spawn(async move {
        // Human: Pushes upload whole objects; each one gets a budget sized to it (see `deliver`).
        let client = crate::cluster::http::upload_client();
        let mut ticker = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    tracing::info!("replication worker stopping after cluster config reload");
                    return;
                }
                _ = ticker.tick() => {}
            }
            while !shutdown.is_cancelled() {
                match drain_batch(&client, &log, &peers, &cluster, &token, &metrics, None).await {
                    Ok(handled) if handled == BATCH => continue,
                    Ok(_) => break,
                    Err(e) => {
                        tracing::error!(error = %e, "replication worker tick failed");
                        break;
                    }
                }
            }
        }
    });
}

/// Human: One delivery batch, for tests and callers that drive the worker themselves.
/// Agent: CALLS drain_batch; DISCARDS the event count.
pub async fn drain_once(
    client: &reqwest::Client,
    log: &ReplicationLog,
    peers: &PeerRegistry,
    cluster: &ClusterConfig,
    token: &str,
    metrics: &NosMetrics,
    peer_sem: Option<Arc<Semaphore>>,
) -> Result<(), StorageError> {
    drain_batch(client, log, peers, cluster, token, metrics, peer_sem)
        .await
        .map(drop)
}

/// Human: Deliver one batch of due events, up to NOS_REPLICATION_PEER_CONCURRENCY at a time; RETURNS how many
/// events the batch held. An event goes to the peers that don't have it yet until the replication factor is met;
/// one that falls short — even when some peers got it — is retried later with backoff and eventually
/// dead-lettered, instead of being resent every second while newer events wait behind it.
/// Agent: `peer_sem` additionally caps concurrent pushes; a peer that is unreachable once is skipped for the
/// rest of the batch, so one dead peer costs one timeout per batch, not one per event.
pub async fn drain_batch(
    client: &reqwest::Client,
    log: &ReplicationLog,
    peers: &PeerRegistry,
    cluster: &ClusterConfig,
    token: &str,
    metrics: &NosMetrics,
    peer_sem: Option<Arc<Semaphore>>,
) -> Result<usize, StorageError> {
    let pending = log.list_pending_deliveries(BATCH as i64).await?;
    let handled = pending.len();
    let batch = Batch {
        client,
        log,
        peers,
        cluster,
        token,
        metrics,
        peer_sem,
        unreachable: Mutex::new(HashSet::new()),
    };
    let concurrency = cluster.replication_peer_concurrency.max(1) as usize;
    let results: Vec<Result<(), StorageError>> = futures_util::stream::iter(pending)
        .map(|pending| batch.deliver(pending))
        .buffer_unordered(concurrency)
        .collect()
        .await;
    results.into_iter().collect::<Result<(), _>>()?;
    Ok(handled)
}

struct Batch<'a> {
    client: &'a reqwest::Client,
    log: &'a ReplicationLog,
    peers: &'a PeerRegistry,
    cluster: &'a ClusterConfig,
    token: &'a str,
    metrics: &'a NosMetrics,
    peer_sem: Option<Arc<Semaphore>>,
    /// Peers that could not be reached during this batch.
    unreachable: Mutex<HashSet<String>>,
}

impl Batch<'_> {
    fn is_unreachable(&self, peer_id: &str) -> bool {
        self.unreachable
            .lock()
            .is_ok_and(|peers| peers.contains(peer_id))
    }

    fn set_unreachable(&self, peer_id: &str) {
        if let Ok(mut peers) = self.unreachable.lock() {
            peers.insert(peer_id.to_string());
        }
    }

    /// Human: Send one event to the peers still missing it, or settle it without sending.
    /// Agent: MARKS sent (factor met) | superseded (key has a newer version) | failed (short; backoff, dead-letter).
    async fn deliver(&self, pending: PendingDelivery) -> Result<(), StorageError> {
        let PendingDelivery {
            event,
            delivered_to,
        } = pending;
        let needed = self.cluster.replication_factor.saturating_sub(1) as usize;
        if needed == 0 {
            return self.log.mark_sent(&event.event_id).await;
        }
        // Human: A newer change to the key was made (or applied) here since; peers get that one instead.
        if self
            .log
            .key_version(&event.bucket, &event.key)
            .await?
            .is_some_and(|current| current.version > event.effective_version())
        {
            return self.log.mark_superseded(&event.event_id).await;
        }

        let targets: Vec<_> = self
            .peers
            .peers_for_replication(&event.storage_class, &event.replication_group)
            .filter(|(peer_id, _)| **peer_id != self.cluster.node_id)
            .collect();
        if targets.is_empty() {
            tracing::error!(
                event_id = %event.event_id,
                storage_class = %event.storage_class,
                replication_group = %event.replication_group,
                "no cluster peers match replication class and group"
            );
            return self
                .log
                .mark_failed(&event.event_id, self.cluster.replication_max_attempts)
                .await;
        }

        let mut delivered = targets
            .iter()
            .filter(|(peer_id, _)| delivered_to.contains(peer_id))
            .count();
        for (peer_id, peer) in &targets {
            if delivered >= needed {
                break;
            }
            if delivered_to.contains(peer_id) || self.is_unreachable(peer_id) {
                continue;
            }
            let _permit = match self.peer_sem.as_ref() {
                Some(sem) => Some(sem.acquire().await.map_err(internal)?),
                None => None,
            };
            let deadline = crate::cluster::http::transfer_timeout(event.size.unwrap_or(0).max(0) as u64);
            let pushed =
                tokio::time::timeout(deadline, push_event(self.client, self.log, &peer.url, self.token, &event))
                    .await
                    .unwrap_or_else(|_| Err(PushError::Unreachable("timed out".into())));
            match pushed {
                Ok(()) => {
                    self.log.mark_delivered(&event.event_id, peer_id).await?;
                    delivered += 1;
                }
                Err(e) => {
                    self.metrics.inc_replication_errors();
                    if matches!(e, PushError::Unreachable(_)) {
                        self.set_unreachable(peer_id);
                    }
                    tracing::warn!(
                        peer_id = %peer_id,
                        event_id = %event.event_id,
                        error = %e,
                        "replication push failed"
                    );
                }
            }
        }

        if delivered >= needed {
            self.log.mark_sent(&event.event_id).await
        } else {
            self.log
                .mark_failed(&event.event_id, self.cluster.replication_max_attempts)
                .await
        }
    }
}

/// Why a push failed: the peer couldn't be reached (skip it for the rest of the batch), or this event failed.
#[derive(Debug)]
enum PushError {
    Unreachable(String),
    Failed(String),
}

impl std::fmt::Display for PushError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PushError::Unreachable(e) => write!(f, "peer unreachable: {e}"),
            PushError::Failed(e) => f.write_str(e),
        }
    }
}

impl From<StorageError> for PushError {
    fn from(e: StorageError) -> Self {
        PushError::Failed(e.to_string())
    }
}

fn transport_error(e: reqwest::Error) -> PushError {
    // Agent: connect or timeout errors → Unreachable (skip the peer this batch); anything else → Failed.
    if e.is_connect() || e.is_timeout() {
        PushError::Unreachable(e.to_string())
    } else {
        PushError::Failed(e.to_string())
    }
}

async fn push_event(
    client: &reqwest::Client,
    log: &ReplicationLog,
    base_url: &str,
    token: &str,
    event: &ReplicationEvent,
) -> Result<(), PushError> {
    let url = format!(
        "{}/_cluster/replicate",
        base_url.trim_end_matches('/')
    );

    let request = match event.op {
        ReplicationOp::Put => {
            // Human: Find the blob now instead of trusting the path recorded at enqueue time, which is always the
            // flat layout — objects still in the legacy nested layout (e.g. queued by backfill) dead-lettered.
            let variants =
                crate::storage::blob_path_variants(log.data_dir(), &event.bucket, &event.key);
            let path = match crate::storage::first_existing_blob_path(&variants)
                .await
                .map_err(internal)?
            {
                Some(path) => path,
                None => {
                    let rel = event
                        .payload_path
                        .as_ref()
                        .ok_or(StorageError::NotFound)?;
                    std::path::Path::new(log.data_dir()).join(rel)
                }
            };
            // Human: Ship the object's content: decode Nebular's container (NOSI/NOSB/NOSZ/NOS2/NOSD) here, or
            // peers store the compressed file itself as the object and serve it back to clients.
            let size = event
                .size
                .and_then(|s| u64::try_from(s).ok())
                .ok_or_else(|| internal(anyhow::anyhow!("put event without size")))?;
            let content = open_object_body_stream(
                &path,
                size,
                0,
                size,
                &ReadContext::for_data_dir(log.data_dir()),
            )
            .await?;
            // Human: The checksum must describe the bytes sent: the object ETag (xxh3 of its content). This
            // also repairs events queued by older versions, which hashed the on-disk container instead.
            let event = ReplicationEvent {
                wire_checksum: event.etag.clone().filter(|e| !e.is_empty()),
                ..event.clone()
            };
            let event_json = serde_json::to_string(&event).map_err(internal)?;
            let part_event = multipart::Part::text(event_json)
                .mime_str("application/json")
                .map_err(internal)?;
            let part_blob = multipart::Part::stream_with_length(reqwest::Body::wrap_stream(content), size)
                .mime_str("application/octet-stream")
                .map_err(internal)?;
            let form = multipart::Form::new()
                .part("event", part_event)
                .part("blob", part_blob);
            client.post(&url).bearer_auth(token).multipart(form)
        }
        ReplicationOp::Delete => client.post(&url).bearer_auth(token).json(event),
    };

    let resp = request.send().await.map_err(transport_error)?;
    if !resp.status().is_success() {
        return Err(PushError::Failed(format!("peer returned {}", resp.status())));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::cluster::config::ClusterMode;
    use crate::cluster::replicated::hooks::LocalChange;
    use crate::storage::engine::{EngineOptions, StorageEngine};
    use crate::storage::WriteConditions;

    /// A peer that accepts every replication push and counts them.
    async fn counting_peer() -> (String, Arc<AtomicUsize>) {
        let count = Arc::new(AtomicUsize::new(0));
        let hits = count.clone();
        let app = axum::Router::new().route(
            "/_cluster/replicate",
            axum::routing::post(move |_body: axum::body::Bytes| {
                let hits = hits.clone();
                async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                    axum::http::StatusCode::OK
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (url, count)
    }

    /// Nothing listens on port 1: pushes there fail to connect.
    const DOWN: &str = "http://127.0.0.1:1";

    struct Origin {
        engine: StorageEngine,
        log: ReplicationLog,
        cluster: ClusterConfig,
        metrics: Arc<NosMetrics>,
        _tmp: tempfile::TempDir,
    }

    impl Origin {
        async fn new(replication_factor: u32) -> Self {
            let tmp = tempfile::TempDir::new().unwrap();
            let data_dir = tmp.path().join("blobs");
            std::fs::create_dir_all(&data_dir).unwrap();
            let data_dir = data_dir.to_string_lossy().replace('\\', "/");
            let meta = format!("file:{}?mode=memory&cache=shared", uuid::Uuid::new_v4());
            let engine = StorageEngine::with_full_options(&meta, &data_dir, EngineOptions::default())
                .await
                .unwrap();
            let log = ReplicationLog::new(engine.write_pool().clone(), data_dir, "node-a".into());
            let cluster = ClusterConfig {
                mode: ClusterMode::Replicated,
                node_id: "node-a".into(),
                replication_factor,
                replication_peer_concurrency: 1,
                ..ClusterConfig::standalone()
            };
            Self {
                engine,
                log,
                cluster,
                metrics: NosMetrics::new(),
                _tmp: tmp,
            }
        }

        async fn put(&self, key: &str, body: &[u8]) {
            let hook = LocalChange {
                log: &self.log,
                cluster: &self.cluster,
                storage_class: "default",
                replication_group: "default",
            };
            let conditions = WriteConditions {
                hook: Some(&hook),
                ..WriteConditions::default()
            };
            self.engine
                .put_object_conditional("b", key, None, None, std::io::Cursor::new(body.to_vec()), conditions)
                .await
                .unwrap();
        }

        async fn drain(&self, peers: &str) -> usize {
            let peers = PeerRegistry::from_peers_raw(peers).unwrap();
            drain_batch(
                &crate::cluster::http::client(),
                &self.log,
                &peers,
                &self.cluster,
                "token",
                &self.metrics,
                None,
            )
            .await
            .unwrap()
        }

        /// (status, delivered_to, attempts) of each event, oldest first.
        async fn events(&self) -> Vec<(String, Option<String>, i64)> {
            sqlx::query_as("SELECT status, delivered_to, attempts FROM replication_log ORDER BY rowid")
                .fetch_all(self.log.pool())
                .await
                .unwrap()
        }

        async fn make_all_due(&self) {
            sqlx::query("UPDATE replication_log SET next_retry_at = 0")
                .execute(self.log.pool())
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn a_partial_delivery_backs_off_and_retries_only_the_peers_missing_it() {
        let origin = Origin::new(3).await;
        let (up, pushes) = counting_peer().await;
        let peers = format!("node-b={up},node-c={DOWN}");
        origin.put("k", b"payload").await;

        origin.drain(&peers).await;
        assert_eq!(pushes.load(Ordering::SeqCst), 1);
        // Human: It used to stay pending — resent to node-b every second, never dead-lettered, and blocking
        // newer events once a batch filled up with such events.
        assert_eq!(origin.events().await, [("failed".into(), Some("node-b".into()), 1)]);
        assert_eq!(origin.drain(&peers).await, 0, "backing off, not due yet");
        let waiting = origin.log.status_report().await.unwrap().oldest_pending_age_secs;
        assert!(waiting.is_some_and(|age| age < 60), "{waiting:?}");

        origin.make_all_due().await;
        origin.drain(&peers).await;
        assert_eq!(pushes.load(Ordering::SeqCst), 1, "node-b already has it");
        assert_eq!(origin.events().await, [("failed".into(), Some("node-b".into()), 2)]);

        let (also_up, _) = counting_peer().await;
        origin.make_all_due().await;
        origin.drain(&format!("node-b={up},node-c={also_up}")).await;
        assert_eq!(origin.events().await[0].0, "sent");
    }

    #[tokio::test]
    async fn only_the_latest_change_to_a_key_is_sent() {
        let origin = Origin::new(2).await;
        let (up, pushes) = counting_peer().await;
        origin.put("k", b"first").await;
        origin.put("k", b"second").await;

        origin.drain(&format!("node-b={up}")).await;
        assert_eq!(pushes.load(Ordering::SeqCst), 1);
        let statuses: Vec<String> = origin.events().await.into_iter().map(|e| e.0).collect();
        assert_eq!(statuses, ["superseded", "sent"]);
        let report = origin.log.status_report().await.unwrap();
        assert_eq!(report.superseded, 1);
        assert_eq!(report.oldest_pending_age_secs, None, "nothing is waiting");
    }

    #[tokio::test]
    async fn an_unreachable_peer_costs_one_attempt_per_batch() {
        let origin = Origin::new(2).await;
        for i in 0..5 {
            origin.put(&format!("k{i}"), b"payload").await;
        }
        assert_eq!(origin.drain(&format!("node-c={DOWN}")).await, 5);
        assert_eq!(origin.metrics.replication_errors_total(), 1);
        assert!(origin.events().await.iter().all(|e| e.0 == "failed"));
    }

    #[tokio::test]
    async fn finished_history_is_pruned_and_pending_work_kept() {
        let origin = Origin::new(2).await;
        for (i, status) in ["sent", "superseded", "applied", "pending", "failed", "dead_letter"]
            .iter()
            .enumerate()
        {
            origin.put(&format!("k{i}"), b"payload").await;
            sqlx::query("UPDATE replication_log SET status = ?, created_at = 0 WHERE key = ?")
                .bind(status)
                .bind(format!("k{i}"))
                .execute(origin.log.pool())
                .await
                .unwrap();
        }
        origin.put("recent", b"payload").await;
        let recent: String = sqlx::query_scalar("SELECT event_id FROM replication_log WHERE key = 'recent'")
            .fetch_one(origin.log.pool())
            .await
            .unwrap();
        origin.log.mark_sent(&recent).await.unwrap();
        sqlx::query("UPDATE replication_versions SET deleted = 1, recorded_at = 0 WHERE key IN ('k0', 'k1')")
            .execute(origin.log.pool())
            .await
            .unwrap();

        let report = origin.log.prune(3600).await.unwrap();
        assert_eq!(report, crate::cluster::replicated::PruneReport { events: 3, tombstones: 2 });
        let left: Vec<String> = origin.events().await.into_iter().map(|e| e.0).collect();
        assert_eq!(left, ["pending", "failed", "dead_letter", "sent"]);
    }
}
