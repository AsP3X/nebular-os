//! Human: HTTP client and peer registry for inter-node replication (Phase 2+).
//! Agent: PeerRegistry parses NOS_CLUSTER_PEERS; optional ;classes= and ;group= per peer.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use reqwest::StatusCode;

/// Human: One cluster peer with base URL, optional class filter, and replication group.
/// Agent: Parsed from id=url[;class-a,class-b][;group=name] in NOS_CLUSTER_PEERS.
#[derive(Debug, Clone)]
pub struct PeerEntry {
    pub url: String,
    pub storage_classes: Vec<String>,
    /// When set, peer receives replication only if this equals the event's replication group.
    pub replication_group: Option<String>,
}

/// Human: Parsed peer list from NOS_CLUSTER_PEERS for outbound replication calls.
/// Agent: Map node_id -> PeerEntry; worker filters by storage_class and replication_group.
#[derive(Debug, Clone, Default)]
pub struct PeerRegistry {
    pub peers: HashMap<String, PeerEntry>,
}

impl PeerRegistry {
    pub fn from_peers_raw(raw: &str) -> Result<Self> {
        let mut peers = HashMap::new();
        for entry in raw.split(',') {
            let entry = entry.trim();
            if entry.is_empty() {
                continue;
            }
            let (id_part, rest) = entry
                .split_once('=')
                .with_context(|| format!("invalid peer entry (expected id=url): {entry}"))?;
            if id_part.is_empty() {
                bail!("invalid peer entry: {entry}");
            }
            let segments: Vec<&str> = rest.split(';').map(str::trim).filter(|s| !s.is_empty()).collect();
            if segments.is_empty() {
                bail!("invalid peer entry (missing url): {entry}");
            }
            let url = segments[0].to_string();
            if url.is_empty() {
                bail!("invalid peer entry: {entry}");
            }
            let mut storage_classes = Vec::new();
            let mut replication_group = None;
            for seg in segments.iter().skip(1) {
                if let Some((key, value)) = seg.split_once('=') {
                    if key.trim() == "group" && !value.trim().is_empty() {
                        replication_group = Some(value.trim().to_string());
                    }
                } else {
                    storage_classes.extend(
                        seg.split(',')
                            .map(str::trim)
                            .filter(|c| !c.is_empty())
                            .map(str::to_string),
                    );
                }
            }
            if peers
                .insert(
                    id_part.to_string(),
                    PeerEntry {
                        url,
                        storage_classes,
                        replication_group,
                    },
                )
                .is_some()
            {
                bail!("duplicate peer id: {id_part}");
            }
        }
        if peers.is_empty() {
            bail!("NOS_CLUSTER_PEERS must list at least one peer");
        }
        Ok(Self { peers })
    }

    pub fn peer_url(&self, node_id: &str) -> Option<&str> {
        self.peers.get(node_id).map(|p| p.url.as_str())
    }

    /// Human: Peers that accept this storage class (empty peer class list = accepts all).
    pub fn peers_for_class<'a>(
        &'a self,
        storage_class: &str,
    ) -> impl Iterator<Item = (&'a String, &'a PeerEntry)> {
        self.peers.iter().filter(move |(_, entry)| {
            entry.storage_classes.is_empty()
                || entry
                    .storage_classes
                    .iter()
                    .any(|c| c == storage_class)
        })
    }

    /// Human: Peers matching storage class and replication group for outbound replication.
    /// Agent: Empty peer `replication_group` means any group; when set, must equal `replication_group`.
    pub fn peers_for_replication<'a>(
        &'a self,
        storage_class: &str,
        replication_group: &str,
    ) -> impl Iterator<Item = (&'a String, &'a PeerEntry)> {
        self.peers.iter().filter(move |(_, entry)| {
            let class_ok = entry.storage_classes.is_empty()
                || entry.storage_classes.iter().any(|c| c == storage_class);
            let group_ok = entry
                .replication_group
                .as_ref()
                .map(|g| g == replication_group)
                .unwrap_or(true);
            class_ok && group_ok
        })
    }
}

/// Human: Periodic HEAD /_cluster/health on peers for ops visibility (non-blocking).
/// Agent: tokio::spawn; logs warn on unreachable peers; does not affect replication decisions.
pub fn spawn_peer_health_checks(
    peers: Arc<PeerRegistry>,
    cluster_token: String,
    node_id: String,
) {
    if peers.peers.is_empty() {
        return;
    }
    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let mut ticker = tokio::time::interval(Duration::from_secs(60));
        loop {
            ticker.tick().await;
            for (peer_id, peer) in &peers.peers {
                if peer_id == &node_id {
                    continue;
                }
                let url = format!(
                    "{}/_cluster/health",
                    peer.url.trim_end_matches('/')
                );
                match client
                    .get(&url)
                    .bearer_auth(&cluster_token)
                    .send()
                    .await
                {
                    Ok(resp) if resp.status() == StatusCode::OK => {}
                    Ok(resp) => {
                        tracing::warn!(
                            peer_id = %peer_id,
                            status = %resp.status(),
                            "cluster peer health check returned non-OK"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            peer_id = %peer_id,
                            error = %e,
                            "cluster peer health check failed"
                        );
                    }
                }
            }
        }
    });
}
