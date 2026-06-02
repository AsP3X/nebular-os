// Human: Runtime cluster configuration — Ownly pushes topology via PUT /_cluster/config.
// Agent: READS/WRITES cluster_runtime_config SQLite row; REBUILDS StorageBackend in-process on apply.

use std::sync::Arc;

use anyhow::Context;
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::cluster::backend::build_backend;
use crate::cluster::config::{ClusterConfig, ClusterMode};
use crate::routes::AppState;
use crate::storage::engine::StorageEngine;

/// Human: JSON body Ownly sends when registering or updating a storage node.
/// Agent: DESERIALIZE PUT /_cluster/config; MAPS to ClusterConfig + peers_raw.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RuntimeClusterPut {
    pub mode: String,
    pub node_id: String,
    #[serde(default)]
    pub region_label: Option<String>,
    #[serde(default)]
    pub cluster_token: Option<String>,
    #[serde(default)]
    pub peers: Vec<RuntimeClusterPeer>,
    #[serde(default)]
    pub storage_classes: Vec<String>,
    #[serde(default)]
    pub default_storage_class: Option<String>,
    #[serde(default)]
    pub replication_group: Option<String>,
    #[serde(default)]
    pub replication_role: Option<String>,
    #[serde(default)]
    pub replication_factor: Option<u32>,
    #[serde(default)]
    pub replication_async: Option<bool>,
    #[serde(default)]
    pub assignment_rules: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RuntimeClusterPeer {
    pub id: String,
    pub url: String,
    #[serde(default)]
    pub storage_classes: Vec<String>,
    #[serde(default)]
    pub group: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct RuntimeClusterGet {
    pub mode: String,
    pub node_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region_label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster_token: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub peers: Vec<RuntimeClusterPeer>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub storage_classes: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_storage_class: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replication_group: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replication_role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replication_factor: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replication_async: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assignment_rules: Option<serde_json::Value>,
}

// Human: Serialize peer registry entries into NOS_CLUSTER_PEERS wire format.
fn peers_to_raw(peers: &[RuntimeClusterPeer]) -> String {
    peers
        .iter()
        .map(|peer| {
            let mut entry = format!(
                "{}={}",
                peer.id.trim(),
                peer.url.trim().trim_end_matches('/')
            );
            if !peer.storage_classes.is_empty() {
                entry.push(';');
                entry.push_str(
                    &peer
                        .storage_classes
                        .iter()
                        .map(|c| c.trim())
                        .filter(|c| !c.is_empty())
                        .collect::<Vec<_>>()
                        .join(","),
                );
            }
            if let Some(group) = peer.group.as_deref().map(str::trim).filter(|g| !g.is_empty()) {
                entry.push_str(&format!(";group={group}"));
            }
            entry
        })
        .collect::<Vec<_>>()
        .join(",")
}

// Human: Parse runtime PUT JSON into an in-memory ClusterConfig.
pub fn cluster_config_from_put(body: &RuntimeClusterPut) -> anyhow::Result<ClusterConfig> {
    let mode = match body.mode.trim().to_ascii_lowercase().as_str() {
        "standalone" | "" => ClusterMode::Standalone,
        "replicated" => ClusterMode::Replicated,
        "assigned" => ClusterMode::Assigned,
        "replicated+assigned" | "replicated_assigned" => ClusterMode::ReplicatedAssigned,
        other => anyhow::bail!("unsupported cluster mode: {other}"),
    };

    let node_id = body.node_id.trim();
    if node_id.is_empty() {
        anyhow::bail!("node_id is required");
    }

    if mode == ClusterMode::Standalone {
        return Ok(ClusterConfig {
            mode,
            node_id: node_id.to_string(),
            instance_id: node_id.to_string(),
            region_label: body
                .region_label
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string),
            cluster_token: None,
            peers_raw: None,
            storage_classes: vec!["default".into()],
            replication_group: "default".into(),
            replication_role: "member".into(),
            replication_factor: 1,
            replication_pending_events: 0,
            replication_read_repair: false,
            replication_async: true,
            default_storage_class: "default".into(),
            assignment_rules_raw: None,
            assignment_forward: false,
        });
    }

    let cluster_token = body
        .cluster_token
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .ok_or_else(|| anyhow::anyhow!("cluster_token is required for non-standalone modes"))?;

    let peers_raw = peers_to_raw(&body.peers);
    if peers_raw.is_empty() && matches!(mode, ClusterMode::Replicated | ClusterMode::ReplicatedAssigned) {
        tracing::warn!("replicated cluster configured with zero peers");
    }

    let storage_classes = if body.storage_classes.is_empty() {
        vec!["default".into()]
    } else {
        body.storage_classes
            .iter()
            .map(|c| c.trim().to_string())
            .filter(|c| !c.is_empty())
            .collect()
    };

    let assignment_rules_raw = body
        .assignment_rules
        .as_ref()
        .map(|v| v.to_string())
        .filter(|s| !s.is_empty());

    if matches!(mode, ClusterMode::Assigned | ClusterMode::ReplicatedAssigned)
        && assignment_rules_raw.is_none()
    {
        anyhow::bail!("assignment_rules is required for assigned cluster modes");
    }

    Ok(ClusterConfig {
        mode,
        node_id: node_id.to_string(),
        instance_id: node_id.to_string(),
        region_label: body
            .region_label
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string),
        cluster_token: Some(cluster_token),
        peers_raw: Some(peers_raw),
        storage_classes,
        replication_group: body
            .replication_group
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("default")
            .to_string(),
        replication_role: body
            .replication_role
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("member")
            .to_string(),
        replication_factor: body.replication_factor.unwrap_or(1).max(1),
        replication_pending_events: 0,
        replication_read_repair: false,
        replication_async: body.replication_async.unwrap_or(true),
        default_storage_class: body
            .default_storage_class
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("default")
            .to_string(),
        assignment_rules_raw,
        assignment_forward: false,
    })
}

pub fn runtime_get_from_cluster(cluster: &ClusterConfig) -> RuntimeClusterGet {
    let peers = cluster
        .peers_raw
        .as_deref()
        .map(parse_peers_raw)
        .unwrap_or_default();
    let assignment_rules = cluster
        .assignment_rules_raw
        .as_deref()
        .and_then(|raw| serde_json::from_str(raw).ok());

    RuntimeClusterGet {
        mode: cluster.mode.as_str().to_string(),
        node_id: cluster.node_id.clone(),
        region_label: cluster.region_label.clone(),
        cluster_token: cluster.cluster_token.as_ref().map(|_| "[redacted]".to_string()),
        peers,
        storage_classes: cluster.storage_classes.clone(),
        default_storage_class: Some(cluster.default_storage_class.clone()),
        replication_group: Some(cluster.replication_group.clone()),
        replication_role: Some(cluster.replication_role.clone()),
        replication_factor: Some(cluster.replication_factor),
        replication_async: Some(cluster.replication_async),
        assignment_rules,
    }
}

// Human: Reverse NOS_CLUSTER_PEERS wire format into structured peers for GET /_cluster/config.
fn parse_peers_raw(raw: &str) -> Vec<RuntimeClusterPeer> {
    raw.split(',')
        .filter_map(|entry| {
            let entry = entry.trim();
            if entry.is_empty() {
                return None;
            }
            let (id_part, rest) = entry.split_once('=')?;
            let segments: Vec<&str> = rest.split(';').map(str::trim).filter(|s| !s.is_empty()).collect();
            let url = segments.first()?.to_string();
            let mut storage_classes = Vec::new();
            let mut group = None;
            for seg in segments.iter().skip(1) {
                if let Some((key, value)) = seg.split_once('=') {
                    if key.trim() == "group" && !value.trim().is_empty() {
                        group = Some(value.trim().to_string());
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
            Some(RuntimeClusterPeer {
                id: id_part.trim().to_string(),
                url,
                storage_classes,
                group,
            })
        })
        .collect()
}

// Human: Load persisted runtime config at startup before building the storage backend.
pub async fn cluster_config_from_storage(engine: &StorageEngine) -> anyhow::Result<Option<ClusterConfig>> {
    let Some(json) = engine.load_cluster_runtime_config().await? else {
        return Ok(None);
    };
    let put: RuntimeClusterPut = serde_json::from_str(&json).context("invalid cluster_runtime_config JSON")?;
    Ok(Some(cluster_config_from_put(&put)?))
}

// Human: Rebuild cluster backend and swap it into AppState after runtime apply.
async fn apply_runtime_cluster(
    state: &Arc<AppState>,
    put: &RuntimeClusterPut,
) -> anyhow::Result<()> {
    let cluster = cluster_config_from_put(put)?;
    let mut cfg = (*state.config).clone();
    cfg.cluster = cluster.clone();

    let engine = state.engine.clone();
    let backend = build_backend(engine.clone(), &cfg, state.metrics.clone())?;

    engine.save_cluster_runtime_config(&serde_json::to_string(put)?).await?;

    {
        let mut guard = state
            .cluster
            .write()
            .map_err(|_| anyhow::anyhow!("cluster lock poisoned"))?;
        *guard = cluster;
    }
    {
        let mut guard = state.backend.write().await;
        *guard = backend;
    }

    Ok(())
}

/// Human: Operator read-back of the active cluster topology (token redacted).
/// Agent: GET /_cluster/config; Bearer bootstrap or cluster token; NO DB write.
pub async fn get_cluster_config(State(state): State<Arc<AppState>>) -> Json<RuntimeClusterGet> {
    let cluster = state
        .cluster
        .read()
        .map(|c| runtime_get_from_cluster(&c))
        .unwrap_or_else(|_| runtime_get_from_cluster(&state.config.cluster));
    Json(cluster)
}

/// Human: Ownly setup/admin pushes node identity and cluster topology at runtime.
/// Agent: PUT /_cluster/config; PERSISTS SQLite; REBUILDS backend; HTTP 200 JSON {ok:true}.
pub async fn put_cluster_config(
    State(state): State<Arc<AppState>>,
    Json(body): Json<RuntimeClusterPut>,
) -> Response {
    match apply_runtime_cluster(&state, &body).await {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({ "ok": true, "mode": body.mode, "node_id": body.node_id })),
        )
            .into_response(),
        Err(error) => {
            tracing::error!(error = %error, "runtime cluster config apply failed");
            (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "invalid cluster configuration" })),
            )
                .into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peers_to_raw_round_trip_group() {
        let peers = vec![RuntimeClusterPeer {
            id: "node-b".into(),
            url: "http://object-storage-b:9000".into(),
            storage_classes: vec![],
            group: Some("default".into()),
        }];
        let raw = peers_to_raw(&peers);
        assert_eq!(raw, "node-b=http://object-storage-b:9000;group=default");
        let parsed = parse_peers_raw(&raw);
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].id, "node-b");
    }

    #[test]
    fn standalone_put_does_not_require_cluster_token() {
        let cfg = cluster_config_from_put(&RuntimeClusterPut {
            mode: "standalone".into(),
            node_id: "node-primary".into(),
            region_label: Some("Primary".into()),
            cluster_token: None,
            peers: vec![],
            storage_classes: vec![],
            default_storage_class: None,
            replication_group: None,
            replication_role: None,
            replication_factor: None,
            replication_async: None,
            assignment_rules: None,
        })
        .expect("standalone config");
        assert!(cfg.is_standalone());
        assert!(cfg.cluster_token.is_none());
    }
}
