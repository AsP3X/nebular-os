//! Human: Runtime cluster configuration API for Ownly setup and admin console.
//! Agent: GET/PUT /_cluster/config; bootstrap or cluster token; persists to SQLite.

use std::sync::Arc;

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;

use crate::routes::AppState;

use super::reload::apply_cluster_snapshot;
use super::store::ClusterConfigSnapshot;

#[derive(Debug, Serialize)]
pub struct ClusterConfigGetResponse {
    pub configured: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<ClusterConfigSnapshotPublic>,
}

/// Human: GET response omits cluster_token secret; operators use cluster_token_set flag.
#[derive(Debug, Serialize)]
pub struct ClusterConfigSnapshotPublic {
    pub mode: String,
    pub node_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region_label: Option<String>,
    pub cluster_token_set: bool,
    pub peers: Vec<super::store::ClusterPeerSnapshot>,
    pub storage_classes: Vec<String>,
    pub replication_group: String,
    pub replication_role: String,
    pub replication_factor: u32,
    pub replication_read_repair: bool,
    pub replication_async: bool,
    pub default_storage_class: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assignment_rules: Option<serde_json::Value>,
    pub assignment_forward: bool,
}

impl From<ClusterConfigSnapshot> for ClusterConfigSnapshotPublic {
    fn from(s: ClusterConfigSnapshot) -> Self {
        Self {
            mode: s.mode,
            node_id: s.node_id,
            instance_id: s.instance_id,
            region_label: s.region_label,
            cluster_token_set: !s.cluster_token.is_empty(),
            peers: s.peers,
            storage_classes: s.storage_classes,
            replication_group: s.replication_group,
            replication_role: s.replication_role,
            replication_factor: s.replication_factor,
            replication_read_repair: s.replication_read_repair,
            replication_async: s.replication_async,
            default_storage_class: s.default_storage_class,
            assignment_rules: s.assignment_rules,
            assignment_forward: s.assignment_forward,
        }
    }
}

/// Human: Read persisted or in-memory cluster topology for admin UIs.
/// Agent: GET /_cluster/config; 200 with configured=false when standalone and no snapshot.
pub async fn get_cluster_config(State(state): State<Arc<AppState>>) -> Response {
    let cluster = state.cluster();
    if cluster.is_standalone() {
        if let Ok(Some(snap)) = state.engine.load_cluster_config_snapshot().await {
            return (
                StatusCode::OK,
                Json(ClusterConfigGetResponse {
                    configured: true,
                    config: Some(ClusterConfigSnapshotPublic::from(snap)),
                }),
            )
                .into_response();
        }
        return (
            StatusCode::OK,
            Json(ClusterConfigGetResponse {
                configured: false,
                config: None,
            }),
        )
            .into_response();
    }

    match super::store::ClusterConfigSnapshot::from_config(&cluster) {
        Ok(snap) => (
            StatusCode::OK,
            Json(ClusterConfigGetResponse {
                configured: true,
                config: Some(ClusterConfigSnapshotPublic::from(snap)),
            }),
        )
            .into_response(),
        Err(e) => {
            tracing::error!(error = %e, "failed to build cluster config response");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "internal error" })),
            )
                .into_response()
        }
    }
}

/// Human: Apply cluster membership and mode without restarting Nebular.
/// Agent: PUT /_cluster/config; VALIDATES snapshot; CALLS apply_cluster_snapshot.
pub async fn put_cluster_config(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ClusterConfigSnapshot>,
) -> impl IntoResponse {
    if let Err(e) = apply_cluster_snapshot(
        &state.engine,
        &state.metrics,
        &state.cluster,
        &state.backend,
        body,
    )
    .await
    {
        tracing::error!(error = %e, "cluster config apply failed");
        let msg = e.to_string();
        let status = if msg.contains("required")
            || msg.contains("unsupported")
            || msg.contains("must")
            || msg.contains("invalid")
            || msg.contains("not supported")
        {
            StatusCode::BAD_REQUEST
        } else {
            StatusCode::INTERNAL_SERVER_ERROR
        };
        return (
            status,
            Json(serde_json::json!({ "error": msg })),
        )
            .into_response();
    }

    (
        StatusCode::OK,
        Json(serde_json::json!({ "status": "applied" })),
    )
        .into_response()
}
