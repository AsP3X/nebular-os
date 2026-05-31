use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::Serialize;
use std::sync::Arc;

use crate::routes::AppState;

#[derive(Serialize)]
pub struct ClusterHealthResponse {
    pub status: &'static str,
    pub cluster_mode: &'static str,
    pub node_id: String,
    pub storage_classes: Vec<String>,
    pub replication_group: String,
    pub replication_role: String,
    pub replication_pending_events: u64,
}

/// Human: Peers and operators probe cluster identity and replication backlog.
/// Agent: GET /_cluster/health; Bearer NOS_CLUSTER_TOKEN; JSON additive ops fields.
pub async fn cluster_health(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let cluster = &state.config.cluster;
    (
        StatusCode::OK,
        Json(ClusterHealthResponse {
            status: "ok",
            cluster_mode: cluster.mode.as_str(),
            node_id: cluster.node_id.clone(),
            storage_classes: cluster.storage_classes.clone(),
            replication_group: cluster.replication_group.clone(),
            replication_role: cluster.replication_role.clone(),
            replication_pending_events: cluster.replication_pending_events,
        }),
    )
}

#[derive(Serialize)]
pub struct ClusterCapabilitiesResponse {
    pub version: &'static str,
    pub cluster_mode: &'static str,
    pub node_id: String,
    pub storage_classes: Vec<String>,
    pub replication_group: String,
    pub replication_role: String,
}

/// Human: Peers discover node capabilities without user JWT.
/// Agent: GET /_cluster/capabilities; same auth as /_cluster/health.
pub async fn cluster_capabilities(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let cluster = &state.config.cluster;
    (
        StatusCode::OK,
        Json(ClusterCapabilitiesResponse {
            version: env!("CARGO_PKG_VERSION"),
            cluster_mode: cluster.mode.as_str(),
            node_id: cluster.node_id.clone(),
            storage_classes: cluster.storage_classes.clone(),
            replication_group: cluster.replication_group.clone(),
            replication_role: cluster.replication_role.clone(),
        }),
    )
}
