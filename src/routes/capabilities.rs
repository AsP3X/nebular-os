use axum::{
    extract::State,
    response::IntoResponse,
    Json,
};
use serde::Serialize;
use std::sync::Arc;

use crate::routes::AppState;

#[derive(Serialize)]
pub struct CapabilitiesResponse {
    pub version: &'static str,
    pub cluster_mode: &'static str,
    pub node_id: String,
    pub max_body_size: usize,
    pub storage_classes: Vec<String>,
    pub replication_group: String,
    pub replication_role: String,
}

/// Human: Clients discover server limits and cluster placement without writing an object.
/// Agent: GET /_nos/capabilities; JWT/presigned middleware; READS AppState.config.cluster.
pub async fn capabilities(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let cluster = state.cluster();
    Json(CapabilitiesResponse {
        version: env!("CARGO_PKG_VERSION"),
        cluster_mode: cluster.mode.as_str(),
        node_id: cluster.node_id.clone(),
        max_body_size: state.max_body_size,
        storage_classes: cluster.storage_classes.clone(),
        replication_group: cluster.replication_group.clone(),
        replication_role: cluster.replication_role.clone(),
    })
}
