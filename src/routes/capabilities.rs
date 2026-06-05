use axum::{
    extract::State,
    response::IntoResponse,
    Json,
};
use serde::Serialize;
use std::sync::Arc;

use crate::routes::AppState;

#[derive(Serialize)]
pub struct ApiCapabilities {
    pub delete_prefix: bool,
    pub batch_delete: bool,
    pub delete_prefix_batch_limit: u64,
    pub bulk_delete_concurrency: u32,
    pub list_count_only: bool,
    pub metadata_mode: &'static str,
    pub upload_max_in_flight_bytes: u64,
    pub orphan_maintenance: bool,
    pub cluster_prefix_delete_fanout: bool,
}

#[derive(Serialize)]
pub struct CapabilitiesResponse {
    pub version: &'static str,
    pub cluster_mode: &'static str,
    pub node_id: String,
    pub max_body_size: usize,
    pub storage_classes: Vec<String>,
    pub replication_group: String,
    pub replication_role: String,
    pub api: ApiCapabilities,
}

/// Human: Clients discover server limits and cluster placement without writing an object.
/// Agent: GET /_nos/capabilities; JWT/presigned middleware; READS AppState.config.cluster.
pub async fn capabilities(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let cluster = state.cluster();
    let engine = state.engine();
    Json(CapabilitiesResponse {
        version: env!("CARGO_PKG_VERSION"),
        cluster_mode: cluster.mode.as_str(),
        node_id: cluster.node_id.clone(),
        max_body_size: state.max_body_size,
        storage_classes: cluster.storage_classes.clone(),
        replication_group: cluster.replication_group.clone(),
        replication_role: cluster.replication_role.clone(),
        api: ApiCapabilities {
            delete_prefix: true,
            batch_delete: true,
            delete_prefix_batch_limit: engine.bulk_delete_batch_limit(),
            bulk_delete_concurrency: engine.bulk_delete_concurrency() as u32,
            list_count_only: true,
            metadata_mode: engine.metadata_mode().as_str(),
            upload_max_in_flight_bytes: state.config.upload_max_in_flight_bytes,
            orphan_maintenance: true,
            cluster_prefix_delete_fanout: !cluster.is_standalone(),
        },
    })
}
