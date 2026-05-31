use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::Serialize;
use std::sync::Arc;

use crate::cluster::assignment::WriteContext;
use crate::cluster::backend::StorageBackend;
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
    let pending = state
        .backend
        .pending_replication_events()
        .await
        .unwrap_or(0);
    (
        StatusCode::OK,
        Json(ClusterHealthResponse {
            status: "ok",
            cluster_mode: cluster.mode.as_str(),
            node_id: cluster.node_id.clone(),
            storage_classes: cluster.storage_classes.clone(),
            replication_group: cluster.replication_group.clone(),
            replication_role: cluster.replication_role.clone(),
            replication_pending_events: pending,
        }),
    )
}

/// Human: Peer checks whether an object exists locally before fetch/repair.
/// Agent: HEAD /_cluster/objects/{bucket}/{key}; 200 if exists else 404 JSON error.
pub async fn cluster_object_head(
    State(state): State<Arc<AppState>>,
    axum::extract::Path((bucket, key)): axum::extract::Path<(String, String)>,
) -> impl IntoResponse {
    match state.backend.engine().object_exists(&bucket, &key).await {
        Ok(true) => StatusCode::OK.into_response(),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "not found" })),
        )
            .into_response(),
        Err(e) => {
            tracing::error!(error = %e, %bucket, %key, "cluster object head failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "storage error" })),
            )
                .into_response()
        }
    }
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
#[derive(serde::Deserialize)]
pub struct AssignmentResolveRequest {
    pub bucket: String,
    pub key: String,
    #[serde(default)]
    pub content_type: Option<String>,
    #[serde(default)]
    pub storage_class_header: Option<String>,
    #[serde(default)]
    pub content_length: Option<u64>,
}

#[derive(serde::Serialize)]
pub struct AssignmentResolveResponse {
    pub storage_class: String,
    pub assigned_node: Option<String>,
    pub accept_local: bool,
}

/// Human: Debug endpoint for Ownly/admin to preview placement before upload.
/// Agent: POST /_cluster/assignment/resolve; Bearer cluster token; no write.
pub async fn assignment_resolve(
    State(state): State<Arc<AppState>>,
    Json(body): Json<AssignmentResolveRequest>,
) -> axum::response::Response {
    let resolution = match &state.backend {
        StorageBackend::Assigned(b) => {
            let ctx = WriteContext {
                storage_class_header: body.storage_class_header.clone(),
                content_type: body.content_type.clone(),
                custom_meta_storage_class: None,
                content_length: body.content_length,
            };
            b.resolve(&body.bucket, &body.key, Some(&ctx))
        }
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "assignment resolve requires assigned cluster mode" })),
            )
                .into_response();
        }
    };

    (
        StatusCode::OK,
        Json(AssignmentResolveResponse {
            storage_class: resolution.storage_class,
            assigned_node: resolution.assigned_node,
            accept_local: resolution.accept_local,
        }),
    )
        .into_response()
}

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
