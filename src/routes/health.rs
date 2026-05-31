use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use serde_json::json;
use std::sync::Arc;

use crate::routes::AppState;
use crate::storage::engine::ReadinessChecks;

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub version: &'static str,
    pub cluster_mode: &'static str,
    pub node_id: String,
    pub instance_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region_label: Option<String>,
    pub storage_classes: Vec<String>,
    pub replication_lag_events: u64,
}

/// Human: Cheap liveness probe — process is up; cluster fields are additive for operators.
/// Agent: HTTP 200 JSON; NO SQLite or disk I/O; READS config.cluster for display fields.
pub async fn health(State(state): State<Arc<AppState>>) -> Json<HealthResponse> {
    let cluster = &state.config.cluster;
    let replication_lag_events = state
        .backend
        .pending_replication_events()
        .await
        .unwrap_or(0);
    Json(HealthResponse {
        status: "ok",
        version: env!("CARGO_PKG_VERSION"),
        cluster_mode: cluster.mode.as_str(),
        node_id: cluster.node_id.clone(),
        instance_id: cluster.instance_id.clone(),
        region_label: cluster.region_label.clone(),
        storage_classes: cluster.storage_classes.clone(),
        replication_lag_events,
    })
}

#[derive(Serialize)]
pub struct ReadyResponse {
    pub status: &'static str,
    pub checks: ReadinessChecks,
}

/// Human: Readiness probe verifies metadata DB and blob directory before accepting traffic.
/// Agent: CALLS StorageBackend::probe_readiness; 200 when all checks true else 503 {error:not ready}.
pub async fn ready(State(state): State<Arc<AppState>>) -> Response {
    let checks = state.backend.probe_readiness().await;
    if checks.ready() {
        return (
            StatusCode::OK,
            Json(ReadyResponse {
                status: "ready",
                checks,
            }),
        )
            .into_response();
    }

    tracing::warn!(
        sqlite_write = checks.sqlite_write,
        sqlite_read = checks.sqlite_read,
        data_dir_writable = checks.data_dir_writable,
        "readiness probe failed"
    );

    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({
            "error": "not ready",
            "checks": checks,
        })),
    )
        .into_response()
}
