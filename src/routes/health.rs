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
}

/// Human: Cheap liveness probe — process is up; does not touch storage.
/// Agent: HTTP 200 JSON {status:ok, version}; NO SQLite or disk I/O.
pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        version: env!("CARGO_PKG_VERSION"),
    })
}

#[derive(Serialize)]
pub struct ReadyResponse {
    pub status: &'static str,
    pub checks: ReadinessChecks,
}

/// Human: Readiness probe verifies metadata DB and blob directory before accepting traffic.
/// Agent: CALLS StorageEngine::probe_readiness; 200 when all checks true else 503 {error:not ready}.
pub async fn ready(State(state): State<Arc<AppState>>) -> Response {
    let checks = state.storage.probe_readiness().await;
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
