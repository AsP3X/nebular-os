use axum::{
    extract::State,
    http::{header, HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use std::sync::Arc;

use crate::routes::AppState;

#[derive(serde::Serialize)]
pub struct StorageClassCount {
    pub class: String,
    pub count: i64,
}

#[derive(serde::Serialize)]
pub struct MetricsResponse {
    pub total_objects: i64,
    pub total_bytes: i64,
    pub logical_bytes: i64,
    pub replication_pending_events: u64,
    pub replication_errors_total: u64,
    pub storage_class_counts: Vec<StorageClassCount>,
}

pub async fn metrics(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, StatusCode> {
    state.metrics.inc_requests();

    let total_objects = state
        .backend
        .read()
        .await
        .object_count()
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "object_count failed");
            state.metrics.inc_errors();
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    let total_bytes = state
        .backend
        .read()
        .await
        .total_bytes()
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "total_bytes failed");
            state.metrics.inc_errors();
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    let replication_pending_events = state
        .backend
        .read()
        .await
        .pending_replication_events()
        .await
        .unwrap_or(0);
    let storage_class_counts = state
        .backend
        .read()
        .await
        .engine()
        .objects_by_storage_class()
        .await
        .unwrap_or_default();

    let accept = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if accept.contains("text/plain") || accept.contains("application/openmetrics-text") {
        let body = state.metrics.render_prometheus(
            total_objects,
            total_bytes,
            replication_pending_events,
            &storage_class_counts,
        );
        return Ok((
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
            body,
        )
            .into_response());
    }

    let storage_class_counts: Vec<StorageClassCount> = storage_class_counts
        .into_iter()
        .map(|(class, count)| StorageClassCount { class, count })
        .collect();

    Ok(Json(MetricsResponse {
        total_objects,
        total_bytes,
        logical_bytes: total_bytes,
        replication_pending_events,
        replication_errors_total: state.metrics.replication_errors_total(),
        storage_class_counts,
    })
    .into_response())
}
