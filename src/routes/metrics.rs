use axum::{
    extract::State,
    http::{header, HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use std::sync::Arc;

use crate::routes::AppState;

#[derive(serde::Serialize)]
pub struct MetricsResponse {
    pub total_objects: i64,
    pub total_bytes: i64,
}

pub async fn metrics(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, StatusCode> {
    state.metrics.inc_requests();

    let total_objects = state
        .backend
        .object_count()
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "object_count failed");
            state.metrics.inc_errors();
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    let total_bytes = state
        .backend
        .total_bytes()
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "total_bytes failed");
            state.metrics.inc_errors();
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let accept = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if accept.contains("text/plain") || accept.contains("application/openmetrics-text") {
        let body = state
            .metrics
            .render_prometheus(total_objects, total_bytes);
        return Ok((
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
            body,
        )
            .into_response());
    }

    Ok(Json(MetricsResponse {
        total_objects,
        total_bytes,
    })
    .into_response())
}
