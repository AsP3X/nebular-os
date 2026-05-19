use axum::{extract::State, http::StatusCode, Json};
use serde::Serialize;
use std::sync::Arc;

use crate::routes::AppState;

#[derive(Serialize)]
pub struct MetricsResponse {
    pub total_objects: i64,
    pub total_bytes: i64,
}

pub async fn metrics(State(state): State<Arc<AppState>>) -> Result<Json<MetricsResponse>, StatusCode> {
    let total_objects = state.storage.object_count().await
        .map_err(|e| {
            tracing::error!("object_count failed: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    let total_bytes = state.storage.total_bytes().await
        .map_err(|e| {
            tracing::error!("total_bytes failed: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(MetricsResponse {
        total_objects,
        total_bytes,
    }))
}
