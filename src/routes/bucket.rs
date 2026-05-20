use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
    Json,
};
use serde::Deserialize;
use std::sync::Arc;

use crate::routes::AppState;

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    prefix: Option<String>,
    delimiter: Option<String>,
    limit: Option<u64>,
    start_after: Option<String>,
}

pub async fn list_objects(
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
    Query(query): Query<ListQuery>,
) -> impl IntoResponse {
    tracing::info!(%bucket, prefix = ?query.prefix, limit = ?query.limit, "list_objects started");

    match state
        .storage
        .list_objects(
            &bucket,
            query.prefix.as_deref(),
            query.delimiter.as_deref(),
            query.limit,
            query.start_after.as_deref(),
        )
        .await
    {
        Ok(result) => {
            tracing::info!(%bucket, item_count = result.items.len(), "list_objects completed");
            Json(result).into_response()
        }
        Err(e) => {
            tracing::error!(%bucket, error = %e, "list_objects failed");
            crate::routes::errors::map_storage_error(e).into_response()
        }
    }
}
